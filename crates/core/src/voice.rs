//! Canal de voz: malha de conexões QUIC diretas, áudio por datagramas.
//!
//! Quem está na voz de cada servidor é anunciado pelo gossip (`Payload::Voice`,
//! repetido a cada `HEARTBEAT`). Entre dois participantes, só o de id menor
//! disca, para não abrir conexão dobrada. O primeiro stream da conexão leva o
//! tópico, assim quem aceita sabe de qual canal é a chamada.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{
    endpoint::{Connection, VarInt},
    protocol::{AcceptError, ProtocolHandler},
    PublicKey,
};
use iroh_gossip::proto::TopicId;
use p2pcord_voice::{Cue, Engine, CUE_LEN};

use crate::{node::Shared, proto::Payload, NodeEvent};

pub const VOICE_ALPN: &[u8] = b"p2pcord/voice/0";
pub const HEARTBEAT: Duration = Duration::from_secs(10);
const EXPIRE: Duration = Duration::from_secs(30);

type Conns = Arc<Mutex<HashMap<PublicKey, Connection>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceMember {
    pub name: String,
    pub speaking: bool,
    pub me: bool,
}

struct Session {
    topic: TopicId,
    engine: Arc<Engine>,
    conns: Conns,
    /// Quem estava na chamada no último snapshot, para tocar o som de entrou/saiu.
    present: HashSet<PublicKey>,
}

impl Drop for Session {
    fn drop(&mut self) {
        for c in self.conns.lock().unwrap().values() {
            c.close(VarInt::from_u32(0), b"saiu");
        }
    }
}

#[derive(Default)]
pub struct VoiceState {
    session: Option<Session>,
    /// Quem anunciou estar na voz, por servidor, e quando foi o último anúncio.
    members: HashMap<TopicId, HashMap<PublicKey, Instant>>,
}

impl Shared {
    pub(crate) fn voice_topic(&self) -> Option<TopicId> {
        self.voice.lock().unwrap().session.as_ref().map(|s| s.topic)
    }

    pub(crate) async fn join_voice(self: &Arc<Self>, topic: TopicId) -> Result<()> {
        self.leave_voice().await;
        let conns: Conns = Default::default();
        let out = conns.clone();
        // Abrir o áudio bloqueia um pouco (drivers); fora do runtime.
        let engine = tokio::task::spawn_blocking(move || {
            Engine::start(move |pkt| {
                let pkt = Bytes::from(pkt);
                for c in out.lock().unwrap().values() {
                    c.send_datagram(pkt.clone()).ok();
                }
            })
        })
        .await??;
        engine.play_cue(Cue::Join);
        let peers: Vec<PublicKey> = {
            let mut v = self.voice.lock().unwrap();
            let me = self.key.public();
            let now = Instant::now();
            // Quem já estava lá não toca som; só quem entrar depois.
            let present: HashSet<PublicKey> = v
                .members
                .get(&topic)
                .map(|m| {
                    m.iter()
                        .filter(|(k, seen)| **k != me && now - **seen < EXPIRE)
                        .map(|(k, _)| *k)
                        .collect()
                })
                .unwrap_or_default();
            let peers = present.iter().copied().collect();
            v.session = Some(Session { topic, engine: Arc::new(engine), conns, present });
            peers
        };
        self.broadcast(&topic, &Payload::voice(true)).await?;
        for p in peers {
            self.maybe_dial(topic, p);
        }
        Ok(())
    }

    pub(crate) async fn leave_voice(&self) {
        let old = self.voice.lock().unwrap().session.take();
        if let Some(s) = old {
            let topic = s.topic;
            // Fecha o áudio fora do runtime: o drop espera a thread de áudio.
            // Antes, corta as conexões e deixa o som de saída terminar de tocar.
            tokio::task::spawn_blocking(move || {
                for c in s.conns.lock().unwrap().values() {
                    c.close(VarInt::from_u32(0), b"saiu");
                }
                s.engine.play_cue(Cue::Leave);
                std::thread::sleep(CUE_LEN + Duration::from_millis(60));
                drop(s);
            })
            .await
            .ok();
            self.broadcast(&topic, &Payload::voice(false)).await.ok();
        }
    }

    /// Com quantas pessoas a chamada tem conexão de áudio aberta.
    pub(crate) fn voice_connections(&self) -> usize {
        let v = self.voice.lock().unwrap();
        v.session.as_ref().map_or(0, |s| s.conns.lock().unwrap().len())
    }

    pub(crate) fn set_muted(&self, on: bool) {
        if let Some(s) = &self.voice.lock().unwrap().session {
            s.engine.set_muted(on);
        }
    }

    pub(crate) fn set_deafened(&self, on: bool) {
        if let Some(s) = &self.voice.lock().unwrap().session {
            s.engine.set_deafened(on);
        }
    }

    /// Anúncio de voz vindo do gossip.
    pub(crate) fn on_voice_state(self: &Arc<Self>, topic: TopicId, from: PublicKey, in_voice: bool) {
        let mut v = self.voice.lock().unwrap();
        let members = v.members.entry(topic).or_default();
        if in_voice {
            members.insert(from, Instant::now());
        } else {
            members.remove(&from);
        }
        let same = v.session.as_ref().filter(|s| s.topic == topic);
        match (in_voice, same) {
            (true, Some(_)) => {
                drop(v);
                self.maybe_dial(topic, from);
            }
            (false, Some(s)) => {
                if let Some(c) = s.conns.lock().unwrap().remove(&from) {
                    c.close(VarInt::from_u32(0), b"saiu");
                }
                s.engine.remove_peer(from.as_bytes());
            }
            _ => {}
        }
    }

    fn maybe_dial(self: &Arc<Self>, topic: TopicId, peer: PublicKey) {
        let me = self.key.public();
        if peer == me || me.as_bytes() > peer.as_bytes() {
            return;
        }
        let Some(conns) = self.session_conns(topic) else { return };
        if conns.lock().unwrap().contains_key(&peer) {
            return;
        }
        let shared = self.clone();
        tokio::spawn(async move {
            let res: Result<()> = async {
                let conn = shared.endpoint.connect(peer, VOICE_ALPN).await?;
                let mut s = conn.open_uni().await?;
                s.write_all(topic.as_bytes()).await?;
                s.finish()?;
                shared.run_call(topic, conn).await;
                Ok(())
            }
            .await;
            if let Err(e) = res {
                let msg = format!("voz: não conectou com {}: {e:#}", peer.fmt_short());
                shared.events.send(NodeEvent::Error(msg)).ok();
            }
        });
    }

    fn session_conns(&self, topic: TopicId) -> Option<Conns> {
        let v = self.voice.lock().unwrap();
        v.session.as_ref().filter(|s| s.topic == topic).map(|s| s.conns.clone())
    }

    fn session_engine(&self, topic: TopicId) -> Option<Arc<Engine>> {
        let v = self.voice.lock().unwrap();
        v.session.as_ref().filter(|s| s.topic == topic).map(|s| s.engine.clone())
    }

    /// Recebe áudio de uma conexão até ela cair.
    async fn run_call(&self, topic: TopicId, conn: Connection) {
        let peer = conn.remote_id();
        let (Some(conns), Some(engine)) = (self.session_conns(topic), self.session_engine(topic))
        else {
            conn.close(VarInt::from_u32(1), b"fora da voz");
            return;
        };
        conns.lock().unwrap().insert(peer, conn.clone());
        self.voice.lock().unwrap().members.entry(topic).or_default().insert(peer, Instant::now());
        while let Ok(pkt) = conn.read_datagram().await {
            engine.push_packet(*peer.as_bytes(), &pkt);
        }
        // Só remove se ainda for esta conexão (pode ter sido trocada por outra).
        let mut map = conns.lock().unwrap();
        if map.get(&peer).is_some_and(|c| c.stable_id() == conn.stable_id()) {
            map.remove(&peer);
            engine.remove_peer(peer.as_bytes());
        }
    }

    /// Lista de quem está na voz de cada servidor, com quem está falando.
    pub(crate) fn voice_snapshot(&self) -> HashMap<TopicId, Vec<VoiceMember>> {
        let mut v = self.voice.lock().unwrap();
        let now = Instant::now();
        let me = self.key.public();
        // Conexão de áudio aberta já prova que a pessoa está na chamada, mesmo
        // que os anúncios do gossip se percam.
        if let Some(s) = &v.session {
            let topic = s.topic;
            let open: Vec<PublicKey> = s.conns.lock().unwrap().keys().copied().collect();
            let members = v.members.entry(topic).or_default();
            for k in open {
                members.insert(k, now);
            }
        }
        for m in v.members.values_mut() {
            m.retain(|k, seen| *k != me && now - *seen < EXPIRE);
        }
        // Som de entrou/saiu: compara quem está agora com o último snapshot.
        let VoiceState { session, members } = &mut *v;
        if let Some(s) = session {
            let current: HashSet<PublicKey> =
                members.get(&s.topic).map(|m| m.keys().copied().collect()).unwrap_or_default();
            if current.difference(&s.present).next().is_some() {
                s.engine.play_cue(Cue::Join);
            } else if s.present.difference(&current).next().is_some() {
                s.engine.play_cue(Cue::Leave);
            }
            s.present = current;
        }
        let (my_topic, me_speaking, speaking) = match &v.session {
            Some(s) => {
                let (mine, others) = s.engine.speaking();
                (Some(s.topic), mine, others)
            }
            None => (None, false, vec![]),
        };
        let store = self.store.lock().unwrap();
        let mut out: HashMap<TopicId, Vec<VoiceMember>> = HashMap::new();
        for (topic, members) in &v.members {
            let list = out.entry(*topic).or_default();
            for k in members.keys() {
                list.push(VoiceMember {
                    name: crate::node::display_name(&store, k.as_bytes()),
                    speaking: my_topic == Some(*topic) && speaking.contains(k.as_bytes()),
                    me: false,
                });
            }
        }
        if let Some(t) = my_topic {
            out.entry(t).or_default().insert(
                0,
                VoiceMember { name: self.name.lock().unwrap().clone(), speaking: me_speaking, me: true },
            );
        }
        for list in out.values_mut() {
            // Eu primeiro, o resto em ordem alfabética.
            let skip = usize::from(list.first().is_some_and(|m| m.me));
            list[skip..].sort_by(|a, b| a.name.cmp(&b.name));
        }
        out
    }
}

#[derive(Clone)]
pub(crate) struct VoiceProtocol(pub Arc<Shared>);

impl std::fmt::Debug for VoiceProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("VoiceProtocol")
    }
}

impl ProtocolHandler for VoiceProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let topic = async {
            let mut s = conn.accept_uni().await?;
            let bytes = s.read_to_end(32).await?;
            let bytes: [u8; 32] = bytes.try_into().ok().context("tópico inválido")?;
            anyhow::Ok(TopicId::from_bytes(bytes))
        }
        .await;
        match topic {
            Ok(topic) => self.0.run_call(topic, conn).await,
            Err(_) => conn.close(VarInt::from_u32(2), b"handshake"),
        }
        Ok(())
    }
}
