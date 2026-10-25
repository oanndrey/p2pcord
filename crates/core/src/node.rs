//! O nó P2P: endpoint iroh + gossip, um tópico por servidor.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{
    address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router, Endpoint,
    EndpointId, PublicKey, SecretKey,
};
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{
    proto::{self, ChatMsg, Payload},
    store::Store,
    ticket::Ticket,
    voice::{VoiceMember, VoiceProtocol, VoiceState, HEARTBEAT, VOICE_ALPN},
};

/// Teto de uma mensagem no gossip. Respostas de sync são fatiadas abaixo disso.
const MAX_MSG: usize = 64 * 1024;
const SYNC_CHUNK: usize = 48 * 1024;
const SYNC_LIMIT: usize = 2000;

/// O que o nó avisa para a interface.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    Chat {
        topic: TopicId,
        channel: String,
        author: String,
        text: String,
        ts_ms: u64,
    },
    Peers { topic: TopicId, online: usize },
    /// Quem está no canal de voz do servidor (só é emitido quando muda).
    Voice { topic: TopicId, members: Vec<VoiceMember> },
    Error(String),
}

pub(crate) struct Shared {
    pub(crate) key: SecretKey,
    pub(crate) endpoint: Endpoint,
    pub(crate) store: Mutex<Store>,
    pub(crate) name: Mutex<String>,
    senders: Mutex<HashMap<TopicId, GossipSender>>,
    pub(crate) events: mpsc::UnboundedSender<NodeEvent>,
    pub(crate) voice: Mutex<VoiceState>,
}

pub struct Node {
    endpoint: Endpoint,
    gossip: Gossip,
    lookup: MemoryLookup,
    _router: Router,
    shared: Arc<Shared>,
}

impl Node {
    pub async fn spawn(dir: &Path, events: mpsc::UnboundedSender<NodeEvent>) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let key = load_or_create_key(&dir.join("identity.key"))?;
        let store = Store::open(&dir.join("p2pcord.db"))?;
        let name = store
            .profile(key.public().as_bytes())?
            .unwrap_or_else(|| format!("user-{}", key.public().fmt_short()));

        let voice = VoiceState::new(store.volumes()?, store.setting("mic_volume")?);
        let lookup = MemoryLookup::new();
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(key.clone())
            .address_lookup(lookup.clone())
            .bind()
            .await?;
        let gossip = Gossip::builder().max_message_size(MAX_MSG).spawn(endpoint.clone());
        let shared = Arc::new(Shared {
            key,
            endpoint: endpoint.clone(),
            store: Mutex::new(store),
            name: Mutex::new(name),
            senders: Mutex::new(HashMap::new()),
            events,
            voice: Mutex::new(voice),
        });
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .accept(VOICE_ALPN, VoiceProtocol(shared.clone()))
            .spawn();
        tokio::spawn(voice_ticker(Arc::downgrade(&shared)));

        let node = Self { endpoint, gossip, lookup, _router: router, shared };

        // Reentra nos servidores salvos, usando os membros conhecidos como bootstrap.
        let servers = node.shared.store.lock().unwrap().servers()?;
        for s in servers {
            let members = node.shared.store.lock().unwrap().members(&s.topic)?;
            let peers = members
                .iter()
                .filter_map(|k| PublicKey::from_bytes(k).ok())
                .collect();
            node.subscribe(s.topic, peers).await?;
        }
        Ok(node)
    }

    pub fn name(&self) -> String {
        self.shared.name.lock().unwrap().clone()
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn servers(&self) -> Result<Vec<crate::store::ServerRow>> {
        self.shared.store.lock().unwrap().servers()
    }

    pub fn history(&self, topic: &TopicId, channel: &str) -> Result<Vec<(String, String, u64)>> {
        let store = self.shared.store.lock().unwrap();
        store
            .history(topic, channel, 200)?
            .into_iter()
            .map(|m| Ok((display_name(&store, &m.author), m.text, m.ts_ms)))
            .collect()
    }

    pub async fn set_name(&self, name: String) -> Result<()> {
        self.shared
            .store
            .lock()
            .unwrap()
            .set_profile(self.shared.key.public().as_bytes(), &name)?;
        *self.shared.name.lock().unwrap() = name.clone();
        let raw = proto::encode(&self.shared.key, &Payload::Hello { name })?;
        for sender in self.senders() {
            sender.broadcast(raw.clone()).await.ok();
        }
        Ok(())
    }

    pub async fn create_server(&self, name: &str) -> Result<TopicId> {
        let topic = TopicId::from_bytes(rand_bytes());
        self.shared.store.lock().unwrap().add_server(&topic, name)?;
        self.subscribe(topic, vec![]).await?;
        Ok(topic)
    }

    pub async fn join(&self, ticket: &Ticket) -> Result<TopicId> {
        let mut ids = Vec::new();
        for peer in &ticket.peers {
            ids.push(peer.id);
            self.lookup.add_endpoint_info(peer.clone());
        }
        {
            let store = self.shared.store.lock().unwrap();
            store.add_server(&ticket.topic, &ticket.name)?;
            for id in &ids {
                store.add_member(&ticket.topic, id.as_bytes())?;
            }
        }
        self.subscribe(ticket.topic, ids).await?;
        Ok(ticket.topic)
    }

    /// Gera um convite com nosso endereço atual.
    pub async fn invite(&self, topic: &TopicId) -> Result<Ticket> {
        let name = self
            .servers()?
            .into_iter()
            .find(|s| &s.topic == topic)
            .map(|s| s.name)
            .context("servidor desconhecido")?;
        self.endpoint.online().await;
        Ok(Ticket { name, topic: *topic, peers: vec![self.endpoint.addr()] })
    }

    pub async fn send(&self, topic: &TopicId, channel: &str, text: &str) -> Result<()> {
        let chat = ChatMsg { channel: channel.into(), text: text.into(), ts_ms: now_ms() };
        let raw = proto::encode(&self.shared.key, &Payload::Chat(chat.clone()))?;
        let msg = proto::decode(raw.clone())?;
        self.shared.handle_chat(topic, &msg, &chat)?;
        let sender = self.sender(topic).context("não conectado nesse servidor")?;
        sender.broadcast(raw).await?;
        Ok(())
    }

    pub async fn join_voice(&self, topic: &TopicId) -> Result<()> {
        self.shared.join_voice(*topic).await
    }

    pub async fn leave_voice(&self) {
        self.shared.leave_voice().await
    }

    pub fn voice_topic(&self) -> Option<TopicId> {
        self.shared.voice_topic()
    }

    pub fn voice_connections(&self) -> usize {
        self.shared.voice_connections()
    }

    /// Volume de uma pessoa na voz (0.0 a 2.0), salvo para as próximas chamadas.
    pub fn set_user_volume(&self, key: [u8; 32], volume: f32) -> Result<()> {
        self.shared.set_user_volume(key, volume)
    }

    /// Volume do meu microfone (0.0 a 2.0), salvo para as próximas chamadas.
    pub fn set_mic_volume(&self, volume: f32) -> Result<()> {
        self.shared.set_mic_volume(volume)
    }

    pub fn set_muted(&self, on: bool) {
        self.shared.set_muted(on)
    }

    pub fn set_deafened(&self, on: bool) {
        self.shared.set_deafened(on)
    }

    async fn subscribe(&self, topic: TopicId, peers: Vec<EndpointId>) -> Result<()> {
        let (sender, receiver) = self.gossip.subscribe(topic, peers).await?.split();
        self.shared.senders.lock().unwrap().insert(topic, sender.clone());
        let shared = self.shared.clone();
        tokio::spawn(async move {
            if let Err(e) = shared.clone().run_topic(topic, sender, receiver).await {
                shared.events.send(NodeEvent::Error(format!("{e:#}"))).ok();
            }
        });
        Ok(())
    }

    fn sender(&self, topic: &TopicId) -> Option<GossipSender> {
        self.shared.senders.lock().unwrap().get(topic).cloned()
    }

    fn senders(&self) -> Vec<GossipSender> {
        self.shared.senders.lock().unwrap().values().cloned().collect()
    }
}

impl Shared {
    pub(crate) async fn broadcast(&self, topic: &TopicId, payload: &Payload) -> Result<()> {
        let sender = self.senders.lock().unwrap().get(topic).cloned();
        if let Some(sender) = sender {
            sender.broadcast(proto::encode(&self.key, payload)?).await?;
        }
        Ok(())
    }

    async fn run_topic(
        self: Arc<Self>,
        topic: TopicId,
        sender: GossipSender,
        mut receiver: GossipReceiver,
    ) -> Result<()> {
        let mut online = HashSet::new();
        while let Some(event) = receiver.try_next().await? {
            match event {
                Event::NeighborUp(id) => {
                    online.insert(id);
                    self.store.lock().unwrap().add_member(&topic, id.as_bytes())?;
                    self.events.send(NodeEvent::Peers { topic, online: online.len() }).ok();
                    // Apresenta-se e pede o que perdeu enquanto estava offline.
                    let name = self.name.lock().unwrap().clone();
                    let since_ms = self.store.lock().unwrap().latest_ts(&topic)?;
                    let mut hello = vec![Payload::Hello { name }, Payload::SyncReq { since_ms }];
                    if self.voice_topic() == Some(topic) {
                        hello.push(Payload::voice(true));
                    }
                    for p in hello {
                        sender.broadcast_neighbors(proto::encode(&self.key, &p)?).await?;
                    }
                }
                Event::NeighborDown(id) => {
                    online.remove(&id);
                    self.events.send(NodeEvent::Peers { topic, online: online.len() }).ok();
                }
                Event::Received(m) => {
                    if let Err(e) = self.handle(&topic, &sender, m.content).await {
                        self.events.send(NodeEvent::Error(format!("{e:#}"))).ok();
                    }
                }
                Event::Lagged => {}
            }
        }
        Ok(())
    }

    async fn handle(self: &Arc<Self>, topic: &TopicId, sender: &GossipSender, raw: Bytes) -> Result<()> {
        let msg = proto::decode(raw)?;
        match &msg.payload {
            Payload::Hello { name } => {
                self.store.lock().unwrap().set_profile(msg.from.as_bytes(), name)?;
            }
            Payload::Chat(chat) => self.handle_chat(topic, &msg, chat)?,
            Payload::Voice { in_voice, .. } => self.on_voice_state(*topic, msg.from, *in_voice),
            Payload::SyncReq { since_ms } => {
                let msgs = self.store.lock().unwrap().raw_since(topic, *since_ms, SYNC_LIMIT)?;
                for chunk in chunk_by_size(msgs, SYNC_CHUNK) {
                    let raw = proto::encode(&self.key, &Payload::SyncResp { msgs: chunk })?;
                    sender.broadcast_neighbors(raw).await?;
                }
            }
            Payload::SyncResp { msgs } => {
                for raw in msgs {
                    // Cada item tem a assinatura do autor original.
                    let Ok(inner) = proto::decode(raw.clone()) else { continue };
                    if let Payload::Chat(chat) = &inner.payload {
                        self.handle_chat(topic, &inner, chat)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_chat(&self, topic: &TopicId, msg: &proto::Signed, chat: &ChatMsg) -> Result<()> {
        let store = self.store.lock().unwrap();
        if store.insert_chat(topic, msg, chat)? {
            self.events
                .send(NodeEvent::Chat {
                    topic: *topic,
                    channel: chat.channel.clone(),
                    author: display_name(&store, msg.from.as_bytes()),
                    text: chat.text.clone(),
                    ts_ms: chat.ts_ms,
                })
                .ok();
        }
        Ok(())
    }
}

/// Repete o anúncio de voz e avisa a UI quando a lista de quem fala muda.
async fn voice_ticker(shared: std::sync::Weak<Shared>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(150));
    let mut last: HashMap<TopicId, Vec<VoiceMember>> = HashMap::new();
    let mut last_beat = tokio::time::Instant::now();
    loop {
        tick.tick().await;
        let Some(shared) = shared.upgrade() else { return };
        if last_beat.elapsed() >= HEARTBEAT {
            last_beat = tokio::time::Instant::now();
            if let Some(t) = shared.voice_topic() {
                shared.broadcast(&t, &Payload::voice(true)).await.ok();
            }
        }
        let now = shared.voice_snapshot();
        for (topic, members) in &now {
            if last.get(topic) != Some(members) {
                shared.events.send(NodeEvent::Voice { topic: *topic, members: members.clone() }).ok();
            }
        }
        for topic in last.keys() {
            if !now.contains_key(topic) {
                shared.events.send(NodeEvent::Voice { topic: *topic, members: vec![] }).ok();
            }
        }
        last = now;
    }
}

pub(crate) fn display_name(store: &Store, key: &[u8; 32]) -> String {
    store.profile(key).ok().flatten().unwrap_or_else(|| {
        PublicKey::from_bytes(key).map_or("?".into(), |k| k.fmt_short().to_string())
    })
}

fn chunk_by_size(msgs: Vec<Bytes>, max: usize) -> Vec<Vec<Bytes>> {
    let mut out = vec![];
    let mut cur = vec![];
    let mut size = 0;
    for m in msgs {
        if size + m.len() > max && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
            size = 0;
        }
        size += m.len() + 4;
        cur.push(m);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn load_or_create_key(path: &Path) -> Result<SecretKey> {
    if let Ok(bytes) = std::fs::read(path) {
        let bytes: [u8; 32] = bytes.try_into().ok().context("identity.key corrompido")?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    std::fs::write(path, key.to_bytes())?;
    Ok(key)
}

fn rand_bytes() -> [u8; 32] {
    // Uma chave nova já é 32 bytes aleatórios de um CSPRNG.
    SecretKey::generate().to_bytes()
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}
