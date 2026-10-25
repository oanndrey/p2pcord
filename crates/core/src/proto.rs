//! Formato das mensagens que trafegam no gossip.
//!
//! Tudo que sai do nó é um `Signed`: o payload serializado em postcard e
//! assinado com a chave ed25519 do autor. Mensagens de chat são guardadas
//! no banco com os bytes assinados originais, assim qualquer peer pode
//! repassá-las no sync e o receptor continua conseguindo verificar o autor.

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    /// Anúncio de perfil (nome de exibição).
    Hello { name: String },
    /// Mensagem de texto num canal.
    Chat(ChatMsg),
    /// "Me manda tudo que você tem depois de `since_ms`."
    SyncReq { since_ms: u64 },
    /// Resposta ao sync: mensagens de chat assinadas, cruas.
    SyncResp { msgs: Vec<Bytes> },
    /// Entrei/saí do canal de voz deste servidor (repetido como heartbeat).
    /// O `ts_ms` deixa cada anúncio único: o gossip descarta mensagens com
    /// bytes iguais por 90 s, e a assinatura ed25519 é determinística.
    Voice { in_voice: bool, ts_ms: u64 },
}

impl Payload {
    pub fn voice(in_voice: bool) -> Self {
        Payload::Voice { in_voice, ts_ms: crate::node::now_ms() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMsg {
    pub channel: String,
    pub text: String,
    pub ts_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    from: PublicKey,
    data: Bytes,
    #[serde(with = "serde_bytes")]
    sig: [u8; Signature::LENGTH],
}

/// Mensagem verificada.
#[derive(Debug, Clone)]
pub struct Signed {
    pub from: PublicKey,
    pub payload: Payload,
    /// Bytes originais do envelope, para guardar e repassar.
    pub raw: Bytes,
}

impl Signed {
    /// Id estável da mensagem (hash do envelope).
    pub fn id(&self) -> [u8; 32] {
        *blake3::hash(&self.raw).as_bytes()
    }
}

pub fn encode(key: &SecretKey, payload: &Payload) -> Result<Bytes> {
    let data: Bytes = postcard::to_stdvec(payload)?.into();
    let sig = key.sign(&data).to_bytes();
    let env = Envelope { from: key.public(), data, sig };
    Ok(postcard::to_stdvec(&env)?.into())
}

pub fn decode(raw: Bytes) -> Result<Signed> {
    let env: Envelope = postcard::from_bytes(&raw).context("envelope inválido")?;
    env.from
        .verify(&env.data, &Signature::from_bytes(&env.sig))
        .context("assinatura inválida")?;
    let payload = postcard::from_bytes(&env.data).context("payload inválido")?;
    Ok(Signed { from: env.from, payload, raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_e_rejeita_adulterado() {
        let key = SecretKey::generate();
        let raw = encode(&key, &Payload::Hello { name: "ana".into() }).unwrap();
        let msg = decode(raw.clone()).unwrap();
        assert_eq!(msg.from, key.public());

        let mut bad = raw.to_vec();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        assert!(decode(bad.into()).is_err());
    }

    #[test]
    fn heartbeat_de_voz_nunca_repete_bytes() {
        let key = SecretKey::generate();
        let a = encode(&key, &Payload::voice(true)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = encode(&key, &Payload::voice(true)).unwrap();
        assert_ne!(a, b, "o gossip descartaria o segundo anúncio como duplicado");
    }
}
