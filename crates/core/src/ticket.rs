//! Convite para um servidor: tópico do gossip + alguns peers para entrar.

use std::{fmt, str::FromStr};

use anyhow::{Context, Result};
use iroh::EndpointAddr;
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticket {
    pub name: String,
    pub topic: TopicId,
    pub peers: Vec<EndpointAddr>,
}

impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bytes = postcard::to_stdvec(self).expect("postcard em Vec não falha");
        let mut text = data_encoding::BASE32_NOPAD.encode(&bytes);
        text.make_ascii_lowercase();
        write!(f, "p2pcord:{text}")
    }
}

impl FromStr for Ticket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let s = s.strip_prefix("p2pcord:").unwrap_or(s);
        let bytes = data_encoding::BASE32_NOPAD
            .decode(s.to_ascii_uppercase().as_bytes())
            .context("convite mal formado")?;
        postcard::from_bytes(&bytes).context("convite inválido")
    }
}
