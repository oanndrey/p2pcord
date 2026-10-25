//! Persistência local em SQLite: servidores, perfis e histórico.

use std::path::Path;

use anyhow::Result;
use bytes::Bytes;
use iroh_gossip::proto::TopicId;
use rusqlite::{params, Connection, OptionalExtension};

use crate::proto::{ChatMsg, Signed};

pub struct Store {
    db: Connection,
}

#[derive(Debug, Clone)]
pub struct ServerRow {
    pub topic: TopicId,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct MsgRow {
    pub author: [u8; 32],
    pub channel: String,
    pub text: String,
    pub ts_ms: u64,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS servers(
                 topic BLOB PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS profiles(
                 key BLOB PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS messages(
                 id BLOB PRIMARY KEY, topic BLOB NOT NULL, channel TEXT NOT NULL,
                 author BLOB NOT NULL, ts INTEGER NOT NULL, text TEXT NOT NULL,
                 raw BLOB NOT NULL);
             CREATE INDEX IF NOT EXISTS messages_by_chan ON messages(topic, channel, ts);
             CREATE TABLE IF NOT EXISTS members(
                 topic BLOB NOT NULL, key BLOB NOT NULL, PRIMARY KEY(topic, key));
             CREATE TABLE IF NOT EXISTS volumes(
                 key BLOB PRIMARY KEY, volume REAL NOT NULL);
             CREATE TABLE IF NOT EXISTS settings(
                 name TEXT PRIMARY KEY, value REAL NOT NULL);",
        )?;
        Ok(Self { db })
    }

    /// Lembra de um peer visto no servidor, para reentrar depois de reiniciar.
    pub fn add_member(&self, topic: &TopicId, key: &[u8; 32]) -> Result<()> {
        self.db.execute(
            "INSERT OR IGNORE INTO members(topic, key) VALUES (?1, ?2)",
            params![topic.as_bytes(), key],
        )?;
        Ok(())
    }

    pub fn members(&self, topic: &TopicId) -> Result<Vec<[u8; 32]>> {
        let mut st = self.db.prepare("SELECT key FROM members WHERE topic = ?1")?;
        let rows = st.query_map([topic.as_bytes()], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn add_server(&self, topic: &TopicId, name: &str) -> Result<()> {
        self.db.execute(
            "INSERT OR IGNORE INTO servers(topic, name) VALUES (?1, ?2)",
            params![topic.as_bytes(), name],
        )?;
        Ok(())
    }

    pub fn servers(&self) -> Result<Vec<ServerRow>> {
        let mut st = self.db.prepare("SELECT topic, name FROM servers ORDER BY rowid")?;
        let rows = st.query_map([], |r| {
            let topic: [u8; 32] = r.get(0)?;
            Ok(ServerRow { topic: TopicId::from_bytes(topic), name: r.get(1)? })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn set_profile(&self, key: &[u8; 32], name: &str) -> Result<()> {
        self.db.execute(
            "INSERT INTO profiles(key, name) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET name = excluded.name",
            params![key, name],
        )?;
        Ok(())
    }

    pub fn profile(&self, key: &[u8; 32]) -> Result<Option<String>> {
        Ok(self
            .db
            .query_row("SELECT name FROM profiles WHERE key = ?1", [key], |r| r.get(0))
            .optional()?)
    }

    /// Volume que eu escolhi para uma pessoa (só vale aqui, não vai para a rede).
    pub fn set_volume(&self, key: &[u8; 32], volume: f32) -> Result<()> {
        self.db.execute(
            "INSERT INTO volumes(key, volume) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET volume = excluded.volume",
            params![key, volume as f64],
        )?;
        Ok(())
    }

    pub fn volumes(&self) -> Result<Vec<([u8; 32], f32)>> {
        let mut st = self.db.prepare("SELECT key, volume FROM volumes")?;
        let rows = st.query_map([], |r| Ok((r.get(0)?, r.get::<_, f64>(1)? as f32)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn set_setting(&self, name: &str, value: f32) -> Result<()> {
        self.db.execute(
            "INSERT INTO settings(name, value) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            params![name, value as f64],
        )?;
        Ok(())
    }

    pub fn setting(&self, name: &str) -> Result<Option<f32>> {
        Ok(self
            .db
            .query_row("SELECT value FROM settings WHERE name = ?1", [name], |r| r.get::<_, f64>(0))
            .optional()?
            .map(|v| v as f32))
    }

    /// Guarda uma mensagem de chat. Retorna `false` se ela já existia.
    pub fn insert_chat(&self, topic: &TopicId, msg: &Signed, chat: &ChatMsg) -> Result<bool> {
        let n = self.db.execute(
            "INSERT OR IGNORE INTO messages(id, topic, channel, author, ts, text, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                msg.id(),
                topic.as_bytes(),
                chat.channel,
                msg.from.as_bytes(),
                chat.ts_ms as i64,
                chat.text,
                &msg.raw[..],
            ],
        )?;
        Ok(n > 0)
    }

    pub fn history(&self, topic: &TopicId, channel: &str, limit: usize) -> Result<Vec<MsgRow>> {
        let mut st = self.db.prepare(
            "SELECT author, channel, text, ts FROM (
                 SELECT * FROM messages WHERE topic = ?1 AND channel = ?2
                 ORDER BY ts DESC LIMIT ?3) ORDER BY ts",
        )?;
        let rows = st.query_map(params![topic.as_bytes(), channel, limit as i64], |r| {
            Ok(MsgRow {
                author: r.get(0)?,
                channel: r.get(1)?,
                text: r.get(2)?,
                ts_ms: r.get::<_, i64>(3)? as u64,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn latest_ts(&self, topic: &TopicId) -> Result<u64> {
        let ts: Option<i64> = self.db.query_row(
            "SELECT MAX(ts) FROM messages WHERE topic = ?1",
            [topic.as_bytes()],
            |r| r.get(0),
        )?;
        Ok(ts.unwrap_or(0) as u64)
    }

    /// Mensagens cruas depois de `since_ms`, para responder um sync.
    pub fn raw_since(&self, topic: &TopicId, since_ms: u64, limit: usize) -> Result<Vec<Bytes>> {
        let mut st = self.db.prepare(
            "SELECT raw FROM messages WHERE topic = ?1 AND ts > ?2 ORDER BY ts LIMIT ?3",
        )?;
        let rows = st.query_map(params![topic.as_bytes(), since_ms as i64, limit as i64], |r| {
            Ok(Bytes::from(r.get::<_, Vec<u8>>(0)?))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volumes_e_ajustes_sobrevivem_reabrir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        {
            let s = Store::open(&path).unwrap();
            s.set_volume(&[1; 32], 0.5).unwrap();
            s.set_volume(&[1; 32], 1.5).unwrap();
            s.set_setting("mic_volume", 0.8).unwrap();
        }
        let s = Store::open(&path).unwrap();
        assert_eq!(s.volumes().unwrap(), vec![([1; 32], 1.5)]);
        assert_eq!(s.setting("mic_volume").unwrap(), Some(0.8));
        assert_eq!(s.setting("outro").unwrap(), None);
    }
}
