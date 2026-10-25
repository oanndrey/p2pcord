//! Teste de microfone: sua voz passa pelo Opus e volta nos fones.
//! `cargo run -p p2pcord-voice --example loopback --release -- 10` (segundos)

use std::{sync::mpsc, time::{Duration, Instant}};

fn main() -> anyhow::Result<()> {
    let secs: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(10);
    let (tx, rx) = mpsc::channel();
    let engine = p2pcord_voice::Engine::start(move |pkt| drop(tx.send(pkt)))?;
    println!("áudio aberto, falando por {secs}s (use fone para não microfonar)");
    let end = Instant::now() + Duration::from_secs(secs);
    let (mut pkts, mut bytes) = (0usize, 0usize);
    while Instant::now() < end {
        if let Ok(pkt) = rx.recv_timeout(Duration::from_millis(100)) {
            pkts += 1;
            bytes += pkt.len();
            engine.push_packet([0; 32], &pkt);
        }
    }
    println!("{pkts} pacotes enviados (gate aberto {:.1}s), média {} bytes/pacote",
        pkts as f32 * 0.02, if pkts > 0 { bytes / pkts } else { 0 });
    Ok(())
}
