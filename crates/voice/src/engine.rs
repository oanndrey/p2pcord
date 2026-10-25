//! Motor de áudio: captura -> gate -> Opus -> pacotes, e pacotes -> Opus ->
//! jitter buffer por pessoa -> mixagem -> saída.
//!
//! Formato do pacote: `[seq: u16 LE][opus]`. O `seq` só avança quando um
//! pacote sai de fato, então qualquer buraco na sequência é perda de rede.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    FromSample, SampleFormat, SizedSample, Stream, StreamConfig,
};
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapProd, HeapRb,
};

use crate::{
    codec::{Decoder, Encoder, FRAME, RATE},
    resample::Resampler,
};

pub type PeerId = [u8; 32];

const BITRATE: i32 = 32_000;
/// Abaixo disso (RMS) o frame conta como silêncio. ~ -45 dBFS.
const GATE: f32 = 0.0056;
/// Quantos frames o gate fica aberto depois da última fala (300 ms).
const HANGOVER: u32 = 15;
/// Áudio que acumula antes de começar a tocar alguém (40 ms).
const PREBUFFER: usize = FRAME * 2;
/// Se a fila de alguém passar disso (200 ms), corta para `TRIM_TO`.
const MAX_QUEUE: usize = FRAME * 10;
const TRIM_TO: usize = FRAME * 3;
/// Maior buraco que vale a pena esconder com PLC/FEC.
const MAX_CONCEAL: u16 = 3;
const SPEAKING_WINDOW: Duration = Duration::from_millis(250);
/// Duração de cada nota dos sons de aviso.
const CUE_NOTE: Duration = Duration::from_millis(90);
/// Duração total de um som de aviso (duas notas).
pub const CUE_LEN: Duration = Duration::from_millis(180);

/// Som curto de aviso. Toca só aqui, não vai para a rede.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cue {
    Join,
    Leave,
}

/// Duas notas com ataque curto e decaimento, subindo (entrar) ou descendo (sair).
fn cue_samples(cue: Cue) -> Vec<f32> {
    let (a, b) = match cue {
        Cue::Join => (587.33, 880.0),
        Cue::Leave => (880.0, 587.33),
    };
    let rate = RATE as f32;
    let note = (rate * CUE_NOTE.as_secs_f32()) as usize;
    let mut out = Vec::with_capacity(note * 2);
    for freq in [a, b] {
        for i in 0..note {
            let t = i as f32 / rate;
            // 5 ms de ataque para não estalar.
            let env = (t / 0.005).min(1.0) * (-t * 40.0).exp();
            out.push((std::f32::consts::TAU * freq * t).sin() * 0.18 * env);
        }
    }
    out
}

struct PeerRx {
    dec: Decoder,
    next_seq: Option<u16>,
    queue: VecDeque<f32>,
    playing: bool,
    last_packet: Instant,
}

impl PeerRx {
    fn new() -> Result<Self> {
        Ok(Self {
            dec: Decoder::new()?,
            next_seq: None,
            queue: VecDeque::with_capacity(MAX_QUEUE),
            playing: false,
            last_packet: Instant::now(),
        })
    }

    fn push(&mut self, seq: u16, payload: &[u8]) {
        let mut frame = [0f32; FRAME];
        if let Some(expected) = self.next_seq {
            let gap = seq.wrapping_sub(expected);
            if gap >= 0x8000 {
                return; // atrasado ou duplicado
            }
            if gap > 0 && gap <= MAX_CONCEAL {
                // PLC para os frames do meio, FEC (dados do pacote atual) para o último.
                for k in 0..gap {
                    let n = if k == gap - 1 {
                        self.dec.decode(Some(payload), true, &mut frame)
                    } else {
                        self.dec.decode(None, false, &mut frame)
                    };
                    self.queue.extend(&frame[..n]);
                }
            }
        }
        let n = self.dec.decode(Some(payload), false, &mut frame);
        self.queue.extend(&frame[..n]);
        self.next_seq = Some(seq.wrapping_add(1));
        self.last_packet = Instant::now();
        if self.queue.len() > MAX_QUEUE {
            let drop = self.queue.len() - TRIM_TO;
            self.queue.drain(..drop);
        }
    }

    /// Soma o áudio desta pessoa em `out`.
    fn mix_into(&mut self, out: &mut [f32]) {
        if !self.playing {
            if self.queue.len() < PREBUFFER {
                return;
            }
            self.playing = true;
        }
        let n = out.len().min(self.queue.len());
        for (o, s) in out.iter_mut().zip(self.queue.drain(..n)) {
            *o += s;
        }
        if self.queue.is_empty() {
            self.playing = false;
        }
    }
}

struct Shared {
    peers: Mutex<HashMap<PeerId, PeerRx>>,
    muted: AtomicBool,
    deafened: AtomicBool,
    speaking: AtomicBool,
    stop: AtomicBool,
    /// Som de aviso que ainda falta tocar (a 48 kHz).
    cue: Mutex<VecDeque<f32>>,
}

/// Áudio ligado enquanto existir. Dropar desliga microfone e saída.
pub struct Engine {
    shared: Arc<Shared>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Engine {
    /// Abre os dispositivos padrão. `on_packet` recebe cada pacote codificado
    /// para mandar aos outros (é chamado de uma thread de áudio, não bloqueie).
    pub fn start(on_packet: impl FnMut(Vec<u8>) + Send + 'static) -> Result<Self> {
        let shared = Arc::new(Shared {
            peers: Mutex::new(HashMap::new()),
            muted: AtomicBool::new(false),
            deafened: AtomicBool::new(false),
            speaking: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            cue: Mutex::new(VecDeque::new()),
        });
        let (ready_tx, ready_rx) = mpsc::channel();
        let sh = shared.clone();
        // `cpal::Stream` não é `Send`; os streams vivem e morrem nesta thread.
        let thread = thread::Builder::new()
            .name("p2pcord-audio".into())
            .spawn(move || audio_thread(sh, on_packet, ready_tx))?;
        ready_rx.recv().context("thread de áudio morreu")??;
        Ok(Self { shared, thread: Some(thread) })
    }

    /// Entrega um pacote recebido de `peer`.
    pub fn push_packet(&self, peer: PeerId, packet: &[u8]) {
        if packet.len() < 3 {
            return;
        }
        let seq = u16::from_le_bytes([packet[0], packet[1]]);
        let mut peers = self.shared.peers.lock().unwrap();
        let rx = match peers.entry(peer) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => match PeerRx::new() {
                Ok(rx) => e.insert(rx),
                Err(_) => return,
            },
        };
        rx.push(seq, &packet[2..]);
    }

    pub fn remove_peer(&self, peer: &PeerId) {
        self.shared.peers.lock().unwrap().remove(peer);
    }

    /// Toca um som de aviso, trocando o que estiver tocando.
    pub fn play_cue(&self, cue: Cue) {
        let mut q = self.shared.cue.lock().unwrap();
        q.clear();
        q.extend(cue_samples(cue));
    }

    pub fn set_muted(&self, on: bool) {
        self.shared.muted.store(on, Ordering::Relaxed);
    }

    pub fn set_deafened(&self, on: bool) {
        self.shared.deafened.store(on, Ordering::Relaxed);
    }

    /// Se eu estou falando agora, e quem mais está.
    pub fn speaking(&self) -> (bool, Vec<PeerId>) {
        let now = Instant::now();
        let peers = self
            .shared
            .peers
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, rx)| now - rx.last_packet < SPEAKING_WINDOW)
            .map(|(id, _)| *id)
            .collect();
        (self.shared.speaking.load(Ordering::Relaxed), peers)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.join().ok();
        }
    }
}

fn audio_thread(
    shared: Arc<Shared>,
    mut on_packet: impl FnMut(Vec<u8>),
    ready: mpsc::Sender<Result<()>>,
) {
    let (prod, mut cons) = HeapRb::<f32>::new(RATE as usize / 2).split();
    let streams = match open_streams(&shared, prod) {
        Ok(s) => s,
        Err(e) => {
            ready.send(Err(e)).ok();
            return;
        }
    };
    let mut enc = match Encoder::new(BITRATE) {
        Ok(e) => e,
        Err(e) => {
            ready.send(Err(e)).ok();
            return;
        }
    };
    ready.send(Ok(())).ok();

    let mut frame = [0f32; FRAME];
    let mut pkt = [0u8; 2 + 400];
    let mut seq: u16 = 0;
    let mut open_for = 0u32;
    while !shared.stop.load(Ordering::Relaxed) {
        while cons.occupied_len() >= FRAME {
            cons.pop_slice(&mut frame);
            if shared.muted.load(Ordering::Relaxed) {
                open_for = 0;
                continue;
            }
            let rms = (frame.iter().map(|s| s * s).sum::<f32>() / FRAME as f32).sqrt();
            if rms > GATE {
                open_for = HANGOVER;
            } else if open_for > 0 {
                open_for -= 1;
            }
            if open_for == 0 {
                continue;
            }
            let Ok(n) = enc.encode(&frame, &mut pkt[2..]) else { continue };
            pkt[..2].copy_from_slice(&seq.to_le_bytes());
            seq = seq.wrapping_add(1);
            on_packet(pkt[..2 + n].to_vec());
        }
        shared.speaking.store(open_for > 0, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(5));
    }
    drop(streams);
}

fn open_streams(shared: &Arc<Shared>, prod: HeapProd<f32>) -> Result<(Stream, Stream)> {
    let host = cpal::default_host();
    let input = host.default_input_device().ok_or_else(|| anyhow!("nenhum microfone encontrado"))?;
    let output = host.default_output_device().ok_or_else(|| anyhow!("nenhuma saída de áudio encontrada"))?;

    let in_cfg = input.default_input_config()?;
    let in_stream = match in_cfg.sample_format() {
        SampleFormat::F32 => build_input::<f32>(&input, in_cfg.into(), prod),
        SampleFormat::I16 => build_input::<i16>(&input, in_cfg.into(), prod),
        SampleFormat::I32 => build_input::<i32>(&input, in_cfg.into(), prod),
        SampleFormat::U16 => build_input::<u16>(&input, in_cfg.into(), prod),
        f => Err(anyhow!("formato de microfone não suportado: {f:?}")),
    }?;

    let out_cfg = output.default_output_config()?;
    let sh = shared.clone();
    let out_stream = match out_cfg.sample_format() {
        SampleFormat::F32 => build_output::<f32>(&output, out_cfg.into(), sh),
        SampleFormat::I16 => build_output::<i16>(&output, out_cfg.into(), sh),
        SampleFormat::I32 => build_output::<i32>(&output, out_cfg.into(), sh),
        SampleFormat::U16 => build_output::<u16>(&output, out_cfg.into(), sh),
        f => Err(anyhow!("formato de saída não suportado: {f:?}")),
    }?;

    in_stream.play()?;
    out_stream.play()?;
    Ok((in_stream, out_stream))
}

fn build_input<T>(device: &cpal::Device, cfg: StreamConfig, mut prod: HeapProd<f32>) -> Result<Stream>
where
    T: SizedSample,
    f32: FromSample<T>,
{
    let ch = cfg.channels as usize;
    let mut rs = Resampler::new(cfg.sample_rate, RATE);
    let mut mono = Vec::with_capacity(4096);
    let mut out = Vec::with_capacity(4096);
    let stream = device.build_input_stream(
        cfg,
        move |data: &[T], _: &cpal::InputCallbackInfo| {
            mono.clear();
            mono.extend(data.chunks(ch).map(|f| {
                f.iter().map(|s| s.to_sample::<f32>()).sum::<f32>() / ch as f32
            }));
            out.clear();
            rs.process(&mono, &mut out);
            prod.push_slice(&out);
        },
        |e| eprintln!("microfone: {e}"),
        None,
    )?;
    Ok(stream)
}

fn build_output<T>(device: &cpal::Device, cfg: StreamConfig, shared: Arc<Shared>) -> Result<Stream>
where
    T: SizedSample + FromSample<f32>,
{
    let ch = cfg.channels as usize;
    let mut rs = Resampler::new(RATE, cfg.sample_rate);
    let mut fifo: VecDeque<f32> = VecDeque::with_capacity(8192);
    let mut mix = [0f32; FRAME / 2];
    let mut resampled = Vec::with_capacity(2048);
    let stream = device.build_output_stream(
        cfg,
        move |data: &mut [T], _: &cpal::OutputCallbackInfo| {
            let frames = data.len() / ch;
            while fifo.len() < frames {
                mix.fill(0.0);
                for rx in shared.peers.lock().unwrap().values_mut() {
                    rx.mix_into(&mut mix);
                }
                if shared.deafened.load(Ordering::Relaxed) {
                    mix.fill(0.0);
                }
                // Avisos tocam mesmo com o som desligado, como no Discord.
                let mut cue = shared.cue.lock().unwrap();
                let n = mix.len().min(cue.len());
                for (o, s) in mix.iter_mut().zip(cue.drain(..n)) {
                    *o += s;
                }
                drop(cue);
                resampled.clear();
                rs.process(&mix, &mut resampled);
                fifo.extend(resampled.iter().map(|s| s.clamp(-1.0, 1.0)));
            }
            for frame in data.chunks_mut(ch) {
                let s = T::from_sample(fifo.pop_front().unwrap_or(0.0));
                frame.fill(s);
            }
        },
        |e| eprintln!("saída de áudio: {e}"),
        None,
    )?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(enc: &mut Encoder, seq: u16) -> Vec<u8> {
        let pcm: Vec<f32> = (0..FRAME).map(|i| (i as f32 * 0.05).sin() * 0.4).collect();
        let mut out = vec![0u8; 402];
        let n = enc.encode(&pcm, &mut out[2..]).unwrap();
        out[..2].copy_from_slice(&seq.to_le_bytes());
        out.truncate(2 + n);
        out
    }

    #[test]
    fn jitter_buffer_esconde_perda_e_ignora_atrasado() {
        let mut enc = Encoder::new(BITRATE).unwrap();
        let mut rx = PeerRx::new().unwrap();
        let p0 = packet(&mut enc, 0);
        let p3 = packet(&mut enc, 3);
        rx.push(0, &p0[2..]);
        assert_eq!(rx.queue.len(), FRAME);
        // Perdeu 1 e 2: deve preencher com 2 frames escondidos + o 3.
        rx.push(3, &p3[2..]);
        assert_eq!(rx.queue.len(), FRAME * 4);
        // O 2 chegando atrasado é descartado.
        rx.push(2, &p0[2..]);
        assert_eq!(rx.queue.len(), FRAME * 4);

        let mut out = [0f32; FRAME];
        rx.mix_into(&mut out);
        assert!(rx.playing);
        assert_eq!(rx.queue.len(), FRAME * 3);
    }
}
