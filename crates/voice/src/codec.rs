//! Wrappers seguros em cima do `unsafe-libopus` (libopus 1.3.1 em Rust puro).

use anyhow::{bail, Result};
use unsafe_libopus as ffi;

pub const RATE: u32 = 48_000;
/// 20 ms a 48 kHz, mono.
pub const FRAME: usize = 960;

pub struct Encoder(*mut ffi::OpusEncoder);

// O estado do Opus é só memória própria; pode trocar de thread.
unsafe impl Send for Encoder {}

impl Encoder {
    pub fn new(bitrate: i32) -> Result<Self> {
        let mut err = 0;
        let st = unsafe { ffi::opus_encoder_create(RATE as i32, 1, ffi::OPUS_APPLICATION_VOIP, &mut err) };
        if st.is_null() || err != ffi::OPUS_OK {
            bail!("opus_encoder_create falhou ({err})");
        }
        unsafe {
            ffi::opus_encoder_ctl!(st, ffi::OPUS_SET_BITRATE_REQUEST, bitrate);
            ffi::opus_encoder_ctl!(st, ffi::OPUS_SET_SIGNAL_REQUEST, ffi::OPUS_SIGNAL_VOICE);
            // FEC deixa o receptor recuperar um pacote perdido a partir do seguinte.
            ffi::opus_encoder_ctl!(st, ffi::OPUS_SET_INBAND_FEC_REQUEST, 1);
            ffi::opus_encoder_ctl!(st, ffi::OPUS_SET_PACKET_LOSS_PERC_REQUEST, 5);
            ffi::opus_encoder_ctl!(st, ffi::OPUS_SET_COMPLEXITY_REQUEST, 5);
        }
        Ok(Self(st))
    }

    /// Codifica um frame de `FRAME` amostras. Retorna o tamanho escrito em `out`.
    pub fn encode(&mut self, pcm: &[f32], out: &mut [u8]) -> Result<usize> {
        debug_assert_eq!(pcm.len(), FRAME);
        let n = unsafe {
            ffi::opus_encode_float(self.0, pcm.as_ptr(), FRAME as i32, out.as_mut_ptr(), out.len() as i32)
        };
        if n < 0 {
            bail!("opus_encode falhou ({n})");
        }
        Ok(n as usize)
    }
}

impl Drop for Encoder {
    fn drop(&mut self) {
        unsafe { ffi::opus_encoder_destroy(self.0) }
    }
}

pub struct Decoder(*mut ffi::OpusDecoder);

unsafe impl Send for Decoder {}

impl Decoder {
    pub fn new() -> Result<Self> {
        let mut err = 0;
        let st = unsafe { ffi::opus_decoder_create(RATE as i32, 1, &mut err) };
        if st.is_null() || err != ffi::OPUS_OK {
            bail!("opus_decoder_create falhou ({err})");
        }
        Ok(Self(st))
    }

    /// Decodifica um pacote. `None` pede ao Opus para inventar o frame perdido (PLC);
    /// `fec` usa a redundância do pacote seguinte para reconstruir o anterior.
    pub fn decode(&mut self, packet: Option<&[u8]>, fec: bool, out: &mut [f32; FRAME]) -> usize {
        let (ptr, len) = packet.map_or((std::ptr::null(), 0), |p| (p.as_ptr(), p.len() as i32));
        let n = unsafe {
            ffi::opus_decode_float(self.0, ptr, len, out.as_mut_ptr(), FRAME as i32, fec as i32)
        };
        n.max(0) as usize
    }
}

impl Drop for Decoder {
    fn drop(&mut self) {
        unsafe { ffi::opus_decoder_destroy(self.0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn senoide_ida_e_volta() {
        let mut enc = Encoder::new(32_000).unwrap();
        let mut dec = Decoder::new().unwrap();
        let mut pkt = [0u8; 400];
        let mut out = [0f32; FRAME];
        let mut energy = 0.0;
        for f in 0..10 {
            let pcm: Vec<f32> = (0..FRAME)
                .map(|i| ((f * FRAME + i) as f32 * 440.0 * std::f32::consts::TAU / RATE as f32).sin() * 0.5)
                .collect();
            let n = enc.encode(&pcm, &mut pkt).unwrap();
            assert!(n > 0 && n < 200);
            assert_eq!(dec.decode(Some(&pkt[..n]), false, &mut out), FRAME);
            energy = out.iter().map(|s| s * s).sum::<f32>() / FRAME as f32;
        }
        // Senoide de amplitude 0.5 tem energia média ~0.125.
        assert!(energy > 0.05, "energia {energy}");
    }
}
