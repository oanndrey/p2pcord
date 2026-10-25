//! Resampler linear. Para voz a 48 kHz <-> taxa do dispositivo é suficiente
//! e não custa nada de CPU.

pub struct Resampler {
    step: f64,
    pos: f64,
    prev: f32,
}

impl Resampler {
    pub fn new(from: u32, to: u32) -> Self {
        Self { step: from as f64 / to as f64, pos: 0.0, prev: 0.0 }
    }

    pub fn process(&mut self, input: &[f32], out: &mut Vec<f32>) {
        if input.is_empty() {
            return;
        }
        if self.step == 1.0 {
            out.extend_from_slice(input);
            return;
        }
        // `pos` vai de -1 (última amostra do bloco anterior) até n-1.
        let n = input.len() as f64;
        while self.pos < n - 1.0 {
            let i = self.pos.floor();
            let frac = (self.pos - i) as f32;
            let i = i as isize;
            let a = if i < 0 { self.prev } else { input[i as usize] };
            let b = input[(i + 1) as usize];
            out.push(a + (b - a) * frac);
            self.pos += self.step;
        }
        self.pos -= n;
        self.prev = input[input.len() - 1];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proporcao_certa() {
        let mut r = Resampler::new(44_100, 48_000);
        let mut out = vec![];
        for _ in 0..100 {
            r.process(&[0.25; 441], &mut out);
        }
        let expected = 48_000.0;
        assert!((out.len() as f64 - expected).abs() < 2.0, "{}", out.len());
        assert!(out.iter().skip(1).all(|s| (s - 0.25).abs() < 1e-6));
    }
}
