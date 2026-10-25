//! Assinatura das versões publicadas (atualização automática).
//!
//! Cada release leva o `p2pcord.exe` e um `.sig` com a assinatura ed25519 de
//! `"p2pcord-update\0" + versão + "\0" + blake3(exe)`. A versão entra na
//! mensagem para ninguém reaproveitar a assinatura de um exe antigo dizendo
//! que é novo.

use anyhow::{ensure, Context, Result};
use iroh::{PublicKey, SecretKey, Signature};

fn message(version: &str, exe: &[u8]) -> Vec<u8> {
    let mut m = b"p2pcord-update\0".to_vec();
    m.extend_from_slice(version.as_bytes());
    m.push(0);
    m.extend_from_slice(blake3::hash(exe).as_bytes());
    m
}

/// Gera um par de chaves de release: (secreta, pública).
pub fn keygen() -> ([u8; 32], [u8; 32]) {
    let key = SecretKey::generate();
    (key.to_bytes(), *key.public().as_bytes())
}

pub fn sign(secret: &[u8; 32], version: &str, exe: &[u8]) -> [u8; 64] {
    SecretKey::from_bytes(secret).sign(&message(version, exe)).to_bytes()
}

pub fn verify(public: &[u8; 32], version: &str, exe: &[u8], sig: &[u8]) -> Result<()> {
    let key = PublicKey::from_bytes(public).context("chave de release inválida")?;
    let sig: [u8; 64] = sig.try_into().ok().context("assinatura com tamanho errado")?;
    key.verify(&message(version, exe), &Signature::from_bytes(&sig))
        .ok()
        .context("assinatura da atualização não confere")
}

/// `"1.2.3"` (com ou sem `v`) em números, para comparar versões.
pub fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let parts: Vec<u64> =
        v.trim().trim_start_matches('v').split('.').map(|p| p.parse().ok()).collect::<Option<_>>()?;
    match parts[..] {
        [a, b, c] => Some((a, b, c)),
        _ => None,
    }
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex<const N: usize>(s: &str) -> Result<[u8; N]> {
    let s = s.trim();
    ensure!(s.len() == N * 2, "esperava {} caracteres hex", N * 2);
    let mut out = [0u8; N];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).context("hex inválido")?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assinatura_amarra_versao_e_binario() {
        let (secret, public) = keygen();
        let exe = b"binario de teste";
        let sig = sign(&secret, "0.2.0", exe);
        assert!(verify(&public, "0.2.0", exe, &sig).is_ok());
        assert!(verify(&public, "0.3.0", exe, &sig).is_err(), "outra versão");
        assert!(verify(&public, "0.2.0", b"outro binario", &sig).is_err(), "outro exe");
        let (_, other) = keygen();
        assert!(verify(&other, "0.2.0", exe, &sig).is_err(), "outra chave");
    }

    #[test]
    fn versoes() {
        assert_eq!(parse_version("v0.2.10"), Some((0, 2, 10)));
        assert!(parse_version("0.10.0") > parse_version("0.9.9"));
        assert_eq!(parse_version("1.2"), None);
        assert_eq!(parse_version("1.2.3.4"), None);
        assert_eq!(unhex::<2>("0aff").unwrap(), [0x0a, 0xff]);
        assert_eq!(hex(&[0x0a, 0xff]), "0aff");
    }
}
