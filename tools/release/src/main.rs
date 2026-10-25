//! Ferramenta de quem publica versões do p2pcord.
//!
//! ```text
//! p2pcord-release keygen                  cria a chave de release (uma vez só)
//! p2pcord-release sign <exe> <versão>     grava <exe>.sig
//! ```
//!
//! A chave secreta fica em `%USERPROFILE%\.p2pcord-release.key`, fora do
//! repositório. Quem tiver esse arquivo consegue mandar atualização para todo
//! mundo: não compartilhe e guarde uma cópia de segurança.

use std::path::PathBuf;

use anyhow::{bail, ensure, Context, Result};
use p2pcord_core::release;

fn key_path() -> Result<PathBuf> {
    let home = std::env::var("USERPROFILE").context("USERPROFILE não definido")?;
    Ok(PathBuf::from(home).join(".p2pcord-release.key"))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["keygen"] => {
            let path = key_path()?;
            ensure!(!path.exists(), "{} já existe; não vou sobrescrever", path.display());
            let (secret, public) = release::keygen();
            std::fs::write(&path, release::hex(&secret))?;
            println!("chave secreta: {}", path.display());
            println!("chave pública (vai no app/src/update.rs): {}", release::hex(&public));
        }
        ["sign", exe, version] => {
            ensure!(release::parse_version(version).is_some(), "versão inválida: {version}");
            let path = key_path()?;
            let secret = std::fs::read_to_string(&path)
                .with_context(|| format!("sem chave em {}; rode `keygen`", path.display()))?;
            let secret = release::unhex::<32>(&secret)?;
            let bytes = std::fs::read(exe).with_context(|| format!("lendo {exe}"))?;
            let sig = release::sign(&secret, version, &bytes);
            let out = format!("{exe}.sig");
            std::fs::write(&out, sig)?;
            println!("{out}");
        }
        _ => bail!("uso: p2pcord-release keygen | sign <exe> <versão>"),
    }
    Ok(())
}
