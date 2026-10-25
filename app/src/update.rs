//! Atualização automática pelas releases do GitHub.
//!
//! O app olha a última release, e se for mais nova baixa `p2pcord.exe` e
//! `p2pcord.exe.sig`. Só troca o próprio executável se a assinatura bater com
//! `RELEASE_KEY` (ver `p2pcord_core::release`), então nem o GitHub nem quem
//! estiver no meio do caminho consegue empurrar outro binário.

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use p2pcord_core::release;

pub const CURRENT: &str = env!("CARGO_PKG_VERSION");
/// Repositório público com as releases ("dono/nome"). Vazio desliga a checagem.
const REPO: &str = "oanndrey/p2pcord";
/// Chave pública de release; a secreta fica com quem publica (`p2pcord-release keygen`).
const RELEASE_KEY: &str = "73ca8c44d1bf5753376885bd3f02bcfa203db9168ec898a4bf8a07d74bd964a9";
const EXE: &str = "p2pcord.exe";

pub struct Release {
    pub version: String,
    exe_url: String,
    sig_url: String,
}

/// Build de desenvolvimento e `P2PCORD_NO_UPDATE` não se atualizam.
pub fn enabled() -> bool {
    !cfg!(debug_assertions) && !REPO.is_empty() && std::env::var_os("P2PCORD_NO_UPDATE").is_none()
}

fn client() -> Result<reqwest::Client> {
    let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(reqwest::Client::builder()
        .tls_backend_preconfigured(tls)
        .user_agent(concat!("p2pcord/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(300))
        .build()?)
}

/// A última release, se for mais nova que esta.
pub async fn check() -> Result<Option<Release>> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/latest");
    let resp = client()?.get(url).header("Accept", "application/vnd.github+json").send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None); // ainda não tem release
    }
    let body: serde_json::Value = serde_json::from_slice(&resp.error_for_status()?.bytes().await?)?;
    let version = body["tag_name"].as_str().context("release sem tag")?.trim_start_matches('v').to_string();
    let newer = match (release::parse_version(&version), release::parse_version(CURRENT)) {
        (Some(new), Some(cur)) => new > cur,
        _ => false,
    };
    if !newer {
        return Ok(None);
    }
    let asset = |name: &str| {
        body["assets"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|a| a["name"] == name)
            .and_then(|a| a["browser_download_url"].as_str())
            .map(String::from)
    };
    let (Some(exe_url), Some(sig_url)) = (asset(EXE), asset(&format!("{EXE}.sig"))) else {
        return Ok(None); // release incompleta: ignora em vez de incomodar
    };
    Ok(Some(Release { version, exe_url, sig_url }))
}

/// Baixa, confere a assinatura e põe o exe novo no lugar do atual.
/// Devolve o caminho para reiniciar.
pub async fn install(rel: &Release) -> Result<PathBuf> {
    let c = client()?;
    let exe = c.get(&rel.exe_url).send().await?.error_for_status()?.bytes().await?;
    let sig = c.get(&rel.sig_url).send().await?.error_for_status()?.bytes().await?;
    let key = release::unhex::<32>(RELEASE_KEY)?;
    release::verify(&key, &rel.version, &exe, &sig)?;

    let cur = std::env::current_exe()?;
    let new = cur.with_extension("exe.new");
    let old = cur.with_extension("exe.old");
    std::fs::write(&new, &exe).context("sem permissão para gravar na pasta do app")?;
    std::fs::remove_file(&old).ok();
    // O Windows deixa renomear um exe em uso, só não deixa sobrescrever.
    std::fs::rename(&cur, &old)?;
    if let Err(e) = std::fs::rename(&new, &cur) {
        std::fs::rename(&old, &cur).ok();
        return Err(e.into());
    }
    Ok(cur)
}

/// Abre a versão nova e encerra esta.
pub fn restart(exe: &PathBuf) -> ! {
    std::process::Command::new(exe).spawn().ok();
    std::process::exit(0);
}

/// Apaga o exe antigo deixado pela última atualização.
pub fn cleanup() {
    if let Ok(cur) = std::env::current_exe() {
        std::fs::remove_file(cur.with_extension("exe.old")).ok();
    }
}
