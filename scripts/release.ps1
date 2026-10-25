# Publica uma versão nova do p2pcord: compila, assina e cria a release no GitHub.
# Os apps abertos acham a versão nova em até 6 h (ou ao abrir) e mostram o botão verde.
#
# Antes: suba o `version` em Cargo.toml ([workspace.package]).
# Uso:   .\scripts\release.ps1

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$version = (Select-String -Path Cargo.toml -Pattern '^version = "(.+)"').Matches[0].Groups[1].Value
$repo = (Select-String -Path app\src\update.rs -Pattern '^const REPO: &str = "(.*)";').Matches[0].Groups[1].Value
if (-not $repo) { throw 'REPO está vazio em app\src\update.rs' }

gh release view "v$version" --repo $repo 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) { throw "a v$version já foi publicada; suba a versão no Cargo.toml" }

cargo build --release -p p2pcord
if ($LASTEXITCODE) { throw 'build falhou' }
cargo run -q --release -p p2pcord-release -- sign target\release\p2pcord.exe $version
if ($LASTEXITCODE) { throw 'assinatura falhou' }

gh release create "v$version" target\release\p2pcord.exe target\release\p2pcord.exe.sig `
    --repo $repo --title "p2pcord $version" --notes "Versão $version"
if ($LASTEXITCODE) { throw 'gh release create falhou' }
Write-Host "publicado: v$version em $repo"
