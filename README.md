# p2pcord

Chat estilo Discord, P2P e nativo para PC. Sem servidor central e sem conta: sua identidade é um par de chaves ed25519 guardado em `%APPDATA%\p2pcord`.

## Stack

- **UI:** Slint com o renderizador por software (~33 MB de RAM, binário de ~14 MB). Os ícones são `Path` com dados SVG inline (feature `software-renderer-path`), sem imagem nem fonte extra
- **Rede:** iroh 1.x (QUIC com hole-punching de NAT e relay de fallback) + iroh-gossip, um tópico por servidor
- **Voz:** cpal + Opus (`unsafe-libopus`, Rust puro, sem cmake) por datagramas QUIC, em malha
- **Dados:** SQLite local. As mensagens são guardadas com a assinatura original, então qualquer peer consegue repassar o histórico e o receptor ainda verifica o autor.

## Estrutura

```
crates/core   identidade, protocolo, store, nó P2P, sinalização e conexões de voz
crates/voice  motor de áudio: captura, gate, Opus, jitter buffer, mixagem
app           janela Slint, liga a UI no nó
```

## Rodar

```sh
cargo run -p p2pcord                 # debug
cargo build --release -p p2pcord     # target/release/p2pcord.exe
cargo test -p p2pcord-core           # dois nós reais: chat, sync e conexão de voz
cargo test -p p2pcord-voice          # codec, resampler, jitter buffer
cargo run -p p2pcord-voice --example loopback --release -- 10   # teste de microfone
```

Para testar duas instâncias na mesma máquina, use pastas de dados diferentes:

```powershell
$env:P2PCORD_DIR="$env:TEMP\p2pcord-b"; .\target\release\p2pcord.exe
```

## Como funciona

- **Servidor** = tópico do gossip. O convite (`p2pcord:...`) leva o nome, o tópico e o endereço de quem convidou.
- **Ao conectar** num vizinho, o nó manda `Hello` (nome) e `SyncReq` (timestamp da última mensagem que tem). Os vizinhos respondem com as mensagens que faltam, fatiadas em blocos de 48 KB.
- **Voz:** entrar no canal anuncia `Voice { in_voice }` no gossip (repetido a cada 10 s). Entre dois participantes, o de id menor disca uma conexão `p2pcord/voice/0`; o primeiro stream leva o tópico e o áudio vai em datagramas `[seq u16][opus]`, 20 ms por pacote, 32 kbps, com FEC. Um noise gate não manda nada no silêncio. Quem recebe tem um jitter buffer por pessoa (40 ms de pré-buffer, PLC/FEC para até 3 pacotes perdidos).
- **Ao reiniciar**, o nó reentra nos servidores usando os membros já vistos como bootstrap (o iroh resolve o endereço atual pela chave pública).

## Atualização automática

O app olha a última release de `oanndrey/p2pcord` ao abrir e a cada 6 h. Se tiver versão nova, aparece um botão verde na coluna de servidores. Ele baixa o `p2pcord.exe`, confere a assinatura ed25519 contra a chave pública embutida em `app/src/update.rs` e troca o próprio executável. Builds de debug e `P2PCORD_NO_UPDATE=1` não se atualizam.

Para publicar (precisa da chave secreta em `%USERPROFILE%\.p2pcord-release.key`, criada uma vez com `cargo run -p p2pcord-release -- keygen`):

```powershell
# 1. suba o version em Cargo.toml ([workspace.package])
.\scripts\release.ps1   # compila, assina e cria a release vX.Y.Z no GitHub
```

## Roadmap

- [x] Texto: servidores, convite, histórico sincronizado, nomes
- [x] Voz: canal por servidor, mudo/surdo, indicador de quem fala
- [ ] Escolher microfone/saída e ajustar sensibilidade do gate
- [ ] Vários canais por servidor
- [ ] DMs
- [ ] Criptografia de grupo (hoje quem tem o convite lê o servidor)
