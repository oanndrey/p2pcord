#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    rc::Rc,
};

use chrono::{DateTime, Datelike, Local, NaiveDate, TimeZone};
use p2pcord_core::{release, Node, NodeEvent, Ticket, TopicId, VoiceMember};
use slint::{Color, ModelRc, SharedString, VecModel, Weak};
use tokio::sync::mpsc;

mod update;

slint::include_modules!();

const CHANNEL: &str = "geral";
/// Mensagens do mesmo autor com menos que isso de intervalo ficam no mesmo grupo.
const GROUP_MS: u64 = 5 * 60 * 1000;
const MAX_NAME: usize = 32;

thread_local! {
    // Modelo das mensagens; só é tocado na thread da UI.
    static MSGS: Rc<VecModel<MsgItem>> = Rc::new(VecModel::default());
}

enum Cmd {
    Select(usize),
    Send(String),
    Create(String),
    Join(String),
    Invite,
    Rename(String),
    JoinVoice,
    LeaveVoice,
    Mute(bool),
    Deafen(bool),
    Update,
    /// Volume em % de alguém da voz (chave em hex) ou do meu microfone.
    Volume { id: String, me: bool, percent: i32 },
}

fn main() -> anyhow::Result<()> {
    let ui = AppWindow::new()?;
    ui.set_messages(MSGS.with(|m| ModelRc::from(m.clone())));
    ui.set_status("conectando…".into());

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let tx = cmd_tx.clone();
    ui.on_select_server(move |i| drop(tx.send(Cmd::Select(i as usize))));
    let tx = cmd_tx.clone();
    ui.on_send(move |t| drop(tx.send(Cmd::Send(t.into()))));
    let tx = cmd_tx.clone();
    ui.on_create_server(move |n| drop(tx.send(Cmd::Create(n.into()))));
    let tx = cmd_tx.clone();
    ui.on_join_server(move |t| drop(tx.send(Cmd::Join(t.into()))));
    let tx = cmd_tx.clone();
    ui.on_request_invite(move || drop(tx.send(Cmd::Invite)));
    let tx = cmd_tx.clone();
    ui.on_rename(move |n| drop(tx.send(Cmd::Rename(n.into()))));
    let tx = cmd_tx.clone();
    ui.on_join_voice(move || drop(tx.send(Cmd::JoinVoice)));
    let tx = cmd_tx.clone();
    ui.on_leave_voice(move || drop(tx.send(Cmd::LeaveVoice)));
    let tx = cmd_tx.clone();
    ui.on_set_muted(move |on| drop(tx.send(Cmd::Mute(on))));
    let tx = cmd_tx.clone();
    ui.on_set_deafened(move |on| drop(tx.send(Cmd::Deafen(on))));
    let tx = cmd_tx.clone();
    ui.on_install_update(move || drop(tx.send(Cmd::Update)));
    let tx = cmd_tx;
    ui.on_set_volume(move |id, me, percent| drop(tx.send(Cmd::Volume { id: id.into(), me, percent })));

    // A rede roda num runtime tokio separado; a UI fica na thread principal.
    let weak = ui.as_weak();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime tokio");
        rt.block_on(async move {
            if let Err(e) = backend(weak.clone(), cmd_rx).await {
                ui_do(&weak, move |ui| ui.set_status(format!("erro: {e:#}").into()));
            }
        });
    });

    ui.run()?;
    Ok(())
}

fn data_dir() -> PathBuf {
    // P2PCORD_DIR permite rodar duas instâncias na mesma máquina para testar.
    if let Ok(dir) = std::env::var("P2PCORD_DIR") {
        return dir.into();
    }
    let base = std::env::var("APPDATA").unwrap_or_else(|_| ".".into());
    PathBuf::from(base).join("p2pcord")
}

/// Mensagem como veio do nó: autor, texto, timestamp em ms.
type Raw = (String, String, u64);

struct State {
    topics: Vec<TopicId>,
    names: Vec<String>,
    current: Option<usize>,
    online: HashMap<TopicId, usize>,
    voice: HashMap<TopicId, Vec<VoiceMember>>,
    unread: HashSet<TopicId>,
    /// Mensagens do canal aberto, em ordem de horário (base do agrupamento).
    chat: Vec<Raw>,
    /// A próxima mensagem do canal aberto é a que acabei de mandar: rola até ela.
    sent_pending: bool,
    muted: bool,
    deafened: bool,
    /// Versão nova encontrada no GitHub, esperando a pessoa clicar.
    update: Option<update::Release>,
}

impl State {
    fn current_topic(&self) -> Option<TopicId> {
        self.current.and_then(|i| self.topics.get(i).copied())
    }
}

async fn backend(ui: Weak<AppWindow>, mut cmds: mpsc::UnboundedReceiver<Cmd>) -> anyhow::Result<()> {
    let (ev_tx, mut events) = mpsc::unbounded_channel();
    let node = Node::spawn(&data_dir(), ev_tx).await?;
    let mut st = State {
        topics: vec![],
        names: vec![],
        current: None,
        online: HashMap::new(),
        voice: HashMap::new(),
        unread: HashSet::new(),
        chat: vec![],
        sent_pending: false,
        muted: false,
        deafened: false,
        update: None,
    };

    // Olha se tem versão nova logo depois de abrir e de tempos em tempos.
    update::cleanup();
    let (upd_tx, mut updates) = mpsc::unbounded_channel();
    if update::enabled() {
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            loop {
                if let Ok(Some(rel)) = update::check().await {
                    if upd_tx.send(rel).is_err() {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
            }
        });
    }

    // Na primeira execução o nome ainda é o gerado a partir da chave: pergunta.
    let first_run = node.name() == format!("user-{}", node.id().fmt_short());
    let id = node.id().to_string();
    show_my_name(&ui, node.name());
    ui_do(&ui, move |ui| {
        ui.set_my_id(id.into());
        ui.set_current_version(update::CURRENT.into());
        ui.set_status("online".into());
        if first_run {
            ui.set_dialog("welcome".into());
        }
    });
    refresh_servers(&node, &ui, &mut st)?;
    if !st.topics.is_empty() {
        select(&node, &ui, &mut st, 0)?;
    }

    loop {
        tokio::select! {
            Some(cmd) = cmds.recv() => {
                if let Err(e) = handle_cmd(&node, &ui, &mut st, cmd).await {
                    toast(&ui, format!("{e:#}"));
                }
            }
            Some(ev) = events.recv() => handle_event(&node, &ui, &mut st, ev),
            Some(rel) = updates.recv() => {
                let v = rel.version.clone();
                st.update = Some(rel);
                ui_do(&ui, move |ui| ui.set_update_version(v.into()));
            }
            else => break,
        }
    }
    Ok(())
}

async fn handle_cmd(node: &Node, ui: &Weak<AppWindow>, st: &mut State, cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::Select(i) => select(node, ui, st, i)?,
        Cmd::Send(text) => {
            let text = text.trim();
            if let (Some(t), false) = (st.current_topic(), text.is_empty()) {
                st.sent_pending = true;
                node.send(&t, CHANNEL, text).await?;
            }
        }
        Cmd::Create(name) => {
            let name = name.trim();
            anyhow::ensure!(!name.is_empty(), "o servidor precisa de um nome");
            node.create_server(name).await?;
            refresh_servers(node, ui, st)?;
            select(node, ui, st, st.topics.len() - 1)?;
        }
        Cmd::Join(text) => {
            let ticket: Ticket = text.trim().parse().map_err(|e: anyhow::Error| e.context("convite inválido"))?;
            let topic = node.join(&ticket).await?;
            refresh_servers(node, ui, st)?;
            let i = st.topics.iter().position(|t| *t == topic).unwrap_or(0);
            select(node, ui, st, i)?;
        }
        Cmd::Invite => {
            if let Some(t) = st.current_topic() {
                ui_do(ui, |ui| ui.set_invite_text("gerando…".into()));
                let ticket = node.invite(&t).await?.to_string();
                ui_do(ui, move |ui| ui.set_invite_text(ticket.into()));
            }
        }
        Cmd::Rename(name) => {
            let name: String = name.trim().chars().take(MAX_NAME).collect();
            anyhow::ensure!(!name.is_empty(), "o nome não pode ficar vazio");
            node.set_name(name.clone()).await?;
            show_my_name(ui, name);
            // Recarrega o canal para as minhas mensagens antigas aparecerem com o nome novo.
            if let Some(i) = st.current {
                select(node, ui, st, i)?;
            }
        }
        Cmd::JoinVoice => {
            if let Some(i) = st.current {
                let server = st.names[i].clone();
                ui_do(ui, move |ui| {
                    ui.set_in_voice(true);
                    ui.set_voice_connecting(true);
                    ui.set_voice_server(server.into());
                    ui.set_voice_server_index(i as i32);
                });
                if let Err(e) = node.join_voice(&st.topics[i]).await {
                    ui_do(ui, |ui| ui.set_in_voice(false));
                    return Err(e.context("não abriu o áudio"));
                }
                // Mudo/surdo valem entre chamadas, como no Discord.
                node.set_muted(st.muted);
                node.set_deafened(st.deafened);
                update_voice_ui(node, ui, st);
            }
        }
        Cmd::LeaveVoice => {
            node.leave_voice().await;
            update_voice_ui(node, ui, st);
        }
        Cmd::Mute(on) => {
            st.muted = on;
            node.set_muted(on);
            update_voice_ui(node, ui, st);
        }
        Cmd::Deafen(on) => {
            st.deafened = on;
            node.set_deafened(on);
            update_voice_ui(node, ui, st);
        }
        Cmd::Volume { id, me, percent } => {
            let volume = percent as f32 / 100.0;
            if me {
                node.set_mic_volume(volume)?;
            } else {
                node.set_user_volume(release::unhex::<32>(&id)?, volume)?;
            }
        }
        Cmd::Update => {
            let Some(rel) = &st.update else { return Ok(()) };
            ui_do(ui, |ui| ui.set_updating(true));
            match update::install(rel).await {
                Ok(exe) => {
                    node.leave_voice().await;
                    update::restart(&exe);
                }
                Err(e) => {
                    ui_do(ui, |ui| ui.set_updating(false));
                    return Err(e.context("não deu para atualizar"));
                }
            }
        }
    }
    Ok(())
}

fn handle_event(node: &Node, ui: &Weak<AppWindow>, st: &mut State, ev: NodeEvent) {
    match ev {
        NodeEvent::Chat { topic, channel, author, text, ts_ms } => {
            if channel != CHANNEL {
                return;
            }
            if st.current_topic() != Some(topic) {
                if st.unread.insert(topic) {
                    set_servers_ui(ui, st);
                }
                return;
            }
            let mine = std::mem::take(&mut st.sent_pending);
            // O sync pode trazer mensagens antigas: insere na ordem do horário.
            let pos = st.chat.partition_point(|m| m.2 <= ts_ms);
            st.chat.insert(pos, (author, text, ts_ms));
            if pos + 1 == st.chat.len() {
                let item = msg_item(pos.checked_sub(1).map(|p| &st.chat[p]), &st.chat[pos], today());
                ui_do(ui, move |ui| {
                    let stick = mine || ui.get_stick();
                    MSGS.with(|m| m.push(item));
                    if stick {
                        ui.invoke_scroll_to_end();
                    } else {
                        ui.set_unseen(ui.get_unseen() + 1);
                    }
                });
            } else {
                // Chegou no meio: refaz os grupos (raro, só no sync).
                let items = build_items(&st.chat);
                ui_do(ui, move |_| MSGS.with(|m| m.set_vec(items)));
            }
        }
        NodeEvent::Peers { topic, online } => {
            st.online.insert(topic, online);
            if st.current_topic() == Some(topic) {
                ui_do(ui, move |ui| ui.set_online(online as i32));
            }
        }
        NodeEvent::Voice { topic, members } => {
            st.voice.insert(topic, members);
            update_voice_ui(node, ui, st);
        }
        NodeEvent::Error(e) => toast(ui, e),
    }
}

/// Sincroniza lista de voz do servidor aberto e o painel da chamada.
fn update_voice_ui(node: &Node, ui: &Weak<AppWindow>, st: &State) {
    let current = st.current_topic();
    // Mudo/surdo só é conhecido para mim: o protocolo não anuncia o dos outros.
    let (muted, deafened) = (st.muted, st.deafened);
    let members: Vec<VoiceItem> = current
        .and_then(|t| st.voice.get(&t))
        .map(|list| {
            list.iter()
                .map(|m| VoiceItem {
                    id: release::hex(&m.id).into(),
                    volume: (m.volume * 100.0).round() as i32,
                    initial: first_initial(&m.name),
                    color: name_color(&m.name),
                    name: m.name.clone().into(),
                    speaking: m.speaking,
                    me: m.me,
                    muted: m.me && muted,
                    deafened: m.me && deafened,
                })
                .collect()
        })
        .unwrap_or_default();
    let voice_topic = node.voice_topic();
    let others = voice_topic.and_then(|t| st.voice.get(&t)).map_or(0, |l| l.iter().filter(|m| !m.me).count());
    let connecting = voice_topic.is_some() && node.voice_connections() < others;
    let here = voice_topic.is_some() && voice_topic == current;
    let in_voice = voice_topic.is_some();
    let my_speaking = voice_topic.and_then(|t| st.voice.get(&t)).is_some_and(|l| l.iter().any(|m| m.me && m.speaking));
    ui_do(ui, move |ui| {
        ui.set_voice_members(ModelRc::new(VecModel::from(members)));
        ui.set_voice_here(here);
        ui.set_in_voice(in_voice);
        ui.set_voice_connecting(connecting);
        ui.set_my_speaking(my_speaking);
    });
}

fn refresh_servers(node: &Node, ui: &Weak<AppWindow>, st: &mut State) -> anyhow::Result<()> {
    let servers = node.servers()?;
    st.topics = servers.iter().map(|s| s.topic).collect();
    st.names = servers.into_iter().map(|s| s.name).collect();
    set_servers_ui(ui, st);
    Ok(())
}

fn set_servers_ui(ui: &Weak<AppWindow>, st: &State) {
    let items: Vec<ServerItem> = st
        .topics
        .iter()
        .zip(&st.names)
        .map(|(t, name)| ServerItem {
            initials: initials(name).into(),
            name: name.into(),
            unread: st.unread.contains(t),
        })
        .collect();
    ui_do(ui, move |ui| ui.set_servers(ModelRc::new(VecModel::from(items))));
}

fn select(node: &Node, ui: &Weak<AppWindow>, st: &mut State, i: usize) -> anyhow::Result<()> {
    let Some(topic) = st.topics.get(i).copied() else { return Ok(()) };
    st.current = Some(i);
    st.sent_pending = false;
    if st.unread.remove(&topic) {
        set_servers_ui(ui, st);
    }
    st.chat = node.history(&topic, CHANNEL)?;
    st.chat.sort_by_key(|m| m.2);
    let items = build_items(&st.chat);
    let online = st.online.get(&topic).copied().unwrap_or(0) as i32;
    ui_do(ui, move |ui| {
        ui.set_current_server(i as i32);
        ui.set_online(online);
        MSGS.with(|m| m.set_vec(items));
        ui.invoke_scroll_to_end();
        ui.invoke_focus_composer();
    });
    update_voice_ui(node, ui, st);
    Ok(())
}

fn show_my_name(ui: &Weak<AppWindow>, name: String) {
    let (initial, color) = (first_initial(&name), name_color(&name));
    ui_do(ui, move |ui| {
        ui.set_my_name(name.into());
        ui.set_my_initial(initial);
        ui.set_my_color(color);
    });
}

fn toast(ui: &Weak<AppWindow>, text: String) {
    ui_do(ui, move |ui| ui.set_toast(text.into()));
}

fn ui_do(ui: &Weak<AppWindow>, f: impl FnOnce(&AppWindow) + Send + 'static) {
    ui.upgrade_in_event_loop(move |ui| f(&ui)).ok();
}

fn initials(name: &str) -> String {
    let s: String = name.split_whitespace().filter_map(|w| w.chars().next()).take(2).collect();
    s.to_uppercase()
}

fn first_initial(name: &str) -> SharedString {
    name.chars()
        .find(|c| c.is_alphanumeric())
        .map_or("?".into(), |c| c.to_uppercase().collect::<String>())
        .into()
}

/// Cor do avatar derivada do nome, para a mesma pessoa ter sempre a mesma cor.
fn name_color(name: &str) -> Color {
    const PALETTE: [u32; 8] = [0x5865f2, 0x3ba55c, 0xe67e22, 0xed4245, 0xeb459e, 0x1abc9c, 0x9b59b6, 0xc29a1d];
    // FNV-1a: estável entre execuções e máquinas.
    let h = name.bytes().fold(0x811c9dc5u32, |h, b| (h ^ b as u32).wrapping_mul(0x01000193));
    let c = PALETTE[(h % PALETTE.len() as u32) as usize];
    Color::from_rgb_u8((c >> 16) as u8, (c >> 8) as u8, c as u8)
}

// ---------------------------------------------------------------- Mensagens

fn local(ts_ms: u64) -> Option<DateTime<Local>> {
    Local.timestamp_millis_opt(ts_ms as i64).single()
}

fn today() -> NaiveDate {
    Local::now().date_naive()
}

fn build_items(msgs: &[Raw]) -> Vec<MsgItem> {
    let today = today();
    let mut prev = None;
    msgs.iter()
        .map(|m| {
            let item = msg_item(prev, m, today);
            prev = Some(m);
            item
        })
        .collect()
}

/// Monta a linha da mensagem; `prev` decide se ela abre um grupo novo.
fn msg_item(prev: Option<&Raw>, (author, text, ts): &Raw, today: NaiveDate) -> MsgItem {
    let t = local(*ts);
    let day = t.map(|t| t.date_naive());
    let new_day = prev.is_none_or(|p| local(p.2).map(|t| t.date_naive()) != day);
    let head = new_day || prev.is_none_or(|p| p.0 != *author || ts.saturating_sub(p.2) > GROUP_MS);
    MsgItem {
        author: author.into(),
        initial: first_initial(author),
        color: name_color(author),
        text: text.into(),
        time: t.map(|t| t.format("%H:%M").to_string()).unwrap_or_default().into(),
        stamp: t.map(|t| fmt_stamp(t, today)).unwrap_or_default().into(),
        head,
        day: if new_day { day.map(|d| fmt_day(d, today)).unwrap_or_default().into() } else { SharedString::new() },
    }
}

fn relative_day(d: NaiveDate, today: NaiveDate) -> Option<&'static str> {
    if d == today {
        Some("Hoje")
    } else if today.pred_opt() == Some(d) {
        Some("Ontem")
    } else {
        None
    }
}

fn fmt_stamp(t: DateTime<Local>, today: NaiveDate) -> String {
    match relative_day(t.date_naive(), today) {
        Some(rel) => format!("{rel} às {}", t.format("%H:%M")),
        None => t.format("%d/%m/%Y %H:%M").to_string(),
    }
}

fn fmt_day(d: NaiveDate, today: NaiveDate) -> String {
    const MESES: [&str; 12] = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro",
        "dezembro",
    ];
    match relative_day(d, today) {
        Some(rel) => rel.into(),
        None => format!("{} de {} de {}", d.day(), MESES[d.month0() as usize], d.year()),
    }
}
