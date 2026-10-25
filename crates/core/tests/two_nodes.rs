//! Dois nós na mesma máquina: cria servidor, convida, conversa e sincroniza.

use std::time::Duration;

use p2pcord_core::{Node, NodeEvent};
use tokio::{sync::mpsc, time::timeout};

async fn wait_chat(rx: &mut mpsc::UnboundedReceiver<NodeEvent>, want: &str) {
    timeout(Duration::from_secs(30), async {
        while let Some(ev) = rx.recv().await {
            if let NodeEvent::Chat { text, .. } = ev {
                if text == want {
                    return;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("não recebeu {want:?}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn conversa_e_sync() {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ta, mut ra) = mpsc::unbounded_channel();
    let (tb, mut rb) = mpsc::unbounded_channel();
    let a = Node::spawn(da.path(), ta).await.unwrap();
    let b = Node::spawn(db.path(), tb).await.unwrap();

    let topic = a.create_server("teste").await.unwrap();
    // Mensagem enviada antes de B entrar: tem que chegar pelo sync.
    a.send(&topic, "geral", "antes").await.unwrap();

    let ticket = a.invite(&topic).await.unwrap();
    b.join(&ticket.to_string().parse().unwrap()).await.unwrap();
    wait_chat(&mut rb, "antes").await;

    b.send(&topic, "geral", "oi do b").await.unwrap();
    wait_chat(&mut ra, "oi do b").await;

    let hist = a.history(&topic, "geral").unwrap();
    assert_eq!(hist.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn voz_conecta_e_lista_membros() {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (ta, mut ra) = mpsc::unbounded_channel();
    let (tb, _rb) = mpsc::unbounded_channel();
    let a = Node::spawn(da.path(), ta).await.unwrap();
    let b = Node::spawn(db.path(), tb).await.unwrap();
    let topic = a.create_server("voz").await.unwrap();
    let ticket = a.invite(&topic).await.unwrap();
    b.join(&ticket).await.unwrap();

    a.join_voice(&topic).await.unwrap();
    b.join_voice(&topic).await.unwrap();

    // A precisa ver os dois na lista de voz.
    timeout(Duration::from_secs(30), async {
        while let Some(ev) = ra.recv().await {
            if let NodeEvent::Voice { members, .. } = ev {
                if members.len() == 2 {
                    return;
                }
            }
        }
    })
    .await
    .expect("lista de voz não chegou a 2");

    // E a conexão de áudio direta tem que subir dos dois lados.
    timeout(Duration::from_secs(30), async {
        while a.voice_connections() == 0 || b.voice_connections() == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("conexão de voz não abriu");

    b.leave_voice().await;
    timeout(Duration::from_secs(10), async {
        while a.voice_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("conexão não fechou depois de sair");
}
