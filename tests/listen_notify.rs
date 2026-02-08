use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Config, NoTls, Notification};
use ulid::Ulid;

use deltat::tenant::TenantManager;
use deltat::wire;

// ── Test infrastructure ──────────────────────────────────────

async fn start_test_server() -> (SocketAddr, Arc<TenantManager>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let dir = std::env::temp_dir().join(format!("deltat_int_test_{}", Ulid::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let tm = Arc::new(TenantManager::new(dir, 1000));

    let tm2 = tm.clone();
    tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let tm = tm2.clone();
            tokio::spawn(async move {
                let _ = wire::process_connection(socket, tm, "deltat".to_string()).await;
            });
        }
    });

    (addr, tm)
}

async fn connect(
    addr: SocketAddr,
) -> (
    tokio_postgres::Client,
    mpsc::UnboundedReceiver<Notification>,
) {
    let mut config = Config::new();
    config
        .host(&addr.ip().to_string())
        .port(addr.port())
        .dbname("test")
        .user("deltat")
        .password("deltat");

    let (client, mut connection) = config.connect(NoTls).await.unwrap();

    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let stream = stream::poll_fn(move |cx| connection.poll_message(cx));
        futures::pin_mut!(stream);
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AsyncMessage::Notification(n)) => {
                    let _ = tx.send(n);
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    (client, rx)
}

/// Wait for a notification with timeout.
async fn recv_notification(
    rx: &mut mpsc::UnboundedReceiver<Notification>,
    timeout: Duration,
) -> Option<Notification> {
    tokio::time::timeout(timeout, rx.recv()).await.ok().flatten()
}

// ── Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn connect_and_query() {
    let (addr, _tm) = start_test_server().await;
    let (client, _rx) = connect(addr).await;

    // Create a resource and verify the query succeeds
    let rid = Ulid::new();
    client
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    // Query it back
    let rows = client
        .simple_query(&format!(
            "SELECT * FROM resources"
        ))
        .await
        .unwrap();

    // Should have at least one data row + command complete
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn listen_receives_notification() {
    let (addr, _tm) = start_test_server().await;

    // Connection 1: subscriber
    let (client1, mut rx1) = connect(addr).await;

    let rid = Ulid::new();
    // Create the resource first
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    // Subscribe
    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    // Connection 2: mutator
    let (client2, _rx2) = connect(addr).await;

    // Mutate — add a rule to trigger an event
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();

    // Wait for notification
    let notif = recv_notification(&mut rx1, Duration::from_secs(5)).await;
    assert!(notif.is_some(), "expected notification");
    let notif = notif.unwrap();
    assert_eq!(notif.channel(), &format!("resource_{rid}"));
}

#[tokio::test]
async fn notification_payload_is_valid_json() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid = Ulid::new();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    let (client2, _) = connect(addr).await;
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();

    let notif = recv_notification(&mut rx1, Duration::from_secs(5))
        .await
        .expect("expected notification");

    // Payload should be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(notif.payload())
        .expect("notification payload should be valid JSON");
    assert!(parsed.is_object());
}

#[tokio::test]
async fn notification_only_on_subscribed_resource() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid_a = Ulid::new();
    let rid_b = Ulid::new();

    // Create both resources
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid_a}')"
        ))
        .await
        .unwrap();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid_b}')"
        ))
        .await
        .unwrap();

    // Listen only on A
    client1
        .batch_execute(&format!("LISTEN resource_{rid_a}"))
        .await
        .unwrap();

    let (client2, _) = connect(addr).await;

    // Mutate B — should NOT trigger notification
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid_b}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();

    let notif = recv_notification(&mut rx1, Duration::from_millis(500)).await;
    assert!(notif.is_none(), "should not receive notification for unsubscribed resource");

    // Mutate A — SHOULD trigger notification
    let rule_id2 = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id2}', '{rid_a}', 3000, 4000, false)"#
        ))
        .await
        .unwrap();

    let notif = recv_notification(&mut rx1, Duration::from_secs(5)).await;
    assert!(notif.is_some(), "should receive notification for subscribed resource");
}

#[tokio::test]
async fn listen_duplicate_is_idempotent() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid = Ulid::new();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    // Listen twice on the same channel — should not error
    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();
    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    let (client2, _) = connect(addr).await;
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();

    // Should get exactly one notification (not duplicated)
    let notif1 = recv_notification(&mut rx1, Duration::from_secs(5)).await;
    assert!(notif1.is_some(), "should receive one notification");

    let notif2 = recv_notification(&mut rx1, Duration::from_millis(500)).await;
    assert!(notif2.is_none(), "should not receive duplicate notification");
}

#[tokio::test]
async fn unlisten_stops_notifications() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid = Ulid::new();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    // UNLISTEN
    client1
        .batch_execute(&format!("UNLISTEN resource_{rid}"))
        .await
        .unwrap();

    // Small delay for unsubscribe to take effect
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (client2, _) = connect(addr).await;
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();

    let notif = recv_notification(&mut rx1, Duration::from_millis(500)).await;
    assert!(notif.is_none(), "should not receive notification after UNLISTEN");
}

#[tokio::test]
async fn unlisten_all_stops_everything() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid_a = Ulid::new();
    let rid_b = Ulid::new();

    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid_a}')"
        ))
        .await
        .unwrap();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid_b}')"
        ))
        .await
        .unwrap();

    client1
        .batch_execute(&format!("LISTEN resource_{rid_a}"))
        .await
        .unwrap();
    client1
        .batch_execute(&format!("LISTEN resource_{rid_b}"))
        .await
        .unwrap();

    // UNLISTEN *
    client1.batch_execute("UNLISTEN *").await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let (client2, _) = connect(addr).await;
    let rule_a = Ulid::new();
    let rule_b = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_a}', '{rid_a}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_b}', '{rid_b}', 3000, 4000, false)"#
        ))
        .await
        .unwrap();

    let notif = recv_notification(&mut rx1, Duration::from_millis(500)).await;
    assert!(notif.is_none(), "should not receive notifications after UNLISTEN *");
}

#[tokio::test]
async fn disconnect_cleans_up() {
    let (addr, _tm) = start_test_server().await;
    let (client1, _rx1) = connect(addr).await;

    let rid = Ulid::new();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    // Drop client — should not panic or leak
    drop(client1);
    drop(_rx1);

    // Wait a bit for the server to clean up
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Another connection should still work fine
    let (client2, _) = connect(addr).await;
    let rule_id = Ulid::new();
    client2
        .batch_execute(&format!(
            r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', 1000, 2000, false)"#
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn multiple_events_on_same_channel() {
    let (addr, _tm) = start_test_server().await;
    let (client1, mut rx1) = connect(addr).await;

    let rid = Ulid::new();
    client1
        .batch_execute(&format!(
            "INSERT INTO resources (id) VALUES ('{rid}')"
        ))
        .await
        .unwrap();

    client1
        .batch_execute(&format!("LISTEN resource_{rid}"))
        .await
        .unwrap();

    let (client2, _) = connect(addr).await;

    // Send 3 mutations
    for i in 0..3 {
        let rule_id = Ulid::new();
        client2
            .batch_execute(&format!(
                r#"INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ('{rule_id}', '{rid}', {start}, {end}, false)"#,
                start = i * 1000,
                end = (i + 1) * 1000,
            ))
            .await
            .unwrap();
    }

    // Should receive all 3 notifications
    let mut count = 0;
    for _ in 0..3 {
        if recv_notification(&mut rx1, Duration::from_secs(5))
            .await
            .is_some()
        {
            count += 1;
        }
    }
    assert_eq!(count, 3, "should receive all 3 notifications");
}
