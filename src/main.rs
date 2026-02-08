use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::info;

use deltat::tenant::TenantManager;
use deltat::wire;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let port = std::env::var("DELTAT_PORT").unwrap_or_else(|_| "5433".into());
    let bind = std::env::var("DELTAT_BIND").unwrap_or_else(|_| "0.0.0.0".into());
    let data_dir = std::env::var("DELTAT_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let password = std::env::var("DELTAT_PASSWORD").unwrap_or_else(|_| "deltat".into());
    let max_connections: usize = std::env::var("DELTAT_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let compact_threshold: u64 = std::env::var("DELTAT_COMPACT_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    // Ensure data directory exists
    std::fs::create_dir_all(&data_dir)?;

    let tenant_manager = Arc::new(TenantManager::new(PathBuf::from(&data_dir), compact_threshold));
    let semaphore = Arc::new(Semaphore::new(max_connections));

    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("deltat listening on {addr}");
    info!("  data_dir: {data_dir}");
    info!("  max_connections: {max_connections}");

    // Graceful shutdown: stop accepting on SIGTERM/ctrl-c, drain in-flight connections
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            ctrl_c.await.ok();
        }
    };
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, peer) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("accept error: {e}");
                        continue;
                    }
                };

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!("connection limit reached, rejecting {peer}");
                        drop(socket);
                        continue;
                    }
                };

                info!("connection from {peer}");
                let tm = tenant_manager.clone();
                let pw = password.clone();

                tokio::spawn(async move {
                    let _permit = permit; // held until connection closes
                    if let Err(e) = wire::process_connection(socket, tm, pw).await {
                        tracing::error!("connection error from {peer}: {e}");
                    }
                });
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    // Wait for in-flight connections to finish (up to 10s)
    info!("draining connections...");
    let drain_deadline = tokio::time::sleep(std::time::Duration::from_secs(10));
    tokio::pin!(drain_deadline);

    loop {
        if semaphore.available_permits() == max_connections {
            info!("all connections drained");
            break;
        }
        tokio::select! {
            _ = &mut drain_deadline => {
                let remaining = max_connections - semaphore.available_permits();
                tracing::warn!("drain timeout, {remaining} connections still open");
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
        }
    }

    info!("deltat stopped");
    Ok(())
}
