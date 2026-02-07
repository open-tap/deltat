mod auth;
mod engine;
mod model;
mod notify;
mod reaper;
mod sql;
mod tenant;
mod wal;
mod wire;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;

use crate::tenant::TenantManager;
use crate::wire::DeltaTFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let port = std::env::var("DELTAT_PORT").unwrap_or_else(|_| "5433".into());
    let bind = std::env::var("DELTAT_BIND").unwrap_or_else(|_| "0.0.0.0".into());
    let data_dir = std::env::var("DELTAT_DATA_DIR").unwrap_or_else(|_| "./data".into());
    let password = std::env::var("DELTAT_PASSWORD").unwrap_or_else(|_| "deltat".into());

    // Ensure data directory exists
    std::fs::create_dir_all(&data_dir)?;

    let tenant_manager = Arc::new(TenantManager::new(PathBuf::from(&data_dir)));

    let addr = format!("{bind}:{port}");
    let listener = TcpListener::bind(&addr).await?;
    info!("deltat listening on {addr}");
    info!("  data_dir: {data_dir}");

    loop {
        let (socket, peer) = listener.accept().await?;
        info!("connection from {peer}");
        let factory = Arc::new(DeltaTFactory::new(tenant_manager.clone(), password.clone()));
        tokio::spawn(async move {
            match pgwire::tokio::process_socket(socket, None, factory).await {
                Ok(_) => {}
                Err(e) => tracing::error!("connection error from {peer}: {e}"),
            }
        });
    }
}
