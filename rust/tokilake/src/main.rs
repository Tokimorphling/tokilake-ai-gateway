use anyhow::Result;
use std::sync::Arc;
use tokilake::{
    api::{self, AppState},
    db::init_db,
    gateway::{ChannelIndex, GatewayConfig},
};
use tokilake_core::session::SessionManager;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (port, token) = parse_args();

    let mut db = init_db().await?;

    info!("Tokilake starting...");

    if let Some(ref t) = token {
        info!("Seeding token: {}", t);
        tokilake::model::Token::create()
            .name("test-token")
            .key(t)
            .status(1)
            .exec(&mut db)
            .await?;
    }

    let index = Arc::new(ChannelIndex::new());

    let config = GatewayConfig {
        db:     db.clone(),
        client: reqwest::Client::new(),
        index:  Arc::clone(&index),
    };

    let session_manager = Arc::new(SessionManager::new());

    let state = AppState {
        start_time:      std::time::Instant::now(),
        gateway_config:  config,
        session_manager: Arc::clone(&session_manager),
        index:           Arc::clone(&index),
        token:           token.unwrap_or_default(),
    };

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("Tokilake listening on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        api::router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;

    Ok(())
}

fn parse_args() -> (u16, Option<String>) {
    let mut port = 3000;
    let mut token = None;
    let args: Vec<_> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-addr" | "--addr" => {
                if let Some(addr) = args.get(i + 1) {
                    if let Some(port_str) = addr.strip_prefix(':') {
                        port = port_str.parse().unwrap_or(3000);
                    } else if let Some(pos) = addr.rfind(':') {
                        port = addr[pos + 1..].parse().unwrap_or(3000);
                    }
                }
                i += 2;
            }
            "-port" | "--port" => {
                if let Some(p) = args.get(i + 1) {
                    port = p.parse().unwrap_or(3000);
                }
                i += 2;
            }
            "-token" | "--token" => {
                if let Some(t) = args.get(i + 1) {
                    token = Some(t.clone());
                }
                i += 2;
            }
            _ => i += 1,
        }
    }
    (port, token)
}
