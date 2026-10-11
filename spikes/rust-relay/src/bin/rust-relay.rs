use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{Query, State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use futures_util::SinkExt;
use supacode_relay_rust_spike::{
    AcceptQuery, AppState, ConnectQuery, ControlQuery, authenticate_control, close_pair,
    endpoint_id, notify_incoming, parse_public_key, register_pair, run_pair, wait_for_pair,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("RELAY_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let pair_timeout = env_duration("RELAY_PAIR_TIMEOUT_MS", Duration::from_secs(5));
    let max_message_bytes = env_usize(
        "RELAY_MAX_MESSAGE_BYTES",
        supacode_relay_rust_spike::DEFAULT_MAX_MESSAGE_BYTES,
    );
    let state = AppState::new(pair_timeout, max_message_bytes);
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/v1/control", get(control))
        .route("/v1/connect", get(connect))
        .route("/v1/accept", get(accept))
        .with_state(state);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    println!(
        "{}",
        serde_json::json!({"event":"listening", "address": listener.local_addr()?.to_string()})
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve relay")?;
    Ok(())
}

fn env_duration(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"status":"ok"})),
    )
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let hosts = state.inner.hosts.read().await.len();
    let pairs = state.inner.pairs.read().await.len();
    axum::Json(serde_json::json!({
        "activeHosts": hosts,
        "activePairs": pairs,
        "dataBufferBytesPerSocket": 0,
        "implementation": "rust-spike"
    }))
}

async fn control(
    State(state): State<AppState>,
    Query(query): Query<ControlQuery>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let public_key = match parse_public_key(&query.public_key) {
        Ok(key) => key,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let endpoint = endpoint_id(public_key.as_bytes());
    upgrade
        .max_message_size(state.inner.max_message_bytes)
        .on_upgrade(move |socket| async move {
            let _ = authenticate_control(socket, public_key, endpoint, state).await;
        })
        .into_response()
}

async fn connect(
    State(state): State<AppState>,
    Query(query): Query<ConnectQuery>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let host = state
        .inner
        .hosts
        .read()
        .await
        .get(&query.endpoint_id)
        .cloned();
    let Some(host) = host else {
        return (StatusCode::NOT_FOUND, "endpoint not found").into_response();
    };
    let pair = register_pair(&state, host.clone()).await;
    let state_for_upgrade = state.clone();
    let pair_for_upgrade = pair.clone();
    upgrade
        .max_message_size(state.inner.max_message_bytes)
        .on_upgrade(move |socket| async move {
            let mut socket = Some(socket);
            if *pair_for_upgrade.state.lock().await == supacode_relay_rust_spike::PairState::Closed
            {
                let _ = socket.take().expect("socket present").close().await;
                return;
            }
            *pair_for_upgrade.client.lock().await = socket.take();
            if !notify_incoming(&pair_for_upgrade.host, &pair_for_upgrade).await {
                close_pair(
                    &state_for_upgrade,
                    &pair_for_upgrade,
                    1013,
                    "control queue full",
                )
                .await;
                return;
            }
            wait_for_pair(&pair_for_upgrade, &state_for_upgrade).await;
        })
        .into_response()
}

async fn accept(
    State(state): State<AppState>,
    Query(query): Query<AcceptQuery>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let pair = match state
        .inner
        .pairs
        .read()
        .await
        .get(&query.connection_id)
        .cloned()
    {
        Some(pair) => pair,
        None => return (StatusCode::NOT_FOUND, "connection not found").into_response(),
    };
    if pair.host.endpoint_id != query.endpoint_id
        || pair.token != query.token
        || *pair.state.lock().await != supacode_relay_rust_spike::PairState::Pending
    {
        return (StatusCode::FORBIDDEN, "invalid token").into_response();
    }
    let state_for_upgrade = state.clone();
    let query_for_upgrade = query;
    upgrade
        .max_message_size(state.inner.max_message_bytes)
        .on_upgrade(move |host_socket| async move {
            {
                let mut pair_state = pair.state.lock().await;
                if *pair_state != supacode_relay_rust_spike::PairState::Pending {
                    let mut socket = host_socket;
                    let _ = socket
                        .send(axum::extract::ws::Message::Close(Some(
                            axum::extract::ws::CloseFrame {
                                code: 1013u16.into(),
                                reason: "pair is no longer pending".into(),
                            },
                        )))
                        .await;
                    let _ = socket.close().await;
                    return;
                }
                *pair_state = supacode_relay_rust_spike::PairState::Active;
            }
            let Some(client_socket) = pair.client.lock().await.take() else {
                close_pair(&state_for_upgrade, &pair, 1013, "client disappeared").await;
                return;
            };
            if pair.host.endpoint_id != query_for_upgrade.endpoint_id
                || pair.token != query_for_upgrade.token
            {
                close_pair(&state_for_upgrade, &pair, 1008, "invalid token").await;
                return;
            }
            run_pair(state_for_upgrade, pair, client_socket, host_socket).await;
        })
        .into_response()
}
