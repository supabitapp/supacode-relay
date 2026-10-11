use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures_util::{SinkExt, StreamExt};
use rand::{RngCore, rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify, RwLock, mpsc};
use tokio::time::timeout;
use uuid::Uuid;

pub const DEFAULT_MAX_MESSAGE_BYTES: usize = (32 << 20) - 14;
pub const DEFAULT_PAIR_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    pub inner: Arc<InnerState>,
}

pub struct InnerState {
    pub hosts: RwLock<HashMap<String, Arc<HostRuntime>>>,
    pub pairs: RwLock<HashMap<String, Arc<Pair>>>,
    pub pair_timeout: Duration,
    pub max_message_bytes: usize,
}

impl AppState {
    pub fn new(pair_timeout: Duration, max_message_bytes: usize) -> Self {
        Self {
            inner: Arc::new(InnerState {
                hosts: RwLock::new(HashMap::new()),
                pairs: RwLock::new(HashMap::new()),
                pair_timeout,
                max_message_bytes,
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ControlQuery {
    #[serde(rename = "publicKey")]
    pub public_key: String,
}

#[derive(Debug, Deserialize)]
pub struct ConnectQuery {
    #[serde(rename = "endpointId")]
    pub endpoint_id: String,
}

#[derive(Debug, Deserialize)]
pub struct AcceptQuery {
    #[serde(rename = "endpointId")]
    pub endpoint_id: String,
    #[serde(rename = "connectionId")]
    pub connection_id: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
struct AuthMessage {
    #[serde(rename = "type")]
    kind: String,
    signature: String,
}

#[derive(Debug, Serialize)]
struct ChallengeMessage<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    nonce: String,
}

#[derive(Debug, Serialize)]
struct RegisteredMessage<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(rename = "endpointId")]
    endpoint_id: &'a str,
}

#[derive(Debug, Serialize, Clone)]
pub struct IncomingMessage<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    #[serde(rename = "connectionId")]
    pub connection_id: &'a str,
    pub token: &'a str,
}

#[derive(Debug, Serialize, Clone)]
pub struct ClosedMessage<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    #[serde(rename = "connectionId")]
    pub connection_id: &'a str,
}

#[derive(Debug)]
pub enum ControlCommand {
    Json(String),
    Close(u16, String),
}

pub struct HostRuntime {
    pub endpoint_id: String,
    pub notify: mpsc::Sender<ControlCommand>,
    pub pairs: Mutex<HashMap<String, Arc<Pair>>>,
    pub closed: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PairState {
    Pending,
    Active,
    Closed,
}

pub struct Pair {
    pub id: String,
    pub token: String,
    pub host: Arc<HostRuntime>,
    pub state: Mutex<PairState>,
    pub client: Mutex<Option<WebSocket>>,
    pub ready: Notify,
    pub done: Notify,
}

pub fn endpoint_id(public_key: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(public_key);
    hex_string(&hasher.finalize())
}

pub fn b64(data: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(data)
}

fn hex_string(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

pub fn random_token(bytes: usize) -> String {
    let mut value = vec![0; bytes];
    rng().fill_bytes(&mut value);
    b64(&value)
}

pub fn parse_public_key(value: &str) -> Result<VerifyingKey> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .context("publicKey is not unpadded Base64URL")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("publicKey must decode to 32 bytes"))?;
    VerifyingKey::from_bytes(&bytes).context("invalid Ed25519 public key")
}

fn close_frame(code: u16, reason: String) -> Message {
    Message::Close(Some(CloseFrame {
        code: code.into(),
        reason: reason.into(),
    }))
}

pub async fn send_control(host: &HostRuntime, command: ControlCommand) -> bool {
    host.notify.try_send(command).is_ok()
}

pub async fn notify_incoming(host: &HostRuntime, pair: &Pair) -> bool {
    let event = IncomingMessage {
        kind: "incoming",
        connection_id: &pair.id,
        token: &pair.token,
    };
    let Ok(json) = serde_json::to_string(&event) else {
        return false;
    };
    send_control(host, ControlCommand::Json(json)).await
}

pub async fn notify_closed(host: &HostRuntime, pair_id: &str) {
    let event = ClosedMessage {
        kind: "closed",
        connection_id: pair_id,
    };
    if let Ok(json) = serde_json::to_string(&event) {
        let _ = send_control(host, ControlCommand::Json(json)).await;
    }
}

pub async fn close_pair(state: &AppState, pair: &Arc<Pair>, code: u16, reason: &str) {
    let mut state_guard = pair.state.lock().await;
    if *state_guard == PairState::Closed {
        return;
    }
    *state_guard = PairState::Closed;
    let client = pair.client.lock().await.take();
    drop(state_guard);
    state.inner.pairs.write().await.remove(&pair.id);
    pair.host.pairs.lock().await.remove(&pair.id);
    notify_closed(&pair.host, &pair.id).await;
    if let Some(mut socket) = client {
        let _ = socket.send(close_frame(code, reason.to_owned())).await;
        let _ = socket.close().await;
    }
    pair.ready.notify_waiters();
    pair.done.notify_waiters();
}

pub async fn register_pair(state: &AppState, host: Arc<HostRuntime>) -> Arc<Pair> {
    let pair = Arc::new(Pair {
        id: Uuid::new_v4().to_string().replace('-', ""),
        token: random_token(32),
        host: host.clone(),
        state: Mutex::new(PairState::Pending),
        client: Mutex::new(None),
        ready: Notify::new(),
        done: Notify::new(),
    });
    state
        .inner
        .pairs
        .write()
        .await
        .insert(pair.id.clone(), pair.clone());
    host.pairs
        .lock()
        .await
        .insert(pair.id.clone(), pair.clone());
    let timer_state = state.clone();
    let timer_pair = pair.clone();
    tokio::spawn(async move {
        tokio::time::sleep(timer_state.inner.pair_timeout).await;
        let pending = *timer_pair.state.lock().await == PairState::Pending;
        if pending {
            close_pair(&timer_state, &timer_pair, 1013, "pair timeout").await;
        }
    });
    pair
}

pub async fn run_pair(state: AppState, pair: Arc<Pair>, client: WebSocket, host: WebSocket) {
    {
        let mut state_guard = pair.state.lock().await;
        *state_guard = PairState::Active;
    }
    pair.ready.notify_waiters();
    let (mut client_sink, mut client_stream) = client.split();
    let (mut host_sink, mut host_stream) = host.split();
    let max_message_bytes = state.inner.max_message_bytes;
    let mut client_to_host = tokio::spawn(async move {
        while let Some(result) = client_stream.next().await {
            let message = result.map_err(|_| ())?;
            if message_size(&message) > max_message_bytes {
                return Err(());
            }
            host_sink.send(message).await.map_err(|_| ())?;
        }
        Ok::<(), ()>(())
    });
    let mut host_to_client = tokio::spawn(async move {
        while let Some(result) = host_stream.next().await {
            let message = result.map_err(|_| ())?;
            if message_size(&message) > max_message_bytes {
                return Err(());
            }
            client_sink.send(message).await.map_err(|_| ())?;
        }
        Ok::<(), ()>(())
    });
    tokio::select! {
        _ = &mut client_to_host => host_to_client.abort(),
        _ = &mut host_to_client => client_to_host.abort(),
    }
    let mut state_guard = pair.state.lock().await;
    if *state_guard != PairState::Closed {
        *state_guard = PairState::Closed;
        drop(state_guard);
        state.inner.pairs.write().await.remove(&pair.id);
        pair.host.pairs.lock().await.remove(&pair.id);
        notify_closed(&pair.host, &pair.id).await;
        pair.done.notify_waiters();
    }
}

fn message_size(message: &Message) -> usize {
    match message {
        Message::Text(value) => value.len(),
        Message::Binary(value) => value.len(),
        Message::Ping(value) | Message::Pong(value) => value.len(),
        Message::Close(_) => 0,
    }
}

pub async fn authenticate_control(
    mut socket: WebSocket,
    public_key: VerifyingKey,
    endpoint: String,
    state: AppState,
) -> Result<()> {
    let nonce = random_token(32);
    let authentication = async {
        let challenge = serde_json::to_string(&ChallengeMessage {
            kind: "challenge",
            nonce: nonce.clone(),
        })?;
        socket.send(Message::Text(challenge.into())).await?;
        let message = timeout(Duration::from_secs(5), socket.next())
            .await
            .context("authentication timeout")?
            .ok_or_else(|| anyhow!("control socket closed during authentication"))??;
        let text = match message {
            Message::Text(text) => text,
            _ => return Err(anyhow!("authentication message must be text")),
        };
        let auth: AuthMessage = serde_json::from_str(&text)?;
        if auth.kind != "authenticate" {
            return Err(anyhow!("unexpected control message"));
        }
        let signature = URL_SAFE_NO_PAD.decode(auth.signature)?;
        let signature = Signature::from_slice(&signature)?;
        let signed = format!("supacode-relay-v1\n{endpoint}\n{nonce}");
        public_key.verify(signed.as_bytes(), &signature)?;
        let registered = serde_json::to_string(&RegisteredMessage {
            kind: "registered",
            endpoint_id: &endpoint,
        })?;
        socket.send(Message::Text(registered.into())).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(error) = authentication {
        let _ = socket
            .send(close_frame(1008, "authentication failed".to_owned()))
            .await;
        return Err(error);
    }
    let (mut sink, mut stream) = socket.split();
    let (notify, mut commands) = mpsc::channel(1024);
    let host = Arc::new(HostRuntime {
        endpoint_id: endpoint.clone(),
        notify,
        pairs: Mutex::new(HashMap::new()),
        closed: Notify::new(),
    });
    if let Some(old) = state
        .inner
        .hosts
        .write()
        .await
        .insert(endpoint.clone(), host.clone())
    {
        let _ = old.notify.try_send(ControlCommand::Close(
            4001,
            "registration superseded".to_owned(),
        ));
        let pairs = old.pairs.lock().await.clone();
        for pair in pairs.values() {
            close_pair(&state, pair, 1001, "host went offline").await;
        }
        old.closed.notify_waiters();
    }
    let writer = tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            let result = match command {
                ControlCommand::Json(json) => sink.send(Message::Text(json.into())).await,
                ControlCommand::Close(code, reason) => {
                    let result = sink.send(close_frame(code, reason)).await;
                    let _ = sink.close().await;
                    result
                }
            };
            if result.is_err() {
                break;
            }
        }
    });
    while let Some(result) = stream.next().await {
        match result {
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {
                let _ = host.notify.try_send(ControlCommand::Close(
                    1008,
                    "unexpected control message".to_owned(),
                ));
                break;
            }
        }
    }
    writer.abort();
    let pairs = host.pairs.lock().await.clone();
    for pair in pairs.values() {
        close_pair(&state, pair, 1001, "host went offline").await;
    }
    let mut hosts = state.inner.hosts.write().await;
    if hosts
        .get(&endpoint)
        .is_some_and(|current| Arc::ptr_eq(current, &host))
    {
        hosts.remove(&endpoint);
    }
    host.closed.notify_waiters();
    Ok(())
}

pub async fn accept_pair(state: &AppState, query: &AcceptQuery) -> Result<(Arc<Pair>, WebSocket)> {
    let pair = state
        .inner
        .pairs
        .read()
        .await
        .get(&query.connection_id)
        .cloned()
        .ok_or_else(|| anyhow!("connection not found"))?;
    if pair.host.endpoint_id != query.endpoint_id || pair.token != query.token {
        return Err(anyhow!("invalid token"));
    }
    if *pair.state.lock().await != PairState::Pending {
        return Err(anyhow!("invalid token"));
    }
    let client = pair
        .client
        .lock()
        .await
        .take()
        .ok_or_else(|| anyhow!("client not ready"))?;
    Ok((pair, client))
}

pub async fn wait_for_pair(pair: &Arc<Pair>, _state: &AppState) {
    pair.ready.notified().await;
}
