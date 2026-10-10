use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use rand::RngCore;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    time::{interval, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async_with_config,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use url::Url;

type Socket = WebSocketStream<TcpStream>;
type Hosts = Arc<Mutex<HashMap<String, Arc<Host>>>>;

const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const HEARTBEAT: Duration = Duration::from_secs(15);
const MAX_MESSAGE: usize = (32 << 20) - 14;

struct Host {
    control: mpsc::Sender<Message>,
    closed: watch::Sender<bool>,
    pending: Mutex<HashMap<String, Pending>>,
}

struct Pending {
    token: String,
    accept: oneshot::Sender<Socket>,
}

enum Route {
    Control([u8; 32]),
    Connect(Arc<Host>),
    Accept(Arc<Host>, String, String),
}

fn random_token(size: usize) -> String {
    let mut bytes = vec![0; size];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn text(value: Value) -> Message {
    Message::text(value.to_string())
}

fn close_frame(code: u16, reason: &'static str) -> CloseFrame {
    CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }
}

async fn close(socket: &mut Socket, code: u16, reason: &'static str) {
    let _ = timeout(
        AUTH_TIMEOUT,
        socket.send(Message::Close(Some(close_frame(code, reason)))),
    )
    .await;
}

fn route(request: &Request, hosts: &Hosts) -> Result<Route, StatusCode> {
    if request.method() != "GET" {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let url = Url::parse(&format!("http://relay{}", request.uri()))
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
    let get = |key: &str| params.get(key).map(String::as_str).unwrap_or("");
    match url.path() {
        "/v1/control" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(get("publicKey"))
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            if URL_SAFE_NO_PAD.encode(&bytes) != get("publicKey") {
                return Err(StatusCode::BAD_REQUEST);
            }
            Ok(Route::Control(
                bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
            ))
        }
        "/v1/connect" | "/v1/accept" => {
            let host = hosts
                .lock()
                .unwrap()
                .get(get("endpointId"))
                .cloned()
                .ok_or(StatusCode::NOT_FOUND)?;
            if *host.closed.borrow() {
                return Err(StatusCode::NOT_FOUND);
            }
            if url.path() == "/v1/connect" {
                return Ok(Route::Connect(host));
            }
            let id = get("connectionId").to_owned();
            let token = get("token").to_owned();
            {
                let pending = host.pending.lock().unwrap();
                let pair = pending.get(&id).ok_or(StatusCode::NOT_FOUND)?;
                if !bool::from(pair.token.as_bytes().ct_eq(token.as_bytes())) {
                    return Err(StatusCode::FORBIDDEN);
                }
            }
            Ok(Route::Accept(host, id, token))
        }
        _ => Err(StatusCode::NOT_FOUND),
    }
}

#[allow(clippy::result_large_err)]
async fn handle(stream: TcpStream, hosts: Hosts) {
    let mut selected = None;
    let config = WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(MAX_MESSAGE + 64 * 1024)
        .max_message_size(Some(MAX_MESSAGE))
        .max_frame_size(Some(MAX_MESSAGE));
    let callback = |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
        match route(request, &hosts) {
            Ok(value) => {
                selected = Some(value);
                Ok(response)
            }
            Err(code) => Err(Response::builder().status(code).body(None).unwrap()),
        }
    };
    let Ok(Ok(mut socket)) = timeout(
        AUTH_TIMEOUT,
        accept_hdr_async_with_config(stream, callback, Some(config)),
    )
    .await
    else {
        return;
    };
    match selected.unwrap() {
        Route::Control(public_key) => control(socket, public_key, hosts).await,
        Route::Connect(host) => connect(socket, host).await,
        Route::Accept(host, id, token) => {
            let pair = {
                let mut pending = host.pending.lock().unwrap();
                if pending
                    .get(&id)
                    .is_some_and(|pair| bool::from(pair.token.as_bytes().ct_eq(token.as_bytes())))
                {
                    pending.remove(&id)
                } else {
                    None
                }
            };
            if let Some(pair) = pair {
                if let Err(mut socket) = pair.accept.send(socket) {
                    close(&mut socket, 1013, "pair expired").await;
                }
            } else {
                close(&mut socket, 1008, "invalid pair").await;
            }
        }
    }
}

async fn control(mut socket: Socket, public_key: [u8; 32], hosts: Hosts) {
    let id: String = Sha256::digest(public_key)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let nonce = random_token(32);
    if socket
        .send(text(json!({"type": "challenge", "nonce": nonce})))
        .await
        .is_err()
    {
        return;
    }
    let authenticated = match timeout(AUTH_TIMEOUT, socket.next()).await {
        Ok(Some(Ok(Message::Text(message)))) if message.len() <= 4096 => {
            let auth = serde_json::from_str::<Value>(&message).ok();
            auth.filter(|value| value["type"] == "authenticate")
                .and_then(|value| {
                    value["signature"]
                        .as_str()
                        .and_then(|signature| URL_SAFE_NO_PAD.decode(signature).ok())
                })
                .and_then(|bytes| Signature::from_slice(&bytes).ok())
                .is_some_and(|signature| {
                    VerifyingKey::from_bytes(&public_key).is_ok_and(|key| {
                        key.verify_strict(
                            format!("supacode-relay-v1\n{id}\n{nonce}").as_bytes(),
                            &signature,
                        )
                        .is_ok()
                    })
                })
        }
        _ => false,
    };
    if !authenticated {
        close(&mut socket, 1008, "authentication failed").await;
        return;
    }
    let (sender, mut notifications) = mpsc::channel(256);
    let (closed, _) = watch::channel(false);
    let host = Arc::new(Host {
        control: sender,
        closed,
        pending: Mutex::new(HashMap::new()),
    });
    let old = hosts.lock().unwrap().insert(id.clone(), host.clone());
    if let Some(old) = old {
        let _ = old.control.try_send(Message::Close(Some(close_frame(
            4001,
            "registration superseded",
        ))));
    }
    if socket
        .send(text(json!({"type": "registered", "endpointId": id})))
        .await
        .is_ok()
    {
        let mut heartbeat = interval(HEARTBEAT);
        heartbeat.tick().await;
        loop {
            let outbound = tokio::select! {
                message = notifications.recv() => message,
                _ = heartbeat.tick() => Some(Message::Ping(Vec::new().into())),
                message = socket.next() => {
                    match message {
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                        Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                            close(&mut socket, 1008, "unexpected control message").await;
                            break;
                        }
                        _ => break,
                    }
                }
            };
            let Some(message) = outbound else { break };
            let closing = message.is_close();
            if !matches!(
                timeout(AUTH_TIMEOUT, socket.send(message)).await,
                Ok(Ok(()))
            ) || closing
            {
                break;
            }
        }
    }
    host.closed.send_replace(true);
    host.pending.lock().unwrap().clear();
    let mut hosts = hosts.lock().unwrap();
    if hosts
        .get(&id)
        .is_some_and(|current| Arc::ptr_eq(current, &host))
    {
        hosts.remove(&id);
    }
}

async fn connect(mut client: Socket, host: Arc<Host>) {
    let id = random_token(16);
    let token = random_token(32);
    let (accept, receiver) = oneshot::channel();
    let mut closed = host.closed.subscribe();
    host.pending.lock().unwrap().insert(
        id.clone(),
        Pending {
            token: token.clone(),
            accept,
        },
    );
    if host
        .control
        .try_send(text(
            json!({"type": "incoming", "connectionId": id, "token": token}),
        ))
        .is_err()
    {
        host.pending.lock().unwrap().remove(&id);
        close(&mut client, 1013, "control queue full").await;
        return;
    }
    let accepted = if *closed.borrow() {
        None
    } else {
        tokio::select! {
            result = timeout(AUTH_TIMEOUT, receiver) => result.ok().and_then(Result::ok),
            _ = closed.changed() => None,
        }
    };
    host.pending.lock().unwrap().remove(&id);
    if let Some(host_socket) = accepted {
        bridge(client, host_socket, closed).await;
    } else {
        close(&mut client, 1013, "pair timeout or host offline").await;
    }
    let _ = host
        .control
        .try_send(text(json!({"type": "closed", "connectionId": id})));
}

async fn forward(
    reader: &mut SplitStream<Socket>,
    writer: &mut SplitSink<Socket, Message>,
) -> CloseFrame {
    let mut heartbeat = interval(HEARTBEAT);
    heartbeat.tick().await;
    loop {
        let message = tokio::select! {
            _ = heartbeat.tick() => Message::Ping(Vec::new().into()),
            message = reader.next() => match message {
                Some(Ok(Message::Text(data))) => Message::Text(data),
                Some(Ok(Message::Binary(data))) => Message::Binary(data),
                Some(Ok(Message::Close(frame))) => return frame.unwrap_or_else(|| close_frame(1001, "peer disconnected")),
                Some(Ok(_)) => continue,
                _ => return close_frame(1001, "peer disconnected"),
            }
        };
        if !matches!(
            timeout(DELIVERY_TIMEOUT, writer.send(message)).await,
            Ok(Ok(()))
        ) {
            return close_frame(1013, "receiver stalled");
        }
    }
}

async fn bridge(client: Socket, host: Socket, mut closed: watch::Receiver<bool>) {
    let (mut client_writer, mut client_reader) = client.split();
    let (mut host_writer, mut host_reader) = host.split();
    let frame = if *closed.borrow() {
        close_frame(1001, "host offline")
    } else {
        tokio::select! {
            frame = forward(&mut client_reader, &mut host_writer) => frame,
            frame = forward(&mut host_reader, &mut client_writer) => frame,
            _ = closed.changed() => close_frame(1001, "host offline"),
        }
    };
    let _ = timeout(AUTH_TIMEOUT, async {
        let _ = tokio::join!(
            client_writer.send(Message::Close(Some(frame.clone()))),
            host_writer.send(Message::Close(Some(frame)))
        );
    })
    .await;
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("RELAY_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8080".into())
        .parse()?;
    if !addr.ip().is_loopback() {
        return Err("the experimental relay requires a loopback address".into());
    }
    let workers: usize = env::var("RELAY_WORKERS")
        .unwrap_or_else(|_| "4".into())
        .parse()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    runtime.block_on(serve(addr))?;
    Ok(())
}

async fn serve(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!(
        "{}",
        json!({"event": "listening", "address": listener.local_addr()?.to_string()})
    );
    let hosts: Hosts = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        tokio::spawn(handle(stream, hosts.clone()));
    }
}
