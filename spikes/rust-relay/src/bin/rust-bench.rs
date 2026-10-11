use std::{
    collections::HashMap,
    fs, io,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use rand::{RngCore, rng};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    sync::{Mutex, Notify, Semaphore, mpsc},
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Parser)]
#[command(about = "Rust relay spike benchmark with the Go relay benchmark contract")]
struct Args {
    #[arg(long)]
    url: Option<String>,
    #[arg(long)]
    spawn: bool,
    #[arg(long, default_value = "target/release/rust-relay")]
    relay_bin: String,
    #[arg(long, default_value = "64,1024,65536")]
    payloads: String,
    #[arg(long, default_value = "1,32")]
    clients: String,
    #[arg(long, default_value_t = 4)]
    inflight: usize,
    #[arg(long, default_value_t = 2.0)]
    warmup: f64,
    #[arg(long, default_value_t = 5.0)]
    duration: f64,
    #[arg(long, default_value_t = 4)]
    hosts: usize,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct Distribution {
    p50: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Usage {
    #[serde(rename = "cpuPercentOfOneCore")]
    cpu_percent_of_one_core: f64,
    #[serde(rename = "rssPeakMiB")]
    rss_peak_mib: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CaseResult {
    #[serde(rename = "payloadBytes")]
    payload_bytes: usize,
    clients: usize,
    #[serde(rename = "messagesInWindow")]
    messages_in_window: i64,
    #[serde(rename = "messagesPerSec")]
    messages_per_sec: f64,
    #[serde(rename = "payloadMiBPerSec")]
    payload_mib_per_sec: f64,
    #[serde(rename = "rttMicros")]
    rtt_micros: Distribution,
    failures: i64,
    corrupt: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    processes: Option<HashMap<String, Usage>>,
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    url: String,
    spawned: bool,
    #[serde(rename = "warmupSec")]
    warmup_sec: f64,
    #[serde(rename = "durationSec")]
    duration_sec: f64,
    #[serde(rename = "inflightPerClient")]
    inflight_per_client: usize,
    hosts: usize,
    driver: DriverInfo,
    results: Vec<CaseResult>,
}

#[derive(Debug, Serialize)]
struct DriverInfo {
    os: &'static str,
    arch: &'static str,
    cpus: usize,
    rust: String,
}

#[derive(Debug, Deserialize)]
struct Challenge {
    #[serde(rename = "type")]
    kind: String,
    nonce: String,
}

#[derive(Debug, Deserialize)]
struct Registered {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "endpointId")]
    endpoint_id: String,
}

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "connectionId")]
    connection_id: Option<String>,
    token: Option<String>,
}

struct Host {
    base: String,
    id: String,
    events: Mutex<mpsc::Receiver<Event>>,
}

struct ClientStats {
    sent: AtomicI64,
    got: AtomicI64,
    messages: AtomicI64,
    bytes: AtomicI64,
    failures: AtomicI64,
    corrupt: AtomicI64,
    rtts: Mutex<Vec<i64>>,
}

impl ClientStats {
    fn new() -> Self {
        Self {
            sent: AtomicI64::new(0),
            got: AtomicI64::new(0),
            messages: AtomicI64::new(0),
            bytes: AtomicI64::new(0),
            failures: AtomicI64::new(0),
            corrupt: AtomicI64::new(0),
            rtts: Mutex::new(Vec::new()),
        }
    }
}

struct SpawnedRelay {
    child: Child,
    url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let output = benchmark(&args).await?;
    let json = serde_json::to_string_pretty(&output)?;
    if let Some(path) = args.output {
        fs::write(path, format!("{json}\n"))?;
    }
    println!("{json}");
    Ok(())
}

async fn benchmark(args: &Args) -> Result<BenchmarkOutput> {
    if args.hosts == 0 || args.inflight == 0 || args.duration <= 0.0 || args.warmup < 0.0 {
        bail!(
            "hosts and inflight must be positive; duration must be positive; warmup must not be negative"
        );
    }
    let payloads = parse_list(&args.payloads)?;
    let clients = parse_list(&args.clients)?;
    let spawned = if args.spawn {
        Some(spawn_relay(&args.relay_bin).await?)
    } else {
        None
    };
    let base = spawned
        .as_ref()
        .map(|relay| relay.url.clone())
        .or_else(|| args.url.clone())
        .ok_or_else(|| anyhow!("--url or --spawn is required"))?;
    let hosts = serve_echo(&base, args.hosts).await?;
    let relay_pid = spawned.as_ref().and_then(|relay| relay.child.id());
    let mut results = Vec::new();
    for payload in payloads {
        for client_count in &clients {
            let result = run_case(&base, &hosts, payload, *client_count, args, relay_pid)
                .await
                .with_context(|| format!("{payload}B x{client_count}"))?;
            eprintln!(
                "{payload:>6}B x{client_count:<3} p50={:.0}us p99={:.0}us {:>8.0} msg/s {:>7.1} MiB/s fail={} corrupt={}",
                result.rtt_micros.p50,
                result.rtt_micros.p99,
                result.messages_per_sec,
                result.payload_mib_per_sec,
                result.failures,
                result.corrupt
            );
            if result.failures > 0 || result.corrupt > 0 {
                bail!("echo validation failed");
            }
            results.push(result);
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
    for host in hosts {
        let _ = host;
    }
    if let Some(mut relay) = spawned {
        let _ = relay.child.kill().await;
        let _ = relay.child.wait().await;
    }
    Ok(BenchmarkOutput {
        url: base,
        spawned: args.spawn,
        warmup_sec: args.warmup,
        duration_sec: args.duration,
        inflight_per_client: args.inflight,
        hosts: args.hosts,
        driver: DriverInfo {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
            rust: rust_version(),
        },
        results,
    })
}

async fn spawn_relay(binary: &str) -> Result<SpawnedRelay> {
    let mut command = Command::new(binary);
    command
        .env("RELAY_ADDR", "127.0.0.1:0")
        .env("RELAY_PRIVATE_ADDR", "")
        .env("RELAY_ADMISSION_RATE", "1000000")
        .env("RELAY_MAX_CONNS", "100000")
        .env("RELAY_MAX_CONNS_PER_IP", "100000")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = command.spawn().with_context(|| format!("start {binary}"))?;
    let stdout = child.stdout.take().context("relay stdout unavailable")?;
    let mut lines = BufReader::new(stdout).lines();
    while let Some(line) = lines.next_line().await? {
        let event: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
        if event.get("event").and_then(|value| value.as_str()) == Some("listening") {
            let address = event
                .get("address")
                .and_then(|value| value.as_str())
                .context("relay listening event has no address")?;
            tokio::spawn(async move { while lines.next_line().await.ok().flatten().is_some() {} });
            return Ok(SpawnedRelay {
                child,
                url: format!("ws://{address}"),
            });
        }
    }
    bail!("{binary} did not report listening")
}

async fn serve_echo(base: &str, count: usize) -> Result<Vec<Arc<Host>>> {
    let mut hosts = Vec::with_capacity(count);
    for _ in 0..count {
        let host = Arc::new(register_host(base).await?);
        let task_host = host.clone();
        tokio::spawn(async move { echo_host(task_host).await });
        hosts.push(host);
    }
    Ok(hosts)
}

async fn register_host(base: &str) -> Result<Host> {
    let mut seed = [0_u8; 32];
    rng().fill_bytes(&mut seed);
    let key = SigningKey::from_bytes(&seed);
    let public_key = URL_SAFE_NO_PAD.encode(key.verifying_key().as_bytes());
    let url = query_url(base, "/v1/control", &[("publicKey", public_key)]);
    let (mut socket, _) = connect_async(url).await.context("connect control")?;
    let challenge = match socket.next().await.context("challenge missing")?? {
        Message::Text(text) => serde_json::from_str::<Challenge>(&text)?,
        _ => bail!("challenge was not text"),
    };
    if challenge.kind != "challenge" {
        bail!("unexpected challenge type {}", challenge.kind);
    }
    let endpoint = endpoint_id(key.verifying_key().as_bytes());
    let signed = format!("supacode-relay-v1\n{endpoint}\n{}", challenge.nonce);
    let auth = serde_json::json!({
        "type": "authenticate",
        "signature": URL_SAFE_NO_PAD.encode(key.sign(signed.as_bytes()).to_bytes())
    });
    socket.send(Message::Text(auth.to_string().into())).await?;
    let registered = match socket.next().await.context("registration missing")?? {
        Message::Text(text) => serde_json::from_str::<Registered>(&text)?,
        _ => bail!("registration was not text"),
    };
    if registered.kind != "registered" || registered.endpoint_id != endpoint {
        bail!("unexpected registration reply");
    }
    let (_, mut stream) = socket.split();
    let (events_tx, events_rx) = mpsc::channel(1024);
    tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = stream.next().await {
            if let Ok(event) = serde_json::from_str::<Event>(&text) {
                if events_tx.send(event).await.is_err() {
                    break;
                }
            }
        }
    });
    Ok(Host {
        base: base.to_owned(),
        id: endpoint,
        events: Mutex::new(events_rx),
    })
}

async fn echo_host(host: Arc<Host>) {
    loop {
        let event = {
            let mut events = host.events.lock().await;
            events.recv().await
        };
        let Some(event) = event else { return };
        if event.kind != "incoming" {
            continue;
        }
        let (Some(connection_id), Some(token)) = (event.connection_id, event.token) else {
            continue;
        };
        let base = host.base.clone();
        let endpoint = host.id.clone();
        tokio::spawn(async move {
            let url = query_url(
                &base,
                "/v1/accept",
                &[
                    ("endpointId", endpoint),
                    ("connectionId", connection_id),
                    ("token", token),
                ],
            );
            let Ok((mut socket, _)) = connect_async(url).await else {
                return;
            };
            while let Some(Ok(message)) = socket.next().await {
                if socket.send(message).await.is_err() {
                    break;
                }
            }
        });
    }
}

async fn run_case(
    base: &str,
    hosts: &[Arc<Host>],
    payload_size: usize,
    client_count: usize,
    args: &Args,
    relay_pid: Option<u32>,
) -> Result<CaseResult> {
    let mut connections = Vec::with_capacity(client_count);
    for index in 0..client_count {
        let host = &hosts[index % hosts.len()];
        let url = query_url(base, "/v1/connect", &[("endpointId", host.id.clone())]);
        connections.push(connect_async(url).await?.0);
    }
    let block = random_payload(payload_size);
    let start = Instant::now();
    let warmup = Duration::from_secs_f64(args.warmup);
    let duration = Duration::from_secs_f64(args.duration);
    let measurement_start = warmup.as_nanos() as u64;
    let measurement_end = (warmup + duration).as_nanos() as u64;
    let stop_at = start + warmup + duration;
    let sampler = relay_pid.map(|pid| tokio::spawn(sample_process(pid, warmup, duration)));
    let mut workers = Vec::with_capacity(client_count);
    for socket in connections {
        workers.push(tokio::spawn(client_worker(
            socket,
            block.clone(),
            args.inflight,
            start,
            measurement_start,
            measurement_end,
            stop_at,
        )));
    }
    let mut stats = Vec::with_capacity(client_count);
    for worker in workers {
        stats.push(worker.await??);
    }
    let usage = match sampler {
        Some(handle) => Some(handle.await??),
        None => None,
    };
    let mut messages = 0_i64;
    let mut bytes = 0_i64;
    let mut failures = 0_i64;
    let mut corrupt = 0_i64;
    let mut rtts = Vec::new();
    for client in stats {
        messages += client.messages.load(Ordering::Relaxed);
        bytes += client.bytes.load(Ordering::Relaxed);
        failures += client.failures.load(Ordering::Relaxed) + client.sent.load(Ordering::Relaxed)
            - client.got.load(Ordering::Relaxed);
        corrupt += client.corrupt.load(Ordering::Relaxed);
        rtts.extend(client.rtts.lock().await.iter().copied());
    }
    let seconds = args.duration;
    let mut processes = HashMap::new();
    if let Some(usage) = usage {
        processes.insert("relay".to_owned(), usage);
    }
    Ok(CaseResult {
        payload_bytes: payload_size,
        clients: client_count,
        messages_in_window: messages,
        messages_per_sec: round(messages as f64 / seconds),
        payload_mib_per_sec: round(bytes as f64 / seconds / (1 << 20) as f64),
        rtt_micros: summarize(&mut rtts),
        failures,
        corrupt,
        processes: (!processes.is_empty()).then_some(processes),
    })
}

async fn client_worker(
    socket: Socket,
    block: Vec<u8>,
    inflight: usize,
    start: Instant,
    measurement_start: u64,
    measurement_end: u64,
    stop_at: Instant,
) -> Result<Arc<ClientStats>> {
    let (mut sink, mut stream) = socket.split();
    let stats = Arc::new(ClientStats::new());
    let reader_stats = stats.clone();
    let permits = Arc::new(Semaphore::new(inflight));
    let reader_permits = permits.clone();
    let done = Arc::new(Notify::new());
    let reader_done = done.clone();
    let expected_block = block.clone();
    let reader = tokio::spawn(async move {
        let mut expected = 0_u64;
        while let Some(result) = stream.next().await {
            let Ok(message) = result else { break };
            let Message::Binary(data) = message else {
                continue;
            };
            let now = start.elapsed().as_nanos() as u64;
            let valid = data.len() == expected_block.len()
                && data.len() >= 16
                && u64::from_be_bytes(data[0..8].try_into().unwrap()) == expected
                && data[16..] == expected_block[16..];
            if !valid {
                reader_stats.corrupt.fetch_add(1, Ordering::Relaxed);
            }
            expected = expected.saturating_add(1);
            if data.len() >= 16 {
                let sent_at = u64::from_be_bytes(data[8..16].try_into().unwrap());
                if sent_at >= measurement_start && sent_at < measurement_end {
                    reader_stats
                        .rtts
                        .lock()
                        .await
                        .push(now.saturating_sub(sent_at) as i64);
                }
            }
            if now >= measurement_start && now < measurement_end {
                reader_stats.messages.fetch_add(1, Ordering::Relaxed);
                reader_stats
                    .bytes
                    .fetch_add(data.len() as i64, Ordering::Relaxed);
            }
            reader_stats.got.fetch_add(1, Ordering::Relaxed);
            reader_permits.add_permits(1);
        }
        reader_done.notify_waiters();
    });
    let mut sequence = 0_u64;
    while Instant::now() < stop_at {
        let permit = tokio::select! {
            permit = permits.acquire() => permit.context("semaphore closed")?,
            _ = done.notified() => {
                stats.failures.fetch_add(1, Ordering::Relaxed);
                break;
            }
        };
        permit.forget();
        let mut message = block.clone();
        if message.len() < 16 {
            message.resize(16, 0);
        }
        message[0..8].copy_from_slice(&sequence.to_be_bytes());
        message[8..16].copy_from_slice(&(start.elapsed().as_nanos() as u64).to_be_bytes());
        if sink.send(Message::Binary(message.into())).await.is_err() {
            stats.failures.fetch_add(1, Ordering::Relaxed);
            break;
        }
        stats.sent.fetch_add(1, Ordering::Relaxed);
        sequence = sequence.saturating_add(1);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while stats.got.load(Ordering::Relaxed) < stats.sent.load(Ordering::Relaxed)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let _ = sink.close().await;
    let _ = timeout(Duration::from_secs(5), reader).await;
    Ok(stats)
}

async fn sample_process(pid: u32, warmup: Duration, duration: Duration) -> Result<Usage> {
    tokio::time::sleep(warmup).await;
    let start = Instant::now();
    let (_, cpu0) = ps_stats(pid).unwrap_or((0, 0.0));
    let mut peak = 0_i64;
    while start.elapsed() < duration {
        if let Ok((rss, _)) = ps_stats(pid) {
            peak = peak.max(rss);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let (rss, cpu) = ps_stats(pid).unwrap_or((0, cpu0));
    peak = peak.max(rss);
    Ok(Usage {
        cpu_percent_of_one_core: round(100.0 * (cpu - cpu0) / start.elapsed().as_secs_f64()),
        rss_peak_mib: round(peak as f64 / 1024.0),
    })
}

fn ps_stats(pid: u32) -> io::Result<(i64, f64)> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("ps failed"));
    }
    let fields: Vec<_> = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    if fields.len() != 2 {
        return Err(io::Error::other("unexpected ps output"));
    }
    let rss = fields[0].parse::<i64>().map_err(io::Error::other)?;
    let cpu = fields[1]
        .replace('-', ":")
        .split(':')
        .try_fold(0.0, |total, value| {
            value.parse::<f64>().map(|value| total * 60.0 + value)
        })
        .map_err(io::Error::other)?;
    Ok((rss, cpu))
}

fn query_url(base: &str, path: &str, pairs: &[(impl AsRef<str>, impl AsRef<str>)]) -> String {
    let mut url = Url::parse(base).expect("benchmark base URL is valid");
    url.set_path(path);
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        for (key, value) in pairs {
            query.append_pair(key.as_ref(), value.as_ref());
        }
    }
    url.to_string()
}

fn endpoint_id(public_key: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(public_key);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn random_payload(size: usize) -> Vec<u8> {
    let mut payload = vec![0_u8; size.max(16)];
    rng().fill_bytes(&mut payload);
    payload
}

fn parse_list(value: &str) -> Result<Vec<usize>> {
    value
        .split(',')
        .map(|part| {
            let value = part
                .trim()
                .parse::<usize>()
                .context("invalid integer list")?;
            if value == 0 {
                bail!("integer list values must be positive");
            }
            Ok(value)
        })
        .collect()
}

fn summarize(values: &mut [i64]) -> Distribution {
    if values.is_empty() {
        return Distribution {
            p50: 0.0,
            p99: 0.0,
            max: 0.0,
        };
    }
    values.sort_unstable();
    let quantile = |p: f64| {
        let index = ((p * values.len() as f64).ceil() as usize).saturating_sub(1);
        round(values[index] as f64 / 1000.0)
    };
    Distribution {
        p50: quantile(0.5),
        p99: quantile(0.99),
        max: quantile(1.0),
    }
}

fn round(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

fn rust_version() -> String {
    std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}
