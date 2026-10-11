import { createServer, type IncomingMessage } from "node:http";
import { createHash, randomBytes, verify } from "node:crypto";
import { URL } from "node:url";
import { WebSocketServer, WebSocket, type RawData } from "ws";

type Config = {
  host: string;
  port: number;
  maxMessageBytes: number;
  maxQueueBytes: number;
  maxQueueMessages: number;
  maxClients: number;
  maxClientsPerHost: number;
  maxPendingPerHost: number;
  maxHosts: number;
  authTimeoutMs: number;
  pairTimeoutMs: number;
};

type Frame = { data: Buffer; binary: boolean };
type Host = {
  id: string;
  ws: WebSocket;
  pairs: Set<Pair>;
  bytesIn: number;
  bytesOut: number;
  gone: boolean;
};
type Pair = {
  id: string;
  token: string;
  host: Host;
  client: WebSocket;
  hostSocket?: WebSocket;
  state: "pending" | "active" | "closed";
  queue: Frame[];
  queuedBytes: number;
  timer: NodeJS.Timeout;
  announced: boolean;
  closing: boolean;
};

const counters = { forwardedMessages: 0, forwardedBytes: 0, rejectedConnections: 0 };
const hosts = new Map<string, Host>();
const pairs = new Map<string, Pair>();
const sockets = new Set<WebSocket>();

function positiveEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) throw new Error(`${name} must be a positive integer`);
  return number;
}

function config(): Config {
  const address = process.env.RELAY_ADDR ?? "127.0.0.1:8080";
  const separator = address.lastIndexOf(":");
  if (separator < 1) throw new Error(`RELAY_ADDR must be host:port, got ${address}`);
  const host = address.slice(0, separator);
  const port = Number(address.slice(separator + 1));
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error(`invalid RELAY_ADDR port: ${address}`);
  const value: Config = {
    host,
    port,
    maxMessageBytes: positiveEnv("RELAY_MAX_MESSAGE_BYTES", (32 << 20) - 14),
    maxQueueBytes: positiveEnv("RELAY_MAX_QUEUE_BYTES", 1 << 20),
    maxQueueMessages: positiveEnv("RELAY_MAX_QUEUE_MESSAGES", 256),
    maxClients: positiveEnv("RELAY_MAX_CLIENTS", 20000),
    maxClientsPerHost: positiveEnv("RELAY_MAX_CLIENTS_PER_HOST", 64),
    maxPendingPerHost: positiveEnv("RELAY_MAX_PENDING_PER_HOST", 64),
    maxHosts: positiveEnv("RELAY_MAX_HOSTS", 20000),
    authTimeoutMs: positiveEnv("RELAY_AUTH_TIMEOUT_MS", 5000),
    pairTimeoutMs: positiveEnv("RELAY_PAIR_TIMEOUT_MS", 5000),
  };
  if (value.maxPendingPerHost > value.maxClientsPerHost) throw new Error("RELAY_MAX_PENDING_PER_HOST exceeds RELAY_MAX_CLIENTS_PER_HOST");
  return value;
}

function rawBase64Url(value: string, length: number): Buffer | undefined {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) return undefined;
  const bytes = Buffer.from(value, "base64url");
  return bytes.length === length && bytes.toString("base64url") === value ? bytes : undefined;
}

function endpointId(publicKey: Buffer): string {
  return createHash("sha256").update(publicKey).digest("hex");
}

function encodeControl(message: unknown): string {
  return JSON.stringify(message);
}

function closeCode(code: number): number {
  return code >= 1000 && code <= 4999 && code !== 1004 && code !== 1005 && code !== 1006 ? code : 1001;
}

function safeClose(ws: WebSocket | undefined, code: number, reason: string): void {
  if (!ws || ws.readyState === WebSocket.CLOSED) return;
  try {
    ws.close(closeCode(code), reason.slice(0, 123));
  } catch {
    ws.terminate();
  }
}

function rawMessage(data: RawData): Buffer {
  return Buffer.isBuffer(data) ? data : Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data);
}

function trackSocket(ws: WebSocket): void {
  sockets.add(ws);
  ws.once("close", () => sockets.delete(ws));
}

function rejectUpgrade(socket: import("node:stream").Duplex, status: number, message: string): void {
  counters.rejectedConnections += 1;
  const body = `${message}\n`;
  socket.end(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
}

function sendFrame(destination: WebSocket | undefined, frame: Frame, onDone: (error?: Error) => void): void {
  if (!destination || destination.readyState !== WebSocket.OPEN) {
    onDone(new Error("destination is closed"));
    return;
  }
  try {
    destination.send(frame.data, { binary: frame.binary }, (error?: Error) => onDone(error));
  } catch (error) {
    onDone(error instanceof Error ? error : new Error(String(error)));
  }
}

function closePair(pair: Pair, code: number, reason: string): void {
  if (pair.closing) return;
  pair.closing = true;
  pair.state = "closed";
  clearTimeout(pair.timer);
  pairs.delete(pair.id);
  pair.host.pairs.delete(pair);
  if (pair.announced && !pair.host.gone && pair.host.ws.readyState === WebSocket.OPEN) {
    pair.host.ws.send(encodeControl({ type: "closed", connectionId: pair.id }));
  }
  safeClose(pair.client, code, reason);
  safeClose(pair.hostSocket, code, reason);
}

function forward(pair: Pair, source: "client" | "host", data: RawData, isBinary: boolean): void {
  if (pair.state !== "active" || pair.closing) return;
  const frame = { data: rawMessage(data), binary: isBinary };
  const destination = source === "client" ? pair.hostSocket : pair.client;
  if (!destination) {
    closePair(pair, 1013, "destination unavailable");
    return;
  }
  sendFrame(destination, frame, (error) => {
    if (error) {
      closePair(pair, 1013, "peer write timeout");
      return;
    }
    counters.forwardedMessages += 1;
    counters.forwardedBytes += frame.data.length;
    if (source === "client") pair.host.bytesIn += frame.data.length;
    else pair.host.bytesOut += frame.data.length;
  });
}

function attachDataSocket(pair: Pair, ws: WebSocket, source: "client" | "host"): void {
  ws.on("message", (data, isBinary) => {
    if (pair.closing) return;
    if (pair.state !== "active") {
      if (source !== "client") return;
      const frame = { data: rawMessage(data), binary: isBinary };
      if (pair.queue.length >= currentConfig.maxQueueMessages || pair.queuedBytes + frame.data.length > currentConfig.maxQueueBytes) {
        closePair(pair, 1013, "control queue full");
        return;
      }
      pair.queue.push(frame);
      pair.queuedBytes += frame.data.length;
      return;
    }
    forward(pair, source, data, isBinary);
  });
  ws.on("error", () => closePair(pair, 1011, "peer disconnected"));
  ws.on("close", (code, reason) => {
    if (!pair.closing) closePair(pair, code || 1001, reason.toString() || "peer disconnected");
  });
}

function flushQueue(pair: Pair): void {
  const queue = pair.queue;
  pair.queue = [];
  pair.queuedBytes = 0;
  for (const frame of queue) forward(pair, "client", frame.data, frame.binary);
}

function handleControlUpgrade(ws: WebSocket, request: IncomingMessage, url: URL): void {
  const publicKeyText = url.searchParams.get("publicKey") ?? "";
  const publicKey = rawBase64Url(publicKeyText, 32);
  if (!publicKey) {
    safeClose(ws, 1008, "invalid publicKey");
    return;
  }
  if (hosts.size >= currentConfig.maxHosts) {
    safeClose(ws, 1013, "host capacity reached");
    return;
  }
  const id = endpointId(publicKey);
  const nonce = randomBytes(32).toString("base64url");
  let authenticated = false;
  const authTimer = setTimeout(() => {
    if (!authenticated) safeClose(ws, 1008, "authentication failed");
  }, currentConfig.authTimeoutMs);
  ws.send(encodeControl({ type: "challenge", nonce }));
  const onAuth = (data: RawData, isBinary: boolean): void => {
    if (authenticated) {
      safeClose(ws, 1008, "unexpected control message");
      return;
    }
    if (isBinary) {
      clearTimeout(authTimer);
      safeClose(ws, 1008, "authentication failed");
      return;
    }
    let auth: { type?: string; signature?: string };
    try {
      auth = JSON.parse(rawMessage(data).toString("utf8")) as { type?: string; signature?: string };
    } catch {
      clearTimeout(authTimer);
      safeClose(ws, 1008, "authentication failed");
      return;
    }
    const signature = auth.signature ? rawBase64Url(auth.signature, 64) : undefined;
    const message = Buffer.from(`supacode-relay-v1\n${id}\n${nonce}`);
    if (auth.type !== "authenticate" || !signature || !verify(null, message, { key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), publicKey]), format: "der", type: "spki" }, signature)) {
      clearTimeout(authTimer);
      safeClose(ws, 1008, "authentication failed");
      return;
    }
    authenticated = true;
    clearTimeout(authTimer);
    ws.off("message", onAuth);
    const host: Host = { id, ws, pairs: new Set(), bytesIn: 0, bytesOut: 0, gone: false };
    const old = hosts.get(id);
    hosts.set(id, host);
    ws.send(encodeControl({ type: "registered", endpointId: id }));
    if (old) {
      old.gone = true;
      safeClose(old.ws, 4001, "registration superseded");
      for (const pair of [...old.pairs]) closePair(pair, 1001, "host offline");
    }
    ws.on("message", (_message, _binary) => safeClose(ws, 1008, "unexpected control message"));
    ws.once("close", () => {
      if (hosts.get(id) !== host) return;
      hosts.delete(id);
      host.gone = true;
      for (const pair of [...host.pairs]) closePair(pair, 1001, "host offline");
    });
  };
  ws.on("message", onAuth);
}

function handleConnectUpgrade(ws: WebSocket, url: URL): void {
  const host = hosts.get(url.searchParams.get("endpointId") ?? "");
  if (!host || host.gone) {
    safeClose(ws, 1001, "endpoint not found");
    return;
  }
  if (pairs.size >= currentConfig.maxClients || host.pairs.size >= currentConfig.maxClientsPerHost || [...host.pairs].filter((pair) => pair.state !== "active").length >= currentConfig.maxPendingPerHost) {
    safeClose(ws, 1013, "client capacity reached");
    return;
  }
  const pair: Pair = {
    id: randomBytes(16).toString("base64url"),
    token: randomBytes(32).toString("base64url"),
    host,
    client: ws,
    state: "pending",
    queue: [],
    queuedBytes: 0,
    timer: setTimeout(() => closePair(pair, 1013, "pair timeout"), currentConfig.pairTimeoutMs),
    announced: true,
    closing: false,
  };
  pairs.set(pair.id, pair);
  host.pairs.add(pair);
  attachDataSocket(pair, ws, "client");
  if (host.ws.readyState !== WebSocket.OPEN) {
    closePair(pair, 1001, "host offline");
    return;
  }
  host.ws.send(encodeControl({ type: "incoming", connectionId: pair.id, token: pair.token }));
}

function handleAcceptUpgrade(ws: WebSocket, url: URL): void {
  const pair = pairs.get(url.searchParams.get("connectionId") ?? "");
  const endpoint = url.searchParams.get("endpointId") ?? "";
  const token = url.searchParams.get("token") ?? "";
  if (!pair || pair.host.id !== endpoint || pair.token !== token || pair.state !== "pending" || pair.client.readyState !== WebSocket.OPEN) {
    safeClose(ws, 1008, "invalid token");
    return;
  }
  pair.state = "active";
  pair.hostSocket = ws;
  clearTimeout(pair.timer);
  attachDataSocket(pair, ws, "host");
  flushQueue(pair);
}

const currentConfig = config();
const http = createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  if (url.pathname === "/healthz") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ status: "ok" }));
    return;
  }
  if (url.pathname === "/metrics") {
    const memory = process.memoryUsage();
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ activeHosts: hosts.size, activePairs: [...pairs.values()].filter((pair) => pair.state === "active").length, pendingPairs: [...pairs.values()].filter((pair) => pair.state !== "active").length, openSockets: sockets.size, forwardedMessages: counters.forwardedMessages, forwardedBytes: counters.forwardedBytes, rejectedConnections: counters.rejectedConnections, heapUsedBytes: memory.heapUsed, rssBytes: memory.rss }));
    return;
  }
  response.writeHead(404);
  response.end();
});
const webSockets = new WebSocketServer({ noServer: true, maxPayload: currentConfig.maxMessageBytes });
http.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
  if (!["/v1/control", "/v1/connect", "/v1/accept"].includes(url.pathname)) {
    rejectUpgrade(socket, 404, "not found");
    return;
  }
  webSockets.handleUpgrade(request, socket, head, (ws) => {
    trackSocket(ws);
    if (url.pathname === "/v1/control") handleControlUpgrade(ws, request, url);
    else if (url.pathname === "/v1/connect") handleConnectUpgrade(ws, url);
    else handleAcceptUpgrade(ws, url);
  });
});

http.listen(currentConfig.port, currentConfig.host, () => {
  const address = http.address();
  const port = typeof address === "object" && address ? address.port : currentConfig.port;
  console.log(JSON.stringify({ event: "listening", address: `${currentConfig.host}:${port}` }));
});

function shutdown(): void {
  for (const pair of [...pairs.values()]) closePair(pair, 1001, "relay shutting down");
  for (const ws of sockets) safeClose(ws, 1001, "relay shutting down");
  http.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
}
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
