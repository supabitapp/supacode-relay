import crypto from "node:crypto";
import http from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { PerformanceObserver } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import v8 from "node:v8";

import { type IP, clientKey, formatIP, invalidIP, joinHostPort } from "./addr.ts";
import { Gate, Limiter, clientIP, isTrusted, remoteIP } from "./admission.ts";
import { type Config, admissionBurst, pongTimeout } from "./config.ts";
import { Dashboard, type Stats } from "./dashboard.ts";
import {
  type ErrorKind,
  Logger,
  Trace,
  closeReason,
  decodeB64,
  endpointID,
  route,
  tag,
  traceHeader,
  validEndpointID,
} from "./diagnostics.ts";
import { Exchange } from "./exchange.ts";
import { Close, type CloseMsg, Op, type ReadEnd, type Sink, WsConn } from "./websocket.ts";

const httpTimeout = 5000;
const topHostsReported = 20;
const controlReadLimit = 4096;
const closeSuperseded = 4001;
const drainTimeout = 5000;
const closeReserve = 750;
const hardStopReserve = 100;
const signaturePrefixes = ["supaterm-relay-v1\n", "supacode-relay-v1\n"];

type PairState = "pending" | "accepting" | "active" | "closed";
type Frame = { op: number; data: Buffer };
type Outcome = "queued" | "queue_full" | "queue_finished" | "host_gone";

function randomB64(n: number): string {
  return crypto.randomBytes(n).toString("base64url");
}

function textFrame(v: Record<string, string>): Frame {
  return { op: Op.text, data: Buffer.from(JSON.stringify(v)) };
}

function errorKind(end: ReadEnd): ErrorKind {
  switch (end.kind) {
    case "close":
      return "websocket_close";
    case "eof":
    case "timeout":
    case "message_too_big":
      return end.kind;
    default:
      return "io_error";
  }
}

function closeCode(end: ReadEnd): number {
  return end.kind === "close" ? end.code : 0;
}

function sendableCode(c: number): boolean {
  return (c >= 1000 && c <= 1003) || (c >= 1007 && c <= 1014) || (c >= 3000 && c <= 4999);
}

function messages(deliver: (op: number, data: Buffer) => void): Sink {
  let parts: Buffer[] = [];
  return {
    chunk: (_op, data) => {
      parts.push(data);
    },
    message: (op) => {
      const data = parts.length === 1 ? parts[0]! : Buffer.concat(parts);
      parts = [];
      deliver(op, data);
    },
  };
}

function formatDuration(ms: number): string {
  return ms === 0 || ms >= 1000 ? `${ms / 1000}s` : `${ms}ms`;
}

function logLine(text: string): void {
  const now = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  const stamp = `${now.getFullYear()}/${pad(now.getMonth() + 1)}/${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  process.stderr.write(`${stamp} ${text}\n`);
}

function distinctHeaders(req: http.IncomingMessage): NodeJS.Dict<string[]> {
  if (req.headersDistinct) {
    return req.headersDistinct;
  }
  const out: NodeJS.Dict<string[]> = {};
  for (let i = 0; i + 1 < req.rawHeaders.length; i += 2) {
    (out[req.rawHeaders[i]!.toLowerCase()] ??= []).push(req.rawHeaders[i + 1]!);
  }
  return out;
}

class Queue {
  readonly #maxBytes: number;
  readonly #maxMessages: number;
  #items: Frame[] = [];
  bytes = 0;
  final: CloseMsg | undefined;
  wake: () => void = () => {};

  constructor(maxBytes: number, maxMessages: number) {
    this.#maxBytes = maxBytes;
    this.#maxMessages = maxMessages;
  }

  push(f: Frame): boolean {
    return this.enqueue(f) !== "queue_full";
  }

  enqueue(f: Frame): Outcome {
    if (this.final) {
      return "queue_finished";
    }
    if (this.#items.length >= this.#maxMessages || f.data.length > this.#maxBytes - this.bytes) {
      return "queue_full";
    }
    this.#items.push(f);
    this.bytes += f.data.length;
    this.wake();
    return "queued";
  }

  finish(c: CloseMsg, discard: boolean): void {
    this.final ??= c;
    if (discard) {
      for (const f of this.#items) {
        this.bytes -= f.data.length;
      }
      this.#items = [];
    }
    this.wake();
  }

  next(): Frame | undefined {
    return this.#items.shift();
  }

  release(f: Frame): void {
    this.bytes -= f.data.length;
  }
}

class Host {
  readonly id: string;
  readonly ctrl: ControlPeer;
  readonly trace: Trace;
  readonly pairs = new Map<string, Pair>();
  readonly clientCounts = new Map<string, number>();
  pending = 0;
  clientSlots = 0;
  gone = false;
  bytesIn = 0;
  bytesOut = 0;

  constructor(id: string, ctrl: ControlPeer, trace: Trace) {
    this.id = id;
    this.ctrl = ctrl;
    this.trace = trace;
  }

  notify(v: Record<string, string>): Outcome {
    if (this.gone) {
      return "host_gone";
    }
    const outcome = this.ctrl.queue.enqueue(textFrame(v));
    if (outcome === "queue_full") {
      this.ctrl.queue.finish({ code: Close.tryAgainLater, reason: "control queue limit exceeded" }, true);
    }
    return outcome;
  }
}

class ControlPeer {
  readonly ws: WsConn;
  readonly queue: Queue;
  readonly trace: Trace;
  readonly #cfg: Config;
  #started = false;
  #inflight = false;
  #closing = false;
  #cause: CloseMsg | undefined;
  #writeTimer: NodeJS.Timeout | undefined;
  #keepAlive: NodeJS.Timeout | undefined;

  onEnded: () => void = () => {};

  constructor(cfg: Config, ws: WsConn, trace: Trace) {
    this.#cfg = cfg;
    this.ws = ws;
    this.trace = trace;
    this.queue = new Queue(cfg.maxQueueBytes, cfg.maxQueueMessages);
    this.queue.wake = () => this.#pump();
    ws.setDeadline(pongTimeout(cfg));
    ws.onPong = () => ws.extend();
    ws.sink = messages((op, data) => {
      trace.event("control.message.unexpected", { message_type: op, bytes: data.length });
      this.queue.finish({ code: Close.policyViolation, reason: "unexpected control message" }, true);
    });
    ws.onEnd = (end) => {
      trace.failure("control.read.ended", errorKind(end), { close_code: closeCode(end) });
      this.queue.finish({ code: Close.goingAway, reason: "peer disconnected" }, true);
      this.onEnded();
    };
    ws.whenClosed(() => {
      clearInterval(this.#keepAlive);
      clearTimeout(this.#writeTimer);
    });
  }

  start(): void {
    this.#started = true;
    this.#keepAlive = setInterval(() => this.ws.ping(), this.#cfg.heartbeat);
    this.#pump();
  }

  #pump(): void {
    if (!this.#started || this.#inflight || this.#closing) {
      return;
    }
    const f = this.queue.next();
    if (f) {
      this.#inflight = true;
      this.#writeTimer = setTimeout(
        () => this.#fail({ code: Close.tryAgainLater, reason: "peer write timeout" }),
        this.#cfg.writeTimeout,
      );
      this.ws.writeFrame(f.op, true, f.data, () => {
        clearTimeout(this.#writeTimer);
        this.#inflight = false;
        this.queue.release(f);
        this.#pump();
      });
      return;
    }
    const final = this.queue.final;
    if (final) {
      this.#closing = true;
      clearInterval(this.#keepAlive);
      this.trace.event("peer.close.sending", { close_code: final.code, reason: closeReason(final.reason) });
      this.ws.writeClose(final.code, final.reason);
      this.ws.terminate();
    }
  }

  #fail(c: CloseMsg): void {
    if (!this.#cause) {
      this.#cause = c;
      this.trace.event("peer.failed", { close_code: c.code, reason: closeReason(c.reason) });
    }
    this.ws.destroy();
  }
}

class DataPeer {
  readonly ws: WsConn;
  readonly pair: Pair;
  readonly trace: Trace;
  readonly #server: Server;
  readonly #toHost: boolean;
  #out: DataPeer | undefined;
  bytes = 0;
  messages = 0;
  #blocked = false;
  #delivery: NodeJS.Timeout | undefined;
  readonly #keepAlive: NodeJS.Timeout;

  constructor(server: Server, ws: WsConn, pair: Pair, toHost: boolean, trace: Trace) {
    this.#server = server;
    this.ws = ws;
    this.pair = pair;
    this.#toHost = toHost;
    this.trace = trace;
    ws.setDeadline(pongTimeout(server.cfg));
    ws.onPong = () => ws.extend();
    ws.onEnd = (end) => {
      if (pair.state === "closed") {
        return;
      }
      this.trace.failure("peer.read.ended", errorKind(end), { close_code: closeCode(end) });
      pair.close(this.#closeCause(end));
    };
    ws.whenClosed(() => this.#exit());
    this.#keepAlive = setInterval(() => ws.ping(), server.cfg.heartbeat);
    this.trace.event("peer.waiting_for_pair");
  }

  start(out: DataPeer): void {
    this.#out = out;
    this.trace.event("peer.forwarding.started");
    this.ws.corkTarget = out.ws.socket;
    this.ws.sink = {
      chunk: (op, data, fin) => this.#forward(out, op, data, fin),
      message: (op, n) => this.#delivered(out, op, n),
    };
    this.ws.resume();
  }

  close(c: CloseMsg): void {
    clearInterval(this.#keepAlive);
    clearTimeout(this.#delivery);
    this.ws.sink = undefined;
    this.ws.writeClose(c.code, c.reason);
    this.ws.terminate();
  }

  addTraffic(n: number): void {
    if (this.#toHost) {
      this.pair.host.bytesIn += n;
    } else {
      this.pair.host.bytesOut += n;
    }
  }

  #forward(out: DataPeer, op: number, data: Buffer, fin: boolean): void {
    this.ws.extend();
    if (!out.ws.writeFrame(op, fin, data) && !out.#blocked) {
      this.#blockOn(out);
    }
  }

  #blockOn(out: DataPeer): void {
    out.#blocked = true;
    this.ws.pause();
    out.#delivery = setTimeout(() => {
      this.trace.failure("peer.forwarding.failed", "timeout", { side: "destination" });
      this.pair.close({ code: Close.tryAgainLater, reason: "peer write timeout" });
    }, this.#server.cfg.deliveryTimeout);
    out.ws.socket.once("drain", () => {
      out.#blocked = false;
      clearTimeout(out.#delivery);
      if (this.pair.state === "active") {
        this.ws.resume();
      }
    });
  }

  #delivered(out: DataPeer, op: number, n: number): void {
    const s = this.#server;
    s.forwardedMessages++;
    s.forwardedBytes += n;
    out.addTraffic(n);
    this.bytes += n;
    if (++this.messages === 1) {
      this.trace.event("peer.message.first", { message_type: op, bytes: n });
    }
  }

  #closeCause(end: ReadEnd): CloseMsg {
    switch (end.kind) {
      case "close":
        if (end.code === Close.noStatus) {
          return { code: Close.normal, reason: "" };
        }
        return sendableCode(end.code) ? { code: end.code, reason: end.reason } : { code: Close.goingAway, reason: "peer disconnected" };
      case "message_too_big":
        return { code: Close.messageTooBig, reason: "message too big" };
      case "timeout":
        return { code: Close.goingAway, reason: "peer timeout" };
      default:
        return { code: Close.goingAway, reason: "peer disconnected" };
    }
  }

  #exit(): void {
    clearInterval(this.#keepAlive);
    clearTimeout(this.#delivery);
    if (this.#out === undefined && this.pair.state === "closed") {
      this.trace.event("peer.pair.closed_before_ready");
    }
    this.trace.event("peer.ended", { forwarded_bytes: this.bytes, forwarded_messages: this.messages });
    this.pair.releaseSlot();
  }
}

class Pair {
  readonly id = randomB64(16);
  readonly token = randomB64(32);
  readonly host: Host;
  readonly clientKey: string;
  readonly trace: Trace;
  readonly #server: Server;
  state: PairState = "pending";
  closedFrom: PairState = "pending";
  cause: CloseMsg = { code: 0, reason: "" };
  announced = false;
  timer: NodeJS.Timeout | undefined;
  client: DataPeer | undefined;
  hostPeer: DataPeer | undefined;
  refs = 1;

  constructor(server: Server, host: Host, clientKey: string, trace: Trace) {
    this.#server = server;
    this.host = host;
    this.clientKey = clientKey;
    this.trace = trace.with({ pair_tag: tag(this.id) });
  }

  expire(): void {
    this.close({ code: Close.tryAgainLater, reason: "pair timeout" }, true);
  }

  close(c: CloseMsg, waitingOnly = false): void {
    const s = this.#server;
    const prev = this.state;
    if (prev === "closed" || (waitingOnly && prev !== "pending" && prev !== "accepting")) {
      return;
    }
    this.state = "closed";
    this.closedFrom = prev;
    this.cause = c;
    const h = this.host;
    h.pairs.delete(this.id);
    s.pairs.delete(this.id);
    if (prev === "active") {
      s.active--;
    } else {
      h.pending--;
      s.pending--;
    }
    if (this.announced) {
      h.notify({ connectionId: this.id, type: "closed" });
    }
    this.trace.event("pair.closing", { phase: prev, close_code: c.code, reason: closeReason(c.reason) });
    clearTimeout(this.timer);
    this.client?.close(c);
    this.hostPeer?.close(c);
  }

  releaseSlot(): void {
    if (--this.refs > 0) {
      return;
    }
    const s = this.#server;
    const h = this.host;
    s.clientSlots--;
    h.clientSlots--;
    const n = (h.clientCounts.get(this.clientKey) ?? 0) - 1;
    if (n <= 0) {
      h.clientCounts.delete(this.clientKey);
    } else {
      h.clientCounts.set(this.clientKey, n);
    }
    this.trace.event("pair.closed", {
      phase: this.closedFrom,
      close_code: this.cause.code,
      reason: closeReason(this.cause.reason),
      bytes_to_host: this.client?.bytes ?? 0,
      bytes_to_client: this.hostPeer?.bytes ?? 0,
      messages_to_host: this.client?.messages ?? 0,
      messages_to_client: this.hostPeer?.messages ?? 0,
    });
  }
}

export class Server {
  readonly cfg: Config;
  readonly hosts = new Map<string, Host>();
  readonly pairs = new Map<string, Pair>();
  readonly conns = new Set<WsConn>();
  draining = false;
  forwardedMessages = 0;
  forwardedBytes = 0;
  pending = 0;
  active = 0;
  clientSlots = 0;
  #controls = 0;
  #superseded = 0;
  #gcCycles = 0;
  readonly #events = new Logger((line) => process.stderr.write(line));
  readonly #limiter: Limiter;
  readonly #publicLimiter: Limiter;
  readonly #gate: Gate;
  readonly #rejections = new Map<string, number>();
  readonly #http: http.Server;
  readonly #private: http.Server;
  readonly #dashboard: Dashboard;

  constructor(cfg: Config) {
    this.cfg = cfg;
    this.#limiter = new Limiter(cfg.admissionRate, admissionBurst(cfg));
    this.#publicLimiter = new Limiter(cfg.admissionRate, admissionBurst(cfg));
    this.#gate = new Gate(cfg.maxConnections, cfg.maxConnectionsPerIP);
    this.#dashboard = new Dashboard(
      () => this.#dashboardStats(),
      () => ({ activeHandles: activeHandles(), gcCycles: () => this.#gcCycles }),
    );
    const options: http.ServerOptions = {
      requestTimeout: httpTimeout,
      headersTimeout: httpTimeout,
      keepAliveTimeout: httpTimeout,
      noDelay: true,
    };
    this.#http = http.createServer(options, (req, res) => this.#route(new Exchange(req, res)));
    this.#http.on("upgrade", (req, socket, head) => this.#route(new Exchange(req, undefined, socket as Socket, head)));
    this.#private = http.createServer(options, (req, res) => this.#admin(new Exchange(req, res)));
    this.#private.on("upgrade", (req, socket, head) => this.#admin(new Exchange(req, undefined, socket as Socket, head)));
    this.#filter(this.#http, cfg.allowedPeers);
    this.#filter(this.#private, cfg.privatePeers);
    try {
      new PerformanceObserver((list) => {
        this.#gcCycles += list.getEntries().length;
      }).observe({ entryTypes: ["gc"] });
    } catch {}
  }

  async listen(addr: { host: string; port: number }, isPrivate: boolean): Promise<string> {
    const server = isPrivate ? this.#private : this.#http;
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen({ host: addr.host || undefined, port: addr.port }, () => {
        server.off("error", reject);
        resolve();
      });
    });
    const a = server.address() as AddressInfo;
    return joinHostPort(a.address, a.port);
  }

  serve(onError: (err: Error) => void): void {
    const c = this.cfg;
    this.#events.info("relay.started", {
      auth_timeout_ms: c.authTimeout,
      pair_timeout_ms: c.pairTimeout,
      delivery_timeout_ms: c.deliveryTimeout,
      heartbeat_ms: c.heartbeat,
      max_hosts: c.maxHosts,
      max_clients: c.maxClients,
      max_clients_per_host: c.maxClientsPerHost,
      max_clients_per_host_per_ip: c.maxClientsPerHostPerIP,
      max_pending_per_host: c.maxPendingPerHost,
    });
    this.#http.on("error", onError);
    this.#private.on("error", onError);
  }

  #filter(server: http.Server, allow: Config["allowedPeers"]): void {
    if (allow.length > 0) {
      server.on("connection", (socket) => {
        if (!isTrusted(remoteIP(socket.remoteAddress), allow)) {
          socket.destroy();
        }
      });
    }
  }

  #clientIP(req: http.IncomingMessage): IP | undefined {
    return clientIP(req.socket.remoteAddress, req.headers, distinctHeaders(req), this.cfg.trustedProxies, this.cfg.clientIPHeader);
  }

  #key(ex: Exchange): string {
    return clientKey(this.#clientIP(ex.req));
  }

  #methodAllowed(ex: Exchange): boolean {
    if (ex.req.method === "GET" || ex.req.method === "HEAD") {
      return true;
    }
    ex.text(405, "Method Not Allowed\n", { Allow: "GET, HEAD" });
    return false;
  }

  #route(ex: Exchange): void {
    ex.keepAlive = !this.draining;
    const path = ex.url.pathname;
    if (path === "/dashboard") {
      ex.respond(301, { Location: "/dashboard/" }, "");
      return;
    }
    if (path.startsWith("/dashboard/")) {
      this.#dashboard.serve(ex, path.slice("/dashboard".length), (e) => this.#allowPublic(e));
      return;
    }
    const ip = this.#clientIP(ex.req);
    let trace = Trace.start(this.#events, route(path)).with({ client_tag: tag(ip ? formatIP(ip) : invalidIP) });
    const q = ex.url.searchParams;
    let id = q.get("endpointId") ?? "";
    if (path === "/v1/control") {
      const pub = decodeB64(q.get("publicKey") ?? "", 32);
      id = pub ? endpointID(pub) : "";
    }
    if (validEndpointID(id)) {
      trace = trace.with({ endpoint_tag: tag(id) });
    }
    trace.event("request.begin");
    ex.headers[traceHeader] = trace.id;
    ex.onDone(() => trace.event("request.end", { status: ex.status, upgraded: ex.upgraded }));
    switch (path) {
      case "/healthz":
        if (this.cfg.privateAddr === "") {
          if (this.#methodAllowed(ex)) {
            this.#health(ex);
          }
          return;
        }
        break;
      case "/metrics":
        if (this.#methodAllowed(ex)) {
          if (this.cfg.privateAddr === "") {
            this.#metrics(ex, true);
          } else if (this.#allowPublic(ex)) {
            this.#metrics(ex, false);
          }
        }
        return;
      case "/v1/control":
        if (this.#methodAllowed(ex)) {
          this.#limitConnections(ex, trace, () => this.#handleControl(ex, trace));
        }
        return;
      case "/v1/connect":
        if (this.#methodAllowed(ex)) {
          this.#limitConnections(ex, trace, () => this.#handleConnect(ex, trace));
        }
        return;
      case "/v1/accept":
        if (this.#methodAllowed(ex)) {
          this.#limitConnections(ex, trace, () => this.#handleAccept(ex, trace));
        }
        return;
    }
    ex.text(404, "404 page not found\n");
  }

  #admin(ex: Exchange): void {
    switch (ex.url.pathname) {
      case "/healthz":
        if (this.#methodAllowed(ex)) {
          this.#health(ex);
        }
        return;
      case "/metrics":
        if (this.#methodAllowed(ex)) {
          this.#metrics(ex, true);
        }
        return;
    }
    ex.text(404, "404 page not found\n");
  }

  #health(ex: Exchange): void {
    ex.json(this.draining ? 503 : 200, { status: this.draining ? "draining" : "ok" });
  }

  #allowPublic(ex: Exchange): boolean {
    if (this.#publicLimiter.allow(this.#key(ex), Date.now())) {
      return true;
    }
    ex.json(429, { error: "rate limited" });
    return false;
  }

  #limitConnections(ex: Exchange, trace: Trace, next: () => void): void {
    const key = this.#key(ex);
    switch (this.#gate.acquire(key)) {
      case "global_full":
        this.#reject(ex, trace, 503, "connection capacity reached");
        return;
      case "per_ip_full":
        this.#reject(ex, trace, 429, "client connection capacity reached");
        return;
    }
    ex.onDone(() => this.#gate.release(key));
    next();
  }

  #reject(ex: Exchange, trace: Trace, status: number, msg: string): void {
    this.#countRejection(msg);
    trace.event("request.rejected", { status, reason: msg });
    ex.json(status, { error: msg });
  }

  #countRejection(reason: string): void {
    this.#rejections.set(reason, (this.#rejections.get(reason) ?? 0) + 1);
  }

  #rejectionCounts(): { total: number; byReason: Record<string, number> } {
    const byReason = Object.fromEntries([...this.#rejections].sort(([a], [b]) => (a < b ? -1 : 1)));
    return { total: Object.values(byReason).reduce((a, b) => a + b, 0), byReason };
  }

  #admit(ex: Exchange, trace: Trace, duringDrain: boolean): boolean {
    if (!duringDrain && this.draining) {
      this.#reject(ex, trace, 503, "draining");
      return false;
    }
    if (!this.#limiter.allow(this.#key(ex), Date.now())) {
      this.#reject(ex, trace, 429, "rate limited");
      return false;
    }
    return true;
  }

  #upgrade(ex: Exchange, trace: Trace, limit: number): WsConn | undefined {
    const up = ex.upgrade();
    if (!up) {
      trace.failure("socket.upgrade.failed", "io_error");
      return undefined;
    }
    const ws = new WsConn(up.socket, up.head, limit);
    this.conns.add(ws);
    ws.whenClosed(() => {
      this.conns.delete(ws);
      ex.done();
    });
    trace.event("socket.upgraded");
    return ws;
  }

  #closeNow(ws: WsConn, c: CloseMsg): void {
    ws.sink = undefined;
    ws.writeClose(c.code, c.reason);
    ws.terminate();
  }

  #handleControl(ex: Exchange, trace: Trace): void {
    if (!this.#admit(ex, trace, false)) {
      return;
    }
    const pub = decodeB64(ex.url.searchParams.get("publicKey") ?? "", 32);
    if (!pub) {
      this.#reject(ex, trace, 400, "invalid publicKey");
      return;
    }
    if (this.#controls >= this.cfg.maxHosts) {
      trace.event("control.capacity.reached", { current_hosts: this.#controls, max_hosts: this.cfg.maxHosts });
      this.#reject(ex, trace, 503, "host capacity reached");
      return;
    }
    this.#controls++;
    ex.onDone(() => this.#controls--);
    const ws = this.#upgrade(ex, trace, controlReadLimit);
    if (!ws) {
      return;
    }
    const id = endpointID(pub);
    trace.event("control.challenge.sending", { auth_timeout_ms: this.cfg.authTimeout });
    const nonce = randomB64(32);
    const challenge = textFrame({ nonce, type: "challenge" });
    ws.writeFrame(challenge.op, true, challenge.data);
    trace.event("control.challenge.sent");
    ws.setDeadline(this.cfg.authTimeout);
    let settled = false;
    const fail = (reason: string) => {
      settled = true;
      this.#countRejection("authentication failed");
      trace.event("control.auth.failed", { reason });
      this.#closeNow(ws, { code: Close.policyViolation, reason: "authentication failed" });
    };
    ws.onEnd = (end) => {
      if (!settled) {
        fail(errorKind(end));
      }
    };
    ws.sink = messages((op, data) => {
      if (settled) {
        return;
      }
      const reason = verifyAuth(op, data, pub, id + "\n" + nonce);
      if (reason !== "") {
        fail(reason);
        return;
      }
      settled = true;
      this.#register(ws, trace, id);
    });
  }

  #register(ws: WsConn, trace: Trace, id: string): void {
    trace.event("control.auth.accepted");
    const ctrl = new ControlPeer(this.cfg, ws, trace.with({ peer: "control" }));
    const h = new Host(id, ctrl, trace);
    ctrl.queue.push(textFrame({ endpointId: id, type: "registered" }));
    if (this.draining) {
      this.#closeNow(ws, { code: Close.goingAway, reason: "relay draining" });
      return;
    }
    ctrl.onEnded = () => this.#removeHost(h);
    const old = this.hosts.get(id);
    this.hosts.set(id, h);
    trace.event("host.registered", { superseded_previous: old !== undefined });
    if (old) {
      old.trace.event("host.superseded", { replacement_trace_id: trace.id });
      this.#superseded++;
      old.ctrl.queue.finish({ code: closeSuperseded, reason: "registration superseded" }, true);
    }
    ctrl.start();
  }

  #removeHost(h: Host): void {
    if (this.hosts.get(h.id) === h) {
      this.hosts.delete(h.id);
    }
    if (h.gone) {
      return;
    }
    h.gone = true;
    const pairs = [...h.pairs.values()];
    h.trace.event("host.removed", { pairs_closing: pairs.length, bytes_to_host: h.bytesIn, bytes_from_host: h.bytesOut });
    for (const p of pairs) {
      p.close({ code: Close.goingAway, reason: "host offline" });
    }
  }

  #handleConnect(ex: Exchange, trace: Trace): void {
    if (!this.#admit(ex, trace, false)) {
      return;
    }
    const cfg = this.cfg;
    const key = this.#key(ex);
    const h = this.hosts.get(ex.url.searchParams.get("endpointId") ?? "");
    if (!h) {
      this.#reject(ex, trace, 404, "endpoint not found");
      return;
    }
    if (this.clientSlots >= cfg.maxClients || h.clientSlots >= cfg.maxClientsPerHost || h.pending >= cfg.maxPendingPerHost) {
      trace.event("pair.capacity.reached", {
        current_clients: this.clientSlots,
        max_clients: cfg.maxClients,
        current_host_clients: h.clientSlots,
        max_host_clients: cfg.maxClientsPerHost,
        current_host_pending: h.pending,
        max_host_pending: cfg.maxPendingPerHost,
      });
      this.#reject(ex, trace, 503, "client capacity reached");
      return;
    }
    if ((h.clientCounts.get(key) ?? 0) >= cfg.maxClientsPerHostPerIP) {
      this.#reject(ex, trace, 429, "client capacity for host reached");
      return;
    }
    const p = new Pair(this, h, key, trace);
    h.pairs.set(p.id, p);
    this.pairs.set(p.id, p);
    h.pending++;
    h.clientSlots++;
    h.clientCounts.set(key, (h.clientCounts.get(key) ?? 0) + 1);
    this.clientSlots++;
    this.pending++;
    p.timer = setTimeout(() => p.expire(), cfg.pairTimeout);
    p.trace.event("pair.created", { pair_timeout_ms: cfg.pairTimeout, control_trace_id: h.trace.id });

    const ws = this.#upgrade(ex, trace, cfg.maxMessageBytes);
    if (!ws) {
      p.close({ code: Close.internalError, reason: "upgrade failed" });
      p.releaseSlot();
      return;
    }
    ws.pause();
    p.client = new DataPeer(this, ws, p, false, p.trace.with({ peer: "client" }));
    p.announced = true;
    const outcome = h.notify({ connectionId: p.id, token: p.token, type: "incoming" });
    p.trace.event("pair.host.notification", { outcome });
  }

  #handleAccept(ex: Exchange, trace: Trace): void {
    if (!this.#admit(ex, trace, true)) {
      return;
    }
    const q = ex.url.searchParams;
    let p = this.pairs.get(q.get("connectionId") ?? "");
    if (p && p.host.id !== q.get("endpointId")) {
      p = undefined;
    }
    if (!p) {
      this.#reject(ex, trace, 404, "connection not found");
      return;
    }
    const token = q.get("token") ?? "";
    if (token === "" || !safeEqual(token, p.token) || p.state !== "pending" || !p.client) {
      trace.event("pair.accept.rejected", { pair_tag: tag(p.id), client_trace_id: p.trace.id, phase: p.state });
      this.#reject(ex, trace, 403, "invalid token");
      return;
    }
    const client = p.client;
    p.state = "accepting";
    p.refs++;
    const acceptTrace = trace.with({ pair_tag: tag(p.id), client_trace_id: p.trace.id });
    acceptTrace.event("pair.accept.authorized");
    const ws = this.#upgrade(ex, trace, this.cfg.maxMessageBytes);
    if (!ws) {
      p.close({ code: Close.internalError, reason: "host accept failed" });
      p.releaseSlot();
      return;
    }
    const hp = new DataPeer(this, ws, p, true, acceptTrace.with({ peer: "host" }));
    p.state = "active";
    clearTimeout(p.timer);
    p.hostPeer = hp;
    p.host.pending--;
    this.pending--;
    this.active++;
    p.trace.event("pair.active", { accept_trace_id: trace.id });
    client.start(hp);
    hp.start(client);
  }

  #dashboardStats(): Stats {
    const gate = this.#gate.stats();
    const rejections = this.#rejectionCounts();
    return {
      draining: this.draining,
      activeHosts: this.hosts.size,
      activePairs: this.active,
      pendingPairs: this.pending,
      controlConnections: this.#controls,
      clientSlots: this.clientSlots,
      openSockets: this.conns.size,
      openConnections: gate.total,
      maxHosts: this.cfg.maxHosts,
      maxClients: this.cfg.maxClients,
      maxConnections: this.cfg.maxConnections,
      forwardedMessages: this.forwardedMessages,
      forwardedBytes: this.forwardedBytes,
      rejectedConnections: rejections.total,
      rejectedConnectionsByReason: rejections.byReason,
    };
  }

  #topHosts(): { endpoint: string; traceTag: string; bytesIn: number; bytesOut: number; pairs: number }[] {
    return [...this.hosts]
      .filter(([, h]) => h.bytesIn + h.bytesOut > 0)
      .map(([id, h]) => ({ endpoint: id.slice(0, 16), traceTag: tag(id), bytesIn: h.bytesIn, bytesOut: h.bytesOut, pairs: h.pairs.size }))
      .sort((a, b) => b.bytesIn + b.bytesOut - (a.bytesIn + a.bytesOut))
      .slice(0, topHostsReported);
  }

  #metrics(ex: Exchange, detailed: boolean): void {
    const gate = this.#gate.stats();
    const memory = process.memoryUsage();
    const heap = v8.getHeapStatistics();
    const rejections = this.#rejectionCounts();
    const m: Record<string, unknown> = {
      activeHosts: this.hosts.size,
      activePairs: this.active,
      arrayBufferBytes: memory.arrayBuffers,
      clientSlots: this.clientSlots,
      closingPairs: this.clientSlots - this.active - this.pending,
      controlConnections: this.#controls,
      dataBufferBytesPerSocket: 65536 + 65536,
      draining: this.draining,
      externalBytes: memory.external,
      forwardedBytes: this.forwardedBytes,
      forwardedMessages: this.forwardedMessages,
      gcCycles: this.#gcCycles,
      goroutines: activeHandles(),
      heapAllocBytes: memory.heapUsed + memory.external,
      heapInuseBytes: heap.total_heap_size,
      heapSysBytes: heap.total_physical_size,
      maxConnections: this.cfg.maxConnections,
      maxConnectionsPerClient: this.cfg.maxConnectionsPerIP,
      openConnectionIPs: gate.ips,
      openConnections: gate.total,
      openSockets: this.conns.size,
      pendingPairs: this.pending,
      rateLimiterEntries: this.#limiter.size,
      rejectedConnections: rejections.total,
      rejectedConnectionsByReason: rejections.byReason,
      runtimeSysBytes: memory.rss,
      supersededRegistrations: this.#superseded,
    };
    if (detailed) {
      m.topHosts = this.#topHosts();
    }
    ex.json(200, Object.fromEntries(Object.entries(m).sort(([a], [b]) => (a < b ? -1 : 1))));
  }

  #pairCount(): number {
    return this.pending + this.active;
  }

  async shutdown(): Promise<void> {
    const start = performance.now();
    const hardStop = start + drainTimeout - hardStopReserve;
    const closeAt = hardStop - closeReserve;
    this.draining = true;
    logLine("relay: draining");
    this.#http.closeIdleConnections();
    while (this.#pairCount() > 0 && performance.now() < closeAt) {
      await sleep(20);
    }
    const remaining = this.#pairCount();
    const conns = [...this.conns];
    for (const ws of conns) {
      ws.writeClose(Close.goingAway, "relay shutting down");
    }
    while (this.conns.size > 0 && performance.now() < hardStop) {
      await sleep(20);
    }
    for (const ws of conns) {
      ws.destroy();
    }
    this.#http.close();
    this.#private.close();
    this.#http.closeAllConnections();
    this.#private.closeAllConnections();
    logLine(`relay: stopped after ${formatDuration(Math.round(performance.now() - start))} with ${remaining} pairs force-closed`);
  }
}

function activeHandles(): number {
  return typeof process.getActiveResourcesInfo === "function" ? process.getActiveResourcesInfo().length : 0;
}

function safeEqual(a: string, b: string): boolean {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

function verifyAuth(op: number, data: Buffer, pub: Buffer, challenge: string): string {
  if (op !== Op.text) {
    return "invalid_frame_type";
  }
  let fields: Record<string, unknown>;
  try {
    fields = JSON.parse(data.toString()) ?? {};
  } catch {
    return "invalid_auth_json";
  }
  if (typeof fields !== "object" || Array.isArray(fields)) {
    return "invalid_auth_json";
  }
  if (fields.type !== "authenticate") {
    return "invalid_auth_type";
  }
  const sig = typeof fields.signature === "string" ? decodeB64(fields.signature, 64) : undefined;
  if (!sig) {
    return "invalid_signature_encoding";
  }
  const key = crypto.createPublicKey({ key: { kty: "OKP", crv: "Ed25519", x: pub.toString("base64url") }, format: "jwk" });
  for (const prefix of signaturePrefixes) {
    if (crypto.verify(null, Buffer.from(prefix + challenge), key, sig)) {
      return "";
    }
  }
  return "signature_mismatch";
}
