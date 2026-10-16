import { createHash } from "node:crypto";
import {
  createServer,
  STATUS_CODES,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import type { Socket } from "node:net";
import { normalize } from "node:path/posix";
import { clientIP, clientKey, splitAddress } from "./address.js";
import { Gate, Limiter } from "./admission.js";
import type { Config } from "./config.js";
import type { Metrics } from "./metrics.js";
import { Deadline } from "./deadline.js";
import { Dashboard } from "./dashboard.js";
import { Pair, Relay, decodeB64 } from "./relay.js";
import { Peer, socketHighWaterMark } from "./transport/peer.js";

export class Reply {
  readonly headers: Record<string, string> = {};
  constructor(
    readonly request: IncomingMessage,
    readonly response?: ServerResponse,
    readonly head = Buffer.alloc(0),
  ) {}

  send(status: number, body: string | Buffer): void {
    const buffer = typeof body === "string" ? Buffer.from(body) : body;
    this.headers["Content-Length"] = String(buffer.length);
    if (this.response) {
      this.response.writeHead(status, this.headers);
      this.response.end(this.request.method === "HEAD" ? undefined : buffer);
    } else {
      const headers = Object.entries({ ...this.headers, Connection: "close" })
        .map(([k, v]) => `${k}: ${v}\r\n`)
        .join("");
      this.request.socket.end(
        `HTTP/1.1 ${status} ${STATUS_CODES[status]}\r\n${headers}\r\n` +
          (this.request.method === "HEAD" ? "" : buffer.toString()),
      );
    }
  }

  json(status: number, body: unknown): void {
    this.headers["Content-Type"] = "application/json";
    this.headers["Cache-Control"] = "no-store";
    this.send(status, JSON.stringify(body, null, 2) + "\n");
  }

  text(status: number, body: string): void {
    this.headers["Content-Type"] = "text/plain; charset=utf-8";
    this.headers["X-Content-Type-Options"] = "nosniff";
    this.send(status, body + "\n");
  }
}

export class RelayServer {
  readonly relay: Relay;
  readonly gate: Gate;
  readonly limiter: Limiter;
  readonly publicLimiter: Limiter;
  readonly dashboard: Dashboard;
  readonly public: Server;
  readonly private: Server;
  private readonly httpTimers = new Map<Socket, Deadline>();

  constructor(readonly cfg: Config) {
    this.relay = new Relay(cfg);
    this.gate = new Gate(cfg.maxConnectionsPerIP);
    this.limiter = new Limiter(cfg.admissionRate);
    this.publicLimiter = new Limiter(cfg.admissionRate);
    this.dashboard = new Dashboard(() => this.metrics(false));
    this.public = this.createHTTP(false);
    this.private = this.createHTTP(true);
  }

  private createHTTP(privateListener: boolean): Server {
    const server = createServer(
      {
        highWaterMark: socketHighWaterMark,
        maxHeaderSize: 1 << 20,
        headersTimeout: 5000,
        requestTimeout: 5000,
        keepAliveTimeout: 5000,
        connectionsCheckingInterval: 1000,
      },
      (request, response) => {
        response.on("finish", () =>
          this.httpTimers.get(request.socket)?.set(5000),
        );
        this.handle(new Reply(request, response), privateListener);
      },
    );
    server.on("connection", (socket) => {
      const deadline = new Deadline(() => socket.destroy());
      this.httpTimers.set(socket, deadline);
      deadline.set(5000);
      socket.on("close", () => {
        deadline.cancel();
        this.httpTimers.delete(socket);
      });
      socket.on("error", () => {});
    });
    server.on("upgrade", (request, _socket, head) =>
      this.handle(new Reply(request, undefined, head), privateListener),
    );
    server.on("clientError", (_error, socket) => socket.destroy());
    return server;
  }

  private handle(reply: Reply, privateListener: boolean): void {
    const request = reply.request;
    let url: URL;
    try {
      url = new URL(request.url ?? "/", "http://relay.local");
    } catch {
      reply.text(400, "Bad Request");
      return;
    }
    let path: string;
    try {
      path = decodeURIComponent((request.url ?? "/").split("?")[0]!);
    } catch {
      reply.text(400, "Bad Request");
      return;
    }
    if (!path.startsWith("/")) {
      reply.text(400, "Bad Request");
      return;
    }
    const cleaned = normalize(path);
    if (cleaned !== path || path === "/dashboard") {
      reply.headers.Location =
        (path === "/dashboard" ? "/dashboard/" : cleaned)
          .split("/")
          .map(encodeURIComponent)
          .join("/") + url.search;
      reply.send(307, "");
      return;
    }
    const admin = path === "/healthz" || path === "/metrics";
    const protocol = ["/v1/control", "/v1/connect", "/v1/accept"].includes(
      path,
    );
    const dashboard = path.startsWith("/dashboard/");
    if (
      (privateListener && !admin) ||
      (!privateListener && !admin && !protocol && !dashboard) ||
      (path === "/healthz" && !privateListener && this.cfg.privateAddr)
    ) {
      reply.text(404, "404 page not found");
      return;
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      reply.headers.Allow = "GET, HEAD";
      reply.text(405, "Method Not Allowed");
      return;
    }
    const key = clientKey(clientIP(request, this.cfg.trustedProxies));
    if (dashboard) {
      this.dashboard.serve(reply, path, () => this.publicLimiter.allow(key));
      return;
    }
    if (admin) {
      if (
        !privateListener &&
        this.cfg.privateAddr &&
        path === "/metrics" &&
        !this.publicLimiter.allow(key)
      ) {
        reply.json(429, { error: "rate limited" });
        return;
      }
      if (path === "/healthz")
        reply.json(this.relay.draining ? 503 : 200, {
          status: this.relay.draining ? "draining" : "ok",
        });
      else
        reply.json(200, this.metrics(privateListener || !this.cfg.privateAddr));
      return;
    }
    const lease = this.gate.acquire(key);
    if (typeof lease !== "function") {
      this.reject(reply, lease.status, lease.reason);
      return;
    }
    const socket = request.socket;
    const release = () => {
      lease();
      socket.off("close", release);
    };
    socket.once("close", release);
    reply.response?.once("finish", release);
    if (this.relay.draining && path !== "/v1/accept") {
      this.reject(reply, 503, "draining");
      return;
    }
    if (!this.limiter.allow(key)) {
      this.reject(reply, 429, "rate limited");
      return;
    }
    if (path === "/v1/control") {
      const pub = decodeB64(url.searchParams.get("publicKey") ?? "", 32);
      if (!pub) {
        this.reject(reply, 400, "invalid publicKey");
        return;
      }
      const peer = this.upgrade(reply, 4096, 5000);
      if (peer) this.relay.authenticate(peer, pub);
    } else if (path === "/v1/connect") {
      const host = this.relay.hosts.get(
        url.searchParams.get("endpointId") ?? "",
      );
      if (!host) {
        this.reject(reply, 404, "endpoint not found");
        return;
      }
      if (host.clientSlots >= this.cfg.maxClientsPerHost) {
        this.reject(reply, 503, "client capacity reached");
        return;
      }
      if (
        (host.clientCounts.get(key) ?? 0) >= this.cfg.maxClientsPerHostPerIP
      ) {
        this.reject(reply, 429, "client capacity for host reached");
        return;
      }
      const pair = new Pair(this.relay, host, key);
      const peer = this.upgrade(
        reply,
        this.cfg.maxMessageBytes,
        this.cfg.deliveryTimeout,
      );
      if (!peer) {
        pair.close({ code: 1011, reason: "upgrade failed" });
        return;
      }
      pair.attach(peer, false);
    } else {
      const pair = this.relay.pairs.get(
        url.searchParams.get("connectionId") ?? "",
      );
      if (!pair || pair.host.id !== url.searchParams.get("endpointId")) {
        this.reject(reply, 404, "connection not found");
        return;
      }
      if (!pair.claim(url.searchParams.get("token") ?? "")) {
        this.reject(reply, 403, "invalid token");
        return;
      }
      const peer = this.upgrade(
        reply,
        this.cfg.maxMessageBytes,
        this.cfg.deliveryTimeout,
      );
      if (!peer) {
        pair.close({ code: 1011, reason: "host accept failed" });
        return;
      }
      pair.attach(peer, true);
    }
  }

  private reject(reply: Reply, status: number, reason: string): void {
    this.relay.rejection(reason);
    reply.json(status, { error: reason });
  }

  private upgrade(
    reply: Reply,
    limit: number,
    delivery: number,
  ): Peer | undefined {
    const request = reply.request;
    const tokens = (name: string, expected: string) =>
      String(request.headers[name] ?? "")
        .toLowerCase()
        .split(",")
        .some((s) => s.trim() === expected);
    if (
      !tokens("connection", "upgrade") ||
      !tokens("upgrade", "websocket") ||
      request.method !== "GET"
    ) {
      reply.text(400, "Bad Request");
      return;
    }
    if (!tokens("sec-websocket-version", "13")) {
      reply.headers["Sec-WebSocket-Version"] = "13";
      reply.text(400, "Bad Request");
      return;
    }
    const key = request.headers["sec-websocket-key"];
    if (
      typeof key !== "string" ||
      !/^[A-Za-z0-9+/]{22}==$/.test(key) ||
      Buffer.from(key, "base64").length !== 16
    ) {
      reply.text(400, "Bad Request");
      return;
    }
    if (reply.response || reply.head.length) {
      request.socket.destroy();
      return;
    }
    this.httpTimers.get(request.socket)?.cancel();
    this.httpTimers.delete(request.socket);
    const accept = createHash("sha1")
      .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
      .digest("base64");
    request.socket.write(
      `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
    const peer = new Peer(request.socket, limit, delivery);
    this.relay.peers.add(peer);
    request.socket.once("close", () => this.relay.peers.delete(peer));
    return peer;
  }

  metrics(detailed: boolean): Metrics {
    return {
      ...this.relay.metrics(detailed),
      openConnections: this.gate.total,
      openConnectionIPs: this.gate.counts.size,
      rateLimiterEntries: this.limiter.entries.size,
      maxConnectionsPerClient: this.cfg.maxConnectionsPerIP,
    };
  }

  async listen(server: Server, address: string): Promise<string> {
    const { host, port } = splitAddress(address);
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen({ host: host || "::", port }, () => {
        server.off("error", reject);
        resolve();
      });
    });
    const bound = server.address();
    if (!bound || typeof bound === "string")
      throw new Error("invalid bound address");
    return `${bound.address.includes(":") ? `[${bound.address}]` : bound.address}:${bound.port}`;
  }

  async shutdown(): Promise<number> {
    this.relay.draining = true;
    this.relay.eventLoop.disable();
    this.public.closeIdleConnections();
    const started = performance.now();
    const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 20));
    while (this.relay.pairs.size && performance.now() - started < 4150)
      await tick();
    const remaining = this.relay.pairs.size;
    for (const peer of this.relay.peers)
      peer.close(
        1001,
        "relay shutting down",
        Math.max(1, 4900 - (performance.now() - started)),
      );
    while (this.relay.peers.size && performance.now() - started < 4900)
      await tick();
    for (const peer of this.relay.peers) peer.socket.destroy();
    for (const [socket, timer] of this.httpTimers) {
      timer.cancel();
      socket.destroy();
    }
    this.public.close();
    if (this.private.listening) this.private.close();
    return remaining;
  }
}
