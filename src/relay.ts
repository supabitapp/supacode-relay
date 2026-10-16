import {
  createHash,
  createPublicKey,
  randomBytes,
  timingSafeEqual,
  verify,
} from "node:crypto";
import { getActiveResourcesInfo, memoryUsage } from "node:process";
import { monitorEventLoopDelay } from "node:perf_hooks";
import type { Config } from "./config.js";
import type { RelayMetrics } from "./metrics.js";
import { Deadline } from "./deadline.js";
import { Peer } from "./transport/peer.js";
import type { CloseMessage } from "./transport/decoder.js";

export function decodeB64(value: string, length: number): Buffer | undefined {
  if (
    value.length !== Math.ceil((length * 4) / 3) ||
    !/^[A-Za-z0-9_-]+$/.test(value)
  )
    return;
  const data = Buffer.from(value, "base64url");
  if (data.length === length && data.toString("base64url") === value)
    return data;
}

export class ControlQueue {
  private items: Buffer[] = [];
  private bytes = 0;
  private inFlight: Buffer | undefined;
  private final: CloseMessage | undefined;
  constructor(private readonly peer: Peer) {}

  push(value: unknown): boolean {
    if (this.final) return true;
    const data = Buffer.from(JSON.stringify(value));
    if (this.items.length >= 256 || data.length > 65536 - this.bytes)
      return false;
    this.bytes += data.length;
    this.items.push(data);
    this.pump();
    return true;
  }

  finish(cause: CloseMessage): void {
    this.final ??= cause;
    for (const item of this.items) this.bytes -= item.length;
    this.items = [];
    this.pump();
  }

  dispose(): void {
    for (const item of this.items) this.bytes -= item.length;
    this.items = [];
    this.final ??= { code: 1001, reason: "peer disconnected" };
  }

  get retainedBackings(): ArrayBufferLike[] {
    return [...this.items, ...(this.inFlight ? [this.inFlight] : [])].map(
      (buffer) => buffer.buffer,
    );
  }

  private pump(): void {
    if (this.inFlight || this.peer.closed) return;
    const data = this.items.shift();
    if (!data) {
      if (this.final) this.peer.close(this.final.code, this.final.reason);
      return;
    }
    this.inFlight = data;
    this.peer.send(1, true, data, (error) => {
      this.bytes -= data.length;
      this.inFlight = undefined;
      if (error) this.peer.fail({ code: 1013, reason: "peer write timeout" });
      else this.pump();
    });
  }
}

export class Host {
  readonly queue: ControlQueue;
  readonly pairs = new Set<Pair>();
  readonly clientCounts = new Map<string, number>();
  clientSlots = 0;
  bytesIn = 0;
  bytesOut = 0;
  gone = false;

  constructor(
    readonly id: string,
    readonly peer: Peer,
  ) {
    this.queue = new ControlQueue(peer);
  }

  notify(value: unknown): void {
    if (!this.gone && !this.queue.push(value))
      this.queue.finish({ code: 1013, reason: "control queue limit exceeded" });
  }
}

export class Relay {
  readonly hosts = new Map<string, Host>();
  readonly pairs = new Map<string, Pair>();
  readonly peers = new Set<Peer>();
  readonly rejections = new Map<string, number>();
  pending = 0;
  active = 0;
  clientSlots = 0;
  forwardedMessages = 0;
  forwardedBytes = 0;
  superseded = 0;
  draining = false;
  readonly eventLoop = monitorEventLoopDelay({ resolution: 20 });

  constructor(readonly cfg: Config) {
    this.eventLoop.enable();
  }

  rejection(reason: string): void {
    this.rejections.set(reason, (this.rejections.get(reason) ?? 0) + 1);
  }

  authenticate(peer: Peer, pub: Buffer): void {
    const id = createHash("sha256").update(pub).digest("hex");
    const nonce = randomBytes(32).toString("base64url");
    const key = createPublicKey({
      key: { kty: "OKP", crv: "Ed25519", x: pub.toString("base64url") },
      format: "jwk",
    });
    let authenticated = false;
    const fail = () => {
      this.rejection("authentication failed");
      peer.close(1008, "authentication failed");
    };
    const auth = new Deadline(fail);
    peer.onClosed = () => auth.cancel();
    peer.onMessage = (opcode, data) => {
      let signature: Buffer | undefined;
      try {
        const parsed: unknown = JSON.parse(data.toString());
        if (
          opcode !== 1 ||
          !parsed ||
          typeof parsed !== "object" ||
          Array.isArray(parsed)
        ) {
          fail();
          return;
        }
        const fields = Object.fromEntries(
          Object.entries(parsed).map(([k, v]) => [k.toLowerCase(), v]),
        );
        if (
          fields.type === "authenticate" &&
          typeof fields.signature === "string"
        )
          signature = decodeB64(fields.signature, 64);
      } catch {
        fail();
        return;
      }
      if (
        !signature ||
        !["supaterm-relay-v1\n", "supacode-relay-v1\n"].some((prefix) =>
          verify(null, Buffer.from(`${prefix}${id}\n${nonce}`), key, signature),
        )
      ) {
        fail();
        return;
      }
      auth.cancel();
      authenticated = true;
      if (this.draining) {
        peer.close(1001, "relay draining");
        return;
      }
      const host = new Host(id, peer);
      const old = this.hosts.get(id);
      this.hosts.set(id, host);
      if (old) {
        this.superseded++;
        old.queue.finish({ code: 4001, reason: "registration superseded" });
      }
      peer.onMessage = () =>
        host.queue.finish({ code: 1008, reason: "unexpected control message" });
      peer.onFailure = () => this.removeHost(host);
      peer.onClosed = () => {
        this.removeHost(host);
        host.queue.dispose();
      };
      peer.enableHeartbeat(this.cfg.heartbeat);
      host.notify({ type: "registered", endpointId: id });
    };
    peer.send(
      1,
      true,
      Buffer.from(JSON.stringify({ type: "challenge", nonce })),
      (error) => {
        if (error) {
          peer.socket.destroy();
          return;
        }
        if (!authenticated && !peer.closing) {
          auth.set(this.cfg.authTimeout);
          peer.start();
        }
      },
    );
  }

  private removeHost(host: Host): void {
    if (host.gone) return;
    host.gone = true;
    if (this.hosts.get(host.id) === host) this.hosts.delete(host.id);
    host.queue.dispose();
    for (const pair of [...host.pairs])
      pair.close({ code: 1001, reason: "host offline" });
  }

  metrics(detailed: boolean): RelayMetrics {
    const memory = memoryUsage();
    const backing = new Set<ArrayBufferLike>();
    let readableBytes = 0;
    for (const peer of this.peers) {
      readableBytes += peer.socket.readableLength;
      for (const allocation of peer.retainedBackings) backing.add(allocation);
    }
    for (const host of this.hosts.values()) {
      for (const allocation of host.queue.retainedBackings)
        backing.add(allocation);
    }
    const result: RelayMetrics = {
      activeHosts: this.hosts.size,
      activePairs: this.active,
      pendingPairs: this.pending,
      clientSlots: this.clientSlots,
      closingPairs: this.clientSlots - this.pending - this.active,
      openSockets: this.peers.size,
      forwardedMessages: this.forwardedMessages,
      forwardedBytes: this.forwardedBytes,
      supersededRegistrations: this.superseded,
      rejectedConnections: [...this.rejections.values()].reduce(
        (sum, n) => sum + n,
        0,
      ),
      rejectedConnectionsByReason: Object.fromEntries(this.rejections),
      draining: this.draining,
      metricsSchemaVersion: 2,
      runtime: "node",
      nodeVersion: process.versions.node,
      rssBytes: memory.rss,
      heapUsedBytes: memory.heapUsed,
      heapTotalBytes: memory.heapTotal,
      externalBytes: memory.external,
      arrayBufferBytes: memory.arrayBuffers,
      activeResources: getActiveResourcesInfo().length,
      bufferedBytes:
        readableBytes +
        [...backing].reduce(
          (sum, allocation) => sum + allocation.byteLength,
          0,
        ),
      eventLoopDelayP99Ms: this.eventLoop.percentile(99) / 1e6,
      eventLoopDelayMaxMs: this.eventLoop.max / 1e6,
      dataBufferBytesPerSocket: 4096 + (16 << 10),
    };
    if (detailed)
      result.topHosts = [...this.hosts.values()]
        .filter((host) => host.bytesIn + host.bytesOut > 0)
        .sort((a, b) => b.bytesIn + b.bytesOut - a.bytesIn - a.bytesOut)
        .slice(0, 20)
        .map((host) => ({
          endpoint: host.id.slice(0, 16),
          bytesIn: host.bytesIn,
          bytesOut: host.bytesOut,
          pairs: host.pairs.size,
        }));
    return result;
  }
}

export class Pair {
  readonly id = randomBytes(16).toString("base64url");
  readonly token = randomBytes(32).toString("base64url");
  readonly timer: Deadline;
  state: "pending" | "accepting" | "active" | "closed" = "pending";
  client: Peer | undefined;
  hostPeer: Peer | undefined;
  private announced = false;
  private released = false;

  constructor(
    private readonly relay: Relay,
    readonly host: Host,
    private readonly key: string,
  ) {
    relay.pairs.set(this.id, this);
    host.pairs.add(this);
    host.clientSlots++;
    host.clientCounts.set(key, (host.clientCounts.get(key) ?? 0) + 1);
    relay.clientSlots++;
    relay.pending++;
    this.timer = new Deadline(() =>
      this.close({ code: 1013, reason: "pair timeout" }),
    );
    this.timer.set(relay.cfg.pairTimeout);
  }

  claim(token: string): boolean {
    if (token.length !== this.token.length) return false;
    const data = Buffer.from(token);
    const expected = Buffer.from(this.token);
    if (
      data.length !== expected.length ||
      !timingSafeEqual(data, expected) ||
      this.state !== "pending" ||
      !this.client
    )
      return false;
    this.state = "accepting";
    return true;
  }

  attach(peer: Peer, accepted: boolean): void {
    if (accepted) this.hostPeer = peer;
    else this.client = peer;
    peer.onFailure = (cause) => this.close(cause);
    peer.onClosed = () => this.release();
    peer.enableHeartbeat(this.relay.cfg.heartbeat);
    if (!accepted) {
      this.announced = true;
      this.host.notify({
        type: "incoming",
        connectionId: this.id,
        token: this.token,
      });
      return;
    }
    this.state = "active";
    this.timer.cancel();
    this.relay.pending--;
    this.relay.active++;
    this.forward(this.client!, peer, true);
    this.forward(peer, this.client!, false);
    this.client!.start();
    peer.start();
  }

  private forward(source: Peer, destination: Peer, toHost: boolean): void {
    source.forwardTo(destination, (bytes) => {
      this.relay.forwardedMessages++;
      this.relay.forwardedBytes += bytes;
      if (toHost) this.host.bytesIn += bytes;
      else this.host.bytesOut += bytes;
    });
  }

  close(cause: CloseMessage): void {
    if (this.state === "closed") return;
    if (this.state === "active") this.relay.active--;
    else this.relay.pending--;
    this.state = "closed";
    this.timer.cancel();
    this.relay.pairs.delete(this.id);
    this.host.pairs.delete(this);
    if (this.announced)
      this.host.notify({ type: "closed", connectionId: this.id });
    this.client?.close(cause.code, cause.reason);
    this.hostPeer?.close(cause.code, cause.reason);
    this.release();
  }

  private release(): void {
    if (
      this.released ||
      this.state !== "closed" ||
      (this.client && !this.client.closed) ||
      (this.hostPeer && !this.hostPeer.closed)
    )
      return;
    this.released = true;
    this.host.clientSlots--;
    this.relay.clientSlots--;
    const n = this.host.clientCounts.get(this.key)! - 1;
    if (n) this.host.clientCounts.set(this.key, n);
    else this.host.clientCounts.delete(this.key);
  }
}
