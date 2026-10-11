import { spawn, execFile, type ChildProcess } from "node:child_process";
import { createHash, generateKeyPairSync, randomFillSync, sign } from "node:crypto";
import { createInterface } from "node:readline";
import { cpus, platform, release } from "node:os";
import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { WebSocket, type RawData } from "ws";

type Dist = { p50: number; p99: number; max: number };
type Usage = { cpuPercentOfOneCore: number; rssPeakMiB: number };
type Result = { payloadBytes: number; clients: number; messagesInWindow: number; messagesPerSec: number; payloadMiBPerSec: number; rttMicros: Dist; failures: number; corrupt: number; processes?: Record<string, Usage> };
type Host = { id: string; control: WebSocket; sockets: Set<WebSocket>; close: () => Promise<void> };
type Child = { process: ChildProcess; address: string; command: string[] };

type Options = { url?: string; spawn: boolean; relayBin: string; relayArgs: string[]; payloads: number[]; clients: number[]; inflight: number; warmupMs: number; durationMs: number; hosts: number; output?: string; label: string };

function parseList(value: string): number[] {
  const values = value.split(",").map((part) => Number(part.trim()));
  if (values.some((value) => !Number.isInteger(value) || value <= 0)) throw new Error(`invalid list ${value}`);
  return values;
}

function options(): Options {
  const out: Options = { spawn: false, relayBin: "node", relayArgs: [], payloads: [64, 1024, 65536], clients: [1, 32], inflight: 4, warmupMs: 2000, durationMs: 5000, hosts: 4, label: "typescript-driver" };
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    const next = (): string => {
      const value = args[++i];
      if (value === undefined) throw new Error(`${arg} requires a value`);
      return value;
    };
    switch (arg) {
      case "--url": out.url = next(); break;
      case "--spawn": out.spawn = true; break;
      case "--relay-bin": out.relayBin = next(); break;
      case "--relay-arg": out.relayArgs.push(next()); break;
      case "--payloads": out.payloads = parseList(next()); break;
      case "--clients": out.clients = parseList(next()); break;
      case "--inflight": out.inflight = Number(next()); break;
      case "--warmup-ms": out.warmupMs = Number(next()); break;
      case "--duration-ms": out.durationMs = Number(next()); break;
      case "--hosts": out.hosts = Number(next()); break;
      case "--output": out.output = next(); break;
      case "--label": out.label = next(); break;
      default: throw new Error(`unknown option ${arg}`);
    }
  }
  if (out.spawn && out.url) throw new Error("--spawn and --url are mutually exclusive");
  if (!out.spawn && !out.url) throw new Error("--url or --spawn is required");
  if (out.inflight <= 0 || out.hosts <= 0 || out.warmupMs < 0 || out.durationMs <= 0) throw new Error("hosts, inflight, duration must be positive and warmup must not be negative");
  if (out.payloads.some((size) => size < 16)) throw new Error("payloads must be at least 16 bytes");
  return out;
}

function sleep(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
function messageBuffer(data: RawData | string): Buffer { if (typeof data === "string") return Buffer.from(data); if (Buffer.isBuffer(data)) return data; if (Array.isArray(data)) return Buffer.concat(data); return Buffer.from(new Uint8Array(data)); }
function b64(value: Buffer): string { return value.toString("base64url"); }
function wsOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    if (ws.readyState === WebSocket.OPEN) { resolve(); return; }
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
}
class MessageInbox {
  private readonly queued: Array<{ data: Buffer; binary: boolean }> = [];
  private readonly waiters: Array<(message: { data: Buffer; binary: boolean }) => void> = [];
  constructor(ws: WebSocket) {
    ws.on("message", (data, binary) => {
      const message = { data: messageBuffer(data), binary };
      const waiter = this.waiters.shift();
      if (waiter) waiter(message);
      else this.queued.push(message);
    });
  }
  next(timeoutMs = 5000): Promise<{ data: Buffer; binary: boolean }> {
    const message = this.queued.shift();
    if (message) return Promise.resolve(message);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("WebSocket message timeout")), timeoutMs);
      this.waiters.push((value) => { clearTimeout(timer); resolve(value); });
    });
  }
}

async function register(base: string): Promise<Host> {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicDer = publicKey.export({ format: "der", type: "spki" }) as Buffer;
  const rawPublic = publicDer.subarray(-32);
  const id = createHash("sha256").update(rawPublic).digest("hex");
  const control = new WebSocket(`${base}/v1/control?publicKey=${encodeURIComponent(b64(rawPublic))}`);
  const inbox = new MessageInbox(control);
  await wsOpen(control);
  const challenge = JSON.parse((await inbox.next()).data.toString("utf8")) as { type: string; nonce: string };
  const signature = sign(null, Buffer.from(`supacode-relay-v1\n${id}\n${challenge.nonce}`), privateKey);
  control.send(JSON.stringify({ type: "authenticate", signature: b64(signature) }));
  const registered = JSON.parse((await inbox.next()).data.toString("utf8")) as { type: string; endpointId: string };
  if (challenge.type !== "challenge" || registered.type !== "registered" || registered.endpointId !== id) throw new Error("host registration failed");
  const sockets = new Set<WebSocket>();
  const host: Host = {
    id,
    control,
    sockets,
    close: async () => {
      for (const socket of sockets) socket.close();
      control.close();
      await sleep(10);
    },
  };
  control.on("message", (data, binary) => {
    if (binary) return;
    const event = JSON.parse(messageBuffer(data).toString("utf8")) as { type?: string; connectionId?: string; token?: string };
    if (event.type !== "incoming" || !event.connectionId || !event.token) return;
    const accepted = new WebSocket(`${base}/v1/accept?endpointId=${encodeURIComponent(id)}&connectionId=${encodeURIComponent(event.connectionId)}&token=${encodeURIComponent(event.token)}`);
    sockets.add(accepted);
    accepted.on("message", (message, isBinary) => {
      if (accepted.readyState === WebSocket.OPEN) accepted.send(message, { binary: isBinary });
    });
    accepted.on("close", () => sockets.delete(accepted));
  });
  return host;
}

class Sampler {
  private readonly pid: number;
  private readonly start = performance.now();
  private readonly timer: NodeJS.Timeout;
  private cpu0 = 0;
  private baselineSet = false;
  private peakRss = 0;
  private stopped = false;
  constructor(pid: number) {
    this.pid = pid;
    this.cpu0 = 0;
    this.sample();
    this.timer = setInterval(() => this.sample(), 250);
  }
  private sample(): void {
    execFile("ps", ["-o", "rss=,time=", "-p", String(this.pid)], (error, stdout) => {
      if (error || this.stopped) return;
      const fields = stdout.trim().split(/\s+/);
      if (fields.length !== 2) return;
      const rss = Number(fields[0]);
      const cpu = parseCpu(fields[1]);
      if (Number.isFinite(rss)) this.peakRss = Math.max(this.peakRss, rss);
      if (!this.baselineSet && Number.isFinite(cpu)) { this.cpu0 = cpu; this.baselineSet = true; }
    });
  }
  async stop(): Promise<Usage> {
    this.stopped = true;
    clearInterval(this.timer);
    await sleep(50);
    return new Promise((resolve) => execFile("ps", ["-o", "rss=,time=", "-p", String(this.pid)], (error, stdout) => {
      const fields = stdout.trim().split(/\s+/);
      const rss = fields.length === 2 ? Number(fields[0]) : 0;
      const cpu = fields.length === 2 ? parseCpu(fields[1]) : this.cpu0;
      const elapsed = Math.max(performance.now() - this.start, 1) / 1000;
      resolve({ cpuPercentOfOneCore: round(100 * Math.max(0, cpu - this.cpu0) / elapsed), rssPeakMiB: round(Math.max(this.peakRss, Number.isFinite(rss) ? rss : 0) / 1024) });
    }));
  }
}
function parseCpu(value: string): number {
  const parts = value.replace("-", ":").split(":").map(Number);
  if (parts.some((part) => !Number.isFinite(part))) return 0;
  return parts.reduce((total, part) => total * 60 + part, 0);
}
function round(value: number): number { return Math.round(value * 10) / 10; }
function runtimeName(): "node" | "bun" { return (globalThis as { Bun?: unknown }).Bun ? "bun" : "node"; }
function runtimeVersion(): string { return runtimeName() === "bun" ? String((globalThis as { Bun?: { version?: string } }).Bun?.version ?? "unknown") : process.version; }
function percentile(values: number[], p: number): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return round(sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)]);
}
function summary(values: number[]): Dist { return { p50: percentile(values, 0.5), p99: percentile(values, 0.99), max: percentile(values, 1) }; }

class ClientRun {
  readonly ws: WebSocket;
  readonly rtts: number[] = [];
  messages = 0;
  bytes = 0;
  failures = 0;
  corrupt = 0;
  sent = 0;
  got = 0;
  private expected = 0n;
  private permits: number;
  private readonly waiters: Array<() => void> = [];
  private ended = false;
  constructor(ws: WebSocket, private readonly block: Buffer, private readonly base: number, private readonly warmupNs: bigint, private readonly measurementEndNs: bigint, private readonly stopAt: number, private readonly inflight: number) {
    this.ws = ws;
    this.permits = inflight;
    ws.on("message", (data, binary) => this.receive(data, binary));
    ws.on("error", () => { this.failures += 1; this.ended = true; });
    ws.on("close", () => { this.ended = true; this.releaseAll(); });
  }
  private acquire(): Promise<void> {
    if (this.permits > 0) { this.permits -= 1; return Promise.resolve(); }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  private release(): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter();
    else this.permits += 1;
  }
  private releaseAll(): void { while (this.waiters.length) this.waiters.shift()?.(); }
  private receive(raw: RawData, binary: boolean): void {
    const data = messageBuffer(raw);
    const nowNs = BigInt(Math.floor((performance.now() - this.base) * 1e6));
    if (!binary || data.length !== this.block.length || data.readBigUInt64BE(0) !== this.expected || !data.subarray(16).equals(this.block.subarray(16))) this.corrupt += 1;
    this.expected += 1n;
    if (data.length >= 16) {
      const sentAt = data.readBigUInt64BE(8);
      if (sentAt >= this.warmupNs && sentAt < this.measurementEndNs) this.rtts.push(Number(nowNs - sentAt) / 1000);
    }
    if (nowNs >= this.warmupNs && nowNs < this.measurementEndNs) { this.messages += 1; this.bytes += data.length; }
    this.got += 1;
    this.release();
  }
  async run(): Promise<void> {
    for (let sequence = 0n; performance.now() < this.stopAt && !this.ended; sequence += 1n) {
      await this.acquire();
      if (this.ended) { this.failures += 1; break; }
      const message = Buffer.from(this.block);
      message.writeBigUInt64BE(sequence, 0);
      message.writeBigUInt64BE(BigInt(Math.floor((performance.now() - this.base) * 1e6)), 8);
      try {
        this.ws.send(message, { binary: true }, (error?: Error) => { if (error) this.failures += 1; });
        this.sent += 1;
      } catch {
        this.failures += 1;
        this.release();
        break;
      }
    }
    const drainUntil = performance.now() + 5000;
    while (this.got < this.sent && performance.now() < drainUntil && !this.ended) await sleep(1);
    this.ws.close();
    await sleep(10);
  }
}

async function spawnRelay(opt: Options): Promise<Child> {
  const child = spawn(opt.relayBin, opt.relayArgs, { env: { ...process.env, RELAY_ADDR: "127.0.0.1:0", RELAY_PRIVATE_ADDR: "", RELAY_ADMISSION_RATE: "1000000", RELAY_MAX_CONNS: "100000", RELAY_MAX_CONNS_PER_IP: "100000" }, stdio: ["ignore", "pipe", "inherit"] });
  const lines = createInterface({ input: child.stdout! });
  const address = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("relay did not report listening")), 5000);
    lines.on("line", (line) => {
      try {
        const event = JSON.parse(line) as { event?: string; address?: string; listener?: string };
        if (event.event === "listening" && event.listener !== "private" && event.address) { clearTimeout(timer); resolve(event.address); }
      } catch { }
    });
    child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`relay exited before listening (${code})`)); });
  });
  if (!child.pid) throw new Error("relay process has no pid");
  return { process: child, address, command: [opt.relayBin, ...opt.relayArgs] };
}

async function runCase(base: string, hosts: Host[], size: number, count: number, opt: Options, relayPid?: number): Promise<Result> {
  const block = Buffer.alloc(size);
  randomFillSync(block);
  const sockets: WebSocket[] = [];
  await Promise.all(Array.from({ length: count }, async (_, index) => {
    const ws = new WebSocket(`${base}/v1/connect?endpointId=${hosts[index % hosts.length].id}`);
    await wsOpen(ws);
    sockets[index] = ws;
  }));
  const baseTime = performance.now();
  const warmupNs = BigInt(Math.floor(opt.warmupMs * 1e6));
  const endNs = BigInt(Math.floor((opt.warmupMs + opt.durationMs) * 1e6));
  const stopAt = baseTime + opt.warmupMs + opt.durationMs;
  const clients = sockets.map((ws) => new ClientRun(ws, block, baseTime, warmupNs, endNs, stopAt, opt.inflight));
  await sleep(opt.warmupMs);
  const sampler = relayPid ? new Sampler(relayPid) : undefined;
  await Promise.all(clients.map((client) => client.run()));
  const processes = sampler ? { relay: await sampler.stop() } : undefined;
  const rtts = clients.flatMap((client) => client.rtts);
  const messages = clients.reduce((sum, client) => sum + client.messages, 0);
  const bytes = clients.reduce((sum, client) => sum + client.bytes, 0);
  const failures = clients.reduce((sum, client) => sum + client.failures + Math.max(0, client.sent - client.got), 0);
  const corrupt = clients.reduce((sum, client) => sum + client.corrupt, 0);
  return { payloadBytes: size, clients: count, messagesInWindow: messages, messagesPerSec: round(messages / (opt.durationMs / 1000)), payloadMiBPerSec: round(bytes / (opt.durationMs / 1000) / (1 << 20)), rttMicros: summary(rtts), failures, corrupt, ...(processes ? { processes } : {}) };
}

async function main(): Promise<void> {
  const opt = options();
  let child: Child | undefined;
  let base = opt.url;
  if (opt.spawn) {
    child = await spawnRelay(opt);
    base = `ws://${child.address}`;
  }
  if (!base) throw new Error("missing relay URL");
  const hosts: Host[] = [];
  try {
    for (let i = 0; i < opt.hosts; i += 1) hosts.push(await register(base));
    const results: Result[] = [];
    for (const size of opt.payloads) {
      for (const count of opt.clients) {
        const result = await runCase(base, hosts, size, count, opt, child?.process.pid);
        results.push(result);
        console.error(`${size}B x${count} p50=${result.rttMicros.p50.toFixed(0)}us p99=${result.rttMicros.p99.toFixed(0)}us ${result.messagesPerSec.toFixed(0)} msg/s ${result.payloadMiBPerSec.toFixed(1)} MiB/s fail=${result.failures} corrupt=${result.corrupt}`);
        if (result.failures || result.corrupt) throw new Error(`${size}B x${count}: echo validation failed`);
        await sleep(300);
      }
    }
    const output = { url: base, spawned: opt.spawn, warmupSec: opt.warmupMs / 1000, durationSec: opt.durationMs / 1000, inflightPerClient: opt.inflight, hosts: opt.hosts, relay: { label: opt.label, command: child?.command ?? [], pid: child?.process.pid ?? null }, driver: { runtime: runtimeName(), runtimeVersion: runtimeVersion(), nodeCompatVersion: process.version, platform, release, cpus: cpus().length }, results };
    const encoded = `${JSON.stringify(output, null, 2)}\n`;
    if (opt.output) { writeFileSync(opt.output, encoded); }
    process.stdout.write(encoded);
  } finally {
    await Promise.all(hosts.map((host) => host.close()));
    if (child) {
      child.process.kill("SIGTERM");
      await new Promise<void>((resolve) => child!.process.once("exit", () => resolve()));
    }
  }
}

main().catch((error) => { console.error(`relay-bench: ${error instanceof Error ? error.message : String(error)}`); process.exitCode = 1; });
