import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";

import type { Exchange } from "./exchange.ts";

const pollInterval = 2000;
const refreshFloor = pollInterval / 2;
const maxRateWindow = 5 * pollInterval;
const trafficAverageWindow = 60_000;
const unknown = "—";
const contentSecurityPolicy =
  "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const staticDir = new URL("../../internal/dashboard/static/", import.meta.url);
const contentTypes: Record<string, string> = {
  css: "text/css; charset=utf-8",
  js: "text/javascript; charset=utf-8",
};

export type Stats = {
  draining: boolean;
  activeHosts: number;
  activePairs: number;
  pendingPairs: number;
  controlConnections: number;
  clientSlots: number;
  openSockets: number;
  openConnections: number;
  maxHosts: number;
  maxClients: number;
  maxConnections: number;
  forwardedMessages: number;
  forwardedBytes: number;
  rejectedConnections: number;
  rejectedConnectionsByReason: Record<string, number>;
};

type Runtime = { activeHandles: number; gcCycles: () => number };

type Sample = {
  at: number;
  relay: Stats | undefined;
  cpu: number;
  cpus: number;
  handles: number;
  rss: number;
  heapTotal: number;
  gcCycles: number;
  load: number[] | undefined;
  system: { used: number; total: number } | undefined;
};

type Row = { label: string; value: string; detail?: string; level?: number };
type Section = { title: string; rows: Row[] };
type View = { at: number; draining: boolean; time: string; uptime: string; started: string; sections: Section[] };

const statics = new Map(
  fs.readdirSync(staticDir).map((name) => {
    const data = fs.readFileSync(new URL(name, staticDir));
    const tag = `"${crypto.createHash("sha256").update(data).digest("hex").slice(0, 24)}"`;
    return [name, { data, tag }];
  }),
);

export class Dashboard {
  readonly #stats: () => Stats;
  readonly #runtime: () => Runtime;
  readonly #started = Date.now();
  #last: Sample;
  #traffic: { at: number; messages: number; bytes: number }[] = [];
  #view: View | undefined;

  constructor(stats: () => Stats, runtime: () => Runtime) {
    this.#stats = stats;
    this.#runtime = runtime;
    this.#last = this.#sample(this.#started, undefined);
  }

  serve(ex: Exchange, path: string, limit: (ex: Exchange) => boolean): void {
    Object.assign(ex.headers, {
      "Content-Security-Policy": contentSecurityPolicy,
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
    });
    if (ex.req.method !== "GET" && ex.req.method !== "HEAD") {
      ex.text(405, "Method Not Allowed\n", { Allow: "GET, HEAD" });
      return;
    }
    if (path === "/" || path === "/stats") {
      if (!limit(ex)) {
        return;
      }
      const view = this.#snapshot(Date.now());
      const body = path === "/" ? page(view) : stats(view);
      ex.respond(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" }, body);
      return;
    }
    const file = path.startsWith("/static/") ? statics.get(path.slice("/static/".length)) : undefined;
    if (!file) {
      ex.text(404, "404 page not found\n");
      return;
    }
    const headers = {
      "Cache-Control": "no-cache",
      ETag: file.tag,
      "Content-Type": contentTypes[path.split(".").pop() ?? ""] ?? "application/octet-stream",
    };
    if (ex.req.headers["if-none-match"] === file.tag) {
      ex.respond(304, headers, "");
      return;
    }
    ex.respond(200, headers, file.data);
  }

  #snapshot(now: number): View {
    if (this.#view && now - this.#view.at < refreshFloor) {
      return this.#view;
    }
    const current = this.#sample(now, this.#stats());
    this.#recordTraffic(current);
    this.#view = this.#build(this.#last, current);
    this.#last = current;
    return this.#view;
  }

  #sample(at: number, relay: Stats | undefined): Sample {
    const cpu = process.cpuUsage();
    const memory = process.memoryUsage();
    const runtime = this.#runtime();
    return {
      at,
      relay,
      cpu: (cpu.user + cpu.system) / 1e6,
      cpus: os.availableParallelism(),
      handles: runtime.activeHandles,
      rss: memory.rss,
      heapTotal: memory.heapTotal,
      gcCycles: runtime.gcCycles(),
      load: fs.existsSync("/proc/loadavg") ? os.loadavg() : undefined,
      system: systemMemory(),
    };
  }

  #recordTraffic(current: Sample): void {
    const relay = current.relay!;
    const last = this.#last.relay;
    if (
      current.at - this.#last.at > trafficAverageWindow ||
      relay.forwardedMessages < (last?.forwardedMessages ?? 0) ||
      relay.forwardedBytes < (last?.forwardedBytes ?? 0)
    ) {
      this.#traffic = [];
    }
    this.#traffic.push({ at: current.at, messages: relay.forwardedMessages, bytes: relay.forwardedBytes });
    while (current.at - this.#traffic[0]!.at > trafficAverageWindow) {
      this.#traffic.shift();
    }
  }

  #build(previous: Sample, current: Sample): View {
    const relay = current.relay!;
    return {
      at: current.at,
      draining: relay.draining,
      time: new Date(current.at).toISOString().replace("T", " ").slice(0, 19) + " UTC",
      uptime: formatUptime(current.at - this.#started),
      started: new Date(this.#started).toISOString().replace("T", " ").slice(0, 16) + " UTC",
      sections: [
        {
          title: "Relay",
          rows: [
            { label: "Connected hosts", value: formatCount(relay.activeHosts) },
            { label: "Active pairs", value: formatCount(relay.activePairs) },
            { label: "Pending pairs", value: formatCount(relay.pendingPairs) },
            { label: "Open sockets", value: formatCount(relay.openSockets) },
          ],
        },
        {
          title: "Capacity",
          rows: [
            capacity("Hosts", relay.controlConnections, relay.maxHosts),
            capacity("Pairs", relay.clientSlots, relay.maxClients),
            capacity("Connections", relay.openConnections, relay.maxConnections),
          ],
        },
        { title: "Rejections", rows: rejectionRows(relay.rejectedConnections, relay.rejectedConnectionsByReason) },
        { title: "Traffic (1 min avg)", rows: this.#trafficRows(current) },
        { title: "CPU", rows: cpuRows(previous, current) },
        { title: "Memory", rows: memoryRows(current) },
      ],
    };
  }

  #trafficRows(current: Sample): Row[] {
    const relay = current.relay!;
    const messages: Row = { label: "Messages", value: unknown, detail: formatCount(relay.forwardedMessages) + " total" };
    const throughput: Row = { label: "Throughput", value: unknown, detail: formatBytes(relay.forwardedBytes) + " total" };
    const first = this.#traffic[0];
    if (this.#traffic.length > 1 && first) {
      const seconds = (current.at - first.at) / 1000;
      if (seconds > 0) {
        messages.value = formatQuantity((relay.forwardedMessages - first.messages) / seconds) + "/s";
        throughput.value = formatBytes((relay.forwardedBytes - first.bytes) / seconds) + "/s";
      }
    }
    return [messages, throughput];
  }
}

function systemMemory(): { used: number; total: number } | undefined {
  let text: string;
  try {
    text = fs.readFileSync("/proc/meminfo", "utf8");
  } catch {
    return undefined;
  }
  const field = (name: string) => {
    const m = text.match(new RegExp(`^${name}:\\s+(\\d+)`, "m"));
    return m ? Number(m[1]) * 1024 : undefined;
  };
  const total = field("MemTotal");
  const available = field("MemAvailable");
  if (!total || available === undefined || available > total) {
    return undefined;
  }
  return { used: total - available, total };
}

function rejectionRows(total: number, byReason: Record<string, number>): Row[] {
  return [
    { label: "Total", value: formatCount(total), detail: "since start" },
    ...Object.keys(byReason)
      .sort()
      .map((reason) => ({ label: reason[0]!.toUpperCase() + reason.slice(1), value: formatCount(byReason[reason]!) })),
  ];
}

function cpuRows(previous: Sample, current: Sample): Row[] {
  const cores = current.cpus === 1 ? "1 core" : formatCount(current.cpus) + " cores";
  let usage: Row = { label: "Relay process", value: unknown, detail: "of " + cores };
  const seconds = (current.at - previous.at) / 1000;
  if (seconds * 1000 >= refreshFloor && seconds * 1000 <= maxRateWindow) {
    const fraction = (current.cpu - previous.cpu) / seconds / current.cpus;
    usage = gauge(usage.label, formatPercent(fraction), usage.detail!, fraction);
  }
  const rows = [usage];
  if (current.load) {
    rows.push({ label: "Load average", value: current.load.map((l) => l.toFixed(2)).join("  "), detail: "1, 5, 15 min" });
  }
  rows.push({ label: "Active handles", value: formatCount(current.handles) });
  return rows;
}

function memoryRows(current: Sample): Row[] {
  const rows: Row[] = [
    { label: "Resident", value: formatBytes(current.rss) },
    { label: "JS heap", value: formatBytes(current.heapTotal) },
  ];
  if (current.system) {
    const { used, total } = current.system;
    rows.push(gauge("System", formatBytes(used), "of " + formatBytes(total), used / total));
  }
  rows.push({ label: "GC cycles", value: formatCount(current.gcCycles) });
  return rows;
}

function capacity(label: string, n: number, limit: number): Row {
  return gauge(label, formatCount(n), "of " + formatCount(limit), n / limit);
}

function gauge(label: string, value: string, detail: string, level: number): Row {
  return { label, value, detail, level: Math.min(Math.max(level, 0), 1) };
}

export function formatCount(n: number): string {
  return Math.trunc(n)
    .toString()
    .replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function formatBytes(n: number): string {
  if (n < 1024) {
    return `${n.toFixed(0)} B`;
  }
  const units = "KMGTPE";
  let unit = -1;
  while (n >= 1024 && unit < units.length - 1) {
    n /= 1024;
    unit++;
  }
  return `${n.toFixed(1)} ${units[unit]}iB`;
}

function formatQuantity(n: number): string {
  return n < 10 ? n.toFixed(1) : formatCount(Math.round(n));
}

function formatPercent(fraction: number): string {
  return (fraction * 100).toFixed(1) + "%";
}

export function formatUptime(ms: number): string {
  const s = Math.floor(ms / 1000);
  const days = Math.floor(s / 86400);
  const hours = Math.floor((s % 86400) / 3600);
  const minutes = Math.floor((s % 3600) / 60);
  const seconds = s % 60;
  if (days > 0) {
    return `${days}d ${hours}h ${minutes}m`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function escape(s: string): string {
  return s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

function stats(view: View): string {
  const sections = view.sections
    .map((section) => {
      const rows = section.rows
        .map((row) => {
          const detail = row.detail ? ` <small>${escape(row.detail)}</small>` : "";
          const meter =
            row.level === undefined
              ? ""
              : `\n      <meter min="0" max="1" low="0.7" high="0.9" optimum="0" value="${row.level}" aria-label="${escape(row.label)}"></meter>`;
          return `
    <div class="row">
      <span class="label">${escape(row.label)}</span>
      <span class="value">${escape(row.value)}${detail}</span>${meter}
    </div>`;
        })
        .join("");
      return `
  <section>
    <h2>${escape(section.title)}</h2>${rows}
  </section>`;
    })
    .join("");
  return `
<header>
  <h1>Supaterm relay</h1>
  <p class="status${view.draining ? " draining" : ""}">${view.draining ? "Draining" : "Online"}</p>
</header>
<p class="summary">
  <span>${escape(view.time)}</span>
  <span>Up ${escape(view.uptime)} since ${escape(view.started)}</span>
</p>
<div class="sections">${sections}
</div>
`;
}

function page(view: View): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="htmx-config" content='{"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"timeout":5000}'>
<title>Supaterm relay</title>
<link rel="stylesheet" href="static/dashboard.css">
<script src="static/htmx-2.0.11.min.js" defer></script>
<script src="static/dashboard.js" defer></script>
</head>
<body>
<main hx-get="stats" hx-trigger="every ${pollInterval / 1000}s">
${stats(view)}
</main>
<p class="stale-notice" role="status">Couldn’t refresh. Retrying…</p>
</body>
</html>
`;
}
