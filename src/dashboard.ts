import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { availableParallelism, loadavg, freemem, totalmem } from "node:os";
import type { Reply } from "./http.js";
import type { RelayMetrics } from "./metrics.js";

const policy =
  "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const escape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

export function fixed(value: number, digits: number): string {
  const scaled = value * 10 ** digits;
  const floor = Math.floor(scaled);
  if (scaled - floor !== 0.5 || (2 * floor + 1) % 5 ** digits !== 0)
    return value.toFixed(digits);
  const rounded = floor % 2 === 0 ? floor : floor + 1;
  const text = (rounded / 10 ** digits).toFixed(digits);
  return value < 0 && rounded === 0 ? `-${text}` : text;
}

export function formatBytes(value: number): string {
  if (value < 1024) return `${fixed(value, 0)} B`;
  let unit = -1;
  do {
    value /= 1024;
    unit++;
  } while (value >= 1024 && unit < 5);
  return `${fixed(value, 1)} ${"KMGTPE"[unit]}iB`;
}

export function formatUptime(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  if (hours >= 24)
    return `${Math.floor(hours / 24)}d ${hours % 24}h ${minutes % 60}m`;
  if (hours) return `${hours}h ${minutes % 60}m`;
  if (minutes) return `${minutes}m ${seconds % 60}s`;
  return `${seconds}s`;
}

const count = (n: number) =>
  String(Math.trunc(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
const quantity = (n: number) => (n < 10 ? fixed(n, 1) : count(Math.round(n)));

interface Row {
  label: string;
  value: string;
  detail?: string;
  level?: number;
}

export class Dashboard {
  private readonly assets = new Map<string, { data: Buffer; tag: string }>();
  private readonly page: string;
  private readonly started = new Date();
  private readonly samples: { at: number; bytes: number; messages: number }[] =
    [];
  private lastAt = 0;
  private lastCPU = process.cpuUsage();
  private cached = "";
  private readonly memoryLimit: number | undefined;

  constructor(private readonly metrics: () => RelayMetrics) {
    const root = new URL("./assets/", import.meta.url);
    this.page = readFileSync(new URL("templates/page.html", root), "utf8")
      .replace('{{define "page"}}', "")
      .replaceAll("{{end}}", "")
      .replace("{{pollInterval}}", "2s");
    const staticRoot = new URL("static/", root);
    for (const file of readdirSync(staticRoot)) {
      const data = readFileSync(new URL(file, staticRoot));
      this.assets.set(file, {
        data,
        tag: `"${createHash("sha256").update(data).digest("hex").slice(0, 24)}"`,
      });
    }
    try {
      const limit = Number(
        readFileSync("/sys/fs/cgroup/memory.max", "utf8").trim(),
      );
      if (Number.isFinite(limit) && limit > 0) this.memoryLimit = limit;
    } catch {}
  }

  serve(reply: Reply, path: string, allow: () => boolean): void {
    Object.assign(reply.headers, {
      "Content-Security-Policy": policy,
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
    });
    if (path === "/dashboard/" || path === "/dashboard/stats") {
      if (!allow()) {
        reply.json(429, { error: "rate limited" });
        return;
      }
      const stats = this.snapshot();
      reply.headers["Content-Type"] = "text/html; charset=utf-8";
      reply.headers["Cache-Control"] = "no-store";
      reply.send(
        200,
        path.endsWith("stats")
          ? stats
          : this.page.replace('{{template "stats" .}}', stats),
      );
      return;
    }
    const file = path.slice("/dashboard/static/".length);
    const asset = path.startsWith("/dashboard/static/")
      ? this.assets.get(file)
      : undefined;
    if (!asset) {
      reply.text(404, "404 page not found");
      return;
    }
    reply.headers["Cache-Control"] = "no-cache";
    reply.headers.ETag = asset.tag;
    reply.headers["Content-Type"] = file.endsWith(".css")
      ? "text/css; charset=utf-8"
      : "text/javascript; charset=utf-8";
    if (
      String(reply.request.headers["if-none-match"] ?? "")
        .split(",")
        .some((tag) => [asset.tag, `W/${asset.tag}`, "*"].includes(tag.trim()))
    )
      reply.send(304, "");
    else reply.send(200, asset.data);
  }

  private snapshot(): string {
    const at = Date.now();
    if (at - this.lastAt < 1000) return this.cached;
    const m = this.metrics();
    const sample = {
      at,
      bytes: m.forwardedBytes,
      messages: m.forwardedMessages,
    };
    if (
      this.samples.length &&
      (at - this.samples.at(-1)!.at > 60000 ||
        sample.bytes < this.samples.at(-1)!.bytes)
    )
      this.samples.length = 0;
    this.samples.push(sample);
    while (this.samples.length > 1 && at - this.samples[0]!.at > 60000)
      this.samples.shift();
    const before = this.samples[0]!;
    const seconds = (at - before.at) / 1000;
    const cpu = process.cpuUsage();
    const elapsed = (at - (this.lastAt || this.started.getTime())) / 1000;
    const usage =
      elapsed >= 1 && elapsed <= 10
        ? (cpu.user + cpu.system - this.lastCPU.user - this.lastCPU.system) /
          1e6 /
          elapsed /
          availableParallelism()
        : undefined;
    const rows: [string, Row[]][] = [
      [
        "Relay",
        [
          { label: "Connected hosts", value: count(m.activeHosts) },
          { label: "Active pairs", value: count(m.activePairs) },
          { label: "Pending pairs", value: count(m.pendingPairs) },
          { label: "Open sockets", value: count(m.openSockets) },
        ],
      ],
      [
        "Rejections",
        [
          {
            label: "Total",
            value: count(m.rejectedConnections),
            detail: "since start",
          },
          ...Object.entries(m.rejectedConnectionsByReason)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([reason, value]) => ({
              label: reason[0]!.toUpperCase() + reason.slice(1),
              value: count(value),
            })),
        ],
      ],
      [
        "Traffic (1 min avg)",
        [
          {
            label: "Messages",
            value: seconds
              ? `${quantity((sample.messages - before.messages) / seconds)}/s`
              : "—",
            detail: `${count(sample.messages)} total`,
          },
          {
            label: "Throughput",
            value: seconds
              ? `${formatBytes((sample.bytes - before.bytes) / seconds)}/s`
              : "—",
            detail: `${formatBytes(sample.bytes)} total`,
          },
        ],
      ],
      [
        "CPU",
        [
          {
            label: "Relay process",
            value: usage === undefined ? "—" : `${fixed(usage * 100, 1)}%`,
            detail: `of ${availableParallelism()} cores`,
            ...(usage === undefined ? {} : { level: usage }),
          },
          {
            label: "Load average",
            value: loadavg()
              .map((n) => fixed(n, 2))
              .join("  "),
            detail: "1, 5, 15 min",
          },
          { label: "Active resources", value: count(m.activeResources) },
        ],
      ],
      [
        "Memory",
        [
          {
            label: "Process RSS",
            value: formatBytes(m.rssBytes),
            ...(this.memoryLimit
              ? {
                  detail: `of ${formatBytes(this.memoryLimit)} cgroup limit`,
                  level: m.rssBytes / this.memoryLimit,
                }
              : {}),
          },
          { label: "Heap used", value: formatBytes(m.heapUsedBytes) },
          { label: "External buffers", value: formatBytes(m.externalBytes) },
          {
            label: "System",
            value: formatBytes(totalmem() - freemem()),
            detail: `of ${formatBytes(totalmem())}`,
            level: 1 - freemem() / totalmem(),
          },
        ],
      ],
    ];
    const stamp =
      new Date(at).toISOString().replace("T", " ").slice(0, 19) + " UTC";
    const started =
      this.started.toISOString().replace("T", " ").slice(0, 16) + " UTC";
    this.cached = `<header><h1>Supaterm relay</h1><p class="status${m.draining ? " draining" : ""}">${m.draining ? "Draining" : "Online"}</p></header><p class="summary"><span>${stamp}</span><span>Up ${formatUptime(at - this.started.getTime())} since ${started}</span></p><div class="sections">${rows.map(([title, rows]) => `<section><h2>${escape(title)}</h2>${rows.map((row) => `<div class="row"><span class="label">${escape(row.label)}</span><span class="value">${escape(row.value)}${row.detail ? ` <small>${escape(row.detail)}</small>` : ""}</span>${row.level === undefined ? "" : `<meter min="0" max="1" low="0.7" high="0.9" optimum="0" value="${Math.min(1, Math.max(0, row.level))}" aria-label="${escape(row.label)}"></meter>`}</div>`).join("")}</section>`).join("")}</div>`;
    this.lastAt = at;
    this.lastCPU = cpu;
    return this.cached;
  }
}
