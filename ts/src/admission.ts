import type { IncomingHttpHeaders } from "node:http";

import { type IP, type Prefix, contains, parseIP, parsePrefix, unmap } from "./addr.ts";

export const maxEntries = 65536;
export const idleTTL = 60_000;
const expireBatch = 64;

type Bucket = { tokens: number; last: number; seen: number };

export class Limiter {
  readonly #rate: number;
  readonly #burst: number;
  readonly #entries = new Map<string, Bucket>();

  constructor(rate: number, burst: number) {
    this.#rate = rate;
    this.#burst = burst;
  }

  allow(key: string, now: number): boolean {
    this.#expire(now);
    let bucket = this.#entries.get(key);
    if (bucket) {
      this.#entries.delete(key);
    } else {
      if (this.#entries.size >= maxEntries) {
        return false;
      }
      bucket = { tokens: this.#burst, last: now, seen: now };
    }
    bucket.seen = now;
    this.#entries.set(key, bucket);
    const tokens = Math.min(this.#burst, bucket.tokens + (Math.max(0, now - bucket.last) * this.#rate) / 1000);
    if (tokens < 1) {
      return false;
    }
    bucket.tokens = tokens - 1;
    bucket.last = now;
    return true;
  }

  #expire(now: number): void {
    let n = 0;
    for (const [key, bucket] of this.#entries) {
      if (n++ >= expireBatch || now - bucket.seen <= idleTTL) {
        return;
      }
      this.#entries.delete(key);
    }
  }

  get size(): number {
    return this.#entries.size;
  }
}

export type GateResult = "admitted" | "global_full" | "per_ip_full";

export class Gate {
  readonly #max: number;
  readonly #perIP: number;
  #total = 0;
  readonly #counts = new Map<string, number>();

  constructor(max: number, perIP: number) {
    this.#max = max;
    this.#perIP = perIP;
  }

  acquire(key: string): GateResult {
    if (this.#total >= this.#max) {
      return "global_full";
    }
    const n = this.#counts.get(key) ?? 0;
    if (n >= this.#perIP) {
      return "per_ip_full";
    }
    this.#total++;
    this.#counts.set(key, n + 1);
    return "admitted";
  }

  release(key: string): void {
    this.#total--;
    const n = this.#counts.get(key) ?? 0;
    if (n <= 1) {
      this.#counts.delete(key);
    } else {
      this.#counts.set(key, n - 1);
    }
  }

  stats(): { total: number; ips: number } {
    return { total: this.#total, ips: this.#counts.size };
  }
}

export function isTrusted(ip: IP | undefined, trusted: readonly Prefix[]): boolean {
  return ip !== undefined && trusted.some((p) => contains(p, ip));
}

export function remoteIP(remoteAddress: string | undefined): IP | undefined {
  const ip = parseIP(remoteAddress ?? "");
  return ip && unmap(ip);
}

export function clientIP(
  remoteAddress: string | undefined,
  headers: IncomingHttpHeaders,
  distinct: NodeJS.Dict<string[]>,
  trusted: readonly Prefix[],
  header: string,
): IP | undefined {
  let ip = remoteIP(remoteAddress);
  if (!ip || !isTrusted(ip, trusted)) {
    return ip;
  }
  if (header !== "") {
    const values = distinct[header.toLowerCase()];
    if (values?.length === 1) {
      const forwarded = parseIP(values[0]!.trim());
      if (forwarded) {
        return unmap(forwarded);
      }
    }
  }
  const xff = headers["x-forwarded-for"];
  const hops = (Array.isArray(xff) ? xff.join(",") : (xff ?? "")).split(",");
  for (let i = hops.length - 1; i >= 0; i--) {
    const parsed = parseIP(hops[i]!.trim());
    if (!parsed) {
      return ip;
    }
    const hop = unmap(parsed);
    if (!isTrusted(hop, trusted)) {
      return hop;
    }
    ip = hop;
  }
  return ip;
}

export function parsePrefixes(v: string): Prefix[] {
  if (v.trim() === "") {
    return [];
  }
  return v.split(",").map((s) => {
    const p = parsePrefix(s.trim());
    if (!p) {
      throw new Error(`netip.ParsePrefix(${JSON.stringify(s.trim())}): invalid prefix`);
    }
    return p;
  });
}
