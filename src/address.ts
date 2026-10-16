import { isIP } from "node:net";
import type { IncomingMessage } from "node:http";

export interface Address {
  family: 4 | 6;
  value: bigint;
  zone: string;
}

export interface Prefix {
  family: 4 | 6;
  value: bigint;
  bits: number;
}

export function parseAddress(input: string): Address | undefined {
  const parts = input.split("%");
  if (parts.length > 2 || parts[1] === "") return;
  const text = parts[0]!;
  const family = isIP(text);
  if (family !== 4 && family !== 6) return;
  if (family === 4) {
    if (parts.length !== 1) return;
    return { family, value: ipv4(text), zone: "" };
  }
  let expanded = text;
  if (text.includes(".")) {
    const colon = text.lastIndexOf(":");
    const tail = ipv4(text.slice(colon + 1));
    expanded = `${text.slice(0, colon)}:${(tail >> 16n).toString(16)}:${(tail & 65535n).toString(16)}`;
  }
  const halves = expanded.split("::");
  const left = halves[0] ? halves[0].split(":") : [];
  const right = halves[1] ? halves[1].split(":") : [];
  const words =
    halves.length === 2
      ? [
          ...left,
          ...Array<string>(8 - left.length - right.length).fill("0"),
          ...right,
        ]
      : left;
  return {
    family,
    value: words.reduce((n, s) => (n << 16n) | BigInt(`0x${s}`), 0n),
    zone: parts[1] ?? "",
  };
}

function ipv4(text: string): bigint {
  return text.split(".").reduce((n, s) => (n << 8n) | BigInt(s), 0n);
}

export function unmap(ip: Address): Address {
  return ip.family === 6 && ip.value >> 32n === 65535n
    ? { family: 4, value: ip.value & 0xffff_ffffn, zone: "" }
    : ip;
}

export function clientKey(ip: Address | undefined): string {
  if (!ip) return "invalid";
  ip = unmap(ip);
  return `${ip.family}:${(ip.family === 6 ? ip.value >> 64n : ip.value).toString(16)}`;
}

function mask(ip: bigint, bits: number, width: number): bigint {
  const shift = BigInt(width - bits);
  return (ip >> shift) << shift;
}

export function parsePrefixes(value: string): Prefix[] {
  if (!value.trim()) return [];
  return value.split(",").map((item) => {
    const text = item.trim();
    const [host, length, extra] = text.split("/");
    const ip = parseAddress(host ?? "");
    const bits = Number(length);
    if (
      !ip ||
      ip.zone ||
      extra !== undefined ||
      !/^(0|[1-9][0-9]*)$/.test(length ?? "") ||
      bits > (ip.family === 4 ? 32 : 128)
    ) {
      throw new Error("invalid network prefix");
    }
    return {
      family: ip.family,
      value: mask(ip.value, bits, ip.family === 4 ? 32 : 128),
      bits,
    };
  });
}

function trusted(ip: Address, prefixes: Prefix[]): boolean {
  return (
    !ip.zone &&
    prefixes.some(
      (p) =>
        p.family === ip.family &&
        mask(ip.value, p.bits, ip.family === 4 ? 32 : 128) === p.value,
    )
  );
}

export function clientIP(
  request: IncomingMessage,
  prefixes: Prefix[],
): Address | undefined {
  let ip = parseAddress(request.socket.remoteAddress ?? "");
  if (!ip) return;
  ip = unmap(ip);
  if (!trusted(ip, prefixes)) return ip;
  const headers: string[] = [];
  for (let i = 0; i < request.rawHeaders.length; i += 2) {
    if (request.rawHeaders[i]!.toLowerCase() === "x-forwarded-for")
      headers.push(request.rawHeaders[i + 1]!);
  }
  const hops = headers.join(",").split(",");
  for (let i = hops.length - 1; i >= 0; i--) {
    const parsed = parseAddress(hops[i]!.trim());
    if (!parsed) return ip;
    const hop = unmap(parsed);
    if (!trusted(hop, prefixes)) return hop;
    ip = hop;
  }
  return ip;
}

export function splitAddress(text: string): { host: string; port: number } {
  const match = /^(?:\[([^\]]+)\]|([^:\[\]]*)):(\+?[0-9]+)$/.exec(text);
  if (!match) throw new Error("must be a host:port address");
  const port = Number(match[3]);
  if (port > 65535) throw new Error("invalid port");
  return { host: match[1] ?? match[2]!, port };
}
