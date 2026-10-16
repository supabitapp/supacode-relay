import net from "node:net";

export type IP = { readonly bytes: Uint8Array; readonly v4: boolean };

export type Prefix = { readonly ip: IP; readonly bits: number };

export const invalidIP = "invalid IP";

export function parseIP(input: string): IP | undefined {
  const s = input.includes("%") ? input.slice(0, input.indexOf("%")) : input;
  if (net.isIPv4(s)) {
    const parts = s.split(".");
    if (parts.some((p) => p.length > 1 && p.startsWith("0"))) {
      return undefined;
    }
    return { bytes: Uint8Array.from(parts, Number), v4: true };
  }
  if (!net.isIPv6(s)) {
    return undefined;
  }
  const bytes = new Uint8Array(16);
  let text = s;
  let tail: number[] = [];
  const lastColon = text.lastIndexOf(":");
  if (text.slice(lastColon + 1).includes(".")) {
    const v4 = parseIP(text.slice(lastColon + 1));
    if (!v4?.v4) {
      return undefined;
    }
    tail = [...v4.bytes];
    text = text.slice(0, lastColon + 1) + "0:0";
  }
  const [head = "", rest] = text.split("::");
  const groups = (part: string) => (part === "" ? [] : part.split(":").map((g) => parseInt(g, 16)));
  const left = groups(head);
  const right = rest === undefined ? [] : groups(rest);
  const fill = 8 - left.length - right.length;
  const all = [...left, ...new Array<number>(rest === undefined ? 0 : fill).fill(0), ...right];
  all.forEach((g, i) => {
    bytes[2 * i] = g >> 8;
    bytes[2 * i + 1] = g & 0xff;
  });
  bytes.set(tail, 12);
  return { bytes, v4: false };
}

export function unmap(ip: IP): IP {
  const b = ip.bytes;
  if (!ip.v4 && b.subarray(0, 10).every((x) => x === 0) && b[10] === 0xff && b[11] === 0xff) {
    return { bytes: b.slice(12), v4: true };
  }
  return ip;
}

export function formatIP(ip: IP): string {
  if (ip.v4) {
    return ip.bytes.join(".");
  }
  const groups = Array.from({ length: 8 }, (_, i) => ((ip.bytes[2 * i] ?? 0) << 8) | (ip.bytes[2 * i + 1] ?? 0));
  let best = -1;
  let bestLen = 1;
  for (let i = 0; i < 8; ) {
    let j = i;
    while (j < 8 && groups[j] === 0) {
      j++;
    }
    if (j - i > bestLen) {
      best = i;
      bestLen = j - i;
    }
    i = j === i ? i + 1 : j;
  }
  const hex = (gs: number[]) => gs.map((g) => g.toString(16)).join(":");
  if (best < 0) {
    return hex(groups);
  }
  return hex(groups.slice(0, best)) + "::" + hex(groups.slice(best + bestLen));
}

function mask(ip: IP, bits: number): IP {
  const bytes = ip.bytes.slice();
  for (let i = 0; i < bytes.length; i++) {
    const keep = Math.max(0, Math.min(8, bits - 8 * i));
    bytes[i] = (bytes[i] ?? 0) & ((0xff << (8 - keep)) & 0xff);
  }
  return { bytes, v4: ip.v4 };
}

export function clientKey(ip: IP | undefined): string {
  if (!ip) {
    return invalidIP;
  }
  const unmapped = unmap(ip);
  return formatIP(unmapped.v4 ? unmapped : mask(unmapped, 64));
}

export function parsePrefix(s: string): Prefix | undefined {
  const slash = s.indexOf("/");
  if (slash < 0 || !/^(0|[1-9]\d{0,2})$/.test(s.slice(slash + 1))) {
    return undefined;
  }
  const ip = parseIP(s.slice(0, slash));
  const bits = Number(s.slice(slash + 1));
  if (!ip || s.includes("%") || bits > ip.bytes.length * 8) {
    return undefined;
  }
  return { ip: mask(ip, bits), bits };
}

export function contains(p: Prefix, ip: IP): boolean {
  if (p.ip.v4 !== ip.v4) {
    return false;
  }
  const masked = mask(ip, p.bits);
  return masked.bytes.every((b, i) => b === p.ip.bytes[i]);
}

export function splitHostPort(addr: string): { host: string; port: string } {
  if (addr.startsWith("[")) {
    const end = addr.indexOf("]");
    if (end < 0) {
      throw new Error(`address ${addr}: missing ']' in address`);
    }
    if (addr[end + 1] !== ":") {
      throw new Error(`address ${addr}: missing port in address`);
    }
    return { host: addr.slice(1, end), port: addr.slice(end + 2) };
  }
  const colon = addr.lastIndexOf(":");
  if (colon < 0) {
    throw new Error(`address ${addr}: missing port in address`);
  }
  if (addr.slice(0, colon).includes(":")) {
    throw new Error(`address ${addr}: too many colons in address`);
  }
  return { host: addr.slice(0, colon), port: addr.slice(colon + 1) };
}

export function joinHostPort(host: string, port: number): string {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}
