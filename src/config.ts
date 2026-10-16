import { parsePrefixes, splitAddress, type Prefix } from "./address.js";

export interface Config {
  addr: string;
  privateAddr: string;
  maxMessageBytes: number;
  maxClientsPerHost: number;
  maxClientsPerHostPerIP: number;
  maxConnectionsPerIP: number;
  authTimeout: number;
  pairTimeout: number;
  deliveryTimeout: number;
  heartbeat: number;
  admissionRate: number;
  trustedProxies: Prefix[];
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  for (const name of [
    "RELAY_ALLOWED_PEERS",
    "RELAY_PRIVATE_ALLOWED_PEERS",
    "RELAY_CLIENT_IP_HEADER",
  ]) {
    if (env[name]) throw new Error(`${name}: no longer supported`);
  }
  function address(name: string, fallback: string): string {
    const value = env[name] || fallback;
    if (value) {
      try {
        splitAddress(value);
      } catch {
        throw new Error(`${name}: invalid listen address`);
      }
    }
    return value;
  }
  function integer(
    name: string,
    fallback: number,
    max = Number.MAX_SAFE_INTEGER,
  ): number {
    const value = env[name];
    if (!value) return fallback;
    if (!/^\+?[0-9]+$/.test(value))
      throw new Error(`${name}: must be a positive integer`);
    const n = BigInt(value);
    if (n < 1n || n > BigInt(max))
      throw new Error(`${name}: integer out of range`);
    return Number(n);
  }
  const rateText = env.RELAY_ADMISSION_RATE || "100";
  const rate = parseRate(rateText);
  if (!Number.isFinite(rate) || rate <= 0 || rate > 1e9)
    throw new Error("RELAY_ADMISSION_RATE: must be a positive number");
  let trustedProxies: Prefix[];
  try {
    trustedProxies = parsePrefixes(env.RELAY_TRUSTED_PROXIES ?? "");
  } catch {
    throw new Error("RELAY_TRUSTED_PROXIES: invalid network prefix");
  }
  return {
    addr: address("RELAY_ADDR", "127.0.0.1:8080"),
    privateAddr: address("RELAY_PRIVATE_ADDR", ""),
    maxMessageBytes: integer("RELAY_MAX_MESSAGE_BYTES", (32 << 20) - 14),
    maxClientsPerHost: integer("RELAY_MAX_CLIENTS_PER_HOST", 64),
    maxClientsPerHostPerIP: integer("RELAY_MAX_CLIENTS_PER_HOST_PER_IP", 16),
    maxConnectionsPerIP: integer("RELAY_MAX_CONNS_PER_IP", 512),
    authTimeout: integer("RELAY_AUTH_TIMEOUT_MS", 5000, 9_223_372_036_854),
    pairTimeout: integer("RELAY_PAIR_TIMEOUT_MS", 5000, 9_223_372_036_854),
    deliveryTimeout: integer(
      "RELAY_DELIVERY_TIMEOUT_MS",
      30000,
      9_223_372_036_854,
    ),
    heartbeat: integer("RELAY_HEARTBEAT_MS", 15000, 4_611_686_018_427),
    admissionRate: rate,
    trustedProxies,
  };
}

function parseRate(text: string): number {
  const decimal =
    /^[+-]?(?:(?:[0-9](?:_?[0-9])*)(?:\.(?:[0-9](?:_?[0-9])*)?)?|\.[0-9](?:_?[0-9])*)(?:[eE][+-]?[0-9](?:_?[0-9])*)?$/;
  if (decimal.test(text)) return Number(text.replaceAll("_", ""));
  const hex =
    /^([+-]?)0[xX]([\da-fA-F]+(?:_[\da-fA-F]+)*)(?:\.([\da-fA-F]+(?:_[\da-fA-F]+)*))?[pP]([+-]?[0-9]+)$/;
  const match = hex.exec(text);
  if (!match) return NaN;
  const whole = parseInt(match[2]!.replaceAll("_", ""), 16);
  const fraction = (match[3] ?? "").replaceAll("_", "");
  const value =
    whole + (fraction ? parseInt(fraction, 16) / 16 ** fraction.length : 0);
  return (match[1] === "-" ? -value : value) * 2 ** Number(match[4]);
}
