import type { Prefix } from "./addr.ts";
import { splitHostPort } from "./addr.ts";
import { parsePrefixes } from "./admission.ts";

export type Config = {
  addr: string;
  maxMessageBytes: number;
  maxQueueBytes: number;
  maxQueueMessages: number;
  maxClients: number;
  maxClientsPerHost: number;
  maxClientsPerHostPerIP: number;
  maxPendingPerHost: number;
  maxHosts: number;
  maxConnections: number;
  maxConnectionsPerIP: number;
  authTimeout: number;
  pairTimeout: number;
  writeTimeout: number;
  deliveryTimeout: number;
  heartbeat: number;
  admissionRate: number;
  trustedProxies: Prefix[];
  clientIPHeader: string;
  privateAddr: string;
  allowedPeers: Prefix[];
  privatePeers: Prefix[];
};

type Getenv = (name: string) => string;

const ints = [
  ["RELAY_MAX_MESSAGE_BYTES", 32 * 2 ** 20 - 14, "maxMessageBytes"],
  ["RELAY_MAX_QUEUE_BYTES", 2 ** 20, "maxQueueBytes"],
  ["RELAY_MAX_QUEUE_MESSAGES", 256, "maxQueueMessages"],
  ["RELAY_MAX_CLIENTS", 20000, "maxClients"],
  ["RELAY_MAX_CLIENTS_PER_HOST", 64, "maxClientsPerHost"],
  ["RELAY_MAX_CLIENTS_PER_HOST_PER_IP", 16, "maxClientsPerHostPerIP"],
  ["RELAY_MAX_PENDING_PER_HOST", 64, "maxPendingPerHost"],
  ["RELAY_MAX_HOSTS", 20000, "maxHosts"],
  ["RELAY_MAX_CONNS", 16384, "maxConnections"],
  ["RELAY_MAX_CONNS_PER_IP", 512, "maxConnectionsPerIP"],
  ["RELAY_AUTH_TIMEOUT_MS", 5000, "authTimeout"],
  ["RELAY_PAIR_TIMEOUT_MS", 5000, "pairTimeout"],
  ["RELAY_WRITE_TIMEOUT_MS", 5000, "writeTimeout"],
  ["RELAY_DELIVERY_TIMEOUT_MS", 30000, "deliveryTimeout"],
  ["RELAY_HEARTBEAT_MS", 15000, "heartbeat"],
] as const;

export function pongTimeout(c: Config): number {
  return 2 * c.heartbeat;
}

export function admissionBurst(c: Config): number {
  return Math.max(1, Math.trunc(c.admissionRate));
}

export function loadConfig(getenv: Getenv): Config {
  const c: Config = {
    addr: getenv("RELAY_ADDR") || "127.0.0.1:8080",
    maxMessageBytes: 0,
    maxQueueBytes: 0,
    maxQueueMessages: 0,
    maxClients: 0,
    maxClientsPerHost: 0,
    maxClientsPerHostPerIP: 0,
    maxPendingPerHost: 0,
    maxHosts: 0,
    maxConnections: 0,
    maxConnectionsPerIP: 0,
    authTimeout: 0,
    pairTimeout: 0,
    writeTimeout: 0,
    deliveryTimeout: 0,
    heartbeat: 0,
    admissionRate: 100,
    trustedProxies: [],
    clientIPHeader: getenv("RELAY_CLIENT_IP_HEADER"),
    privateAddr: "",
    allowedPeers: [],
    privatePeers: [],
  };
  validAddr("RELAY_ADDR", c.addr);
  for (const [name, def, key] of ints) {
    c[key] = positiveInt(getenv, name, def);
  }

  const rate = getenv("RELAY_ADMISSION_RATE");
  if (rate !== "") {
    const r = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(rate) ? Number(rate) : NaN;
    if (!(r > 0 && r <= 1e9)) {
      throw new Error(`RELAY_ADMISSION_RATE: must be a positive number, got ${JSON.stringify(rate)}`);
    }
    c.admissionRate = r;
  }

  for (const [name, key] of [
    ["RELAY_TRUSTED_PROXIES", "trustedProxies"],
    ["RELAY_ALLOWED_PEERS", "allowedPeers"],
    ["RELAY_PRIVATE_ALLOWED_PEERS", "privatePeers"],
  ] as const) {
    try {
      c[key] = parsePrefixes(getenv(name));
    } catch (err) {
      throw new Error(`${name}: ${(err as Error).message}`);
    }
  }

  const privateAddr = getenv("RELAY_PRIVATE_ADDR");
  if (privateAddr !== "") {
    validAddr("RELAY_PRIVATE_ADDR", privateAddr);
    c.privateAddr = privateAddr;
  }

  if (c.maxPendingPerHost > c.maxClientsPerHost) {
    throw new Error(
      `RELAY_MAX_PENDING_PER_HOST (${c.maxPendingPerHost}) must not exceed RELAY_MAX_CLIENTS_PER_HOST (${c.maxClientsPerHost})`,
    );
  }
  return c;
}

function positiveInt(getenv: Getenv, name: string, def: number): number {
  const v = getenv(name);
  if (v === "") {
    return def;
  }
  const n = /^[+-]?\d+$/.test(v) ? Number(v) : NaN;
  if (!Number.isSafeInteger(n) || n <= 0) {
    throw new Error(`${name}: must be a positive integer, got ${JSON.stringify(v)}`);
  }
  return n;
}

function validAddr(name: string, v: string): void {
  let port: string;
  try {
    port = splitHostPort(v).port;
  } catch (err) {
    throw new Error(`${name}: ${(err as Error).message}`);
  }
  const p = /^[+-]?\d+$/.test(port) ? Number(port) : NaN;
  if (!(p >= 0 && p <= 65535)) {
    throw new Error(`${name}: invalid port ${JSON.stringify(port)}`);
  }
}
