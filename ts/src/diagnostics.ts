import crypto from "node:crypto";

export const traceHeader = "X-Supaterm-Relay-Trace";

export type Fields = Record<string, unknown>;

export type ErrorKind = "none" | "cancelled" | "timeout" | "eof" | "message_too_big" | "websocket_close" | "io_error";

export class Logger {
  readonly #write: (line: string) => void;
  readonly #attrs: Fields;

  constructor(write: (line: string) => void, attrs: Fields = { component: "relay" }) {
    this.#write = write;
    this.#attrs = attrs;
  }

  with(fields: Fields): Logger {
    return new Logger(this.#write, { ...this.#attrs, ...fields });
  }

  info(msg: string, fields: Fields = {}): void {
    this.#write(JSON.stringify({ time: new Date().toISOString(), level: "INFO", msg, ...this.#attrs, ...fields }) + "\n");
  }
}

export function tag(value: string): string {
  if (value === "") {
    return "";
  }
  return crypto
    .createHash("sha256")
    .update("supacode-relay-diagnostics/v1/" + value)
    .digest()
    .subarray(0, 9)
    .toString("base64url");
}

const base32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

function randomText(n: number): string {
  return Array.from(crypto.randomBytes(n), (b) => base32[b & 31]).join("");
}

export class Trace {
  readonly id: string;
  readonly #started: number;
  readonly #log: Logger;

  constructor(log: Logger, id: string, started: number) {
    this.id = id;
    this.#started = started;
    this.#log = log;
  }

  static start(log: Logger, route: string): Trace {
    const id = randomText(16);
    return new Trace(log.with({ trace_id: id, route }), id, performance.now());
  }

  with(fields: Fields): Trace {
    return new Trace(this.#log.with(fields), this.id, this.#started);
  }

  event(event: string, fields: Fields = {}): void {
    this.#log.info(event, { elapsed_ms: Math.floor(performance.now() - this.#started), ...fields });
  }

  failure(event: string, kind: ErrorKind, fields: Fields = {}): void {
    this.event(event, { ...fields, error_kind: kind });
  }
}

export function route(path: string): string {
  switch (path) {
    case "/v1/control":
      return "control";
    case "/v1/connect":
      return "connect";
    case "/v1/accept":
      return "accept";
    default:
      return "other";
  }
}

const knownReasons = new Set([
  "",
  "peer disconnected",
  "peer timeout",
  "peer unreachable",
  "peer write timeout",
  "pair timeout",
  "host offline",
  "upgrade failed",
  "host accept failed",
  "message too big",
  "control queue limit exceeded",
  "authentication failed",
  "relay draining",
  "relay shutting down",
  "unexpected control message",
  "writer stopped",
]);

export function closeReason(reason: string): string {
  return knownReasons.has(reason) ? reason : "peer_closed";
}

export function validEndpointID(id: string): boolean {
  return /^[0-9a-f]{64}$/.test(id);
}

export function decodeB64(s: string, n: number): Buffer | undefined {
  if (!/^[A-Za-z0-9_-]*$/.test(s)) {
    return undefined;
  }
  const b = Buffer.from(s, "base64url");
  if (b.length !== n || b.toString("base64url") !== s) {
    return undefined;
  }
  return b;
}

export function endpointID(pub: Uint8Array): string {
  return crypto.createHash("sha256").update(pub).digest("hex");
}

export function traceCommand(args: string[], stdout: NodeJS.WritableStream, stderr: NodeJS.WritableStream): number | undefined {
  if (args.length < 2 || args[1] !== "trace-tag") {
    return undefined;
  }
  if (args.length !== 3 || !validEndpointID(args[2]!)) {
    stderr.write("usage: trace-tag <64-character endpoint ID>\n");
    return 2;
  }
  stdout.write(tag(args[2]!) + "\n");
  return 0;
}
