import { isUtf8 } from "node:buffer";
import crypto from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Socket } from "node:net";
import os from "node:os";

export const Op = { continuation: 0, text: 1, binary: 2, close: 8, ping: 9, pong: 10 } as const;

export const Close = {
  normal: 1000,
  goingAway: 1001,
  protocolError: 1002,
  noStatus: 1005,
  policyViolation: 1008,
  messageTooBig: 1009,
  internalError: 1011,
  tryAgainLater: 1013,
} as const;

export type CloseMsg = { readonly code: number; readonly reason: string };

export type ReadEnd =
  | { readonly kind: "close"; readonly code: number; readonly reason: string }
  | { readonly kind: "eof" | "timeout" | "message_too_big" | "protocol" | "io_error" };

export type Sink = {
  chunk(opcode: number, data: Buffer, fin: boolean): void;
  message(opcode: number, length: number): void;
};

export const closeGrace = 1000;
const maxControlPayload = 125;
const littleEndian = os.endianness() === "LE";
const empty = Buffer.alloc(0);

export function acceptKey(key: string): string {
  return crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
}

function headerTokens(req: IncomingMessage, name: string): string[] {
  const v = req.headers[name];
  return (Array.isArray(v) ? v.join(",") : (v ?? "")).split(",").map((t) => t.trim().toLowerCase());
}

export function handshakeError(req: IncomingMessage): number | undefined {
  if (!headerTokens(req, "connection").includes("upgrade") || !headerTokens(req, "upgrade").includes("websocket")) {
    return 400;
  }
  if (req.method !== "GET") {
    return 405;
  }
  if (!headerTokens(req, "sec-websocket-version").includes("13")) {
    return 400;
  }
  const key = req.headers["sec-websocket-key"];
  if (typeof key !== "string" || Buffer.from(key.trim(), "base64").length !== 16) {
    return 400;
  }
  return undefined;
}

export function handshakeResponse(req: IncomingMessage, extra: Record<string, string>): string {
  const lines = [
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${acceptKey(String(req.headers["sec-websocket-key"]).trim())}`,
    ...Object.entries(extra).map(([k, v]) => `${k}: ${v}`),
  ];
  return lines.join("\r\n") + "\r\n\r\n";
}

export function frameHeader(opcode: number, fin: boolean, length: number): Buffer {
  const first = (fin ? 0x80 : 0) | opcode;
  if (length < 126) {
    const h = Buffer.allocUnsafe(2);
    h[0] = first;
    h[1] = length;
    return h;
  }
  if (length < 65536) {
    const h = Buffer.allocUnsafe(4);
    h[0] = first;
    h[1] = 126;
    h.writeUInt16BE(length, 2);
    return h;
  }
  const h = Buffer.allocUnsafe(10);
  h[0] = first;
  h[1] = 127;
  h.writeBigUInt64BE(BigInt(length), 2);
  return h;
}

export function closePayload(code: number | undefined, reason: string): Buffer {
  if (code === undefined) {
    return empty;
  }
  const text = Buffer.from(reason);
  const b = Buffer.allocUnsafe(2 + text.length);
  b.writeUInt16BE(code, 0);
  text.copy(b, 2);
  return b.subarray(0, maxControlPayload);
}

export function unmask(buf: Buffer, mask: Uint8Array, offset: number): void {
  const len = buf.length;
  let i = 0;
  if (littleEndian && len >= 16) {
    const lead = (4 - (buf.byteOffset & 3)) & 3;
    for (; i < lead; i++) {
      buf[i]! ^= mask[(offset + i) & 3]!;
    }
    const words = (len - i) >>> 2;
    const a = offset + i;
    const m =
      (mask[a & 3]! | (mask[(a + 1) & 3]! << 8) | (mask[(a + 2) & 3]! << 16) | (mask[(a + 3) & 3]! << 24)) >>> 0;
    const view = new Uint32Array(buf.buffer, buf.byteOffset + i, words);
    for (let w = 0; w < words; w++) {
      view[w]! ^= m;
    }
    i += words * 4;
  }
  for (; i < len; i++) {
    buf[i]! ^= mask[(offset + i) & 3]!;
  }
}

function validReceivedCloseCode(code: number): boolean {
  return (code >= 1000 && code <= 1013 && ![1004, 1005, 1006].includes(code)) || (code >= 3000 && code <= 4999);
}

export class WsConn {
  readonly socket: Socket;
  sink: Sink | undefined;
  onEnd: (end: ReadEnd) => void = () => {};
  onPong: () => void = () => {};
  corkTarget: Socket | undefined;
  readLimit: number;
  closeSent = false;
  discard = false;
  readonly #closed: (() => void)[] = [];

  readonly #header = Buffer.allocUnsafe(14);
  #headerLength = 0;
  #inPayload = false;
  #remaining = 0;
  #frameOp = 0;
  #frameFin = false;
  readonly #mask = new Uint8Array(4);
  #maskOffset = 0;
  #messageOp = 0;
  #messageStarted = false;
  #messageLength = 0;
  #control: Buffer = empty;
  #controlLength = 0;
  #ended = false;
  #paused = false;
  #terminating = false;
  #deadlineMs = 0;
  #deadlineAt = 0;
  #deadlineTimer: NodeJS.Timeout | undefined;
  #graceTimer: NodeJS.Timeout | undefined;

  constructor(socket: Socket, head: Buffer, readLimit: number) {
    this.socket = socket;
    this.readLimit = readLimit;
    socket.setNoDelay(true);
    socket.setTimeout(0);
    if (head.length > 0) {
      socket.unshift(head);
    }
    socket.on("data", (chunk: Buffer) => this.#read(chunk));
    socket.on("end", () => this.#end({ kind: "eof" }));
    socket.on("error", () => this.#end({ kind: "io_error" }));
    socket.on("close", () => {
      this.#end({ kind: "eof" });
      clearTimeout(this.#graceTimer);
      for (const fn of this.#closed) {
        fn();
      }
    });
  }

  whenClosed(fn: () => void): void {
    this.#closed.push(fn);
  }

  get ended(): boolean {
    return this.#ended;
  }

  get open(): boolean {
    return !this.socket.destroyed && !this.socket.writableEnded;
  }

  get paused(): boolean {
    return this.#paused;
  }

  pause(): void {
    this.#paused = true;
    this.socket.pause();
  }

  resume(): void {
    this.#paused = false;
    if (this.#deadlineMs > 0) {
      this.#deadlineAt = Math.max(this.#deadlineAt, performance.now() + this.#deadlineMs);
    }
    this.socket.resume();
  }

  setDeadline(ms: number): void {
    this.#deadlineMs = ms;
    this.#deadlineAt = performance.now() + ms;
    clearTimeout(this.#deadlineTimer);
    this.#deadlineTimer = this.#ended ? undefined : setTimeout(() => this.#checkDeadline(), ms);
  }

  extend(): void {
    this.#deadlineAt = performance.now() + this.#deadlineMs;
  }

  #checkDeadline(): void {
    this.#deadlineTimer = undefined;
    if (this.#ended) {
      return;
    }
    const now = performance.now();
    if (this.#paused) {
      this.#deadlineAt = Math.max(this.#deadlineAt, now + this.#deadlineMs);
    }
    if (now < this.#deadlineAt) {
      this.#deadlineTimer = setTimeout(() => this.#checkDeadline(), this.#deadlineAt - now);
      return;
    }
    this.#end({ kind: "timeout" });
  }

  writeFrame(opcode: number, fin: boolean, payload: Buffer, done?: (err?: Error | null) => void): boolean {
    if (this.closeSent || !this.open) {
      done?.(new Error("websocket: close sent"));
      return true;
    }
    const header = frameHeader(opcode, fin, payload.length);
    if (payload.length <= 1024) {
      return this.socket.write(payload.length === 0 ? header : Buffer.concat([header, payload]), done);
    }
    this.socket.cork();
    this.socket.write(header);
    const ok = this.socket.write(payload, done);
    this.socket.uncork();
    return ok;
  }

  writeClose(code: number | undefined, reason: string): void {
    if (this.closeSent || !this.open) {
      return;
    }
    this.closeSent = true;
    const payload = closePayload(code, reason);
    this.socket.write(Buffer.concat([frameHeader(Op.close, true, payload.length), payload]));
  }

  ping(): void {
    this.writeFrame(Op.ping, true, empty);
  }

  terminate(): void {
    if (this.#terminating) {
      return;
    }
    this.#terminating = true;
    this.discard = true;
    this.#paused = false;
    this.socket.resume();
    this.socket.end();
    this.#graceTimer = setTimeout(() => this.socket.destroy(), closeGrace);
  }

  destroy(): void {
    this.socket.destroy();
  }

  #end(end: ReadEnd): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    clearTimeout(this.#deadlineTimer);
    this.#deadlineTimer = undefined;
    this.onEnd(end);
  }

  #protocolError(message: string): void {
    this.writeClose(Close.protocolError, message);
    this.#end({ kind: "protocol" });
  }

  #headerSize(): number {
    if (this.#headerLength < 2) {
      return 2;
    }
    const b1 = this.#header[1]!;
    const len = b1 & 127;
    return 2 + (len === 126 ? 2 : len === 127 ? 8 : 0) + (b1 & 0x80 ? 4 : 0);
  }

  #read(chunk: Buffer): void {
    const target = this.corkTarget;
    if (target === undefined) {
      this.#parse(chunk);
      return;
    }
    target.cork();
    this.#parse(chunk);
    target.uncork();
  }

  #parse(chunk: Buffer): void {
    const len = chunk.length;
    let off = 0;
    while (off < len && !this.#ended) {
      if (!this.#inPayload) {
        let need = this.#headerSize();
        while (this.#headerLength < need && off < len) {
          const n = Math.min(need - this.#headerLength, len - off);
          chunk.copy(this.#header, this.#headerLength, off, off + n);
          this.#headerLength += n;
          off += n;
          need = this.#headerSize();
        }
        if (this.#headerLength < need) {
          return;
        }
        this.#headerLength = 0;
        this.#beginFrame();
        continue;
      }
      const n = Math.min(this.#remaining, len - off);
      const slice = chunk.subarray(off, off + n);
      off += n;
      this.#remaining -= n;
      if (this.#frameOp >= Op.close) {
        slice.copy(this.#control, this.#controlLength);
        this.#controlLength += n;
      } else if (!this.discard) {
        unmask(slice, this.#mask, this.#maskOffset);
        this.#emit(slice, this.#frameFin && this.#remaining === 0);
      }
      this.#maskOffset = (this.#maskOffset + n) & 3;
      if (this.#remaining === 0) {
        this.#endFrame();
      }
    }
  }

  #beginFrame(): void {
    const h = this.#header;
    const b0 = h[0]!;
    const b1 = h[1]!;
    const fin = (b0 & 0x80) !== 0;
    const opcode = b0 & 0x0f;
    let length = b1 & 127;
    let p = 2;
    if (length === 126) {
      length = h.readUInt16BE(2);
      p = 4;
    } else if (length === 127) {
      const big = h.readBigUInt64BE(2);
      if (big > BigInt(Number.MAX_SAFE_INTEGER)) {
        this.#readLimitExceeded();
        return;
      }
      length = Number(big);
      p = 10;
    }
    if ((b0 & 0x70) !== 0) {
      this.#protocolError("unexpected reserved bits 0x" + (b0 & 0x70).toString(16));
      return;
    }
    if ((b1 & 0x80) === 0) {
      this.#protocolError("incorrect mask flag");
      return;
    }
    this.#mask.set(h.subarray(p, p + 4));
    this.#maskOffset = 0;
    this.#frameOp = opcode;
    this.#frameFin = fin;
    this.#remaining = length;
    switch (opcode) {
      case Op.close:
      case Op.ping:
      case Op.pong:
        if (length > maxControlPayload) {
          this.#protocolError("control frame length > 125");
          return;
        }
        if (!fin) {
          this.#protocolError("control frame not final");
          return;
        }
        this.#control = Buffer.allocUnsafe(length);
        this.#controlLength = 0;
        break;
      case Op.text:
      case Op.binary:
        if (this.#messageOp !== 0) {
          this.#protocolError("message start before final message frame");
          return;
        }
        this.#messageOp = opcode;
        this.#messageStarted = false;
        this.#messageLength = 0;
        break;
      case Op.continuation:
        if (this.#messageOp === 0) {
          this.#protocolError("continuation after final message frame");
          return;
        }
        break;
      default:
        this.#protocolError("unknown opcode " + opcode);
        return;
    }
    if (opcode < Op.close) {
      this.#messageLength += length;
      if (this.readLimit > 0 && this.#messageLength > this.readLimit) {
        this.#readLimitExceeded();
        return;
      }
    }
    if (length === 0) {
      this.#endFrame();
    } else {
      this.#inPayload = true;
    }
  }

  #readLimitExceeded(): void {
    this.writeClose(Close.messageTooBig, "");
    this.#end({ kind: "message_too_big" });
  }

  #emit(data: Buffer, fin: boolean): void {
    const op = this.#messageStarted ? Op.continuation : this.#messageOp;
    this.#messageStarted = true;
    const messageOp = this.#messageOp;
    if (fin) {
      this.#messageOp = 0;
    }
    this.sink?.chunk(op, data, fin);
    if (fin) {
      this.sink?.message(messageOp, this.#messageLength);
    }
  }

  #endFrame(): void {
    this.#inPayload = false;
    const op = this.#frameOp;
    if (op < Op.close) {
      if (this.#frameFin && this.#messageOp !== 0) {
        if (this.discard) {
          this.#messageOp = 0;
        } else {
          this.#emit(empty, true);
        }
      }
      return;
    }
    const payload = this.#control;
    unmask(payload, this.#mask, 0);
    this.#control = empty;
    if (op === Op.ping) {
      this.writeFrame(Op.pong, true, payload);
    } else if (op === Op.pong) {
      this.onPong();
    } else {
      this.#receiveClose(payload);
    }
  }

  #receiveClose(payload: Buffer): void {
    if (payload.length === 1) {
      this.#protocolError("invalid close frame");
      return;
    }
    const code = payload.length >= 2 ? payload.readUInt16BE(0) : Close.noStatus;
    const reasonBytes = payload.subarray(2);
    if (payload.length >= 2 && !validReceivedCloseCode(code)) {
      this.#protocolError("bad close code " + code);
      return;
    }
    if (!isUtf8(reasonBytes)) {
      this.#protocolError("invalid utf8 payload in close frame");
      return;
    }
    this.writeClose(code === Close.noStatus ? undefined : code, "");
    this.#end({ kind: "close", code, reason: reasonBytes.toString() });
  }
}
