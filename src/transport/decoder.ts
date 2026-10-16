import { isUtf8 } from "node:buffer";

export interface CloseMessage {
  code: number;
  reason: string;
}

export class FrameError extends Error {
  constructor(readonly code: number) {
    super("invalid WebSocket frame");
  }
}

export interface FrameSink {
  capacity?(): number;
  data(
    opcode: number,
    first: boolean,
    payload: Buffer,
    final: boolean,
  ): boolean;
  control(opcode: number, payload: Buffer): boolean;
}

export class Decoder {
  private readonly header = Buffer.alloc(14);
  private headerBytes = 0;
  private headerSize = 2;
  private remaining = 0;
  private length = 0;
  private opcode = 0;
  private final = false;
  private mask = Buffer.alloc(4);
  private fragmented = 0;
  private messageBytes = 0;
  private first = true;
  private controlPayload = Buffer.alloc(125);
  private input: Buffer | undefined;
  private cursor = 0;
  private payloadPhase = false;

  constructor(
    private readonly limit: number,
    private readonly sink: FrameSink,
  ) {}

  get bufferedBacking(): ArrayBufferLike | undefined {
    return this.input?.buffer;
  }

  feed(input?: Buffer): boolean {
    if (input) {
      if (this.input) throw new Error("decoder input still owned");
      this.input = input;
      this.cursor = 0;
    }
    while (this.input && this.cursor < this.input.length) {
      if (!this.payloadPhase) {
        const n = Math.min(
          this.headerSize - this.headerBytes,
          this.input.length - this.cursor,
        );
        this.input.copy(
          this.header,
          this.headerBytes,
          this.cursor,
          this.cursor + n,
        );
        this.cursor += n;
        this.headerBytes += n;
        if (this.headerBytes !== this.headerSize) continue;
        if (this.headerSize === 2) {
          this.readPrefix();
          const length = this.header[1]! & 127;
          this.headerSize = 6 + (length === 126 ? 2 : length === 127 ? 8 : 0);
          continue;
        }
        this.beginFrame();
        if (this.remaining !== 0) continue;
      }
      const n = Math.min(
        this.remaining,
        this.input.length - this.cursor,
        16 << 10,
        this.opcode < 8 ? (this.sink.capacity?.() ?? 16 << 10) : 125,
      );
      const payload = this.input.subarray(this.cursor, this.cursor + n);
      const offset = this.length - this.remaining;
      for (let i = 0; i < n; i++)
        payload[i] = payload[i]! ^ this.mask[(offset + i) & 3]!;
      this.cursor += n;
      this.remaining -= n;
      const complete = this.remaining === 0;
      let proceed = true;
      if (this.opcode >= 8) {
        payload.copy(this.controlPayload, offset);
        if (complete)
          proceed = this.sink.control(
            this.opcode,
            this.controlPayload.subarray(0, this.length),
          );
      } else {
        const first = this.first;
        this.first = false;
        const final = complete && this.final;
        if (final) {
          this.first = true;
          this.messageBytes = 0;
        }
        proceed = this.sink.data(this.opcode, first, payload, final);
      }
      if (complete) {
        this.payloadPhase = false;
        this.headerBytes = 0;
        this.headerSize = 2;
      }
      if (!proceed) {
        this.releaseConsumed();
        return false;
      }
    }
    this.releaseConsumed();
    return true;
  }

  clear(): void {
    this.input = undefined;
    this.cursor = 0;
  }

  private releaseConsumed(): void {
    if (this.input && this.cursor === this.input.length) this.clear();
  }

  private readPrefix(): void {
    const first = this.header[0]!;
    const second = this.header[1]!;
    const opcode = first & 15;
    this.final = (first & 128) !== 0;
    if ((first & 112) !== 0 || !(second & 128)) throw new FrameError(1002);
    if (opcode >= 8) {
      if (![8, 9, 10].includes(opcode) || !this.final || (second & 127) > 125)
        throw new FrameError(1002);
      this.opcode = opcode;
    } else if (opcode === 0) {
      if (!this.fragmented) throw new FrameError(1002);
      this.opcode = this.fragmented;
    } else {
      if ((opcode !== 1 && opcode !== 2) || this.fragmented)
        throw new FrameError(1002);
      this.opcode = opcode;
    }
  }

  private beginFrame(): void {
    const size = this.header[1]! & 127;
    const wide =
      size === 127
        ? this.header.readBigUInt64BE(2)
        : BigInt(size === 126 ? this.header.readUInt16BE(2) : size);
    if (wide > BigInt(Number.MAX_SAFE_INTEGER)) throw new FrameError(1009);
    this.length = Number(wide);
    if (this.opcode < 8) {
      this.fragmented = this.final ? 0 : this.opcode;
      this.messageBytes += this.length;
      if (this.messageBytes > this.limit) throw new FrameError(1009);
    }
    this.header.copy(this.mask, 0, this.headerSize - 4, this.headerSize);
    this.remaining = this.length;
    this.payloadPhase = true;
  }
}

export function decodeClose(payload: Buffer): CloseMessage {
  if (payload.length < 2) return { code: 1000, reason: "" };
  const code = payload.readUInt16BE();
  if (
    !(
      (code >= 1000 && code <= 1003) ||
      (code >= 1007 && code <= 1013) ||
      (code >= 3000 && code <= 4999)
    ) ||
    !isUtf8(payload.subarray(2))
  )
    throw new FrameError(1002);
  return { code, reason: payload.subarray(2).toString() };
}

export function frame(opcode: number, final: boolean, payload: Buffer): Buffer {
  const header = payload.length < 126 ? 2 : payload.length < 65536 ? 4 : 10;
  const output = Buffer.allocUnsafe(header + payload.length);
  output[0] = opcode | (final ? 128 : 0);
  output[1] = header === 2 ? payload.length : header === 4 ? 126 : 127;
  if (header === 4) output.writeUInt16BE(payload.length, 2);
  if (header === 10) output.writeBigUInt64BE(BigInt(payload.length), 2);
  payload.copy(output, header);
  return output;
}
