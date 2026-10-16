import type { Socket } from "node:net";
import { Deadline } from "../deadline.js";
import {
  Decoder,
  FrameError,
  decodeClose,
  frame,
  type CloseMessage,
} from "./decoder.js";

export const socketHighWaterMark = 64 << 10;
const forwardingBufferBytes = 16 << 10;
export const dataBufferBytesPerSocket =
  socketHighWaterMark + forwardingBufferBytes;

export class Peer {
  readonly decoder: Decoder;
  private readonly writes = new Set<Buffer>();
  readonly waiters = new Set<Peer>();
  private readonly readDeadline: Deadline;
  private readonly writeDeadline: Deadline;
  private readonly closeDeadline: Deadline;
  private readonly pingDeadline: Deadline;
  private readonly controlWriteDeadline: Deadline;
  private blockedOn: Peer | undefined;
  private heartbeat = 0;
  private pingPending = false;
  private dataWrites = 0;
  private controlWrites = 0;
  private reading = false;
  private pumping = false;
  private ended = false;
  private messageParts: Buffer[] = [];
  closing = false;
  closed = false;
  private onData:
    | ((
        opcode: number,
        first: boolean,
        payload: Buffer,
        final: boolean,
      ) => boolean)
    | undefined;
  onMessage: ((opcode: number, payload: Buffer) => void) | undefined;
  private onCapacity: (() => number) | undefined;
  private forwardingBuffer: Buffer | undefined;
  private forwardingDestination: Socket | undefined;
  onFailure: (cause: CloseMessage) => void = () => {};
  onClosed: () => void = () => {};

  constructor(
    readonly socket: Socket,
    limit: number,
    readonly deliveryTimeout: number,
  ) {
    socket.pause();
    socket.setTimeout(0);
    socket.setNoDelay(true);
    this.readDeadline = new Deadline(() =>
      this.fail({ code: 1001, reason: "peer timeout" }),
    );
    this.writeDeadline = new Deadline(() =>
      this.fail({ code: 1013, reason: "peer write timeout" }),
    );
    this.closeDeadline = new Deadline(() => socket.destroy());
    this.pingDeadline = new Deadline(() => this.ping());
    this.controlWriteDeadline = new Deadline(() =>
      this.fail({ code: 1001, reason: "peer unreachable" }),
    );
    this.decoder = new Decoder(limit, {
      capacity: () => this.onCapacity?.() ?? 16 << 10,
      data: (opcode, first, payload, final) => {
        if (this.heartbeat && this.onData && (payload.length > 0 || final))
          this.extend();
        if (this.onData) return this.onData(opcode, first, payload, final);
        if (payload.length) this.messageParts.push(Buffer.from(payload));
        if (final) {
          const data = Buffer.concat(this.messageParts);
          this.messageParts = [];
          this.onMessage?.(opcode, data);
        }
        return !this.closing;
      },
      control: (opcode, payload) => {
        if (opcode === 8) {
          const cause = decodeClose(payload);
          this.close(cause.code, "");
          this.failure(cause);
          return false;
        }
        if (opcode === 10) {
          if (this.heartbeat) this.extend();
          return true;
        }
        const ready = this.send(10, true, payload);
        if (!ready) this.pauseFor(this);
        return ready;
      },
    });
    socket.on("readable", () => this.pump());
    socket.on("drain", () => {
      for (const peer of [...this.waiters]) peer.unblock();
    });
    socket.on("error", () =>
      this.failure({ code: 1001, reason: "peer disconnected" }),
    );
    socket.on("end", () => {
      this.failure({ code: 1001, reason: "peer disconnected" });
      socket.destroy();
    });
    socket.on("close", () => {
      this.closed = true;
      this.closing = true;
      this.readDeadline.cancel();
      this.writeDeadline.cancel();
      this.pingDeadline.cancel();
      this.controlWriteDeadline.cancel();
      this.closeDeadline.cancel();
      this.decoder.clear();
      this.messageParts = [];
      this.onData = undefined;
      this.onCapacity = undefined;
      this.forwardingBuffer = undefined;
      this.forwardingDestination = undefined;
      this.blockedOn?.waiters.delete(this);
      this.blockedOn = undefined;
      this.failure({ code: 1001, reason: "peer disconnected" });
      this.onClosed();
      this.waiters.clear();
    });
  }

  forwardTo(destination: Peer, delivered: (bytes: number) => void): void {
    const buffer = Buffer.allocUnsafe(forwardingBufferBytes);
    this.forwardingBuffer = buffer;
    this.forwardingDestination = destination.socket;
    let used = 0;
    let messageBytes = 0;
    let opcode = 2;
    let firstFragment = true;
    this.onCapacity = () => buffer.length - used;
    this.onData = (type, first, payload, final) => {
      if (first) opcode = type;
      messageBytes += payload.length;
      if (destination.dataWrites)
        destination.writeDeadline.set(destination.deliveryTimeout);
      let output = payload;
      if (used > 0 || (!final && payload.length < buffer.length)) {
        payload.copy(buffer, used);
        used += payload.length;
        if (used < buffer.length && !final) return true;
        output = buffer.subarray(0, used);
      }
      const bytes = messageBytes;
      const ready = destination.send(
        firstFragment ? opcode : 0,
        final,
        output,
        (error) => {
          if (error)
            this.onFailure({ code: 1013, reason: "peer write timeout" });
          else if (final) delivered(bytes);
        },
      );
      used = 0;
      firstFragment = final;
      if (final) messageBytes = 0;
      if (!ready) this.pauseFor(destination);
      return ready;
    };
  }

  start(): void {
    this.reading = true;
    this.readDeadline.resume();
    this.pump();
  }

  enableHeartbeat(heartbeat: number): void {
    this.heartbeat = heartbeat;
    this.extend();
    if (!this.reading) this.readDeadline.suspend();
    this.pingDeadline.set(heartbeat);
  }

  extend(): void {
    this.readDeadline.set(2 * this.heartbeat);
  }

  pauseFor(destination: Peer): void {
    this.blockedOn = destination;
    destination.waiters.add(this);
    this.readDeadline.suspend();
  }

  private unblock(): void {
    this.blockedOn?.waiters.delete(this);
    this.blockedOn = undefined;
    if (!this.reading || this.closing) return;
    this.readDeadline.resume();
    this.pump();
  }

  private pump(): void {
    if (!this.reading || this.closing || this.blockedOn || this.pumping) return;
    this.pumping = true;
    const destination = this.forwardingDestination;
    destination?.cork();
    try {
      if (!this.decoder.feed()) return;
      for (
        let count = 0;
        count < 64 && !this.closing && !this.blockedOn;
        count++
      ) {
        const available = this.socket.readableLength;
        if (!available) return;
        const chunk = this.socket.read(
          Math.min(available, 16 << 10),
        ) as Buffer | null;
        if (!chunk) return;
        if (!this.decoder.feed(chunk)) return;
      }
      if (!this.closing && !this.blockedOn && this.socket.readableLength)
        setImmediate(() => this.pump());
    } catch (error) {
      if (!(error instanceof FrameError)) throw error;
      this.close(error.code, "");
      this.failure(
        error.code === 1009
          ? { code: 1009, reason: "message too big" }
          : { code: 1001, reason: "peer disconnected" },
      );
    } finally {
      destination?.uncork();
      this.pumping = false;
    }
  }

  send(
    opcode: number,
    final: boolean,
    payload: Buffer,
    done?: (error?: Error | null) => void,
  ): boolean {
    if (this.closed || (this.closing && opcode !== 8)) {
      done?.(new Error("peer closed"));
      return false;
    }
    const output = frame(opcode, final, payload);
    if (opcode < 8) {
      this.dataWrites++;
      this.writeDeadline.set(this.deliveryTimeout);
    } else if (opcode !== 8) {
      this.controlWrites++;
      this.controlWriteDeadline.set(this.deliveryTimeout);
    }
    this.writes.add(output);
    return this.socket.write(output, (error) => {
      this.writes.delete(output);
      if (opcode < 8 && --this.dataWrites === 0) this.writeDeadline.cancel();
      if (opcode > 8 && --this.controlWrites === 0)
        this.controlWriteDeadline.cancel();
      done?.(error);
    });
  }

  close(code: number, reason: string, grace = 1000): void {
    if (this.closing) return;
    this.closing = true;
    this.readDeadline.cancel();
    this.writeDeadline.cancel();
    this.pingDeadline.cancel();
    this.controlWriteDeadline.cancel();
    const text = Buffer.from(reason);
    const payload = Buffer.alloc(2 + Math.min(text.length, 123));
    payload.writeUInt16BE(code);
    text.copy(payload, 2, 0, 123);
    this.closeDeadline.set(grace);
    this.send(8, true, payload, () => this.socket.destroy());
  }

  fail(cause: CloseMessage): void {
    this.failure(cause);
    this.socket.destroy();
  }

  private failure(cause: CloseMessage): void {
    if (this.ended) return;
    this.ended = true;
    this.onFailure(cause);
  }

  private ping(): void {
    if (this.closing) return;
    if (!this.pingPending) {
      this.pingPending = true;
      this.send(9, true, Buffer.alloc(0), () => {
        this.pingPending = false;
      });
    }
    this.pingDeadline.set(this.heartbeat);
  }

  get retainedBackings(): Set<ArrayBufferLike> {
    const backing = new Set<ArrayBufferLike>(
      [...this.writes].map((buffer) => buffer.buffer),
    );
    if (this.decoder.bufferedBacking) backing.add(this.decoder.bufferedBacking);
    for (const part of this.messageParts) backing.add(part.buffer);
    if (this.forwardingBuffer) backing.add(this.forwardingBuffer.buffer);
    return backing;
  }
}
