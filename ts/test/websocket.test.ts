import assert from "node:assert/strict";
import crypto from "node:crypto";
import { once } from "node:events";
import net from "node:net";
import { after, test } from "node:test";

import { Op, type ReadEnd, WsConn, frameHeader, unmask } from "../src/websocket.ts";

const servers: net.Server[] = [];
after(() => servers.forEach((s) => s.close()));

type Chunk = { op: number; data: Buffer; fin: boolean };

async function connected(readLimit = 0) {
  const server = net.createServer({ allowHalfOpen: true });
  servers.push(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const accepted = once(server, "connection");
  const peer = net.connect((server.address() as net.AddressInfo).port, "127.0.0.1");
  const [socket] = (await accepted) as [net.Socket];
  const conn = new WsConn(socket, Buffer.alloc(0), readLimit);
  const chunks: Chunk[] = [];
  const messages: { op: number; length: number }[] = [];
  conn.sink = {
    chunk: (op, data, fin) => chunks.push({ op, data: Buffer.from(data), fin }),
    message: (op, length) => messages.push({ op, length }),
  };
  const ended = new Promise<ReadEnd>((resolve) => {
    conn.onEnd = resolve;
  });
  let received = Buffer.alloc(0);
  peer.on("data", (b: Buffer) => {
    received = Buffer.concat([received, b]);
  });
  const frames = async (n: number) => {
    for (;;) {
      const parsed = parseFrames(received);
      if (parsed.length >= n) {
        return parsed;
      }
      await once(peer, "data");
    }
  };
  return { conn, peer, chunks, messages, ended, frames, done: () => peer.destroy() };
}

function masked(opcode: number, fin: boolean, payload: Buffer): Buffer {
  const header = frameHeader(opcode, fin, payload.length);
  header[1]! |= 0x80;
  const mask = crypto.randomBytes(4);
  const body = Buffer.from(payload);
  for (let i = 0; i < body.length; i++) {
    body[i]! ^= mask[i & 3]!;
  }
  return Buffer.concat([header, mask, body]);
}

function parseFrames(b: Buffer): { op: number; fin: boolean; payload: Buffer }[] {
  const out = [];
  let off = 0;
  while (off + 2 <= b.length) {
    let len = b[off + 1]! & 127;
    let p = off + 2;
    if (len === 126) {
      len = b.readUInt16BE(p);
      p += 2;
    } else if (len === 127) {
      len = Number(b.readBigUInt64BE(p));
      p += 8;
    }
    if (p + len > b.length) {
      break;
    }
    out.push({ op: b[off]! & 15, fin: (b[off]! & 0x80) !== 0, payload: b.subarray(p, p + len) });
    off = p + len;
  }
  return out;
}

function closeFrame(code: number, reason: string): Buffer {
  const b = Buffer.alloc(2 + Buffer.byteLength(reason));
  b.writeUInt16BE(code);
  b.write(reason, 2);
  return masked(Op.close, true, b);
}

test("frame headers switch length encodings at 126 and 65536 bytes", () => {
  assert.deepEqual([...frameHeader(Op.binary, true, 125)], [0x82, 125]);
  assert.deepEqual([...frameHeader(Op.text, false, 126)], [0x01, 126, 0, 126]);
  assert.deepEqual([...frameHeader(Op.continuation, true, 65535)], [0x80, 126, 0xff, 0xff]);
  assert.deepEqual([...frameHeader(Op.binary, true, 65536)], [0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0]);
});

test("word-wise unmasking matches the byte-wise definition at every alignment", () => {
  const mask = crypto.randomBytes(4);
  for (const length of [0, 3, 16, 17, 1000, 4099]) {
    for (let start = 0; start < 4; start++) {
      for (let offset = 0; offset < 4; offset++) {
        const data = crypto.randomBytes(length + start).subarray(start);
        const expected = Buffer.from(data.map((b, i) => b ^ mask[(offset + i) & 3]!));
        unmask(data, mask, offset);
        assert.deepEqual(data, expected);
      }
    }
  }
});

test("streams fragments with message opcode, continuations, and interleaved pings", async () => {
  const c = await connected();
  c.peer.write(masked(Op.text, false, Buffer.from("he")));
  c.peer.write(masked(Op.ping, true, Buffer.from("p")));
  c.peer.write(masked(Op.continuation, false, Buffer.from("ll")));
  c.peer.write(masked(Op.continuation, true, Buffer.from("o")));
  c.peer.write(masked(Op.binary, true, Buffer.alloc(0)));
  const [pong] = await c.frames(1);
  assert.deepEqual(pong, { op: Op.pong, fin: true, payload: Buffer.from("p") });
  while (c.messages.length < 2) {
    await new Promise((r) => setImmediate(r));
  }
  assert.deepEqual(
    c.chunks.map((x) => [x.op, x.data.toString(), x.fin]),
    [
      [Op.text, "he", false],
      [Op.continuation, "ll", false],
      [Op.continuation, "o", true],
      [Op.binary, "", true],
    ],
  );
  assert.deepEqual(c.messages, [
    { op: Op.text, length: 5 },
    { op: Op.binary, length: 0 },
  ]);
  c.done();
});

test("reassembles payloads split across arbitrary TCP reads", async () => {
  const c = await connected();
  const payload = crypto.randomBytes(200_000);
  const wire = masked(Op.binary, true, payload);
  for (let off = 0; off < wire.length; ) {
    const n = Math.min(wire.length - off, 1 + Math.floor(Math.random() * 9000));
    c.peer.write(wire.subarray(off, off + n));
    off += n;
  }
  while (c.messages.length < 1) {
    await new Promise((r) => setImmediate(r));
  }
  assert.deepEqual(Buffer.concat(c.chunks.map((x) => x.data)), payload);
  assert.equal(c.chunks.at(-1)!.fin, true);
  assert.ok(c.chunks.slice(1).every((x) => x.op === Op.continuation));
  c.done();
});

test("enforces the read limit across fragments before forwarding the oversized frame", async () => {
  const c = await connected(10);
  c.peer.write(masked(Op.binary, false, Buffer.alloc(6)));
  c.peer.write(masked(Op.continuation, true, Buffer.alloc(6)));
  assert.deepEqual(await c.ended, { kind: "message_too_big" });
  const [close] = await c.frames(1);
  assert.equal(close!.op, Op.close);
  assert.equal(close!.payload.readUInt16BE(), 1009);
  assert.equal(Buffer.concat(c.chunks.map((x) => x.data)).length, 6);
  c.done();
});

test("echoes a received close code without its reason", async () => {
  const c = await connected();
  c.peer.write(closeFrame(4001, "bye now"));
  assert.deepEqual(await c.ended, { kind: "close", code: 4001, reason: "bye now" });
  const [echo] = await c.frames(1);
  assert.equal(echo!.op, Op.close);
  assert.deepEqual(echo!.payload, Buffer.from([0x0f, 0xa1]));
  c.done();
});

test("treats an empty close frame as no status and echoes it empty", async () => {
  const c = await connected();
  c.peer.write(masked(Op.close, true, Buffer.alloc(0)));
  assert.deepEqual(await c.ended, { kind: "close", code: 1005, reason: "" });
  const [echo] = await c.frames(1);
  assert.equal(echo!.payload.length, 0);
  c.done();
});

for (const [name, wire] of [
  ["unmasked frames", Buffer.from([0x82, 0x01, 0x00])],
  ["reserved bits", masked(Op.binary | 0x40, true, Buffer.from("x"))],
  ["continuations without a message", masked(Op.continuation, true, Buffer.from("x"))],
  ["fragmented control frames", masked(Op.ping, false, Buffer.alloc(0))],
  ["invalid close codes", closeFrame(1005, "")],
] as const) {
  test(`closes ${name} with a protocol error`, async () => {
    const c = await connected();
    c.peer.write(wire);
    assert.deepEqual(await c.ended, { kind: "protocol" });
    const [close] = await c.frames(1);
    assert.equal(close!.payload.readUInt16BE(), 1002);
    c.done();
  });
}

test("a paused connection does not time out until it resumes reading", async () => {
  const c = await connected();
  c.conn.pause();
  c.conn.setDeadline(20);
  const outcome = await Promise.race([c.ended, new Promise((r) => setTimeout(() => r("alive"), 80))]);
  assert.equal(outcome, "alive");
  c.conn.resume();
  assert.deepEqual(await c.ended, { kind: "timeout" });
  c.done();
});
