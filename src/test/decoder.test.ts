import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { Decoder, FrameError, decodeClose } from "../transport/decoder.js";

function masked(opcode: number, final: boolean, payload: Buffer): Buffer {
  const header = payload.length < 126 ? 6 : payload.length < 65536 ? 8 : 14;
  const data = Buffer.alloc(header + payload.length);
  data[0] = opcode | (final ? 128 : 0);
  data[1] = 128 | (header === 6 ? payload.length : header === 8 ? 126 : 127);
  if (header === 8) data.writeUInt16BE(payload.length, 2);
  if (header === 14) data.writeBigUInt64BE(BigInt(payload.length), 2);
  const mask = Buffer.from([17, 219, 33, 199]);
  mask.copy(data, header - 4);
  for (let i = 0; i < payload.length; i++)
    data[header + i] = payload[i]! ^ mask[i & 3]!;
  return data;
}

test("decoder preserves fragmented messages across arbitrary TCP boundaries", () => {
  for (let width = 1; width <= 31; width++) {
    const messages: Buffer[] = [];
    let parts: Buffer[] = [];
    let pings = 0;
    const decoder = new Decoder(65536, {
      data: (_opcode, _first, payload, final) => {
        parts.push(Buffer.from(payload));
        if (final) {
          messages.push(Buffer.concat(parts));
          parts = [];
        }
        return true;
      },
      control: () => {
        pings++;
        return true;
      },
    });
    const input = Buffer.concat([
      masked(2, false, Buffer.alloc(333, 7)),
      masked(9, true, Buffer.from("ping")),
      masked(0, true, Buffer.alloc(777, 8)),
      masked(1, true, Buffer.alloc(0)),
    ]);
    for (let i = 0; i < input.length; i += width)
      decoder.feed(Buffer.from(input.subarray(i, i + width)));
    assert.deepEqual(messages, [
      Buffer.concat([Buffer.alloc(333, 7), Buffer.alloc(777, 8)]),
      Buffer.alloc(0),
    ]);
    assert.equal(pings, 1);
  }
});

test("decoder streams inside one large frame and does not finalize a truncated frame", () => {
  const payload = Buffer.alloc(4 << 20, 79);
  const expected = createHash("sha256").update(payload).digest("hex");
  const hash = createHash("sha256");
  let bytes = 0,
    finals = 0,
    largest = 0;
  const decoder = new Decoder(payload.length, {
    data: (_opcode, _first, chunk, final) => {
      bytes += chunk.length;
      largest = Math.max(largest, chunk.length);
      hash.update(chunk);
      if (final) finals++;
      return true;
    },
    control: () => true,
  });
  const input = masked(2, true, payload);
  decoder.feed(Buffer.from(input.subarray(0, 65536)));
  assert.ok(bytes > 16384);
  assert.equal(finals, 0);
  for (let i = 65536; i < input.length; i += 65536)
    decoder.feed(Buffer.from(input.subarray(i, i + 65536)));
  assert.equal(largest, 16384);
  assert.equal(finals, 1);
  assert.equal(hash.digest("hex"), expected);
});

test("decoder unmasks every alignment and mask offset without touching adjacent bytes", () => {
  for (const size of [0, 1, 15, 16, 17, 31, 125, 126, 127, 16385, 65536]) {
    const payload = Buffer.from(
      Uint8Array.from({ length: size }, (_, i) => (i * 73 + 129) & 255),
    );
    for (let alignment = 0; alignment < 4; alignment++) {
      for (let split = 0; split < 4; split++) {
        const chunks: Buffer[] = [];
        let finals = 0;
        const decoder = new Decoder(size, {
          data: (_opcode, _first, chunk, final) => {
            chunks.push(Buffer.from(chunk));
            if (final) finals++;
            return true;
          },
          control: () => true,
        });
        const wire = masked(2, true, payload);
        const headerSize = wire.length - payload.length;
        const boundary = Math.min(wire.length, headerSize + split);
        decoder.feed(Buffer.from(wire.subarray(0, boundary)));
        const storage = Buffer.allocUnsafeSlow(wire.length - boundary + 8);
        storage.fill(0xa5);
        const input = storage.subarray(
          alignment + 1,
          alignment + 1 + wire.length - boundary,
        );
        wire.copy(input, 0, boundary);
        decoder.feed(input);
        assert.deepEqual(Buffer.concat(chunks), payload);
        assert.equal(finals, 1);
        assert.ok(
          storage.subarray(0, alignment + 1).every((byte) => byte === 0xa5),
        );
        assert.ok(
          storage
            .subarray(alignment + 1 + input.length)
            .every((byte) => byte === 0xa5),
        );
      }
    }
  }
});

test("backpressure retains only unconsumed input and resumes without replay", () => {
  let calls = 0;
  const lengths: number[] = [];
  const decoder = new Decoder(65536, {
    data: (_opcode, _first, chunk) => {
      lengths.push(chunk.length);
      return ++calls !== 1;
    },
    control: () => true,
  });
  assert.equal(decoder.feed(masked(2, true, Buffer.alloc(32768))), false);
  assert.equal(decoder.feed(), true);
  assert.equal(
    lengths.reduce((sum, n) => sum + n, 0),
    32768,
  );
  assert.equal(decoder.bufferedBacking, undefined);
});

test("malformed headers and cumulative limits fail before consuming payload", () => {
  const sink = { data: () => true, control: () => true };
  for (const header of [
    Buffer.from([0x82, 0, 0, 0, 0, 0]),
    masked(0, true, Buffer.alloc(0)),
    masked(9, false, Buffer.alloc(0)),
    masked(3, true, Buffer.alloc(0)),
  ])
    assert.throws(() => new Decoder(32, sink).feed(header), FrameError);
  const decoder = new Decoder(5, sink);
  decoder.feed(masked(2, false, Buffer.alloc(4)));
  assert.throws(
    () => decoder.feed(masked(0, true, Buffer.alloc(2))),
    (error) => error instanceof FrameError && error.code === 1009,
  );
  assert.throws(() => decodeClose(Buffer.from([3, 238])), FrameError);
  assert.throws(() => decodeClose(Buffer.from([3, 232, 255])), FrameError);
});
