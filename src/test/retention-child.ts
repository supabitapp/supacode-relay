import assert from "node:assert/strict";
import { createServer, connect } from "node:net";
import { setImmediate as tick } from "node:timers/promises";
import { once } from "node:events";
import { Peer, socketHighWaterMark } from "../transport/peer.js";

const peers = new Set<Peer>();
let echo = true;
const server = createServer(
  { highWaterMark: socketHighWaterMark },
  (socket) => {
    const peer = new Peer(socket, 32 << 20, 1000);
    peers.add(peer);
    if (echo) peer.forwardTo(peer, () => {});
    peer.onClosed = () => peers.delete(peer);
    peer.start();
  },
);
server.listen(0, "127.0.0.1");
await once(server, "listening");
const address = server.address();
assert.ok(address && typeof address !== "string");
async function retained(): Promise<number> {
  global.gc!();
  await tick();
  global.gc!();
  await tick();
  const memory = process.memoryUsage();
  return memory.heapUsed + memory.external;
}
const baseline = await retained();
for (let round = 0; round < 5; round++) {
  for (let i = 0; i < 32; i++) {
    const socket = connect(address.port, "127.0.0.1");
    await once(socket, "connect");
    const frame = Buffer.alloc(65536 + 14);
    frame[0] = 0x82;
    frame[1] = 0xff;
    frame.writeBigUInt64BE(32n << 20n, 2);
    await new Promise<void>((resolve) => socket.write(frame, () => resolve()));
    socket.destroy();
    await once(socket, "close");
  }
  for (let i = 0; i < 100 && peers.size; i++) await tick();
  assert.equal(peers.size, 0);
}
assert.ok(
  (await retained()) - baseline < 8 << 20,
  "transport retained more than 8 MiB after churn",
);
echo = false;
const control = connect(address.port, "127.0.0.1");
await once(control, "connect");
const wire = Buffer.alloc(150000 * 6);
for (let i = 0; i < wire.length; i += 6) {
  wire[i] = i === 0 ? 2 : 0;
  wire[i + 1] = 128;
}
await new Promise<void>((resolve) => control.write(wire, () => resolve()));
control.write(Buffer.from([0x89, 0x80, 0, 0, 0, 0]));
let pong = Buffer.alloc(0);
for await (const chunk of control.iterator({ destroyOnReturn: false })) {
  pong = Buffer.concat([pong, chunk]);
  if (pong.length >= 2) break;
}
assert.deepEqual(pong, Buffer.from([0x8a, 0]));
assert.ok(
  (await retained()) - baseline < 8 << 20,
  "empty fragments retained an unbounded control assembly",
);
control.destroy();
await once(control, "close");
server.close();
await once(server, "close");
