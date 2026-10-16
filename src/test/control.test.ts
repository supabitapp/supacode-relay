import assert from "node:assert/strict";
import { createServer, connect } from "node:net";
import { once } from "node:events";
import { test } from "node:test";
import { Peer } from "../transport/peer.js";

test("pong writes expire while the receiver is blocked before the next heartbeat", async () => {
  let release: () => void = () => {};
  const closed = new Promise<void>((resolve) => {
    release = resolve;
  });
  const server = createServer({ highWaterMark: 4096 }, (socket) => {
    const peer = new Peer(socket, 4096, 50);
    peer.onClosed = release;
    peer.enableHeartbeat(10000);
    peer.start();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  const client = connect(address.port, "127.0.0.1");
  client.on("error", () => {});
  await once(client, "connect");
  client.pause();
  const wire = Buffer.alloc(131 * 1024);
  for (let i = 0; i < wire.length; i += 131) {
    wire[i] = 0x89;
    wire[i + 1] = 0xfd;
  }
  const flood = () => {
    if (client.destroyed) return;
    if (client.write(wire, () => {})) setImmediate(flood);
    else client.once("drain", flood);
  };
  flood();
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      closed,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error("pong write outlived its delivery deadline")),
          5000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
    client.destroy();
    server.close();
  }
});
