import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { Gate, Limiter } from "../admission.js";
import {
  clientIP,
  clientKey,
  parseAddress,
  parsePrefixes,
} from "../address.js";
import { loadConfig } from "../config.js";
import { Deadline } from "../deadline.js";
import { decodeB64 } from "../relay.js";
import { fixed, formatBytes, formatUptime } from "../dashboard.js";
import type { IncomingMessage } from "node:http";

const fixtures = JSON.parse(
  readFileSync(
    new URL("../../testdata/addresses.json", import.meta.url),
    "utf8",
  ),
) as { address: string; key: string | null }[];
for (const fixture of fixtures)
  test(`address fixture ${fixture.address}`, () => {
    const ip = parseAddress(fixture.address);
    assert.equal(ip ? clientKey(ip) : null, fixture.key);
  });

test("trusted proxy walks repeated headers right to left and stops on invalid hops", () => {
  const request = {
    socket: { remoteAddress: "::ffff:127.0.0.1" },
    rawHeaders: [
      "X-Forwarded-For",
      "198.51.100.9, invalid",
      "X-Forwarded-For",
      "10.0.0.2",
    ],
  } as unknown as IncomingMessage;
  const proxies = parsePrefixes("127.0.0.1/32,10.0.0.0/8");
  assert.equal(
    clientKey(clientIP(request, proxies)),
    clientKey(parseAddress("10.0.0.2")),
  );
  assert.equal(
    clientKey(clientIP(request, [])),
    clientKey(parseAddress("127.0.0.1")),
  );
});

test("gate retains global and network ownership until idempotent release", () => {
  const gate = new Gate(1, 2);
  const first = gate.acquire("a");
  assert.equal(typeof first, "function");
  assert.deepEqual(gate.acquire("a"), {
    status: 429,
    reason: "client connection capacity reached",
  });
  const second = gate.acquire("b");
  assert.deepEqual(gate.acquire("c"), {
    status: 503,
    reason: "connection capacity reached",
  });
  if (typeof first === "function") {
    first();
    first();
  }
  assert.equal(gate.total, 1);
  if (typeof second === "function") second();
  assert.equal(gate.counts.size, 0);
});

test("limiter refills denied attempts and expires only idle entries", () => {
  const limiter = new Limiter(2);
  assert.equal(limiter.allow("a", 0), true);
  assert.equal(limiter.allow("a", 0), true);
  assert.equal(limiter.allow("a", 250), false);
  assert.equal(limiter.allow("a", 500), true);
  limiter.allow("b", 60001);
  assert.equal(limiter.entries.has("a"), true);
  limiter.allow("b", 60501);
  assert.equal(limiter.entries.has("a"), false);
});

test("limiter bounds cardinality and expiration work", () => {
  const limiter = new Limiter(1);
  for (let i = 0; i < 65536; i++)
    assert.equal(limiter.allow(String(i), 0), true);
  assert.equal(limiter.allow("extra", 0), false);
  assert.equal(limiter.allow("extra", 60001), true);
  assert.equal(limiter.entries.size, 65536 - 64 + 1);
});

test("canonical base64 rejects ignored characters, padding and trailing bits", () => {
  const canonical = Buffer.alloc(32, 255).toString("base64url");
  assert.equal(decodeB64(canonical, 32)?.length, 32);
  for (const value of [
    canonical + "=",
    canonical + "\n",
    canonical.slice(0, -1) + "_",
    "!!!",
    canonical.slice(1),
  ])
    assert.equal(decodeB64(value, 32), undefined);
});

test("configuration preserves integer grammar and long durations", () => {
  assert.equal(
    loadConfig({
      RELAY_MAX_CLIENTS_PER_HOST: "+007",
      RELAY_PAIR_TIMEOUT_MS: "2147483648",
    }).maxClientsPerHost,
    7,
  );
  assert.equal(loadConfig({ RELAY_ADMISSION_RATE: "0x1p3" }).admissionRate, 8);
  for (const value of [
    " 5",
    "5 ",
    "1e3",
    "0",
    "-1",
    "1_000",
    "9007199254740992",
  ])
    assert.throws(
      () => loadConfig({ RELAY_MAX_CLIENTS_PER_HOST: value }),
      /RELAY_MAX_CLIENTS_PER_HOST/,
    );
  for (const value of ["NaN", "nan", "Infinity", "0"])
    assert.throws(
      () => loadConfig({ RELAY_ADMISSION_RATE: value }),
      /RELAY_ADMISSION_RATE/,
    );
  assert.throws(
    () => loadConfig({ RELAY_HEARTBEAT_MS: "4611686018428" }),
    /RELAY_HEARTBEAT_MS/,
  );
});

test("deadline supports renewal, deferred reads and long delays without warnings", async () => {
  let expired = 0;
  const deadline = new Deadline(() => expired++);
  deadline.set(2_147_483_648);
  await delay(10);
  assert.equal(expired, 0);
  deadline.set(10);
  deadline.suspend();
  await delay(20);
  assert.equal(expired, 0);
  deadline.resume();
  await delay(10);
  assert.equal(expired, 1);
  deadline.set(10);
  deadline.cancel();
  await delay(20);
  assert.equal(expired, 1);
});

test("dashboard formatting preserves ties and units", () => {
  assert.equal(fixed(2.25, 1), "2.2");
  assert.equal(fixed(2.75, 1), "2.8");
  assert.equal(fixed(0.15, 1), "0.1");
  assert.equal(fixed(-2.25, 1), "-2.2");
  assert.equal(formatBytes(1536), "1.5 KiB");
  assert.equal(formatUptime(90061000), "1d 1h 1m");
});
