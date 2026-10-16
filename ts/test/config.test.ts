import assert from "node:assert/strict";
import { test } from "node:test";

import { loadConfig } from "../src/config.ts";

const env = (vars: Record<string, string>) => (name: string) => vars[name] ?? "";

test("defaults match the Go relay", () => {
  const c = loadConfig(env({}));
  assert.equal(c.addr, "127.0.0.1:8080");
  assert.equal(c.maxMessageBytes, 33554418);
  assert.equal(c.maxClientsPerHostPerIP, 16);
  assert.equal(c.deliveryTimeout, 30000);
  assert.equal(c.admissionRate, 100);
  assert.deepEqual(c.trustedProxies, []);
});

test("invalid values name their variable", () => {
  const cases: [Record<string, string>, RegExp][] = [
    [{ RELAY_MAX_CLIENTS: "zero" }, /^RELAY_MAX_CLIENTS: must be a positive integer, got "zero"$/],
    [{ RELAY_HEARTBEAT_MS: "0" }, /^RELAY_HEARTBEAT_MS/],
    [{ RELAY_ADMISSION_RATE: "-1" }, /^RELAY_ADMISSION_RATE/],
    [{ RELAY_ADDR: "localhost" }, /^RELAY_ADDR: address localhost: missing port in address$/],
    [{ RELAY_ADDR: "127.0.0.1:70000" }, /^RELAY_ADDR: invalid port "70000"$/],
    [{ RELAY_TRUSTED_PROXIES: "10.0.0.0" }, /^RELAY_TRUSTED_PROXIES/],
    [{ RELAY_MAX_PENDING_PER_HOST: "65" }, /must not exceed RELAY_MAX_CLIENTS_PER_HOST \(64\)/],
  ];
  for (const [vars, message] of cases) {
    assert.throws(() => loadConfig(env(vars)), { message });
  }
});

test("overrides parse addresses, rates, and prefixes", () => {
  const c = loadConfig(
    env({
      RELAY_ADDR: "[::]:9000",
      RELAY_PRIVATE_ADDR: ":9001",
      RELAY_ADMISSION_RATE: "2.5",
      RELAY_ALLOWED_PEERS: "10.0.0.0/8,2001:db8::/32",
    }),
  );
  assert.equal(c.addr, "[::]:9000");
  assert.equal(c.privateAddr, ":9001");
  assert.equal(c.admissionRate, 2.5);
  assert.equal(c.allowedPeers.length, 2);
});
