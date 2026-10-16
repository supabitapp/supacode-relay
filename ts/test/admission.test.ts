import assert from "node:assert/strict";
import { test } from "node:test";

import { clientKey, formatIP, parseIP, parsePrefix } from "../src/addr.ts";
import { Gate, Limiter, clientIP, idleTTL, parsePrefixes } from "../src/admission.ts";

const ip = (s: string) => parseIP(s)!;

test("client keys unmap IPv4 and group IPv6 by /64", () => {
  assert.equal(clientKey(ip("::ffff:10.1.2.3")), "10.1.2.3");
  assert.equal(clientKey(ip("2001:db8:1:2:aaaa:bbbb:cccc:dddd")), "2001:db8:1:2::");
  assert.equal(clientKey(ip("fe80::1%en0")), "fe80::");
  assert.equal(clientKey(undefined), "invalid IP");
});

test("IPv6 formatting compresses the longest zero run", () => {
  assert.equal(formatIP(ip("2001:0db8:0:0:1:0:0:1")), "2001:db8::1:0:0:1");
  assert.equal(formatIP(ip("0:0:0:0:0:0:0:1")), "::1");
  assert.equal(formatIP(ip("2001:db8:0:1:1:1:1:1")), "2001:db8:0:1:1:1:1:1");
  assert.equal(formatIP(ip("::ffff:1.2.3.4")), "::ffff:102:304");
});

test("prefixes reject malformed input", () => {
  for (const bad of ["10.0.0.0", "10.0.0.0/33", "10.0.0.0/08", "::/129", "nope/8", "01.2.3.4/8"]) {
    assert.equal(parsePrefix(bad), undefined, bad);
  }
  assert.throws(() => parsePrefixes("10.0.0.0/8, bad"), /invalid prefix/);
  assert.equal(parsePrefixes(" ").length, 0);
});

test("the limiter allows a burst, refills at the configured rate, and expires idle clients", () => {
  const l = new Limiter(2, 2);
  assert.deepEqual([l.allow("a", 0), l.allow("a", 0), l.allow("a", 0)], [true, true, false]);
  assert.equal(l.allow("a", 499), false);
  assert.equal(l.allow("a", 500), true);
  assert.equal(l.allow("b", 500), true);
  assert.equal(l.size, 2);
  l.allow("c", 500 + idleTTL + 1);
  assert.equal(l.size, 1);
});

test("the gate enforces global and per-client limits and releases both", () => {
  const g = new Gate(3, 2);
  assert.deepEqual(["a", "a", "a", "b", "c"].map((k) => g.acquire(k)), [
    "admitted",
    "admitted",
    "per_ip_full",
    "admitted",
    "global_full",
  ]);
  g.release("a");
  g.release("a");
  assert.deepEqual(g.stats(), { total: 1, ips: 1 });
});

test("client IPs trust only configured proxies", () => {
  const trusted = parsePrefixes("10.0.0.0/8");
  const resolve = (remote: string, xff?: string, header = "", value?: string[]) =>
    formatIP(
      clientIP(
        remote,
        xff === undefined ? {} : { "x-forwarded-for": xff },
        value === undefined ? {} : { "x-real-ip": value },
        trusted,
        header,
      )!,
    );
  assert.equal(resolve("203.0.113.9", "198.51.100.1"), "203.0.113.9");
  assert.equal(resolve("10.0.0.1", "198.51.100.1, 10.2.2.2"), "198.51.100.1");
  assert.equal(resolve("10.0.0.1", "garbage, 10.2.2.2"), "10.2.2.2");
  assert.equal(resolve("::ffff:10.0.0.1", "10.3.3.3"), "10.3.3.3");
  assert.equal(resolve("10.0.0.1", "198.51.100.1", "X-Real-IP", ["192.0.2.7"]), "192.0.2.7");
  assert.equal(resolve("10.0.0.1", "198.51.100.1", "X-Real-IP", ["192.0.2.7", "192.0.2.8"]), "198.51.100.1");
});
