import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";

test("closed transports release buffers after repeated partial-message churn", () => {
  const result = spawnSync(
    process.execPath,
    ["--expose-gc", new URL("retention-child.js", import.meta.url).pathname],
    { encoding: "utf8", timeout: 30000 },
  );
  assert.equal(result.status, 0, result.stderr || result.stdout);
});
