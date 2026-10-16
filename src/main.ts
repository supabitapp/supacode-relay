import { writeSync } from "node:fs";
import { command } from "./cli.js";
import { loadConfig } from "./config.js";
import { RelayServer } from "./http.js";

function log(text: string): void {
  const now = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  writeSync(
    2,
    `${now.getFullYear()}/${pad(now.getMonth() + 1)}/${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())} relay: ${text}\n`,
  );
}

process.on("uncaughtException", () => {
  log("unexpected runtime failure");
  process.exit(1);
});
process.on("unhandledRejection", () => {
  log("unexpected runtime failure");
  process.exit(1);
});

async function main(): Promise<void> {
  let cfg;
  try {
    cfg = loadConfig();
  } catch (error) {
    writeSync(2, `relay: invalid configuration: ${(error as Error).message}\n`);
    process.exitCode = 2;
    return;
  }
  const cli = command(process.argv.slice(2), cfg.privateAddr || cfg.addr);
  if (cli) {
    process.exitCode = await cli;
    return;
  }
  const server = new RelayServer(cfg);
  let shuttingDown = false;
  const stop = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    const start = performance.now();
    log("draining");
    const remaining = await server.shutdown();
    const ms = Math.round(performance.now() - start);
    log(
      `stopped after ${ms >= 1000 ? `${(ms / 1000).toFixed(3).replace(/0+$/, "").replace(/\.$/, "")}s` : `${ms}ms`} with ${remaining} pairs force-closed`,
    );
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  try {
    const address = await server.listen(server.public, cfg.addr);
    if (cfg.privateAddr) {
      const privateAddress = await server.listen(
        server.private,
        cfg.privateAddr,
      );
      writeSync(1, JSON.stringify({ address, event: "listening" }) + "\n");
      writeSync(
        1,
        JSON.stringify({
          address: privateAddress,
          event: "listening",
          listener: "private",
        }) + "\n",
      );
    } else writeSync(1, JSON.stringify({ address, event: "listening" }) + "\n");
  } catch {
    writeSync(2, "relay: listen failed\n");
    process.exit(1);
  }
}

await main();
