import { splitHostPort } from "./addr.ts";
import { type Config, loadConfig } from "./config.ts";
import { traceCommand } from "./diagnostics.ts";
import { Server } from "./relay.ts";

const args = ["relay", ...process.argv.slice(2)];

function healthURL(addr: string, path: string): string {
  const { host, port } = splitHostPort(addr);
  const local = host === "" || host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  return `http://${local.includes(":") ? `[${local}]` : local}:${port}${path}`;
}

async function probe(addr: string, path: string, print: boolean): Promise<number> {
  try {
    const res = await fetch(healthURL(addr, path), { signal: AbortSignal.timeout(2000) });
    const body = await res.text();
    if (print) {
      process.stdout.write(body);
    }
    if (res.status !== 200) {
      process.stderr.write(`probe: status ${res.status}\n`);
      return 1;
    }
    return 0;
  } catch (err) {
    process.stderr.write(`probe: ${(err as Error).message}\n`);
    return 1;
  }
}

function listenAddr(addr: string): { host: string; port: number } {
  const { host, port } = splitHostPort(addr);
  return { host, port: Number(port) };
}

function announce(address: string, listener: string): void {
  const ev: Record<string, string> = { address, event: "listening" };
  if (listener !== "") {
    ev.listener = listener;
  }
  process.stdout.write(JSON.stringify(ev) + "\n");
}

async function main(): Promise<number | undefined> {
  const traced = traceCommand(args, process.stdout, process.stderr);
  if (traced !== undefined) {
    return traced;
  }
  let cfg: Config;
  try {
    cfg = loadConfig((name) => process.env[name] ?? "");
  } catch (err) {
    process.stderr.write(`relay: invalid configuration: ${(err as Error).message}\n`);
    return 2;
  }
  const admin = cfg.privateAddr || cfg.addr;
  if (args[1] === "healthcheck") {
    return probe(admin, "/healthz", false);
  }
  if (args[1] === "metrics") {
    return probe(admin, "/metrics", true);
  }

  const s = new Server(cfg);
  let address: string;
  let privateAddress = "";
  try {
    address = await s.listen(listenAddr(cfg.addr), false);
  } catch (err) {
    process.stderr.write(`relay: listen: ${(err as Error).message}\n`);
    return 1;
  }
  if (cfg.privateAddr !== "") {
    try {
      privateAddress = await s.listen(listenAddr(cfg.privateAddr), true);
    } catch (err) {
      process.stderr.write(`relay: listen private: ${(err as Error).message}\n`);
      return 1;
    }
  }
  announce(address, "");
  if (privateAddress !== "") {
    announce(privateAddress, "private");
  }
  s.serve((err) => {
    process.stderr.write(`relay: serve: ${err.message}\n`);
    process.exit(1);
  });

  await new Promise<void>((resolve) => {
    process.once("SIGTERM", resolve);
    process.once("SIGINT", resolve);
  });
  await s.shutdown();
  return 0;
}

process.exit((await main()) ?? 0);
