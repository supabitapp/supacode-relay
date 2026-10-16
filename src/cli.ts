import { get } from "node:http";
import { writeSync } from "node:fs";
import { basename } from "node:path";
import { splitAddress } from "./address.js";

export function command(
  args: string[],
  address: string,
): Promise<number> | undefined {
  if (!args.length) return;
  if (args.length !== 1 || !["healthcheck", "metrics"].includes(args[0]!)) {
    writeSync(
      2,
      `usage: ${basename(process.argv[1] ?? "relay")} [healthcheck | metrics]\n`,
    );
    return Promise.resolve(2);
  }
  let { host, port } = splitAddress(address);
  if (["", "0.0.0.0", "::"].includes(host)) host = "127.0.0.1";
  return new Promise((resolve) => {
    let settled = false;
    const finish = (code: number) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve(code);
      }
    };
    const request = get(
      {
        hostname: host,
        port,
        path: args[0] === "healthcheck" ? "/healthz" : "/metrics",
        agent: false,
      },
      (response) => {
        let bytes = 0;
        response.on("data", (data: Buffer) => {
          const n = Math.min(data.length, (1 << 20) - bytes);
          if (args[0] === "metrics" && n > 0) writeSync(1, data.subarray(0, n));
          bytes += n;
          if (bytes === 1 << 20) {
            response.destroy();
            finish(response.statusCode === 200 ? 0 : 1);
          }
        });
        response.on("end", () => {
          if (response.statusCode !== 200)
            writeSync(2, `probe: status ${response.statusCode}\n`);
          finish(response.statusCode === 200 ? 0 : 1);
        });
        response.on("error", () => {
          writeSync(2, "probe: response failed\n");
          finish(1);
        });
      },
    );
    request.on("error", () => {
      writeSync(2, "probe: request failed\n");
      finish(1);
    });
    const timer = setTimeout(() => {
      request.destroy();
      finish(1);
    }, 2000);
  });
}
