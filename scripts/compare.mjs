import { execFileSync, spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";

const output = process.argv[2] ?? "bin/typescript-comparison.json";
const rounds = Number(process.env.BENCH_ROUNDS ?? 3);
const network = `relay-compare-${process.pid}`;
const images = {
  go: "supaterm-relay-go:compare",
  typescript: "supaterm-relay-ts:e2e",
};
const policy = {
  minThroughputRatio: 0.8,
  maxP99Ratio: 1.25,
  memoryBytes: 3 * 1024 ** 3,
  cpus: 2,
  fileDescriptors: 131072,
};
const run = (...args) =>
  execFileSync("docker", args, { encoding: "utf8", maxBuffer: 16 << 20 });
const results = {
  policy,
  platform: run("info", "--format", "{{.Architecture}}").trim(),
  imageDigests: {},
  runs: { go: [], typescript: [] },
  comparisons: [],
  accepted: false,
};
run("network", "create", network);
try {
  for (const [implementation, image] of Object.entries(images)) {
    results.imageDigests[implementation] = run(
      "image",
      "inspect",
      "--format",
      "{{.Id}}",
      image,
    ).trim();
    for (let round = 0; round < rounds; round++) {
      const name = `relay-compare-${implementation}-${process.pid}`;
      let sampler;
      const samples = [];
      try {
        run(
          "run",
          "-d",
          "--name",
          name,
          "--network",
          network,
          "--network-alias",
          "relay",
          "--memory",
          "3g",
          "--memory-swap",
          "3g",
          "--cpus",
          "2",
          "--ulimit",
          "nofile=131072:131072",
          "-e",
          "RELAY_ADMISSION_RATE=100000",
          "-e",
          "RELAY_MAX_CONNS_PER_IP=8192",
          "-e",
          "RELAY_MAX_CLIENTS_PER_HOST_PER_IP=64",
          image,
        );
        for (let attempt = 0; ; attempt++) {
          try {
            run("exec", name, "/relay", "healthcheck");
            break;
          } catch {
            if (attempt === 20)
              throw new Error("benchmark relay did not start");
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        }
        sampler = spawn("docker", ["stats", "--format", "{{json .}}", name], {
          stdio: ["ignore", "pipe", "ignore"],
        });
        sampler.stdout.on("data", (data) => samples.push(data.toString()));
        const benchmark = await new Promise((resolve, reject) => {
          const process = spawn(
            "docker",
            [
              "run",
              "--rm",
              "--network",
              network,
              "--cpus",
              "2",
              "--entrypoint",
              "/relay-bench",
              "supaterm-relay-tools:e2e",
              "-url",
              "ws://relay:8080",
              "-payloads",
              "1024,65536",
              "-clients",
              "1,32",
              "-warmup",
              "1s",
              "-duration",
              "2s",
            ],
            { stdio: ["ignore", "pipe", "pipe"] },
          );
          let out = "",
            error = "";
          process.stdout.on("data", (data) => (out += data));
          process.stderr.on("data", (data) => (error += data));
          process.on("error", reject);
          process.on("close", (code) =>
            code === 0 ? resolve(JSON.parse(out)) : reject(new Error(error)),
          );
        });
        const metrics = JSON.parse(run("exec", name, "/relay", "metrics"));
        results.runs[implementation].push({
          round,
          benchmark,
          metrics,
          samples,
        });
        console.log(`${implementation} round ${round + 1} complete`);
      } finally {
        sampler?.kill();
        run("rm", "-f", name);
      }
    }
  }
  const median = (values) =>
    values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];
  for (const first of results.runs.go[0].benchmark.results) {
    const select = (implementation) =>
      results.runs[implementation].map((run) =>
        run.benchmark.results.find(
          (value) =>
            value.payloadBytes === first.payloadBytes &&
            value.clients === first.clients,
        ),
      );
    const baseline = select("go"),
      candidate = select("typescript");
    const throughputRatio =
      median(candidate.map((r) => r.messagesPerSec)) /
      median(baseline.map((r) => r.messagesPerSec));
    const p99Ratio =
      median(candidate.map((r) => r.rttMicros.p99)) /
      median(baseline.map((r) => r.rttMicros.p99));
    results.comparisons.push({
      payloadBytes: first.payloadBytes,
      clients: first.clients,
      throughputRatio,
      p99Ratio,
      accepted:
        throughputRatio >= policy.minThroughputRatio &&
        p99Ratio <= policy.maxP99Ratio &&
        candidate.every((r) => r.failures === 0 && r.corrupt === 0),
    });
  }
  results.accepted = results.comparisons.every(
    (comparison) => comparison.accepted,
  );
} finally {
  run("network", "rm", network);
  results.measuredAt = new Date().toISOString();
  await mkdir("bin", { recursive: true });
  await writeFile(output, JSON.stringify(results, null, 2) + "\n");
}
console.log(
  JSON.stringify(
    { accepted: results.accepted, comparisons: results.comparisons },
    null,
    2,
  ),
);
