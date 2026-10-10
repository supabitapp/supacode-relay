import argparse
import datetime
import itertools
import json
import os
from pathlib import Path
import platform
import selectors
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]


def command_output(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def start_server(binary, workers, log):
    environment = os.environ.copy()
    environment.update(
        RELAY_ADDR="127.0.0.1:0",
        RELAY_ADMISSION_RATE="1000000",
        GOMAXPROCS=str(workers),
        RELAY_WORKERS=str(workers),
    )
    for key in ("RELAY_ROUTERS", "RELAY_NODE_ID", "RELAY_PRIVATE_ADDR"):
        environment.pop(key, None)
    server = subprocess.Popen(
        [str(binary)], cwd=ROOT, env=environment, stdout=subprocess.PIPE,
        stderr=log, text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(server.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=10):
                raise RuntimeError("relay did not report readiness")
        ready = json.loads(server.stdout.readline())
        if ready.get("event") != "listening":
            raise RuntimeError(f"unexpected startup event: {ready}")
        return server, "ws://" + ready["address"]
    except BaseException:
        stop_server(server)
        raise


def stop_server(server):
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
    server.stdout.close()


def relay_binary(implementation):
    return ROOT / ("bin/spike-go" if implementation == "go" else "spikes/rust-relay/target/release/relay-spike")


def check_protocol(args):
    results = []
    command = ["go", "test", "-race", "-count=1", "-json", "./spikes/rust-relay"]
    for implementation in ("go", "rust"):
        with (args.out / f"{implementation}-protocol.log").open("w") as log:
            server, base = start_server(relay_binary(implementation), args.workers, log)
            try:
                checked = subprocess.run(command, cwd=ROOT, env={**os.environ, "RELAY_SPIKE_URL": base}, text=True, capture_output=True, timeout=60)
                events = [json.loads(line) for line in checked.stdout.splitlines()]
                tests = [event["Test"] for event in events if event["Action"] == "pass" and "Test" in event]
                passed = checked.returncode == 0 and bool(tests)
                results.append({"implementation": implementation, "passed": passed, "tests": tests, "command": "RELAY_SPIKE_URL=<loopback URL> " + " ".join(command)})
                print(f"{implementation}: {len(tests)} protocol checks passed", flush=True)
                if not passed:
                    raise RuntimeError(checked.stdout + checked.stderr)
            finally:
                stop_server(server)
    (args.out / "protocol.json").write_text(json.dumps(results, indent=2) + "\n")


def measure(args, implementation, payload, clients, round_number):
    log_path = args.out / f"{implementation}-{payload}-{clients}-{round_number}.log"
    with log_path.open("w") as log:
        server, base = start_server(relay_binary(implementation), args.workers, log)
        try:
            command = [
                str(ROOT / "bin/spike-pathbench"), "-path", "direct", "-direct", base,
                "-direct-pid", str(server.pid), "-sample-driver",
                "-payloads", str(payload), "-clients", str(clients),
                "-hosts", "4", "-inflight", "4",
                "-warmup", f"{args.warmup}s", "-duration", f"{args.duration}s",
            ]
            benchmark = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=args.warmup + args.duration + 30)
            if benchmark.returncode:
                raise RuntimeError(benchmark.stderr)
            result = json.loads(benchmark.stdout)["results"][0]
            result.update(implementation=implementation, round=round_number)
            print(f"round {round_number} {implementation:4} {payload:7}B x{clients:<3} {result['messagesPerSec']:9.0f} msg/s p50={result['rttMicros']['p50']:.0f}us p99={result['rttMicros']['p99']:.0f}us CPU={result['processes']['node']['cpuPercentOfOneCore']:.1f}% RSS={result['processes']['node']['rssPeakMiB']:.1f}MiB failures={result['failures']} corrupt={result['corrupt']}", flush=True)
            return result
        finally:
            stop_server(server)


def summarize(results):
    rows = []
    cases = sorted({(row["payloadBytes"], row["clients"]) for row in results})
    for payload, clients in cases:
        row = {"payloadBytes": payload, "clients": clients}
        for implementation in ("go", "rust"):
            samples = [sample for sample in results if (sample["payloadBytes"], sample["clients"], sample["implementation"]) == (payload, clients, implementation)]
            row[implementation] = {
                "messagesPerSec": statistics.median(sample["messagesPerSec"] for sample in samples),
                "throughputMin": min(sample["messagesPerSec"] for sample in samples),
                "throughputMax": max(sample["messagesPerSec"] for sample in samples),
                "p50Micros": statistics.median(sample["rttMicros"]["p50"] for sample in samples),
                "p99Micros": statistics.median(sample["rttMicros"]["p99"] for sample in samples),
                "cpuPercentOfOneCore": statistics.median(sample["processes"]["node"]["cpuPercentOfOneCore"] for sample in samples),
                "rssPeakMiB": statistics.median(sample["processes"]["node"]["rssPeakMiB"] for sample in samples),
                "driverCPUPercentOfOneCore": statistics.median(sample["processes"]["driver"]["cpuPercentOfOneCore"] for sample in samples),
                "cpuSecondsPerMillionMessages": statistics.median(sample["processes"]["node"]["cpuPercentOfOneCore"] * 10000 / sample["messagesPerSec"] for sample in samples),
                "failures": sum(sample["failures"] for sample in samples),
                "corrupt": sum(sample["corrupt"] for sample in samples),
            }
        row["rustThroughputRatio"] = row["rust"]["messagesPerSec"] / row["go"]["messagesPerSec"]
        row["rustCPUPerMessageRatio"] = row["rust"]["cpuSecondsPerMillionMessages"] / row["go"]["cpuSecondsPerMillionMessages"]
        row["validPerformanceComparison"] = all(sample["failures"] == 0 and sample["corrupt"] == 0 for sample in results if (sample["payloadBytes"], sample["clients"]) == (payload, clients))
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--duration", type=float, default=3)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--payloads", default="64,1024,65536,1048576")
    parser.add_argument("--clients", default="1,32,128")
    parser.add_argument("--protocol-only", action="store_true")
    parser.add_argument("--out", type=Path, default=Path("spikes/rust-relay/results"))
    args = parser.parse_args()
    if min(args.rounds, args.warmup, args.duration, args.workers) <= 0:
        parser.error("rounds, warmup, duration, and workers must be positive")
    payloads = [int(value) for value in args.payloads.split(",")]
    clients = [int(value) for value in args.clients.split(",")]
    if min(payloads) < 16 or min(clients) <= 0 or max(payloads) > 1 << 20 or max(clients) > 256:
        parser.error("payloads must be 16..1048576 bytes and clients 1..256")
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    (ROOT / "bin").mkdir(exist_ok=True)
    subprocess.run(["go", "build", "-trimpath", "-ldflags=-s -w", "-o", "bin/spike-go", "./cmd/relay"], cwd=ROOT, check=True, env={**os.environ, "CGO_ENABLED": "0"})
    subprocess.run(["go", "build", "-trimpath", "-ldflags=-s -w", "-o", "bin/spike-pathbench", "./e2e/pathbench"], cwd=ROOT, check=True)
    subprocess.run(["cargo", "build", "--release", "--locked", "--manifest-path", "spikes/rust-relay/Cargo.toml"], cwd=ROOT, check=True)
    if args.protocol_only:
        check_protocol(args)
        return
    report = {
        "measuredAtUTC": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "baselineCommit": command_output("git", "rev-parse", "HEAD"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logicalCPUs": os.cpu_count(),
        "go": command_output("go", "version"),
        "rust": command_output("rustc", "--version"),
        "workers": args.workers, "rounds": args.rounds,
        "warmupSec": args.warmup, "durationSec": args.duration,
        "hosts": 4, "inflightPerClient": 4,
        "resourceSampling": "measurement window, excluding drain",
        "results": [],
    }
    for round_number in range(1, args.rounds + 1):
        for case_number, (payload, count) in enumerate(itertools.product(payloads, clients)):
            order = ("go", "rust") if (round_number + case_number) % 2 else ("rust", "go")
            for implementation in order:
                report["results"].append(measure(args, implementation, payload, count, round_number))
                (args.out / "raw.json").write_text(json.dumps(report, indent=2) + "\n")
                time.sleep(0.3)
    summary = summarize(report["results"])
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    if any(not row["validPerformanceComparison"] for row in summary):
        raise SystemExit("Some cases had failures; inspect raw.json before interpreting throughput.")


if __name__ == "__main__":
    main()
