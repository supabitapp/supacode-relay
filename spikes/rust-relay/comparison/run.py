import argparse
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

from manifest import cgroup, sha


TARGETS = {
    "go": "/work/bin/go-relay",
    "rust": "/work/bin/rust-relay",
    "typescript-node": "/work/typescript/run-node.sh",
    "typescript-bun": "/work/typescript/run-bun.sh",
    "elixir": "/comparison/launch-elixir.sh",
}
EXPECTED_EXE = {
    "go": "/work/bin/go-relay",
    "rust": "/work/bin/rust-relay",
    "typescript-node": "/opt/node/bin/node",
    "typescript-bun": "/opt/bun-linux-aarch64/bun",
    "elixir": "beam.smp",
}
FLAGS = ["-payloads", "64,1024,65536", "-clients", "1,32", "-hosts", "4",
         "-inflight", "4", "-warmup", "2s", "-duration", "5s"]
ORDERS = [
    ["go", "rust", "typescript-node", "typescript-bun", "elixir"],
    ["typescript-node", "typescript-bun", "elixir", "go", "rust"],
    ["elixir", "go", "rust", "typescript-node", "typescript-bun"],
]
OUT = Path("/results")


def now():
    return datetime.now(timezone.utc).isoformat()


def process_info(pid):
    root = Path(f"/proc/{pid}")
    return {
        "pid": pid,
        "executable": str((root / "exe").resolve(strict=True)),
        "command": (root / "cmdline").read_bytes().decode().strip("\0").split("\0"),
        "status": (root / "status").read_text(),
    }


def verify_executable(target, info):
    assert info["executable"].endswith(EXPECTED_EXE[target]), info


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def assert_idle():
    names = {"go-relay", "rust-relay", "node", "bun", "beam.smp", "relay-bench"}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            executable = (entry / "exe").resolve(strict=True).name
        except FileNotFoundError:
            continue
        assert executable not in names, f"other relay or driver is running: {entry} {executable}"


def validate(data):
    assert data["warmupSec"] == 2 and data["durationSec"] == 5
    assert data["hosts"] == 4 and data["inflightPerClient"] == 4
    assert data["spawned"] is True and data["url"].startswith("ws://127.0.0.1:")
    assert data["driver"] == {"cpus": 4, "go": "go1.25.1", "goarch": "arm64", "goos": "linux"}
    assert [(r["payloadBytes"], r["clients"]) for r in data["results"]] == [
        (size, clients) for size in [64, 1024, 65536] for clients in [1, 32]
    ]
    for row in data["results"]:
        assert row["failures"] == 0 and row["corrupt"] == 0, row
        assert row["messagesInWindow"] > 0 and row["messagesPerSec"] > 0 and row["payloadMiBPerSec"] > 0
        assert row["rttMicros"]["p99"] >= row["rttMicros"]["p50"] > 0
        assert row["processes"]["relay"]["rssPeakMiB"] > 0
        assert row["processes"]["relay"]["cpuPercentOfOneCore"] >= 0
        assert abs(row["messagesPerSec"] - row["messagesInWindow"] / 5) < 0.051
        assert abs(row["payloadMiBPerSec"] - row["messagesInWindow"] * row["payloadBytes"] / 5 / 2**20) < 0.051


def smoke():
    results = []
    env = dict(os.environ, RELAY_ADDR="127.0.0.1:0", RELAY_PRIVATE_ADDR="",
               RELAY_ADMISSION_RATE="1000000", RELAY_MAX_CONNS="100000", RELAY_MAX_CONNS_PER_IP="100000")
    for target, launcher in TARGETS.items():
        assert_idle()
        with (OUT / f"smoke-{target}.stderr").open("wb") as error:
            process = subprocess.Popen([launcher], env=env, stdout=subprocess.PIPE,
                                       stderr=error, start_new_session=True)
            try:
                deadline = time.monotonic() + 20
                selector = selectors.DefaultSelector()
                selector.register(process.stdout, selectors.EVENT_READ)
                while time.monotonic() < deadline:
                    if not selector.select(max(0, deadline - time.monotonic())):
                        break
                    line = process.stdout.readline()
                    if not line:
                        raise RuntimeError(f"{target} exited before listening")
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if event.get("event") == "listening":
                        break
                else:
                    raise RuntimeError(f"{target} readiness timeout")
                assert event.get("event") == "listening"
                info = process_info(process.pid)
                verify_executable(target, info)
                probe = subprocess.run(["/work/bin/relay-probe", "-mode", "smoke", "-n", "4",
                                        "-url", "ws://" + event["address"]],
                                       capture_output=True, timeout=90, check=True)
                data = json.loads(probe.stdout)
                assert data.get("pairsVerified") == 4 and "error" not in data
                process.terminate()
                exit_code = process.wait(timeout=15)
                results.append({"implementation": target, "result": data, "process": info,
                                "sigtermExitCode": exit_code})
                print(f"smoke {target}: PASS", flush=True)
            finally:
                stop_group(process)
    (OUT / "smoke.json").write_text(json.dumps(results, indent=2) + "\n")


def suite():
    raw = OUT / "raw"
    raw.mkdir(exist_ok=False)
    assert not (OUT / "runs.json").exists(), "remove prior comparison/results before a fresh suite"
    expected_hash = json.loads((OUT / "environment.json").read_text())["binarySha256"]["/work/bin/relay-bench"]
    runs = []
    for repeat, order in enumerate(ORDERS, 1):
        for position, target in enumerate(order, 1):
            assert_idle()
            assert sha("/work/bin/relay-bench") == expected_hash
            stem = f"{repeat:02d}-{position:02d}-{target}"
            command = ["/work/bin/relay-bench", "-spawn", "-relay-bin", TARGETS[target], *FLAGS]
            record = {"repeat": repeat, "position": position, "implementation": target,
                      "command": command, "startedAt": now(), "before": cgroup(),
                      "rawFile": f"raw/{stem}.json", "driverSha256": expected_hash}
            print(f"starting {stem}", flush=True)
            with (raw / f"{stem}.json").open("wb") as stdout, (raw / f"{stem}.stderr").open("wb") as stderr:
                process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
                try:
                    time.sleep(1)
                    children = Path(f"/proc/{process.pid}/task/{process.pid}/children").read_text().split()
                    assert len(children) == 1, children
                    record["relayProcess"] = process_info(int(children[0]))
                    verify_executable(target, record["relayProcess"])
                    record["driverProcess"] = process_info(process.pid)
                    record["exitCode"] = process.wait(timeout=180)
                    assert record["exitCode"] == 0, record
                finally:
                    stop_group(process)
            record["finishedAt"] = now()
            record["after"] = cgroup()
            data = json.loads((OUT / record["rawFile"]).read_bytes())
            validate(data)
            record["rawSha256"] = sha(OUT / record["rawFile"])
            record["validation"] = "passed"
            runs.append(record)
            (OUT / "runs.json").write_text(json.dumps(runs, indent=2) + "\n")
            print(f"finished {stem}: all six cases passed", flush=True)
            time.sleep(2)
    for path in raw.glob("*.stderr"):
        with gzip.open(str(path) + ".gz", "wb") as output:
            output.write(path.read_bytes())
        path.unlink()
    subprocess.run(["python3", "/comparison/aggregate.py"], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["smoke", "suite"])
    args = parser.parse_args()
    {"smoke": smoke, "suite": suite}[args.mode]()
