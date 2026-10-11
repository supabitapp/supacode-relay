import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def command(*args):
    return subprocess.check_output(args, stderr=subprocess.STDOUT, text=True).strip()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def cgroup():
    root = Path("/sys/fs/cgroup")
    values = {}
    for name in [
        "cpu.max", "cpu.stat", "cpu.pressure", "cpuset.cpus.effective",
        "memory.max", "memory.swap.max", "memory.current", "memory.events",
    ]:
        path = root / name
        values[name] = path.read_text().strip() if path.exists() else None
    return values


def main():
    expected_driver_sha = "663f7af8868e19ad75541b19653f84b32ca46228fd67388dce28fb7209eea851"
    expected_typescript_sha = "288bce1e9f8fe75048c33b939d9bfa5489112329c66abbe3ca087f949cce8c82"
    manifest = {
        "platform": platform.platform(),
        "osRelease": Path("/etc/os-release").read_text(),
        "cpuAffinity": sorted(os.sched_getaffinity(0)),
        "lscpu": command("lscpu"),
        "cgroup": cgroup(),
        "environment": {name: os.environ.get(name) for name in [
            "PATH", "LANG", "GOMAXPROCS", "GOGC", "GOMEMLIMIT", "NODE_OPTIONS",
            "ERL_FLAGS", "ELIXIR_ERL_OPTIONS", "TOKIO_WORKER_THREADS",
        ]},
        "versions": {
            "go": command("go", "version"),
            "rustc": command("rustc", "--version", "--verbose"),
            "cargo": command("cargo", "--version"),
            "node": command("node", "--version"),
            "bun": command("bun", "--version"),
            "bunRevision": command("bun", "--revision"),
            "elixir": command("elixir", "--version"),
            "otp": command("erl", "-noshell", "-eval", 'io:format("~s", [erlang:system_info(otp_release)]), halt().'),
            "otpExact": command("sh", "-c", "cat /usr/local/lib/erlang/releases/27/OTP_VERSION 2>/dev/null || true"),
            "ps": command("ps", "--version"),
            "typescript": command("/work/typescript/node_modules/.bin/tsc", "--version"),
        },
        "executables": {name: command("which", name) for name in ["go", "rustc", "cargo", "node", "bun", "elixir", "erl", "ps"]},
        "downloadSha256": Path("/opt/toolchain-manifest/downloads.sha256").read_text(),
        "binarySha256": {path: sha(path) for path in [
            "/work/bin/relay-bench", "/work/bin/relay-probe", "/work/bin/go-relay", "/work/bin/rust-relay",
            "/opt/node/bin/node", "/opt/bun-linux-aarch64/bun", "/work/typescript/dist/relay.js",
        ]},
        "driverBuild": command("go", "version", "-m", "/work/bin/relay-bench"),
    }
    assert manifest["binarySha256"]["/work/typescript/dist/relay.js"] == expected_typescript_sha
    assert manifest["binarySha256"]["/work/bin/relay-bench"]
    assert expected_driver_sha == hashlib.sha256(Path("/work/go/e2e/bench/main.go").read_bytes()).hexdigest()
    Path("/results/environment.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
