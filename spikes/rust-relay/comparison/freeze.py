import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--typescript-repo", type=Path, required=True)
    parser.add_argument("--typescript-ref", required=True)
    parser.add_argument("--elixir-repo", type=Path, required=True)
    parser.add_argument("--elixir-ref", required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    repo = here.parents[2]
    inputs = here / "inputs"
    inputs.mkdir(exist_ok=True)
    definitions = [
        ("go", repo, "060472b", ["go.mod", "go.sum", "cmd/relay", "internal", "e2e/bench", "e2e/docker/probe", "docs/protocol.md"]),
        ("rust", repo, "3597a1b", ["spikes/rust-relay/Cargo.toml", "spikes/rust-relay/Cargo.lock", "spikes/rust-relay/src"]),
        ("typescript", args.typescript_repo, args.typescript_ref, ["spikes/typescript-relay"]),
        ("elixir", args.elixir_repo, args.elixir_ref, ["spikes/elixir-relay"]),
    ]
    manifest = {}
    for name, source, ref, paths in definitions:
        commit = git(source, "rev-parse", f"{ref}^{{commit}}").decode().strip()
        archive = git(source, "archive", "--format=tar", commit, *paths)
        (inputs / f"{name}.tar").write_bytes(archive)
        files = git(source, "ls-tree", "-r", "--name-only", commit, "--", *paths).decode().splitlines()
        manifest[name] = {
            "commit": commit,
            "repository": "https://github.com/supabitapp/supacode-relay.git",
            "paths": paths,
            "archiveSha256": hashlib.sha256(archive).hexdigest(),
            "filesSha256": {
                path: hashlib.sha256(git(source, "show", f"{commit}:{path}")).hexdigest()
                for path in files
            },
        }
    driver = repo / "e2e/bench/main.go"
    assert hashlib.sha256(driver.read_bytes()).hexdigest() == manifest["go"]["filesSha256"]["e2e/bench/main.go"]
    (here / "sources.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
