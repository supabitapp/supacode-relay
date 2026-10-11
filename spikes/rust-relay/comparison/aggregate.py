import json
from pathlib import Path
from statistics import median


RESULTS = Path("/results")
METRICS = {
    "messagesPerSec": lambda row: row["messagesPerSec"],
    "payloadMiBPerSec": lambda row: row["payloadMiBPerSec"],
    "rttP50Micros": lambda row: row["rttMicros"]["p50"],
    "rttP99Micros": lambda row: row["rttMicros"]["p99"],
    "cpuPercentOfOneCore": lambda row: row["processes"]["relay"]["cpuPercentOfOneCore"],
    "rssPeakMiB": lambda row: row["processes"]["relay"]["rssPeakMiB"],
}


def summary(values):
    ordered = sorted(values)
    return {"min": ordered[0], "median": median(ordered), "max": ordered[-1]}


def main():
    runs = json.loads((RESULTS / "runs.json").read_text())
    grouped = {}
    for run in runs:
        raw = json.loads((RESULTS / run["rawFile"]).read_text())
        for row in raw["results"]:
            key = (run["implementation"], row["payloadBytes"], row["clients"])
            grouped.setdefault(key, []).append(row)

    expected = {"go", "rust", "typescript-node", "typescript-bun", "elixir"}
    assert {key[0] for key in grouped} == expected
    measurements = []
    for (implementation, payload, clients), rows in sorted(grouped.items()):
        assert len(rows) == 3, (implementation, payload, clients, len(rows))
        assert all(row["failures"] == 0 and row["corrupt"] == 0 for row in rows)
        assert all(row["messagesPerSec"] > 0 and row["payloadMiBPerSec"] > 0 for row in rows)
        measurements.append({
            "implementation": implementation,
            "payloadBytes": payload,
            "clients": clients,
            "repeats": [{
                "messagesPerSec": row["messagesPerSec"],
                "payloadMiBPerSec": row["payloadMiBPerSec"],
                "rttP50Micros": row["rttMicros"]["p50"],
                "rttP99Micros": row["rttMicros"]["p99"],
                "cpuPercentOfOneCore": row["processes"]["relay"]["cpuPercentOfOneCore"],
                "rssPeakMiB": row["processes"]["relay"]["rssPeakMiB"],
                "failures": row["failures"],
                "corrupt": row["corrupt"],
            } for row in rows],
            "summary": {
                name: summary([extract(row) for row in rows])
                for name, extract in METRICS.items()
            },
        })

    aggregate = {
        "schema": "relay-common-comparison/v1",
        "matrix": {
            "payloadBytes": [64, 1024, 65536],
            "clients": [1, 32],
            "hosts": 4,
            "inflightPerClient": 4,
            "warmupSec": 2,
            "durationSec": 5,
            "repeats": 3,
            "compression": False,
            "tls": False,
            "transport": "loopback",
        },
        "measurementNotes": {
            "cpu": "relay process ps elapsed CPU time, percent of one core; Linux ps time has one-second resolution and is approximate over five seconds",
            "rss": "relay process peak RSS sampled by the unchanged Go driver",
            "summary": "min, median, and max across the three rotated-order repeats",
        },
        "measurements": measurements,
    }
    (RESULTS / "aggregate.json").write_text(json.dumps(aggregate, indent=2) + "\n")


if __name__ == "__main__":
    main()
