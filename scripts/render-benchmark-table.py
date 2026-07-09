#!/usr/bin/env python3
"""
Parses JMH's -rff JSON output and rewrites the Benchmarks table in README.md
between the BENCHMARK_TABLE_START / BENCHMARK_TABLE_END markers, with real
measured numbers and a timestamp instead of hand-estimated ranges.

Usage: render-benchmark-table.py <benchmark-results.json> <README.md>
"""
import json
import re
import sys
import platform
from datetime import datetime, timezone

DESCRIPTIONS = {
    "benchmarkMurmur3Token": "Partition key hashing",
    "benchmarkBloomFilterMiss": "Definitive miss (zero disk I/O path)",
    "benchmarkBloomFilterHit": "Probable hit check",
    "benchmarkBloomFilterAdd": "Insert into Bloom filter",
    "benchmarkCommitLogSerialization": "Row to bytes, no I/O",
    "benchmarkCommitLogAppend": "Full FileChannel write, PERIODIC mode",
    "benchmarkRowMerge": "Compaction reconciliation per row",
    "benchmarkTombstonePurge": "Tombstone GC-grace check",
    "benchmarkFullEngineWrite": "CommitLog to MemTable end-to-end",
    "benchmarkEngineReadMemTable": "ConcurrentSkipListMap.get()",
}

MODE_NAMES = {"avgt": "Avg time / op", "thrpt": "Throughput"}

START = "<!-- BENCHMARK_TABLE_START -->"
END = "<!-- BENCHMARK_TABLE_END -->"


def fmt(score, unit):
    return f"{score:.3g} {unit}"


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    json_path, readme_path = sys.argv[1], sys.argv[2]

    with open(json_path) as f:
        data = json.load(f)

    if not data:
        print("No benchmark results found in JSON — JMH run may have failed.")
        sys.exit(1)

    groups = {}
    for entry in data:
        short_name = entry["benchmark"].rsplit(".", 1)[-1]
        mode = entry["mode"]
        metric = entry["primaryMetric"]
        groups.setdefault(short_name, {})[mode] = (metric["score"], metric["scoreUnit"])

    lines = [
        "| Benchmark | What it measures | Avg time/op | Throughput |",
        "|---|---|---|---|",
    ]
    for name in sorted(groups):
        modes = groups[name]
        desc = DESCRIPTIONS.get(name, "")
        avg = modes.get("avgt")
        thr = modes.get("thrpt")
        avg_str = fmt(*avg) if avg else "\u2014"
        thr_str = fmt(*thr) if thr else "\u2014"
        lines.append(f"| `{name}` | {desc} | {avg_str} | {thr_str} |")

    table_md = "\n".join(lines)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    machine = f"{platform.system()} {platform.machine()}"

    block = (
        f"{START}\n"
        f"_Measured {timestamp} on {machine}, inside the same container image "
        f"used everywhere else in this project. Re-run `./scripts/update-benchmark-numbers.sh` "
        f"any time to refresh these numbers on your own hardware._\n\n"
        f"{table_md}\n"
        f"{END}"
    )

    with open(readme_path) as f:
        readme = f.read()

    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.DOTALL)
    if not pattern.search(readme):
        print(f"Markers {START} / {END} not found in {readme_path} — nothing updated.")
        sys.exit(1)

    readme = pattern.sub(lambda _: block, readme)

    with open(readme_path, "w") as f:
        f.write(readme)

    print("README.md benchmark table updated.")


if __name__ == "__main__":
    main()
