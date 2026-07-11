#!/usr/bin/env python3
"""
Prints a side-by-side STCS vs LCS comparison from /stats and /levels JSON
dumps captured by compare-compaction-strategies.sh — real measured numbers,
not hand-estimated ones.

Usage: render-compaction-comparison.py <stcs_stats.json> <stcs_levels.json> <lcs_stats.json> <lcs_levels.json>
"""
import json
import sys


def load(path):
    with open(path) as f:
        return json.load(f)


def main():
    if len(sys.argv) != 5:
        print(__doc__)
        sys.exit(1)

    stcs_stats  = load(sys.argv[1])
    stcs_levels = load(sys.argv[2])
    lcs_stats   = load(sys.argv[3])
    lcs_levels  = load(sys.argv[4])

    def row(label, stcs_val, lcs_val, fmt="{}"):
        print(f"  {label:<34}{fmt.format(stcs_val):>14}{fmt.format(lcs_val):>14}")

    print("=== STCS vs LCS — identical workload, measured on this machine ===\n")
    print(f"  {'':34}{'STCS':>14}{'LCS':>14}")
    print(f"  {'-' * 34}{'-' * 14:>14}{'-' * 14:>14}")
    row("Total writes",               stcs_stats["totalWrites"],   lcs_stats["totalWrites"])
    row("Total reads",                stcs_stats["totalReads"],    lcs_stats["totalReads"])
    row("SSTables on disk",           stcs_stats["sstableCount"],  lcs_stats["sstableCount"])
    row("Compactions run",            stcs_stats["compactionsRun"], lcs_stats["compactionsRun"])
    row("Avg SSTables scanned/read",  stcs_stats["avgSStablesScannedPerSStableRead"],
                                       lcs_stats["avgSStablesScannedPerSStableRead"], "{:.2f}")
    row("Compaction bytes written",   stcs_stats["compactionBytesWritten"], lcs_stats["compactionBytesWritten"])
    row("Bytes reclaimed",            stcs_stats["bytesReclaimed"], lcs_stats["bytesReclaimed"])

    print()
    print("--- Read amplification ---")
    stcs_amp = stcs_stats["avgSStablesScannedPerSStableRead"]
    lcs_amp  = lcs_stats["avgSStablesScannedPerSStableRead"]
    print(f"  STCS opens {stcs_amp:.2f} SSTables per read that reaches the SSTable tier.")
    print(f"  LCS  opens {lcs_amp:.2f} SSTables per read that reaches the SSTable tier.")
    if lcs_amp > 0 and stcs_amp > lcs_amp:
        print(f"  -> STCS touches {stcs_amp / lcs_amp:.1f}x more SSTables per read than LCS on this workload.")
    elif stcs_amp > 0 and lcs_amp >= stcs_amp:
        print("  -> No read-amplification advantage showed up — the dataset is probably still small enough")
        print("     that everything sits in L0/L1. Try a larger write_count to see the gap widen.")

    print()
    print("--- Write amplification ---")
    print(f"  STCS wrote {stcs_stats['compactionBytesWritten']:,} bytes total during compaction.")
    print(f"  LCS  wrote {lcs_stats['compactionBytesWritten']:,} bytes total during compaction.")
    print("  (Same input data in both cases — the gap here is the cost LCS pays for its read-side win.)")

    print()
    print("--- Level structure ---")
    for name, levels in (("STCS", stcs_levels["levels"]), ("LCS", lcs_levels["levels"])):
        print(f"  {name}:")
        if not levels:
            print("    (no SSTables)")
            continue
        for entry in sorted(levels, key=lambda l: l["level"]):
            kb = entry["totalBytes"] / 1024
            print(f"    L{entry['level']}: {entry['sstableCount']} SSTable(s), {kb:.1f} KB")

    print()
    print("Numbers above are from this run only — re-run the script for a fresh measurement,")
    print("and try a larger write_count if LCS hasn't split past L1 yet.")


if __name__ == "__main__":
    main()
