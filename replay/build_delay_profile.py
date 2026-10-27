#!/usr/bin/env python3
"""Turn the live recorder's output into a delay profile for the replay tool.

Reads data/live/ and writes data/delay_profile.json with:
  staleness_quantiles_s  101 quantiles (0th to 100th percentile) of how old a
                         sensor's latest reading was when we polled it
  outage_rate_per_s      how often a sensor starts an outage, per sensor-second
  outage_durations_s     observed outage lengths, sampled from during replay

Only pairs of consecutive successful polls taken a normal interval apart are
used: gaps from the recorder itself being down (laptop asleep) never count as a
sensor outage, and polls squeezed in between scheduled ones (a manual "once")
are too close together to judge whether a sensor went quiet.

Usage: python replay/build_delay_profile.py [--live-dir data/live] [--out data/delay_profile.json]
"""

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_polls(live_dir):
    with (live_dir / "polls.csv").open() as f:
        return [p for p in csv.DictReader(f) if p["status"] == "ok"]


def load_last_seen(live_dir):
    """{poll_epoch: {sensor_index: last_seen}} plus every staleness value seen."""
    by_poll, staleness = defaultdict(dict), []
    for path in sorted(live_dir.glob("readings_*.csv")):
        with path.open() as f:
            for r in csv.DictReader(f):
                if not r.get("last_seen"):
                    continue
                by_poll[r["poll_epoch"]][r["sensor_index"]] = int(float(r["last_seen"]))
                if r["staleness_s"]:
                    staleness.append(max(0.0, float(r["staleness_s"])))
    return by_poll, staleness


def build(live_dir, poll_seconds):
    polls = load_polls(live_dir)
    by_poll, staleness = load_last_seen(live_dir)
    if len(polls) < 2 or not staleness:
        raise SystemExit("Not enough recorded data yet.")

    # Consecutive poll pairs a normal interval apart.
    pairs = []
    for p, q in zip(polls, polls[1:]):
        gap = float(q["poll_epoch"]) - float(p["poll_epoch"])
        if 0.5 * poll_seconds <= gap <= 1.5 * poll_seconds:
            pairs.append((p["poll_epoch"], q["poll_epoch"], gap))

    # A sensor is in an outage while its last_seen does not advance between polls.
    outages, stuck, observed_s = [], defaultdict(float), 0.0
    for p, q, gap in pairs:
        before, after = by_poll.get(p, {}), by_poll.get(q, {})
        for sensor, seen_before in before.items():
            if sensor not in after:
                continue
            observed_s += gap
            if after[sensor] == seen_before:
                stuck[sensor] += gap
            elif stuck[sensor]:
                outages.append(stuck.pop(sensor))
    outages.extend(v for v in stuck.values() if v)  # still out when recording ended

    quantiles = statistics.quantiles(staleness, n=100, method="inclusive")
    return {
        "staleness_quantiles_s": [round(min(staleness), 1)] + [round(q, 1) for q in quantiles] + [round(max(staleness), 1)],
        "outage_rate_per_s": len(outages) / observed_s if observed_s else 0.0,
        "outage_durations_s": [round(d, 1) for d in sorted(outages)],
        "poll_seconds": poll_seconds,
        "polls_used": len(pairs),
        "staleness_samples": len(staleness),
        "median_staleness_s": round(statistics.median(staleness), 1),
        "outages_observed": len(outages),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live-dir", type=Path, default=ROOT / "data" / "live")
    parser.add_argument("--out", type=Path, default=ROOT / "data" / "delay_profile.json")
    parser.add_argument("--poll-seconds", type=int, default=300)
    args = parser.parse_args()

    profile = build(args.live_dir, args.poll_seconds)
    args.out.write_text(json.dumps(profile, indent=2))
    print(f"Wrote {args.out}")
    print(f"  median staleness {profile['median_staleness_s']} s, "
          f"p95 {profile['staleness_quantiles_s'][95]} s, "
          f"{profile['outages_observed']} outages from {profile['polls_used']} poll pairs")


if __name__ == "__main__":
    main()
