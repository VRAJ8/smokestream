#!/usr/bin/env python3
"""RQ4: throughput and processing latency of a real streaming run.

Needs a replay run with --speed > 0 and --meta (so simulated arrival times map
to wall-clock send times) and the streaming job's output for that run (which
records emitted_at, the wall-clock time each window was written).

  processing latency  emitted_at minus the wall-clock time the window's last
                      reading was sent; this is the system's own delay, on top of
                      the window length and watermark the design adds on purpose
  throughput          messages sent per second, and the share of windows whose
                      latency stayed under a target (60 s by default)

Usage:
  python analysis/measure_streaming.py --output data/out/kafka --config w10_wm5_q120_c60_r7 --meta data/replay/kafka_meta.json
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402


def measure(out: pd.DataFrame, meta: dict, target_s: float = 60.0) -> dict:
    sent = meta["start_wall"] + (out["last_arrival_time"] - meta["first_arrival"]) / meta["speed"]
    emitted = pd.to_datetime(out["emitted_at"], utc=True).astype("int64") / 1e9
    lat = (emitted - sent).clip(lower=0)
    run_s = emitted.max() - meta["start_wall"]
    return {
        "speed": meta["speed"],
        "messages": meta["messages"],
        "windows": len(out),
        "run_seconds": round(run_s, 1),
        "throughput_msgs_per_s": round(meta["messages"] / (meta["finished_wall"] - meta["start_wall"]), 1),
        "latency_p50_s": round(lat.quantile(0.5), 2),
        "latency_p95_s": round(lat.quantile(0.95), 2),
        "latency_p99_s": round(lat.quantile(0.99), 2),
        "share_under_target": round(float((lat <= target_s).mean()), 4),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--meta", required=True)
    parser.add_argument("--target-s", type=float, default=60.0)
    args = parser.parse_args()
    if not json.loads(Path(args.meta).read_text()).get("speed"):
        sys.exit("The replay ran with --speed 0 (as fast as possible); latency needs a fixed speed.")
    out = pd.read_parquet(args.output, filters=[("config_id", "==", args.config)])
    print(json.dumps(measure(out, json.loads(Path(args.meta).read_text()), args.target_s), indent=2))


if __name__ == "__main__":
    main()
