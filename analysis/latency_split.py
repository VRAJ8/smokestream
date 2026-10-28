#!/usr/bin/env python3
"""RQ4: how much of the streaming latency is designed waiting, and how much is the system.

A window cannot be emitted until a later reading pushes the watermark past its
end, so part of the latency measure_streaming.py reports is the wait that the
window length, watermark and reading cadence impose by design. Redrawing the
replay's arrivals (same inputs and seed, recorded in the replay's --meta file)
gives each window's closing reading; whatever latency remains is the system's
own overhead: trigger cadence, processing and writes.

  designed wait   closing reading's arrival minus the window's last reading, in
                  simulated minutes and in wall seconds at the replay speed
  overhead        measured latency minus the designed wait, wall seconds

Usage:
  python analysis/latency_split.py --output data/out/demo_live/stream --config w10_wm5_q120_c60_r7 \\
      --meta data/out/demo_live/replay_meta.json
"""

import argparse
import json
import random
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from replay.replay import DelayModel, arrivals, load_sensors, read_history  # noqa: E402


def replayed_arrivals(inputs):
    profile = None if inputs["profile"] == "none" else json.loads(Path(inputs["profile"]).read_text())
    model = DelayModel(profile, random.Random(inputs["seed"]))
    msgs = arrivals(read_history(inputs["history"], load_sensors(inputs["sensors"])), model, inputs["interval_seconds"])
    start, end = inputs.get("start"), inputs.get("end")
    rows = [(m["event_time"], m["arrival_time"]) for m in msgs
            if (start is None or m["arrival_time"] >= start) and (end is None or m["arrival_time"] < end)]
    return pd.DataFrame(rows, columns=["event_time", "arrival_time"])


def split(out, sent, meta, watermark_s):
    arrived = sent.sort_values("arrival_time")
    watermark = arrived["event_time"].cummax().to_numpy() - watermark_s
    window_end = pd.to_datetime(out["window_end"]).astype("int64").to_numpy() // 10**9
    i = np.searchsorted(watermark, window_end, side="left")
    closed = i < len(arrived)  # windows still open when the replay ended were flushed, not closed
    wait_sim = arrived["arrival_time"].to_numpy()[i[closed]] - out["last_arrival_time"].to_numpy()[closed]
    sent_wall = meta["start_wall"] + (out["last_arrival_time"].to_numpy()[closed] - meta["first_arrival"]) / meta["speed"]
    latency = pd.to_datetime(out["emitted_at"], utc=True).astype("int64").to_numpy()[closed] / 1e9 - sent_wall
    overhead = latency - wait_sim / meta["speed"]
    q = lambda x, p: round(float(np.percentile(x, p)), 2)
    return {
        "speed": meta["speed"],
        "windows": int(closed.sum()),
        "designed_wait_sim_min_p50": round(q(wait_sim, 50) / 60, 1),
        "designed_wait_sim_min_p95": round(q(wait_sim, 95) / 60, 1),
        "designed_wait_wall_s_p50": round(q(wait_sim, 50) / meta["speed"], 2),
        "latency_s_p50": q(latency, 50),
        "latency_s_p95": q(latency, 95),
        "overhead_s_p50": q(overhead, 50),
        "overhead_s_p95": q(overhead, 95),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--meta", required=True)
    args = parser.parse_args()
    meta = json.loads(Path(args.meta).read_text())
    if not meta.get("speed") or "inputs" not in meta:
        sys.exit("Needs a replay run with --speed > 0 and --meta (which records the replay inputs).")
    watermark_s = int(re.search(r"_wm(\d+)_", args.config).group(1)) * 60
    out = pd.read_parquet(args.output, filters=[("config_id", "==", args.config)])
    print(json.dumps(split(out, replayed_arrivals(meta["inputs"]), meta, watermark_s), indent=2))


if __name__ == "__main__":
    main()
