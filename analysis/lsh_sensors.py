#!/usr/bin/env python3
"""Locality-sensitive hashing of sensor-days: which sensors move together?

Wildfire smoke raises PM2.5 over a whole region at once, so on smoke days many
sensors trace the same curve; a local source moves one or a few. This finds
sensor-days with correlated PM2.5 curves using random-hyperplane LSH (SimHash,
Charikar 2002) instead of comparing every pair.

  vector     one sensor's corrected PM2.5 over one local day, 144 ten-minute slots,
             log-scaled and z-normalized, so cosine similarity is Pearson correlation
  signature  bands x rows random hyperplanes; sensor-days that agree on every bit
             of any band become candidate pairs, which are then checked exactly
  coherence  share of the other sensors on the same day correlated above the
             threshold: the "how regional is it" signal, per day class

Two checks: within each day (the real use, small n) and across all sensor-days of
all windows pooled (where all-pairs comparison is what LSH is for).

Usage:
  python analysis/lsh_sensors.py --out data/out/lsh_sensors.json
"""

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common import airquality as aq  # noqa: E402

RUNS = ["2020", "2021-july4", "2021-smoke", "2024-july4"]
SLOTS = 144
JULY4 = {"2021-07-04", "2024-07-04"}


def sensor_days(run, min_slots=120):
    raw = pd.read_csv(ROOT / "data" / "history" / run / "history_avg10.csv")
    a, b, rh = raw["pm2.5_cf_1_a"], raw["pm2.5_cf_1_b"], raw["humidity_a"]
    raw = raw[aq.valid_ab_mask(a, b, rh)]
    pm = np.vectorize(aq.epa_correct)((raw["pm2.5_cf_1_a"] + raw["pm2.5_cf_1_b"]) / 2, raw["humidity_a"])
    local = pd.to_datetime(raw["time_stamp"], unit="s", utc=True).dt.tz_convert("America/Los_Angeles")
    df = pd.DataFrame({"sensor": raw["sensor_index"].to_numpy(), "day": local.dt.strftime("%Y-%m-%d").to_numpy(),
                       "slot": (local.dt.hour * 6 + local.dt.minute // 10).to_numpy(), "pm": pm})
    grid = df.pivot_table(index=["day", "sensor"], columns="slot", values="pm", aggfunc="mean")
    grid = grid.reindex(columns=range(SLOTS))
    grid = grid[grid.notna().sum(axis=1) >= min_slots]
    grid = grid.interpolate(axis=1, limit_direction="both")
    return grid


def normalize(grid):
    x = np.log1p(grid.to_numpy())
    x = x - x.mean(axis=1, keepdims=True)
    norm = np.linalg.norm(x, axis=1, keepdims=True)
    keep = norm[:, 0] > 1e-9  # a flat line has no shape to compare
    return x[keep] / norm[keep], grid.index[keep]


def lsh_candidates(x, bands, rows, seed):
    planes = np.random.default_rng(seed).standard_normal((x.shape[1], bands * rows))
    bits = (x @ planes > 0).reshape(len(x), bands, rows)
    weights = 1 << np.arange(rows)
    keys = (bits * weights).sum(axis=2)  # one integer per band
    pairs = set()
    for band in range(bands):
        buckets = {}
        for i, k in enumerate(keys[:, band]):
            buckets.setdefault(int(k), []).append(i)
        for members in buckets.values():
            for p in range(len(members)):
                for q in range(p + 1, len(members)):
                    pairs.add((members[p], members[q]))
    return pairs


def compare(x, threshold, bands, rows, seed):
    t0 = time.perf_counter()
    corr = x @ x.T
    exact = {(i, j) for i, j in zip(*np.nonzero(np.triu(corr >= threshold, k=1)))}
    t_exact = time.perf_counter() - t0
    t0 = time.perf_counter()
    cand = lsh_candidates(x, bands, rows, seed)
    found = {(i, j) for i, j in cand if float(x[i] @ x[j]) >= threshold}
    t_lsh = time.perf_counter() - t0
    n_pairs = len(x) * (len(x) - 1) // 2
    return {"items": len(x), "all_pairs": n_pairs, "similar_pairs": len(exact), "candidates": len(cand),
            "candidate_share": round(len(cand) / n_pairs, 4) if n_pairs else None,
            "recall": round(len(found & exact) / len(exact), 3) if exact else None,
            "exact_s": round(t_exact, 3), "lsh_s": round(t_lsh, 3)}, found


def day_class(day, regional_pm):
    if day in JULY4:
        return "july4"
    if regional_pm > 35.4:  # the day's typical sensor above AQI 100: regional smoke
        return "smoke"
    return "clean" if regional_pm < 12.0 else "moderate"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--threshold", type=float, default=0.8, help="correlation that counts as moving together")
    parser.add_argument("--bands", type=int, default=16)
    parser.add_argument("--rows", type=int, default=8)
    parser.add_argument("--seed", type=int, default=226)
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "lsh_sensors.json"))
    args = parser.parse_args()

    grids = {run: sensor_days(run) for run in RUNS}
    per_day, coherence = [], {}
    for run, grid in grids.items():
        for day, g in grid.groupby(level="day"):
            x, _ = normalize(g)
            if len(x) < 5:
                continue
            res, found = compare(x, args.threshold, args.bands, args.rows, args.seed)
            n = len(x)
            partners = np.zeros(n)
            for i, j in found:
                partners[i] += 1
                partners[j] += 1
            cls = day_class(day, float(g.median(axis=0).mean()))
            coherence.setdefault(cls, []).append(float((partners / (n - 1)).mean()))
            per_day.append({"run": run, "day": day, "class": cls, **res,
                            "coherence": round(float((partners / (n - 1)).mean()), 3)})

    pooled = pd.concat(grids.values())
    x, _ = normalize(pooled)
    pooled_res, _ = compare(x, args.threshold, args.bands, args.rows, args.seed)
    days = pd.DataFrame(per_day)
    result = {
        "threshold": args.threshold, "bands": args.bands, "rows": args.rows,
        "within_day": {"days": len(days), "mean_recall": round(days["recall"].dropna().mean(), 3),
                       "mean_candidate_share": round(days["candidate_share"].mean(), 3)},
        "pooled": pooled_res,
        "coherence_by_class": {c: {"days": len(v), "mean": round(float(np.mean(v)), 3)}
                               for c, v in sorted(coherence.items())},
        "july4_days": days[days["class"] == "july4"][["run", "day", "items", "coherence"]].to_dict("records"),
    }
    text = json.dumps(result, indent=2)
    print(text)
    Path(args.out).write_text(text)
    days.to_csv(Path(args.out).with_suffix(".days.csv"), index=False)


if __name__ == "__main__":
    main()
