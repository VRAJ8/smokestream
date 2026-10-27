#!/usr/bin/env python3
"""Run the RQ1/RQ2 knob sweep for one season in a single Spark session.

For each setting it simulates the streaming output from the replay folder
(common/simulate.py), builds the matching hindsight baseline once per
(window, QC history) pair, compares the two (analysis/compare_stream_batch.py)
and appends one row to the results CSV. With --monitors, each setting (and its
baseline) is also scored against regulatory monitors (common/evaluate.py), and
those mon_* columns go in the same row. Settings already in the CSV are
skipped, so an interrupted sweep resumes where it stopped.

Plan: every knob varied one at a time around the default setting, plus,
optionally, a full grid over two knobs (--grid watermark,qc_history).

Usage:
  python analysis/run_sweep.py --label 2020 --replay data/replay/2020 \\
      --history data/history/2020/history_avg10.csv --sensors data/history/2020/sensors.csv \\
      --context data/history/2020/context --results data/out/sweep.csv
"""

import argparse
import csv
import itertools
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from analysis.compare_stream_batch import append_result, compare  # noqa: E402
from common.context import load_context  # noqa: E402
from common.enrich import EnrichConfig, enrich, to_output  # noqa: E402
from common.evaluate import evaluate, load_monitors, load_output  # noqa: E402
from common.loaders import load_history  # noqa: E402
from common.simulate import simulate  # noqa: E402
from common.spark import session  # noqa: E402
from common.transforms import MESSAGE_SCHEMA, sensor_windows  # noqa: E402

# Levels from the proposal's Table 2. With 10-minute history data the smallest
# window is 10; override with --levels window=2,10,30 for 2-minute data.
LEVELS = {"window": [10, 30, 60], "watermark": [0, 5, 15], "qc_history": [30, 120, 360],
          "context_refresh": [15, 60, 360]}
DEFAULT = {"window": 10, "watermark": 5, "qc_history": 120, "context_refresh": 60}


def plan(levels, default, grid):
    settings = [dict(default)]
    for knob, values in levels.items():
        settings += [{**default, knob: v} for v in values]
    if grid:
        a, b = grid
        settings += [{**default, a: va, b: vb} for va, vb in itertools.product(levels[a], levels[b])]
    unique = []
    for st in settings:
        if st not in unique:
            unique.append(st)
    return unique


def stream_id(st, res):
    return f"w{st['window']}_wm{st['watermark']}_q{st['qc_history']}_c{st['context_refresh']}_r{res}"


def parse_levels(items):
    levels = {k: list(v) for k, v in LEVELS.items()}
    for item in items or []:
        knob, values = item.split("=")
        if knob not in levels:
            sys.exit(f"unknown knob {knob}; choose from {', '.join(levels)}")
        levels[knob] = [int(v) for v in values.split(",")]
    return levels


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True, help="season label stored with each result row, e.g. 2020")
    parser.add_argument("--replay", required=True, help="replay folder (replay.py --sink files)")
    parser.add_argument("--file-seconds", type=int, default=60, help="must match the replay's --file-seconds")
    parser.add_argument("--history", required=True, help="history CSV for the hindsight baseline")
    parser.add_argument("--sensors", required=True)
    parser.add_argument("--context", help="context folder; omit to run without context")
    parser.add_argument("--monitors", help="regulatory monitor CSV/Parquet; adds ground-truth (mon_*) metrics")
    parser.add_argument("--h3-res", type=int, default=7)
    parser.add_argument("--levels", nargs="*", help="override knob levels, e.g. window=2,10,30")
    parser.add_argument("--grid", help="two knobs for a full grid, e.g. watermark,qc_history")
    parser.add_argument("--out", default=str(ROOT / "data" / "out"))
    parser.add_argument("--results", default=str(ROOT / "data" / "out" / "sweep.csv"))
    args = parser.parse_args()

    levels = parse_levels(args.levels)
    grid = args.grid.split(",") if args.grid else None
    settings = plan(levels, DEFAULT, grid)
    done = set()
    if Path(args.results).exists():
        with open(args.results) as f:
            done = {(r["season"], r["stream_config"]) for r in csv.DictReader(f)}

    spark = session("smokestream-sweep")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    messages = spark.read.schema(MESSAGE_SCHEMA).json(args.replay).cache()
    readings = load_history(spark, args.history, args.sensors).cache()
    context = load_context(spark, args.context)
    stream_out, batch_out = f"{args.out}/stream_{args.label}", f"{args.out}/batch_{args.label}"
    baselines, baseline_truth = {}, {}
    monitors = load_monitors(args.monitors) if args.monitors else None

    print(f"{len(settings)} settings for season {args.label}", flush=True)
    for st in settings:
        sid = stream_id(st, args.h3_res)
        if (args.label, sid) in done:
            print(f"  {sid}: already in results, skipped")
            continue
        started = time.time()

        key = (st["window"], st["qc_history"])
        if key not in baselines:
            bid = f"w{st['window']}_q{st['qc_history']}_r{args.h3_res}"
            sw = sensor_windows(readings, args.h3_res, st["window"], None)
            hw = enrich(sw, context, EnrichConfig(qc_history_minutes=st["qc_history"]), cutoff_offset_s=None)
            (to_output(hw).withColumn("config_id", F.lit(bid))
                          .write.mode("overwrite").partitionBy("config_id").parquet(batch_out))
            baselines[key] = bid
            if monitors is not None:
                truth, _, _ = evaluate(load_output(batch_out, bid), monitors)
                baseline_truth[key] = {"base_mon_rmse_ugm3": truth["mon_rmse_ugm3"],
                                       "base_mon_lead_median_min": truth["mon_lead_median_min"]}

        cfg = EnrichConfig(qc_history_minutes=st["qc_history"], context_refresh_minutes=st["context_refresh"])
        (simulate(messages, args.h3_res, st["window"], st["watermark"], args.file_seconds, context, cfg)
            .withColumn("config_id", F.lit(sid))
            .write.mode("overwrite").partitionBy("config_id").parquet(stream_out))

        result = compare(spark.read.parquet(stream_out).where(F.col("config_id") == sid),
                         spark.read.parquet(batch_out).where(F.col("config_id") == baselines[key]),
                         sid, baselines[key])
        if monitors is not None:
            truth, _, _ = evaluate(load_output(stream_out, sid), monitors, watermark_s=st["watermark"] * 60)
            result.update(truth, **baseline_truth[key])
        append_result(args.results, {"season": args.label, **st, **result})
        print(f"  {sid}: latency {result['decision_latency_min']} min, RMSE {result['rmse_ugm3']:.2f}, "
              f"label match {result['label_match'] or 0:.3f} ({time.time() - started:.0f} s)", flush=True)


if __name__ == "__main__":
    main()
