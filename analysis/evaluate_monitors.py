#!/usr/bin/env python3
"""Evaluate pipeline outputs against regulatory monitors (see common/evaluate.py).

For each config in a Parquet output folder (stream, simulated or batch), prints
and optionally appends to a CSV: accuracy of hourly estimates at each monitor,
hourly alert precision / recall / false-alarm rate (value-only and smoke alerts),
and episode lead time over the official hourly value.

Usage:
  python analysis/evaluate_monitors.py --output data/out/batch --monitors data/history/2020/monitors
  python analysis/evaluate_monitors.py --output data/out/stream --configs w10_wm5_q120_c60_r7 \\
      --monitors data/history/2020/monitors --results data/out/monitor_eval.csv --details data/out/monitor_details
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from analysis.compare_stream_batch import append_result  # noqa: E402
from common.evaluate import EvalConfig, evaluate, load_monitors, load_output  # noqa: E402


def watermark_of(config_id):
    m = re.search(r"_wm(\d+)", config_id)
    return int(m.group(1)) * 60 if m else 0  # batch runs have no watermark


def configs_in(output):
    return sorted(p.name.split("=", 1)[1] for p in Path(output).glob("config_id=*"))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True, help="Parquet output folder (partitioned by config_id)")
    parser.add_argument("--configs", help="comma-separated config ids; default: all in the folder")
    parser.add_argument("--monitors", required=True, help="monitor CSV/Parquet file or folder")
    parser.add_argument("--label", default="", help="season label stored with each result row")
    parser.add_argument("--radius-km", type=float, default=5.0)
    parser.add_argument("--publish-delay-min", type=float, default=30,
                        help="assumed minutes after the hour before the official value is published")
    parser.add_argument("--results", help="append one row per config to this CSV")
    parser.add_argument("--details", help="folder for per-site and per-episode CSVs")
    args = parser.parse_args()

    cfg = EvalConfig(radius_km=args.radius_km, publish_delay_s=int(args.publish_delay_min * 60))
    monitors = load_monitors(args.monitors)
    configs = args.configs.split(",") if args.configs else configs_in(args.output)
    if not configs:
        sys.exit(f"No config_id=... folders in {args.output}")

    rows = []
    for config_id in configs:
        metrics, per_site, eps = evaluate(load_output(args.output, config_id), monitors, cfg, watermark_of(config_id))
        row = {"season": args.label, "config_id": config_id, **metrics}
        rows.append(row)
        if args.results:
            append_result(args.results, row)
        if args.details:
            d = Path(args.details)
            d.mkdir(parents=True, exist_ok=True)
            per_site.to_csv(d / f"sites_{config_id}.csv", index=False)
            eps.to_csv(d / f"episodes_{config_id}.csv", index=False)

    table = pd.DataFrame(rows).set_index("config_id")
    show = ["mon_coverage", "mon_rmse_ugm3", "mon_bias_ugm3", "mon_r2", "mon_value_alert_precision",
            "mon_smoke_alert_precision", "mon_smoke_alert_recall", "mon_episodes", "mon_lead_median_min"]
    with pd.option_context("display.width", 200, "display.max_columns", 20, "display.float_format", "{:.3f}".format):
        print(table[show].rename(columns=lambda c: c.replace("mon_", "")))


if __name__ == "__main__":
    main()
