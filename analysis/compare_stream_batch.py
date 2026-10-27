#!/usr/bin/env python3
"""Compare a streaming run against the hindsight baseline (RQ1 and RQ2).

Joins the two outputs on (window_start, h3) and reports:
  coverage         share of baseline windows the stream produced at all
  RMSE / MAE       PM2.5 error of the stream vs. the baseline, ug/m3
  category match   share of windows with the same AQI category
  missed readings  share of the baseline's readings the stream never used
  alert precision / recall for "Unhealthy or worse" (AQI > 150) windows
  smoke alert precision / recall  the same, for smoke alerts (AQI > 150 and not
                   labeled local), against the baseline's smoke alerts
  label match      share of the baseline's labeled (elevated) windows that the
                   stream gave the same wildfire / local / uncertain label
  decision latency minutes from a window's start until its result exists:
                   the window length plus the median measured delay after the
                   window ends (simulated runs record it; for real streaming
                   runs it falls back to the watermark). This is the x-axis of
                   the latency-accuracy frontier.

The comparison stops one window plus the watermark before the stream's last
event, since windows after that cannot be finalized in any streaming setting.

Usage:
  python analysis/compare_stream_batch.py --stream-config w10_wm5_q120_c60_r7 --batch-config w10_q120_r7
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from common.spark import session  # noqa: E402

UNHEALTHY_AQI = 150


def compare(stream, batch, stream_config, batch_config):
    """Metrics for one streaming run (DataFrame) against one baseline run (DataFrame)."""
    knobs = re.match(r"w(\d+)_wm(\d+)", stream_config)
    if not knobs:
        raise ValueError("stream config id should look like w10_wm5_q120_c60_r7")
    window_min, watermark_min = int(knobs.group(1)), int(knobs.group(2))

    cutoff = stream.agg(F.max("window_end")).first()[0]
    if cutoff is None:
        raise ValueError(f"streaming run {stream_config} has no output")
    batch = batch.where(F.col("window_end") <= cutoff)

    s = stream.select("window_start", "h3", F.col("pm25").alias("pm25_s"), F.col("aqi").alias("aqi_s"),
                      F.col("aqi_category").alias("cat_s"), F.col("n_readings").alias("n_s"),
                      F.col("smoke_label").alias("label_s"), F.col("smoke_alert").alias("smoke_s"))
    b = batch.select("window_start", "h3", F.col("pm25").alias("pm25_b"), F.col("aqi").alias("aqi_b"),
                     F.col("aqi_category").alias("cat_b"), F.col("n_readings").alias("n_b"),
                     F.col("smoke_label").alias("label_b"), F.col("smoke_alert").alias("smoke_b"))
    j = b.join(s, ["window_start", "h3"], "left")

    m = j.agg(
        F.count(F.lit(1)).alias("baseline_windows"),
        F.count("pm25_s").alias("stream_windows"),
        F.sqrt(F.avg(F.pow(F.col("pm25_s") - F.col("pm25_b"), 2))).alias("rmse"),
        F.avg(F.abs(F.col("pm25_s") - F.col("pm25_b"))).alias("mae"),
        F.avg(F.when(F.col("pm25_s").isNotNull(), (F.col("cat_s") == F.col("cat_b")).cast("double"))).alias("category_match"),
        F.sum("n_b").alias("baseline_readings"),
        F.sum(F.coalesce("n_s", F.lit(0))).alias("stream_readings"),
        F.sum(((F.col("aqi_s") > UNHEALTHY_AQI) & (F.col("aqi_b") > UNHEALTHY_AQI)).cast("int")).alias("tp"),
        F.sum(((F.col("aqi_s") > UNHEALTHY_AQI) & (F.col("aqi_b") <= UNHEALTHY_AQI)).cast("int")).alias("fp"),
        F.sum((F.coalesce(F.col("aqi_s"), F.lit(0)) <= UNHEALTHY_AQI).cast("int")
              * (F.col("aqi_b") > UNHEALTHY_AQI).cast("int")).alias("fn"),
        F.sum((F.col("smoke_s") & F.col("smoke_b")).cast("int")).alias("stp"),
        F.sum((F.col("smoke_s") & ~F.col("smoke_b")).cast("int")).alias("sfp"),
        F.sum((~F.coalesce(F.col("smoke_s"), F.lit(False)) & F.col("smoke_b")).cast("int")).alias("sfn"),
        F.avg(F.when(F.col("label_b").isNotNull(),
                     F.coalesce(F.col("label_s") == F.col("label_b"), F.lit(False)).cast("double"))).alias("label_match"),
    ).first().asDict()

    if "decision_delay_s" in stream.columns:
        delay_min = stream.agg(F.percentile_approx("decision_delay_s", 0.5)).first()[0] / 60
    else:
        delay_min = watermark_min

    tp, fp, fn = m.pop("tp"), m.pop("fp"), m.pop("fn")
    stp, sfp, sfn = m.pop("stp"), m.pop("sfp"), m.pop("sfn")
    return {
        "stream_config": stream_config,
        "batch_config": batch_config,
        "decision_latency_min": round(window_min + delay_min, 2),
        "coverage": m["stream_windows"] / m["baseline_windows"] if m["baseline_windows"] else None,
        "rmse_ugm3": m["rmse"],
        "mae_ugm3": m["mae"],
        "category_match": m["category_match"],
        "missed_readings": 1 - m["stream_readings"] / m["baseline_readings"] if m["baseline_readings"] else None,
        "alert_precision": tp / (tp + fp) if tp + fp else None,
        "alert_recall": tp / (tp + fn) if tp + fn else None,
        "smoke_alert_precision": stp / (stp + sfp) if stp + sfp else None,
        "smoke_alert_recall": stp / (stp + sfn) if stp + sfn else None,
        "label_match": m["label_match"],
        "baseline_windows": m["baseline_windows"],
    }


def append_result(path, result):
    path = Path(path)
    new_file = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(result))
        if new_file:
            writer.writeheader()
        writer.writerow(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", default=str(ROOT / "data" / "out" / "stream"))
    parser.add_argument("--batch", default=str(ROOT / "data" / "out" / "batch"))
    parser.add_argument("--stream-config", required=True)
    parser.add_argument("--batch-config", required=True)
    parser.add_argument("--results", help="also append the result as a row to this CSV (for sweeps)")
    args = parser.parse_args()

    spark = session("smokestream-compare")
    stream = spark.read.parquet(args.stream).where(F.col("config_id") == args.stream_config)
    batch = spark.read.parquet(args.batch).where(F.col("config_id") == args.batch_config)
    try:
        result = compare(stream, batch, args.stream_config, args.batch_config)
    except ValueError as err:
        sys.exit(str(err))
    print(json.dumps(result, indent=2, default=str))
    if args.results:
        append_result(args.results, result)


if __name__ == "__main__":
    main()
