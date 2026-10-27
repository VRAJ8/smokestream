#!/usr/bin/env python3
"""SmokeStream hindsight baseline: the same pipeline over the complete history.

No watermark, no simulated delays or dropouts, and all context data as finally
published: every reading and every fire detection counts. This is the "best
possible answer" each streaming setting is compared to (RQ1, RQ2). It uses the
same neighbour check as the stream, so --qc-history should match the runs it is
compared with.

Usage:
  python spark/batch_job.py --history data/synthetic/history.csv --sensors data/synthetic/sensors.csv \
      --context data/synthetic/context --window 10
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from common.context import load_context  # noqa: E402
from common.enrich import EnrichConfig, enrich, to_output  # noqa: E402
from common.loaders import load_history  # noqa: E402
from common.spark import session  # noqa: E402
from common.transforms import sensor_windows  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--history", required=True)
    parser.add_argument("--sensors", required=True)
    parser.add_argument("--context", help="context folder (fires/, plume_cells/, wind/); omit to run without")
    parser.add_argument("--window", type=int, default=10, help="aggregation window, minutes")
    parser.add_argument("--qc-history", type=int, default=120, help="neighbour-check history, minutes")
    parser.add_argument("--h3-res", type=int, default=7)
    parser.add_argument("--no-correction", action="store_true",
                        help="skip the EPA correction (to measure its effect against monitors)")
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "batch"))
    args = parser.parse_args()

    config_id = f"w{args.window}_q{args.qc_history}_r{args.h3_res}" + ("_raw" if args.no_correction else "")
    spark = session(f"smokestream-batch-{config_id}")
    # Overwrite only this config's partition, not baselines for other window sizes.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sw = sensor_windows(load_history(spark, args.history, args.sensors), args.h3_res, args.window, None,
                        apply_correction=not args.no_correction)
    hw = enrich(sw, load_context(spark, args.context), EnrichConfig(qc_history_minutes=args.qc_history),
                cutoff_offset_s=None)
    (to_output(hw).withColumn("config_id", F.lit(config_id))
            .write.mode("overwrite").partitionBy("config_id").parquet(args.out))
    print(f"Batch baseline {config_id} written to {args.out}")


if __name__ == "__main__":
    main()
