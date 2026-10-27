#!/usr/bin/env python3
"""Fast stand-in for the streaming job, for the RQ1/RQ2 knob sweep.

Produces the same windows the streaming job would emit for a replay folder
(see common/simulate.py), in seconds instead of hours. Output goes to the same
place and format as spark/streaming_job.py, so analysis/compare_stream_batch.py
works on either. Use the real streaming job for RQ4 (throughput and latency).

Usage:
  python spark/simulate_stream.py --input data/replay/run1 --context data/history/2020/context \
      --window 10 --watermark 5 --qc-history 120 --context-refresh 60
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from common.context import load_context  # noqa: E402
from common.enrich import EnrichConfig  # noqa: E402
from common.simulate import simulate  # noqa: E402
from common.spark import session  # noqa: E402
from common.transforms import MESSAGE_SCHEMA  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, help="replay folder written with --sink files")
    parser.add_argument("--file-seconds", type=int, default=60, help="must match the replay's --file-seconds")
    parser.add_argument("--context", help="context folder (fires/, plume_cells/, wind/); omit to run without")
    parser.add_argument("--window", type=int, default=10, help="aggregation window, minutes")
    parser.add_argument("--watermark", type=int, default=5, help="late-data tolerance, minutes")
    parser.add_argument("--qc-history", type=int, default=120, help="neighbour-check history, minutes")
    parser.add_argument("--context-refresh", type=int, default=60, help="context refresh interval, minutes")
    parser.add_argument("--h3-res", type=int, default=7)
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "stream"))
    parser.add_argument("--config-id", help="defaults to w{window}_wm{watermark}_q{qc}_c{refresh}_r{res}")
    args = parser.parse_args()

    config_id = args.config_id or (f"w{args.window}_wm{args.watermark}_q{args.qc_history}"
                                   f"_c{args.context_refresh}_r{args.h3_res}")
    cfg = EnrichConfig(qc_history_minutes=args.qc_history, context_refresh_minutes=args.context_refresh)
    spark = session(f"smokestream-simulate-{config_id}")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    messages = spark.read.schema(MESSAGE_SCHEMA).json(args.input)
    windows = simulate(messages, args.h3_res, args.window, args.watermark, args.file_seconds,
                       load_context(spark, args.context), cfg)
    (windows.withColumn("config_id", F.lit(config_id))
            .write.mode("overwrite").partitionBy("config_id").parquet(args.out))
    print(f"Simulated streaming run {config_id} written to {args.out}")


if __name__ == "__main__":
    main()
