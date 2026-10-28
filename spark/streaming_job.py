#!/usr/bin/env python3
"""SmokeStream streaming job: Kafka (or a folder of JSON files) -> labeled hexagon AQI.

Spark Structured Streaming does steps 1-3 and the windowed per-sensor average
with a watermark. Each micro-batch's finalized sensor windows then go through
steps 4-5 (neighbour check, context, smoke labels; common/enrich.py) and are
appended to Parquet, and upserted into MongoDB if MONGODB_URI is set.

The neighbour check needs the last --qc-history minutes of sensor windows, so
the job keeps that much history in a small buffer, saved after every batch so a
restart picks up where it left off.

Every run is tagged with a config_id built from its knob settings.

Usage:
  python spark/streaming_job.py --source files --input data/replay/run1 --context data/history/2020/context --once
  python spark/streaming_job.py --source kafka --topic purpleair --context data/context
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.context import load_context  # noqa: E402
from common.purpleair import load_env  # noqa: E402
from common.enrich import EnrichConfig, enrich, to_output  # noqa: E402
from common.spark import session  # noqa: E402
from common.transforms import MESSAGE_SCHEMA, SENSOR_WINDOW_SCHEMA, sensor_windows  # noqa: E402


def read_stream(spark, args):
    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .load())
        return raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m")).select("m.*")
    return spark.readStream.schema(MESSAGE_SCHEMA).option("maxFilesPerTrigger", args.files_per_trigger).json(args.input)


def mongo_upsert(rows, collection):
    """Keep one document per hexagon with its most recent finalized window.

    The first set of updates replaces a hexagon's document only if the new window
    is later; the second inserts hexagons seen for the first time. A batch can
    finalize more than one window, so only each hexagon's latest row is written.
    """
    import h3
    from pymongo import UpdateOne
    latest = {}
    for r in rows:
        if r["h3"] not in latest or r["window_end"] > latest[r["h3"]]["window_end"]:
            latest[r["h3"]] = r
    for r in latest.values():
        lat, lon = h3.cell_to_latlng(r["h3"])
        r["location"] = {"type": "Point", "coordinates": [lon, lat]}  # GeoJSON order is lon, lat
    ops = [UpdateOne({"h3": r["h3"], "window_end": {"$lt": r["window_end"]}},
                     {"$set": r}, upsert=False) for r in latest.values()]
    ops += [UpdateOne({"h3": r["h3"]}, {"$setOnInsert": r}, upsert=True) for r in latest.values()]
    if ops:
        collection.bulk_write(ops, ordered=True)


def to_spark(spark, frame):
    """pandas -> Spark with real nulls (pandas stores missing numbers as NaN)."""
    records = frame.astype(object).where(pd.notna(frame), None).to_dict("records")
    return spark.createDataFrame(records, schema=SENSOR_WINDOW_SCHEMA)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=["files", "kafka"], default="files")
    parser.add_argument("--input", help="folder of JSON files written by replay.py --sink files")
    parser.add_argument("--files-per-trigger", type=int, default=1)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="purpleair")
    parser.add_argument("--context", help="context folder (fires/, plume_cells/, wind/); omit to run without")
    parser.add_argument("--window", type=int, default=10, help="aggregation window, minutes")
    parser.add_argument("--watermark", type=int, default=5, help="late-data tolerance, minutes")
    parser.add_argument("--qc-history", type=int, default=120, help="neighbour-check history, minutes")
    parser.add_argument("--context-refresh", type=int, default=60, help="context refresh interval, minutes")
    parser.add_argument("--h3-res", type=int, default=7)
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "stream"))
    parser.add_argument("--config-id", help="defaults to w{window}_wm{watermark}_q{qc}_c{refresh}_r{res}")
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args()
    if args.source == "files" and not args.input:
        parser.error("--input is required with --source files")

    config_id = args.config_id or (f"w{args.window}_wm{args.watermark}_q{args.qc_history}"
                                   f"_c{args.context_refresh}_r{args.h3_res}")
    cfg = EnrichConfig(qc_history_minutes=args.qc_history, context_refresh_minutes=args.context_refresh)
    spark = session(f"smokestream-stream-{config_id}", kafka=args.source == "kafka")
    windows = sensor_windows(read_stream(spark, args), args.h3_res, args.window, args.watermark)
    load_env(ROOT)
    hex_state = None
    if os.environ.get("MONGODB_URI"):
        from pymongo import MongoClient
        hex_state = MongoClient(os.environ["MONGODB_URI"])[os.environ.get("MONGODB_DB", "smokestream")]["hex_state"]

    state_path = Path(args.out) / "_qc_state" / f"{config_id}.parquet"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    keep_s = (args.qc_history + args.window) * 60
    state = {"history": pd.read_parquet(state_path) if state_path.exists() else None}

    def write_batch(batch_df, batch_id):
        current = batch_df.toPandas()
        if current.empty:
            return
        history = state["history"]
        all_sw = current if history is None else pd.concat([history, current], ignore_index=True)
        # If Spark replays a batch after a crash, its windows may already be in the saved history.
        all_sw = all_sw.drop_duplicates(["sensor_index", "window_end_s"], keep="last")

        hw = enrich(to_spark(spark, all_sw), load_context(spark, args.context), cfg,
                    cutoff_offset_s=args.watermark * 60)
        finalized_now = [int(e) for e in current["window_end_s"].unique()]
        out = (to_output(hw.where(F.col("window_end_s").isin(finalized_now)))
               .withColumn("config_id", F.lit(config_id))
               .withColumn("batch_id", F.lit(batch_id))
               .withColumn("emitted_at", F.current_timestamp()))
        out.write.mode("append").partitionBy("config_id").parquet(args.out)
        if hex_state is not None:
            mongo_upsert([r.asDict() for r in out.drop("batch_id").collect()], hex_state)

        newest = all_sw["window_end_s"].max()
        state["history"] = all_sw[all_sw["window_end_s"] > newest - keep_s]
        state["history"].to_parquet(state_path, index=False)

    writer = (windows.writeStream.outputMode("append")
              .foreachBatch(write_batch)
              .option("checkpointLocation", str(Path(args.out) / "_checkpoints" / config_id)))
    query = (writer.trigger(availableNow=True) if args.once else writer.trigger(processingTime="5 seconds")).start()
    print(f"Streaming job {config_id} running. Output: {args.out}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
