"""Batch simulation of Spark Structured Streaming's watermark semantics.

Running the real streaming job over a 60-day season in 1-minute micro-batches
takes hours per setting, which is too slow for the RQ2 sweep. This module
reproduces, in one batch job, exactly what the streaming job would output for
the same replay, so a whole season takes seconds. tests/test_simulate.py checks
it against the real streaming job.

How Spark decides (one micro-batch per arrival slice, as replay.py writes it).
Since Spark 3.4 a stateful query keeps two watermarks per batch:
  * The eviction watermark of batch k is (max event time seen in batches before
    k) minus the late-data tolerance. A window is finalized and emitted in the
    first batch whose eviction watermark reaches its end. We record that moment
    as emitted_sim_time, in the replay's simulated arrival clock.
  * Late readings are filtered with the PREVIOUS batch's eviction watermark,
    i.e. max event time seen before batch k-1, minus the tolerance. A reading in
    batch k is dropped only if its window's end is at or before that value. This
    one-batch lag makes Spark slightly more lenient than a naive reading of the
    docs suggests; tests/test_simulate.py confirms it against the real engine.
  * Batches with no data are skipped, so "previous batch" means the previous
    non-empty slice.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.enrich import EnrichConfig, enrich, to_output
from common.transforms import add_h3, correct, validate, with_event_ts


def simulate_sensor_windows(df: DataFrame, h3_res: int, window_minutes: int, watermark_minutes: int,
                            slice_seconds: int):
    """Replay messages -> (sensor windows the streaming job would finalize, emission time per window)."""
    window_s, watermark_s = window_minutes * 60, watermark_minutes * 60
    df = add_h3(correct(validate(with_event_ts(df))), h3_res)
    df = (df.withColumn("slice", F.floor(F.col("arrival_time") / slice_seconds).cast("long"))
            .withColumn("window_start_s", (F.floor(F.col("event_time") / window_s) * window_s).cast("long"))
            .withColumn("window_end_s", F.col("window_start_s") + window_s))

    # Running max event time per slice; small (one row per minute of the season).
    slices = df.groupBy("slice").agg(F.max("event_time").alias("slice_max"))
    # One row per non-empty slice, so a global ordering is cheap (hence no partitionBy).
    two_back = Window.orderBy("slice").rowsBetween(Window.unboundedPreceding, -2)
    upto = Window.orderBy("slice").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    slices = (slices.withColumn("late_watermark", F.max("slice_max").over(two_back) - watermark_s)
                    .withColumn("running_max", F.max("slice_max").over(upto)))

    counted = (df.join(F.broadcast(slices.select("slice", "late_watermark")), "slice")
                 .where(F.col("late_watermark").isNull() | (F.col("window_end_s") > F.col("late_watermark"))))

    windows = (counted.groupBy("window_start_s", "window_end_s", "sensor_index", "h3")
                      .agg(F.avg("pm25").alias("pm25"),
                           F.avg("pm1").alias("pm1"),
                           F.count(F.lit(1)).alias("n_readings"),
                           F.max("arrival_time").alias("last_arrival_time")))

    # Emission: the first slice whose running max reaches window_end + tolerance.
    # Windows are few compared with readings, so a range join on the small slice table is fine.
    emit = (windows.select("window_end_s").distinct()
                   .join(F.broadcast(slices.select("slice", "running_max")),
                         F.col("running_max") >= F.col("window_end_s") + watermark_s)
                   .groupBy("window_end_s")
                   .agg(((F.min("slice") + 1) * slice_seconds).alias("emitted_sim_time")))

    # Inner join: windows that are never finalized are never emitted.
    emit = emit.localCheckpoint(eager=True)
    return windows.join(F.broadcast(emit.select("window_end_s")), "window_end_s"), emit


def simulate(df: DataFrame, h3_res: int, window_minutes: int, watermark_minutes: int, slice_seconds: int,
             context: dict | None = None, cfg: EnrichConfig | None = None) -> DataFrame:
    """Replay messages -> the labeled hexagon windows the streaming job would emit."""
    sw, emit = simulate_sensor_windows(df, h3_res, window_minutes, watermark_minutes, slice_seconds)
    # Context is judged as of window_end + watermark, the same cutoff the streaming job uses.
    hw = enrich(sw, context or {}, cfg or EnrichConfig(), cutoff_offset_s=watermark_minutes * 60)
    hw = (hw.join(emit, "window_end_s")
            .withColumn("decision_delay_s", F.col("emitted_sim_time") - F.col("window_end_s")))
    return to_output(hw, extra=("emitted_sim_time", "decision_delay_s"))
