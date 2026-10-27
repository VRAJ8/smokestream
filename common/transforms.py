"""Spark transforms shared by the streaming job and the hindsight batch job.

Both pipelines run exactly these functions, so any difference in their output
comes from real-time constraints (late data, dropouts, watermarks), not from
different logic. The formulas mirror common/airquality.py.
"""

from functools import lru_cache

import h3
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import airquality as aq

MESSAGE_SCHEMA = T.StructType([
    T.StructField("sensor_index", T.LongType()),
    T.StructField("event_time", T.DoubleType()),
    T.StructField("arrival_time", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("pm25_a", T.DoubleType()),
    T.StructField("pm25_b", T.DoubleType()),
    T.StructField("pm1", T.DoubleType()),
    T.StructField("rh", T.DoubleType()),
])


# Finalized sensor windows: the output of aggregate_sensors and input of enrich.enrich.
SENSOR_WINDOW_SCHEMA = T.StructType([
    T.StructField("window_start_s", T.LongType()),
    T.StructField("window_end_s", T.LongType()),
    T.StructField("sensor_index", T.LongType()),
    T.StructField("h3", T.StringType()),
    T.StructField("pm25", T.DoubleType()),
    T.StructField("pm1", T.DoubleType()),
    T.StructField("n_readings", T.LongType()),
    T.StructField("last_arrival_time", T.DoubleType()),
])


def with_event_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("event_ts", F.timestamp_seconds("event_time"))


def validate(df: DataFrame) -> DataFrame:
    """Step 1: drop implausible readings and readings whose A/B channels disagree."""
    a, b, rh = F.col("pm25_a"), F.col("pm25_b"), F.col("rh")
    in_range = (a.between(aq.PM_MIN_VALID, aq.PM_MAX_VALID)
                & b.between(aq.PM_MIN_VALID, aq.PM_MAX_VALID)
                & rh.between(0.0, 100.0))
    diff = F.abs(a - b)
    mean = (a + b) / 2.0
    pct = F.when(mean > 0, diff / mean).otherwise(F.lit(0.0))
    agree = (diff <= aq.AB_ABS_LIMIT) | (pct <= aq.AB_PCT_LIMIT)
    # isNotNull checks keep nulls out explicitly; a null comparison would also drop them.
    return (df.where(a.isNotNull() & b.isNotNull() & rh.isNotNull() & in_range & agree)
              .withColumn("pa", mean))


def epa_correct_expr(pa: Column, rh: Column) -> Column:
    """Column version of airquality.epa_correct."""
    w1 = pa / 20 - 1.5
    w2 = pa / 50 - 4.2
    value = (
        F.when(pa < 30, 0.524 * pa - 0.0862 * rh + 5.75)
         .when(pa < 50, (0.786 * w1 + 0.524 * (1 - w1)) * pa - 0.0862 * rh + 5.75)
         .when(pa < 210, 0.786 * pa - 0.0862 * rh + 5.75)
         .when(pa < 260, (0.69 * w2 + 0.786 * (1 - w2)) * pa - 0.0862 * rh * (1 - w2)
               + 2.966 * w2 + 5.75 * (1 - w2) + 8.84e-4 * pa * pa * w2)
         .otherwise(2.966 + 0.69 * pa + 8.84e-4 * pa * pa)
    )
    return F.greatest(F.lit(0.0), value)


def correct(df: DataFrame) -> DataFrame:
    """Step 2: apply the EPA correction. Requires validate() first."""
    return df.withColumn("pm25", epa_correct_expr(F.col("pa"), F.col("rh")))


@lru_cache(maxsize=None)
def _cell(lat, lon, res):
    return h3.latlng_to_cell(lat, lon, res)


def add_h3(df: DataFrame, res: int = 7) -> DataFrame:
    """Step 3: map each reading to an H3 hexagon. Resolution 7 is about 5 km2."""
    to_cell = F.udf(lambda lat, lon: _cell(lat, lon, res) if lat is not None and lon is not None else None,
                    T.StringType())
    return df.withColumn("h3", to_cell("lat", "lon"))


def aqi_expr(conc: Column) -> Column:
    """Column version of airquality.pm25_to_aqi. A missing concentration stays missing
    (it must not fall through every breakpoint to the "beyond the index" value of 500)."""
    c = F.floor(conc * 10) / 10
    expr = F.when(c.isNull(), F.lit(None)).when(c < 0, F.lit(0))
    for c_lo, c_hi, i_lo, i_hi in aq.AQI_BREAKPOINTS:
        expr = expr.when(c <= c_hi, F.round((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo))
    return expr.otherwise(F.lit(500)).cast("int")


def aqi_category_expr(aqi: Column) -> Column:
    expr = F.when(aqi.isNull(), F.lit(None).cast("string"))
    for upper, name in aq.AQI_CATEGORIES:
        expr = expr.when(aqi <= upper, name)
    return expr.otherwise("Hazardous")


def aggregate_sensors(df: DataFrame, window_minutes: int, watermark_minutes: int | None = None) -> DataFrame:
    """Step 6 (stateful part): mean corrected PM2.5 per sensor per tumbling window.

    With a watermark (streaming), Spark finalizes a window once the watermark
    passes the window's end, and readings that arrive after that are dropped.
    That is exactly the real-time cost RQ1 and RQ2 measure. The batch job passes
    no watermark and sees every reading. Hexagon averages, AQI and labels are
    computed afterwards from these finalized windows (common/enrich.py).
    """
    if watermark_minutes is not None:
        df = df.withWatermark("event_ts", f"{watermark_minutes} minutes")
    out = (df.groupBy(F.window("event_ts", f"{window_minutes} minutes"), "sensor_index", "h3")
             .agg(F.avg("pm25").alias("pm25"),
                  F.avg("pm1").alias("pm1"),
                  F.count(F.lit(1)).alias("n_readings"),
                  F.max("arrival_time").alias("last_arrival_time")))
    return out.select(F.col("window.start").cast("long").alias("window_start_s"),
                      F.col("window.end").cast("long").alias("window_end_s"),
                      "sensor_index", "h3", "pm25", "pm1", "n_readings", "last_arrival_time")


def sensor_windows(df: DataFrame, h3_res: int, window_minutes: int, watermark_minutes: int | None,
                   apply_correction: bool = True) -> DataFrame:
    """Steps 1-3 and the windowed aggregation, shared by every pipeline.

    apply_correction=False keeps the raw channel mean (for measuring how much the
    EPA correction improves accuracy against monitors; batch baseline only).
    """
    df = validate(with_event_ts(df))
    df = correct(df) if apply_correction else df.withColumn("pm25", F.col("pa"))
    return aggregate_sensors(add_h3(df, h3_res), window_minutes, watermark_minutes)
