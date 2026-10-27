"""The Spark column expressions must agree with the plain-Python reference."""

import random

import pytest
from pyspark.sql import functions as F

from common import airquality as aq
from common import transforms as tr


def test_spark_correction_and_aqi_match_reference(spark):
    rng = random.Random(1)
    # Dense coverage of every correction band and AQI band, plus exact breakpoints.
    pas = [rng.uniform(0, 600) for _ in range(3000)] + [29.99, 30.0, 49.99, 50.0, 209.99, 210.0, 259.99, 260.0]
    rows = [(pa, rng.uniform(5, 95)) for pa in pas]
    df = spark.createDataFrame(rows, ["pa", "rh"])
    df = df.withColumn("pm25", tr.epa_correct_expr(F.col("pa"), F.col("rh")))
    df = df.withColumn("aqi", tr.aqi_expr(F.col("pm25")))
    df = df.withColumn("cat", tr.aqi_category_expr(F.col("aqi")))

    for r in df.collect():
        expected = aq.epa_correct(r.pa, r.rh)
        assert r.pm25 == pytest.approx(expected, rel=1e-9, abs=1e-9)
        assert r.aqi == aq.pm25_to_aqi(r.pm25)
        assert r.cat == aq.aqi_category(r.aqi)


def test_validate_matches_reference(spark):
    rows = [(10.0, 14.0, 50.0), (100.0, 106.0, 50.0), (10.0, 30.0, 50.0), (-1.0, 2.0, 50.0),
            (5.0, 5.0, 120.0), (None, 5.0, 50.0), (2000.0, 2000.0, 50.0), (0.0, 0.0, 0.0)]
    df = spark.createDataFrame(rows, "pm25_a double, pm25_b double, rh double")
    kept = {(r.pm25_a, r.pm25_b, r.rh) for r in tr.validate(df).collect()}
    expected = {r for r in rows if aq.is_valid(*r) and aq.ab_agree(r[0], r[1])}
    assert kept == expected


def test_sensor_windows_average_each_sensor_per_window(spark):
    t0 = 1598918400.0
    msgs = [
        # sensor 1: two readings in the first 10-minute window, one in the next
        (1, t0 + 60, t0 + 60, 37.33, -121.88, 20.0, 20.0, 15.0, 50.0),
        (1, t0 + 180, t0 + 180, 37.33, -121.88, 40.0, 40.0, 30.0, 50.0),
        (1, t0 + 660, t0 + 660, 37.33, -121.88, 20.0, 20.0, 15.0, 50.0),
        # sensor 2 in the same place and first window
        (2, t0 + 120, t0 + 120, 37.33, -121.88, 30.0, 30.0, 20.0, 50.0),
    ]
    df = spark.createDataFrame(msgs, tr.MESSAGE_SCHEMA)
    out = {(r.sensor_index, r.window_start_s): r
           for r in tr.sensor_windows(df, h3_res=7, window_minutes=10, watermark_minutes=None).collect()}
    assert set(out) == {(1, int(t0)), (1, int(t0) + 600), (2, int(t0))}
    assert out[(1, int(t0))].n_readings == 2
    assert out[(1, int(t0))].pm25 == pytest.approx((aq.epa_correct(20, 50) + aq.epa_correct(40, 50)) / 2)
    assert out[(1, int(t0))].window_end_s == int(t0) + 600


def test_collected_timestamps_are_utc(spark):
    # 1598918400 is 2020-09-01 00:00 UTC; without the UTC pin it comes back as Aug 31 local time.
    from datetime import datetime
    row = spark.createDataFrame([(1598918400.0,)], "t double").select(F.timestamp_seconds("t").alias("ts")).first()
    assert row.ts == datetime(2020, 9, 1, 0, 0)


def test_missing_concentration_gives_missing_aqi_not_hazardous(spark):
    df = spark.createDataFrame([(None,), (30.0,)], "pm25 double")
    rows = df.select(tr.aqi_expr(F.col("pm25")).alias("aqi")).withColumn(
        "cat", tr.aqi_category_expr(F.col("aqi"))).collect()
    assert rows[0].aqi is None and rows[0].cat is None
    assert rows[1].aqi == aq.pm25_to_aqi(30.0)
