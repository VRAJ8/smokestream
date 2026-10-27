"""Steps 4-5 on small hand-built inputs."""

import h3
import pytest
from pyspark.sql import functions as F

from common.context import SCHEMAS
from common.enrich import EnrichConfig, enrich, hex_windows, qc_sensors
from common.transforms import SENSOR_WINDOW_SCHEMA

T0 = 1598918400  # 2020-09-01 00:00 UTC, on the hour
W = 600
SJ = (37.3382, -121.8863)
CELL = h3.latlng_to_cell(*SJ, 7)


def sw_rows(values, cell=CELL):
    """values: {sensor: [pm25 per 10-minute window]} -> sensor-window rows."""
    rows = []
    for sensor, series in values.items():
        for i, pm in enumerate(series):
            rows.append((T0 + i * W, T0 + (i + 1) * W, sensor, cell, float(pm), float(pm) * 0.8, 5, float(T0 + (i + 1) * W)))
    return rows


def hex_pm25(spark, values, qc_minutes):
    sw = spark.createDataFrame(sw_rows(values), SENSOR_WINDOW_SCHEMA)
    hw = hex_windows(qc_sensors(sw, EnrichConfig(qc_history_minutes=qc_minutes)))
    return [r.pm25 for r in hw.orderBy("window_end_s").collect()]


def test_sustained_faulty_sensor_is_excluded(spark):
    pm = hex_pm25(spark, {1: [10] * 12, 2: [10] * 12, 3: [10] * 12, 4: [100] * 12}, qc_minutes=120)
    assert pm[0] == pytest.approx(32.5)          # first window: not enough history to judge yet
    assert pm[1:] == [pytest.approx(10.0)] * 11  # then the faulty sensor is dropped


def test_brief_spike_survives_long_history_but_not_short(spark):
    spike = [10, 10, 10, 10, 100, 100, 10, 10, 10, 10, 10, 10]
    values = {1: [10] * 12, 2: [10] * 12, 3: [10] * 12, 4: spike}
    long_qc = hex_pm25(spark, values, qc_minutes=120)
    short_qc = hex_pm25(spark, values, qc_minutes=20)
    assert long_qc[4] == pytest.approx(32.5) and long_qc[5] == pytest.approx(32.5)  # kept: spike is real-looking
    assert short_qc[4] == pytest.approx(32.5)   # 1 of 2 recent windows deviant: kept
    assert short_qc[5] == pytest.approx(10.0)   # 2 of 2 deviant: excluded


def context_frames(spark, fires=(), plumes=(), wind=()):
    return {
        "fires": spark.createDataFrame(list(fires), SCHEMAS["fires"]),
        "plume_cells": spark.createDataFrame(list(plumes), SCHEMAS["plume_cells"]),
        "wind": spark.createDataFrame(list(wind), SCHEMAS["wind"]),
    }


def one_hex(spark, pm25, n_windows=7):
    return spark.createDataFrame(sw_rows({1: [pm25] * n_windows, 2: [pm25] * n_windows}), SENSOR_WINDOW_SCHEMA)


def test_plume_counts_only_after_it_is_published_and_refreshed(spark):
    # Plume valid from T0, published 30 min later; with hourly refresh it is usable from T0 + 60 min.
    ctx = context_frames(spark, plumes=[(CELL, float(T0), float(T0 + 86400), "Heavy", float(T0 + 1800))])
    cfg = EnrichConfig(context_refresh_minutes=60)
    stream = {r.window_end_s: r.in_plume for r in enrich(one_hex(spark, 80), ctx, cfg, cutoff_offset_s=300).collect()}
    hindsight = {r.window_end_s: r.in_plume for r in enrich(one_hex(spark, 80), ctx, cfg, cutoff_offset_s=None).collect()}
    # Decision for the window ending at T0+3000 is made at T0+3300: before the refresh.
    assert stream[T0 + 3000] is False and stream[T0 + 3600] is True
    assert all(hindsight.values())


@pytest.mark.parametrize("wind_from, speed, fire_km_east, expected", [
    (90, 4.0, 30, True),     # east wind carries smoke from an eastern fire to us
    (270, 4.0, 30, False),   # west wind blows it away
    (90, 0.5, 30, True),     # calm: a close fire counts regardless of direction
    (90, 0.5, 100, False),   # calm: a distant fire does not
])
def test_upwind_fire(spark, wind_from, speed, fire_km_east, expected):
    fire_lon = SJ[1] + fire_km_east / (111.32 * 0.7946)  # km to degrees of longitude at 37.3 N
    ctx = context_frames(
        spark,
        fires=[(float(T0 - 3600), SJ[0], fire_lon, 50.0, float(T0 - 3600))],
        wind=[("KSJC", SJ[0], SJ[1], float(T0), float(wind_from), speed, float(T0))],
    )
    out = enrich(one_hex(spark, 80, n_windows=2), ctx, EnrichConfig(), cutoff_offset_s=None).collect()
    assert {r.upwind_fire for r in out} == {expected}


def test_labels_wildfire_local_and_alerts(spark):
    ring = sorted(h3.grid_ring(CELL, 1))[:4]
    # Regional smoke: center and four neighbours all elevated, inside a plume.
    smoke = sw_rows({1: [90]}) + [r for i, c in enumerate(ring) for r in sw_rows({10 + i: [90]}, cell=c)]
    plumes = [(c, float(T0), float(T0 + W), "Medium", None) for c in [CELL, *ring]]
    out = enrich(spark.createDataFrame(smoke, SENSOR_WINDOW_SCHEMA), context_frames(spark, plumes=plumes),
                 EnrichConfig(), cutoff_offset_s=None)
    center = out.where(F.col("h3") == CELL).first()
    assert center.extent == 1.0 and center.in_plume and center.smoke_label == "wildfire" and center.smoke_alert

    # Barbecue: only the center is elevated, no context.
    bbq = sw_rows({1: [90]}) + [r for i, c in enumerate(ring) for r in sw_rows({10 + i: [8]}, cell=c)]
    out = enrich(spark.createDataFrame(bbq, SENSOR_WINDOW_SCHEMA), context_frames(spark), EnrichConfig(),
                 cutoff_offset_s=None)
    center = out.where(F.col("h3") == CELL).first()
    assert center.aqi > 150 and center.extent == 0.0
    assert center.smoke_label == "local" and center.smoke_alert is False

    # Clean air gets no label at all.
    assert out.where(F.col("h3") == ring[0]).first().smoke_label is None


def test_hexagon_with_every_sensor_excluded_raises_no_alert(spark):
    # A faulty sensor alone in the next hexagon, with three healthy neighbours nearby.
    values = {1: [10] * 6, 2: [10] * 6, 3: [10] * 6, 4: [300] * 6}
    cell_b = sorted(h3.grid_ring(CELL, 1))[0]
    rows = sw_rows({1: values[1], 2: values[2], 3: values[3]}) + sw_rows({4: values[4]}, cell=cell_b)
    out = enrich(spark.createDataFrame(rows, SENSOR_WINDOW_SCHEMA), context_frames(spark), EnrichConfig(),
                 cutoff_offset_s=None)
    lone = out.where(F.col("h3") == cell_b).orderBy("window_end_s").collect()
    # After two windows the lone faulty sensor is excluded, leaving its hexagon with no value.
    assert lone[-1].n_excluded == 1 and lone[-1].pm25 is None
    assert lone[-1].aqi is None and lone[-1].smoke_alert is False


def test_qc_history_zero_switches_the_check_off(spark):
    pm = hex_pm25(spark, {1: [10] * 6, 2: [10] * 6, 3: [10] * 6, 4: [100] * 6}, qc_minutes=0)
    assert pm == [pytest.approx(32.5)] * 6
