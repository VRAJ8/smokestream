"""The batch simulator must reproduce the real streaming job's output exactly.

Runs one small replay, with context, through spark/streaming_job.py (real
Structured Streaming, one micro-batch per file, neighbour-check history carried
between batches) and through common.simulate, for two late-data tolerances,
and compares every emitted hexagon window, including QC exclusions, context
features and smoke labels. Takes a few minutes.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from common.context import load_context
from common.enrich import EnrichConfig
from common.simulate import simulate
from common.transforms import MESSAGE_SCHEMA

ROOT = Path(__file__).resolve().parent.parent
SLICE_S = 1200
QC_MINUTES, REFRESH_MINUTES = 30, 60

# Heavy-tailed delays so a good share of readings arrive after their window closes.
PROFILE = {"staleness_quantiles_s": [0, 30, 60, 120, 300, 600, 900, 1200, 1800, 2700, 3600],
           "outage_rate_per_s": 1 / 7200, "outage_durations_s": [600, 1800]}

EXACT = ["n_readings", "n_sensors", "n_excluded", "aqi", "aqi_category", "in_plume", "plume_density",
         "upwind_fire", "n_fires", "smoke_label", "smoke_alert"]
CLOSE = ["pm25", "pm1", "extent", "fine_ratio", "nearest_fire_km", "wind_dir_deg"]


@pytest.fixture(scope="module")
def replay_dir(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("sim")
    run = lambda *a: subprocess.run([sys.executable, *map(str, a)], check=True, capture_output=True)
    # One day: barbecue at hour 2, before the fire starts (hour 4), smoke from about hour 12,
    # so both "local" and "wildfire" labels appear. 30 sensors give the barbecue enough
    # neighbours to look isolated; with sparse sensors the rule cannot tell (an RQ3 finding).
    run(ROOT / "tools" / "make_synthetic_history.py", "--out", tmp / "synth", "--sensors", "30", "--days", "1",
        "--smoke-hour", "14", "--bbq-hour", "2", "--drift-hour", "6")
    (tmp / "profile.json").write_text(json.dumps(PROFILE))
    run(ROOT / "replay" / "replay.py", "--history", tmp / "synth" / "history.csv",
        "--sensors", tmp / "synth" / "sensors.csv", "--profile", tmp / "profile.json",
        "--out", tmp / "replay", "--file-seconds", SLICE_S)
    return tmp


@pytest.mark.slow
@pytest.mark.parametrize("watermark", [0, 10])
def test_simulator_matches_streaming_job(spark, replay_dir, watermark):
    out = replay_dir / f"stream_wm{watermark}"
    subprocess.run([sys.executable, str(ROOT / "spark" / "streaming_job.py"), "--source", "files",
                    "--input", str(replay_dir / "replay"), "--context", str(replay_dir / "synth" / "context"),
                    "--window", "10", "--watermark", str(watermark), "--qc-history", str(QC_MINUTES),
                    "--context-refresh", str(REFRESH_MINUTES), "--out", str(out), "--once"],
                   check=True, capture_output=True)

    real = {(r.window_start, r.h3): r for r in spark.read.parquet(str(out)).collect()}
    messages = spark.read.schema(MESSAGE_SCHEMA).json(str(replay_dir / "replay"))
    cfg = EnrichConfig(qc_history_minutes=QC_MINUTES, context_refresh_minutes=REFRESH_MINUTES)
    sim_df = simulate(messages, 7, 10, watermark, SLICE_S, load_context(spark, replay_dir / "synth" / "context"), cfg)
    sim = {(r.window_start, r.h3): r for r in sim_df.collect()}

    assert real, "streaming job emitted nothing"
    assert set(sim) == set(real)
    for key, r in real.items():
        for col in EXACT:
            assert getattr(sim[key], col) == getattr(r, col), (key, col)
        for col in CLOSE:
            expected = getattr(r, col)
            assert getattr(sim[key], col) == (None if expected is None else pytest.approx(expected, rel=1e-9)), (key, col)

    # Only meaningful if lateness dropped readings and the labels and QC were exercised.
    labels = {r.smoke_label for r in real.values()}
    assert {"wildfire", "local"} <= labels
    assert sum(r.n_excluded for r in real.values()) > 0  # the QC history carried between batches was used
    if watermark == 0:
        assert sum(r.n_readings for r in real.values()) < 0.97 * messages.count()
