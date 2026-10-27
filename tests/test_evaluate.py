"""Monitor-based evaluation and the RQ3 labeling tool."""

import subprocess
import sys
from pathlib import Path

import h3
import pandas as pd
import pytest

from analysis.label_events import cohen_kappa, find_events, stratified_sample
from common.evaluate import EvalConfig, episodes, evaluate, load_monitors, load_output, site_weights

ROOT = Path(__file__).resolve().parent.parent
T0 = 1598918400
SITE = ("060850005", 37.3485, -121.8949)
CELL = h3.latlng_to_cell(SITE[1], SITE[2], 7)


def output(values, labels=None, alerts=None, cell=CELL):
    """Hexagon output rows: one 10-minute window per value."""
    n = len(values)
    return pd.DataFrame({
        "window_start_s": [T0 + i * 600 for i in range(n)], "window_end_s": [T0 + (i + 1) * 600 for i in range(n)],
        "h3": cell, "pm25": values, "aqi": [200 if v > 55.4 else 50 for v in values],
        "smoke_label": labels or [None] * n, "smoke_alert": alerts or [v > 55.4 for v in values],
        "extent": [None] * n, "in_plume": [False] * n, "upwind_fire": [False] * n,
    })


def monitors(hourly):
    return pd.DataFrame({"site_id": SITE[0], "latitude": SITE[1], "longitude": SITE[2],
                         "hour_start": [T0 + h * 3600 for h in range(len(hourly))], "pm25": hourly})


def test_weights_only_cover_nearby_hexagons():
    far = h3.latlng_to_cell(37.80, -122.27, 7)  # Oakland, ~50 km away
    w = site_weights([CELL, far], monitors([10.0]), EvalConfig(radius_km=5))
    assert list(w["h3"]) == [CELL]


def test_accuracy_against_monitor_hours():
    # Hour 0 windows average 12, hour 1 windows average 20; the monitor saw 10 and 20.
    out = output([12.0] * 6 + [20.0] * 6)
    m, per_site, _ = evaluate(out, monitors([10.0, 20.0]))
    assert m["mon_coverage"] == 1.0
    assert m["mon_rmse_ugm3"] == pytest.approx((2 ** 2 / 2) ** 0.5)
    assert m["mon_bias_ugm3"] == pytest.approx(1.0)
    assert per_site.loc[0, "hours"] == 2


def test_local_labels_suppress_smoke_alerts_but_not_value_alerts():
    # Monitor stays clean, but the hexagon spikes from a "local" source: a false alarm on value alone.
    out = output([80.0] * 6, labels=["local"] * 6, alerts=[False] * 6)
    m, _, _ = evaluate(out, monitors([10.0]))
    assert m["mon_value_false_alarm_rate"] == 1.0
    assert m["mon_smoke_false_alarm_rate"] == 0.0


def test_episodes_group_consecutive_unhealthy_hours():
    eps = episodes(monitors([10, 60, 70, 10, 80]))
    assert list(eps["onset"]) == [T0 + 3600, T0 + 4 * 3600]
    assert list(eps["hours"]) == [2, 1]


def test_lead_time_is_measured_against_the_official_publication():
    # Unhealthy from hour 1. The stream alerts in the window starting at T0+3600 and,
    # with a 5-minute watermark, decides at T0+3600+600+300. Official: T0+7200+1800.
    vals = [10.0] * 6 + [80.0] * 6
    m, _, eps = evaluate(output(vals), monitors([10.0, 80.0]), watermark_s=300)
    assert m["mon_episodes"] == 1 and m["mon_detected_share"] == 1.0
    assert eps.loc[0, "lead_min"] == pytest.approx((T0 + 9000 - (T0 + 4500)) / 60)
    assert m["mon_early_share"] == 1.0


def test_cohen_kappa_known_value():
    a = ["w", "w", "l", "l", "u"]
    b = ["w", "l", "l", "l", "u"]
    assert cohen_kappa(a, b) == pytest.approx(0.6875)
    assert cohen_kappa(a, a) == 1.0


def test_events_and_stratified_sample():
    vals = [80.0, 80.0, 10.0, 10.0, 80.0]
    events = find_events(output(vals, labels=["wildfire", "wildfire", None, None, "local"]).assign(
        aqi=[200, 200, 50, 50, 200]))
    assert len(events) == 2  # the 2-window gap splits them
    assert sorted(events["pipeline_label"]) == ["local", "wildfire"]
    assert len(stratified_sample(events, 1, seed=1)) == 1


def test_correction_and_qc_help_against_synthetic_monitors(tmp_path):
    """End to end: the batch job on synthetic data, scored against the synthetic monitors."""
    run = lambda *a: subprocess.run([sys.executable, *map(str, a)], check=True, capture_output=True)
    synth = tmp_path / "synth"
    run(ROOT / "tools" / "make_synthetic_history.py", "--out", synth, "--days", "2", "--smoke-hour", "20")
    out = tmp_path / "batch"
    common = ["--history", synth / "history.csv", "--sensors", synth / "sensors.csv",
              "--context", synth / "context", "--window", "10", "--out", out]
    run(ROOT / "spark" / "batch_job.py", *common)
    run(ROOT / "spark" / "batch_job.py", *common, "--no-correction")
    mon = load_monitors(synth / "monitors")
    corrected, _, eps = evaluate(load_output(out, "w10_q120_r7"), mon)
    raw, _, _ = evaluate(load_output(out, "w10_q120_r7_raw"), mon)
    assert corrected["mon_rmse_ugm3"] < 5 and abs(corrected["mon_bias_ugm3"]) < 2
    assert raw["mon_rmse_ugm3"] > 3 * corrected["mon_rmse_ugm3"]  # raw PurpleAir reads high in smoke
    assert corrected["mon_episodes"] > 0 and corrected["mon_smoke_alert_recall"] > 0.8
