import json
import random
import subprocess
import sys
from pathlib import Path

from replay.replay import DelayModel, arrivals

ROOT = Path(__file__).resolve().parent.parent


def readings(n_sensors=5, n_steps=200, step=120):
    for k in range(n_steps):
        for s in range(n_sensors):
            yield {"sensor_index": s, "event_time": 1_600_000_000 + k * step}


def test_no_profile_means_no_delay_and_no_dropouts():
    out = list(arrivals(readings(), DelayModel(None, random.Random(0))))
    assert len(out) == 5 * 200
    assert all(r["arrival_time"] == r["event_time"] for r in out)


def test_delays_are_nonnegative_and_output_is_in_arrival_order():
    profile = {"staleness_quantiles_s": [0, 30, 90, 200, 900], "outage_rate_per_s": 0.0, "outage_durations_s": []}
    out = list(arrivals(readings(), DelayModel(profile, random.Random(0))))
    assert len(out) == 1000
    assert all(r["arrival_time"] >= r["event_time"] for r in out)
    times = [r["arrival_time"] for r in out]
    assert times == sorted(times)
    assert any(r["arrival_time"] > r["event_time"] for r in out)


def test_outages_drop_whole_stretches():
    profile = {"staleness_quantiles_s": [0, 0], "outage_rate_per_s": 1 / 3600, "outage_durations_s": [1800]}
    out = list(arrivals(readings(n_steps=2000), DelayModel(profile, random.Random(0))))
    assert 0 < len(out) < 5 * 2000
    # A 30-minute outage removes about 15 consecutive 2-minute readings.
    sensor0 = [r["event_time"] for r in out if r["sensor_index"] == 0]
    longest_gap = max(b - a for a, b in zip(sensor0, sensor0[1:]))
    assert longest_gap >= 1800


def test_replay_cli_writes_json_files(tmp_path):
    synth = tmp_path / "synth"
    subprocess.run([sys.executable, str(ROOT / "tools" / "make_synthetic_history.py"), "--out", str(synth),
                    "--sensors", "4", "--days", "1"], check=True)
    out = tmp_path / "replay"
    subprocess.run([sys.executable, str(ROOT / "replay" / "replay.py"), "--history", str(synth / "history.csv"),
                    "--sensors", str(synth / "sensors.csv"), "--profile", "none", "--out", str(out),
                    "--file-seconds", "600"], check=True)
    lines = [json.loads(line) for f in sorted(out.glob("part-*.json")) for line in f.read_text().splitlines()]
    assert len(lines) == 4 * 720
    # One file per 10 simulated minutes of arrivals, in order.
    assert len(list(out.glob("part-*.json"))) == 24 * 6
    times = [m["arrival_time"] for m in lines]
    assert times == sorted(times)
    assert set(lines[0]) == {"sensor_index", "event_time", "arrival_time", "lat", "lon", "pm25_a", "pm25_b", "pm1", "rh"}


def test_averaged_history_arrives_after_its_interval_ends():
    out = list(arrivals(readings(n_sensors=2, n_steps=50, step=600), DelayModel(None, random.Random(0)), 600))
    assert all(r["arrival_time"] == r["event_time"] + 600 for r in out)
    times = [r["arrival_time"] for r in out]
    assert times == sorted(times)
