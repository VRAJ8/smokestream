#!/usr/bin/env python3
"""Generate a small synthetic PurpleAir history, plus context, for testing.

Sensors are scattered around San José and report every 2 minutes. A fire
starts east of the city, the wind turns to blow from the east, and a smoke
plume reaches the eastern sensors first and pushes PM2.5 into the Unhealthy
range. Separately, one sensor has a short backyard-barbecue spike before the
fire, which the pipeline should label "local", not wildfire, and the sensor
with the most neighbours drifts badly for 4 hours, which the neighbour check
should exclude. A few readings have a faulty channel for validation to catch.

Sensors report raw PurpleAir values, which read high in smoke: each raw value
is chosen so that the EPA correction maps it back to the true concentration.
Four regulatory monitors measure the true concentration (hourly, small noise),
so the monitor-based evaluation has a known ground truth.

Writes (PurpleAir column names, sorted by time_stamp, like a real download):
  history.csv, sensors.csv
  context/fires/fires.csv, context/wind/wind.csv, context/plume_cells/plume_cells.csv
  monitors/monitors.csv (never an input to the pipelines; ground truth only)

Usage: python tools/make_synthetic_history.py --out data/synthetic [--sensors 30 --days 3]
"""

import argparse
import csv
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.airquality import epa_correct  # noqa: E402

START = 1598918400  # 2020-09-01 00:00 UTC
CENTER = (37.3382, -121.8863)  # San José
FIRE = (37.42, -121.55)  # hills about 30 km east
BBQ_SENSOR = 1003
MONITORS = [("060850005", 37.3485, -121.8949), ("060850006", 37.2900, -121.9500),
            ("060851001", 37.4100, -121.8000), ("060852006", 37.2500, -121.7600)]


def true_pm(hours, lon, args):
    """True PM2.5 (ug/m3) away from local sources: daily cycle plus the smoke plume."""
    smoke_end = args.smoke_hour + 30
    # Eastern locations see the plume up to 4 hours before western ones.
    arrival = args.smoke_hour + (lon - CENTER[1] + 0.2) * -10 + 2
    fade = 1 if hours < smoke_end else math.exp(-(hours - smoke_end) / 6)
    smoke = 140 / (1 + math.exp(-(hours - arrival))) * fade
    base = 8 + 4 * math.sin(2 * math.pi * hours / 24)
    return base + smoke, smoke


def raw_from_true(pm, rh):
    """Raw PurpleAir (cf_1) value that the EPA correction maps back to pm (bisection)."""
    if epa_correct(0.0, rh) >= pm:
        return 0.0
    lo, hi = 0.0, 3000.0
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if epa_correct(mid, rh) < pm else (lo, mid)
    return hi


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--sensors", type=int, default=30)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--smoke-hour", type=float, default=30, help="hour the plume reaches the city center")
    parser.add_argument("--bbq-hour", type=float, default=18, help="hour of the backyard-barbecue spike")
    parser.add_argument("--drift-hour", type=float, default=8, help="start of a 4-hour sensor drift")
    parser.add_argument("--h3-res", type=int, default=7, help="resolution for plume cells")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    sensors = [(1000 + i, CENTER[0] + rng.uniform(-0.15, 0.15), CENTER[1] + rng.uniform(-0.2, 0.2))
               for i in range(args.sensors)]
    end = START + args.days * 86400
    drift_sensor = most_neighbours(sensors)
    fire_start = args.smoke_hour - 10
    smoke_end = args.smoke_hour + 30

    with (args.out / "sensors.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sensor_index", "latitude", "longitude"])
        w.writerows([(s, round(lat, 5), round(lon, 5)) for s, lat, lon in sensors])

    with (args.out / "history.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["time_stamp", "sensor_index", "pm2.5_cf_1_a", "pm2.5_cf_1_b", "pm1.0_cf_1", "humidity"])
        for t in range(START, end, 120):
            hours = (t - START) / 3600
            for s, lat, lon in sensors:
                truth, smoke = true_pm(hours, lon, args)
                local = 120 if s == BBQ_SENSOR and args.bbq_hour <= hours < args.bbq_hour + 0.5 else 0
                pm = max(0.0, truth + local + rng.gauss(0, 1.5))
                rh = min(95, max(10, 55 + 20 * math.sin(2 * math.pi * (hours + 6) / 24) + rng.gauss(0, 3)))
                raw = raw_from_true(pm, rh)
                if s == drift_sensor and args.drift_hour <= hours < args.drift_hour + 4:
                    raw = raw * 3 + 40  # both channels drift together, so the A/B check cannot catch it
                a, b = raw * rng.uniform(0.95, 1.05), raw * rng.uniform(0.95, 1.05)
                if rng.random() < 0.003:
                    b = raw * 4 + 30  # faulty channel B
                pm1 = raw * (0.85 if smoke > local else 0.5)
                w.writerow([t, s, round(a, 2), round(b, 2), round(pm1, 2), round(rh, 1)])

    write_context(args, rng, end, fire_start, smoke_end)
    write_monitors(args, rng, end)
    print(f"Wrote {args.out}/history.csv, sensors.csv and context/ "
          f"({args.sensors} sensors, {args.days} days, smoke at hour {args.smoke_hour:g})")


def write_monitors(args, rng, end):
    """Hourly means of the true concentration at each monitor site, with small instrument noise."""
    (args.out / "monitors").mkdir(parents=True, exist_ok=True)
    with (args.out / "monitors" / "monitors.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["site_id", "latitude", "longitude", "hour_start", "pm25"])
        for hour_start in range(START, end, 3600):
            for site, lat, lon in MONITORS:
                samples = [true_pm((hour_start + m * 60 - START) / 3600, lon, args)[0] for m in range(60)]
                w.writerow([site, lat, lon, hour_start, round(max(0.0, sum(samples) / 60 + rng.gauss(0, 1.0)), 2)])


def most_neighbours(sensors, km=4.0):
    """The sensor with the most others within km (ties: lowest id)."""
    def close(a, b):
        return math.hypot((a[1] - b[1]) * 111.32, (a[2] - b[2]) * 111.32 * math.cos(math.radians(a[1]))) <= km
    counts = {s[0]: sum(close(s, o) for o in sensors if o is not s) for s in sensors}
    return max(sorted(counts), key=lambda k: counts[k])


def write_context(args, rng, end, fire_start, smoke_end):
    ctx = args.out / "context"
    for name in ("fires", "wind", "plume_cells"):
        (ctx / name).mkdir(parents=True, exist_ok=True)

    # Satellite detections every 3 hours while the fire burns; published 3 h later (FIRMS NRT).
    with (ctx / "fires" / "fires.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["detected_at", "latitude", "longitude", "frp_mw", "published_at"])
        t = START + fire_start * 3600
        while t < min(end, START + smoke_end * 3600):
            for _ in range(5):
                w.writerow([t, round(FIRE[0] + rng.uniform(-0.05, 0.05), 4),
                            round(FIRE[1] + rng.uniform(-0.05, 0.05), 4), round(rng.uniform(5, 80), 1), t + 3 * 3600])
            t += 3 * 3600

    # Hourly wind at three stations: from the east (90 deg) while the fire burns, else from the west.
    stations = [("KSJC", 37.3626, -121.9290), ("KRHV", 37.3329, -121.8197), ("KPAO", 37.4611, -122.1151)]
    with (ctx / "wind" / "wind.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_id", "latitude", "longitude", "observed_at", "wind_dir_deg", "wind_speed_ms", "published_at"])
        for t in range(START, end, 3600):
            hours = (t - START) / 3600
            east = fire_start <= hours < smoke_end
            for sid, lat, lon in stations:
                w.writerow([sid, lat, lon, t, 90 if east else 270, 4.0 if east else 3.0, t + 600])

    # One analyst-drawn plume over the whole area, valid while the smoke is overhead,
    # published 4 hours after it starts (HMS products lag the satellite images).
    try:
        from common.context import plume_to_cells
    except ImportError:
        sys.exit("Context generation needs the h3 package; run with the project .venv.")
    ring = [(37.05, -122.25), (37.05, -121.45), (37.65, -121.45), (37.65, -122.25)]
    valid_start = START + (args.smoke_hour - 2) * 3600
    with (ctx / "plume_cells" / "plume_cells.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["h3", "valid_start", "valid_end", "density", "published_at"])
        for cell in plume_to_cells(ring, args.h3_res):
            w.writerow([cell, int(valid_start), int(START + smoke_end * 3600), "Heavy", int(valid_start + 4 * 3600)])


if __name__ == "__main__":
    main()
