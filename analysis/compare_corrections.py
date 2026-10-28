#!/usr/bin/env python3
"""Which PurpleAir correction matches the regulatory monitors? (Milestone check, paper Table)

For every monitor, sensors within --radius-km estimate its hourly PM2.5
(inverse-distance weighted, sensor-hour means), once per correction:
  raw         channel mean of cf_1, uncorrected
  us_wide     EPA U.S.-wide linear correction on cf_1 (Barkjohn et al. 2021), used by the pipeline
  extended    piecewise "extended" correction on cf_1 (common/airquality.epa_correct_extended)
and reports RMSE, bias, and the sensor/monitor ratio overall and by concentration band.

Usage:
  python analysis/compare_corrections.py --season 2020 --out data/out/season_2020
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common import airquality as aq  # noqa: E402

BANDS = [0, 12, 35.4, 55.4, 150, 1000]


def km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 12742 * np.arcsin(np.sqrt(a))


def valid_readings(history):
    ok = aq.valid_ab_mask(history["pm2.5_cf_1_a"], history["pm2.5_cf_1_b"], history["humidity_a"])
    out = history[ok].copy()
    out["pa"] = (out["pm2.5_cf_1_a"] + out["pm2.5_cf_1_b"]) / 2
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", required=True)
    parser.add_argument("--average", type=int, default=10)
    parser.add_argument("--radius-km", type=float, default=5.0)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    base = ROOT / "data" / "history" / args.season
    sensors = pd.read_csv(base / "sensors.csv")[["sensor_index", "latitude", "longitude"]]
    monitors = pd.read_csv(base / "monitors" / "monitors.csv", dtype={"site_id": str})
    readings = valid_readings(pd.read_csv(base / f"history_avg{args.average}.csv"))
    readings["hour_start"] = (readings["time_stamp"] // 3600) * 3600

    sites = monitors.drop_duplicates("site_id")[["site_id", "latitude", "longitude"]]
    pairs = sites.merge(sensors, how="cross", suffixes=("_m", "_s"))
    pairs["d"] = km(pairs["latitude_m"], pairs["longitude_m"], pairs["latitude_s"], pairs["longitude_s"])
    pairs = pairs[pairs["d"] <= args.radius_km]
    pairs = pairs.assign(w=1 / np.maximum(pairs["d"], 0.5) ** 2)[["site_id", "sensor_index", "w"]]

    variants = {
        "raw": readings["pa"],
        "us_wide": [aq.epa_correct(p, r) for p, r in zip(readings["pa"], readings["humidity_a"])],
        "extended": [aq.epa_correct_extended(p, r) for p, r in zip(readings["pa"], readings["humidity_a"])],
    }
    rows = []
    for name, values in variants.items():
        hourly = readings.assign(v=values).groupby(["sensor_index", "hour_start"])["v"].mean().reset_index()
        est = (hourly.merge(pairs, on="sensor_index").assign(wv=lambda x: x["w"] * x["v"])
               .groupby(["site_id", "hour_start"]).agg(wv=("wv", "sum"), w=("w", "sum")).reset_index())
        est["est"] = est["wv"] / est["w"]
        j = est.merge(monitors, on=["site_id", "hour_start"])
        err = j["est"] - j["pm25"]
        row = {"correction": name, "monitor_hours": len(j), "sites": j["site_id"].nunique(),
               "rmse_ugm3": round(float(np.sqrt((err ** 2).mean())), 2), "bias_ugm3": round(float(err.mean()), 2),
               "r2": round(float(1 - (err ** 2).sum() / ((j["pm25"] - j["pm25"].mean()) ** 2).sum()), 3)}
        for lo, hi in zip(BANDS, BANDS[1:]):
            band = j[(j["pm25"] > lo) & (j["pm25"] <= hi)]
            row[f"ratio_{lo:g}_{hi:g}"] = round(float(band["est"].mean() / band["pm25"].mean()), 2) if len(band) else None
        rows.append(row)

    table = pd.DataFrame(rows)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table.to_csv(out / "correction_comparison.csv", index=False)
    (out / "correction_comparison.md").write_text(table.to_markdown(index=False) + "\n")
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
