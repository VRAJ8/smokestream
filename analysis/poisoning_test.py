#!/usr/bin/env python3
"""Data poisoning: can fake sensors trigger smoke alerts, and does the neighbour check stop them?

Anyone can register a PurpleAir sensor, so a crowdsourced alerting system has to
expect devices that report false values. This injects fake sensors into a real
history window and runs the hindsight pipeline with the neighbour check on and off.

  lone       four fakes, each placed in the hexagon of a different honest sensor
  colluding  three fakes together in one hexagon that also has an honest sensor

Each fake reports a steady, plausible-looking reading (about 120 ug/m3 after
correction, AQI 184) with both laser channels in agreement, so the A/B channel
check cannot catch it; only the neighbour check can. Reported: hexagon-windows
during the attack whose AQI crossed 150 (an Unhealthy alert), per scenario and setting.

Usage:
  python analysis/poisoning_test.py --run 2021-july4 --start 2021-07-03T12:00 --hours 24
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import h3  # noqa: E402
import pandas as pd  # noqa: E402

FAKE_PA, FAKE_RH = 224.6, 40.0  # EPA correction: 0.524 * 224.6 - 0.0862 * 40 + 5.75 = about 120 ug/m3
OFFSET_DEG = 0.002              # about 200 m from the honest sensor, same resolution-7 hexagon


def fakes(sensors, history, scenario):
    """[(fake sensor row)] placed next to honest sensors that report during the window."""
    busy = history.groupby("sensor_index").size().sort_values(ascending=False).index
    honest = sensors.set_index("sensor_index").loc[[s for s in busy if s in set(sensors["sensor_index"])]]
    rows = []
    if scenario == "lone":
        used = set()
        for idx, s in honest.iterrows():
            cell = h3.latlng_to_cell(s.latitude, s.longitude, 7)
            if cell in used:
                continue
            used.add(cell)
            rows.append((s.latitude + OFFSET_DEG, s.longitude + OFFSET_DEG))
            if len(rows) == 4:
                break
    else:
        s = honest.iloc[0]
        rows = [(s.latitude + OFFSET_DEG * d, s.longitude - OFFSET_DEG * d) for d in (1, 1.5, 2)]
    return [{"sensor_index": 9_000_000 + i, "latitude": lat, "longitude": lon,
             "date_created": 1_500_000_000, "last_seen": 1_800_000_000} for i, (lat, lon) in enumerate(rows)]


def run_batch(history, sensors, context, qc, out):
    subprocess.run([sys.executable, "spark/batch_job.py", "--history", history, "--sensors", sensors,
                    "--context", context, "--window", "10", "--qc-history", str(qc), "--out", out],
                   check=True, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return pd.read_parquet(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", default="2021-july4")
    parser.add_argument("--start", default="2021-07-03T12:00", help="attack start, UTC")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--work", default=str(ROOT / "data" / "out" / "poisoning"))
    args = parser.parse_args()

    base = ROOT / "data" / "history" / args.run
    context = ROOT / "data" / "history" / args.run[:4] / "context"
    history = pd.read_csv(base / "history_avg10.csv")
    sensors = pd.read_csv(base / "sensors.csv")
    t0 = int(datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc).timestamp())
    t1 = t0 + args.hours * 3600
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)

    results = {"run": args.run, "attack_start_utc": args.start, "hours": args.hours, "scenarios": {}}
    clean = run_batch(base / "history_avg10.csv", base / "sensors.csv", context, 120, str(work / "clean"))
    for scenario in ("lone", "colluding"):
        fake = fakes(sensors, history[(history["time_stamp"] >= t0) & (history["time_stamp"] < t1)], scenario)
        cells = {h3.latlng_to_cell(f["latitude"], f["longitude"], 7) for f in fake}
        rows = [{"time_stamp": t, "sensor_index": f["sensor_index"], "pm2.5_cf_1_a": FAKE_PA,
                 "pm2.5_cf_1_b": FAKE_PA, "pm1.0_cf_1_a": round(FAKE_PA * 0.6, 1), "humidity_a": FAKE_RH}
                for f in fake for t in range(t0, t1, 600)]
        hist_path, sens_path = work / f"{scenario}_history.csv", work / f"{scenario}_sensors.csv"
        pd.concat([history, pd.DataFrame(rows)]).sort_values("time_stamp").to_csv(hist_path, index=False)
        pd.concat([sensors, pd.DataFrame(fake)]).to_csv(sens_path, index=False)

        def alerts(out):
            ts = pd.to_datetime(out["window_start"]).astype("int64") // 10**9
            hit = out[out["h3"].isin(cells) & (ts >= t0) & (ts < t1)]
            return {"hex_windows": int(len(hit)), "unhealthy_alerts": int((hit["aqi"] > 150).sum()),
                    "first_hours_alerts": int((hit[(pd.to_datetime(hit["window_start"]).astype("int64") // 10**9)
                                                  < t0 + 3600]["aqi"] > 150).sum())}

        res = {"fake_sensors": len(fake), "target_hexagons": len(cells), "clean": alerts(clean)}
        for qc in (120, 0):
            out = run_batch(hist_path, sens_path, context, qc, str(work / f"{scenario}_q{qc}"))
            res["neighbour_check_on" if qc else "neighbour_check_off"] = alerts(out)
        results["scenarios"][scenario] = res
        print(scenario, json.dumps(res), flush=True)
    (work / "poisoning_results.json").write_text(json.dumps(results, indent=2))
    print(f"Wrote {work / 'poisoning_results.json'}")


if __name__ == "__main__":
    main()
