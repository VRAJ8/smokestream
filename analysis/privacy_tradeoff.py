#!/usr/bin/env python3
"""What does publishing hexagon air quality privately cost in accuracy?

PurpleAir sensors sit at homes, so a hexagon with one sensor publishes one
household's readings. Two protections, applied to hourly hexagon means:

  k-anonymity   publish a hexagon-hour only if at least k distinct sensors contribute;
                otherwise generalize up the H3 hierarchy (resolution 7 -> 6 -> 5) until
                k sensors share the published value, and suppress it if even
                resolution 5 has fewer than k
  differential  Laplace noise on each published mean. Each sensor's hourly value is
  privacy       clipped to [0, C], so replacing one sensor moves the mean of n sensors by
                at most C/n; the noise scale is C/(n * epsilon). epsilon is per hourly
                release; a day of releases composes to 24 * epsilon.

Accuracy is measured against the non-private resolution-7 hexagon-hour means:
RMSE and agreement on the AQI-above-100 alert flag. Run on the 2020 season.

Usage:
  python analysis/privacy_tradeoff.py --out data/out/privacy_tradeoff.csv
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import h3  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from common import airquality as aq  # noqa: E402

ELEVATED = 35.4  # AQI 100


def sensor_hours(run):
    raw = pd.read_csv(ROOT / "data" / "history" / run / "history_avg10.csv")
    raw = raw[aq.valid_ab_mask(raw["pm2.5_cf_1_a"], raw["pm2.5_cf_1_b"], raw["humidity_a"])]
    raw = raw.assign(pm=np.vectorize(aq.epa_correct)((raw["pm2.5_cf_1_a"] + raw["pm2.5_cf_1_b"]) / 2,
                                                     raw["humidity_a"]),
                     hour=raw["time_stamp"] // 3600 * 3600)
    sh = raw.groupby(["sensor_index", "hour"], as_index=False)["pm"].mean()
    sensors = pd.read_csv(ROOT / "data" / "history" / run / "sensors.csv")
    cells = {int(r.sensor_index): h3.latlng_to_cell(r.latitude, r.longitude, 7) for r in sensors.itertuples()}
    sh["r7"] = sh["sensor_index"].map(cells)
    sh["r6"] = sh["r7"].map(lambda c: h3.cell_to_parent(c, 6))
    sh["r5"] = sh["r7"].map(lambda c: h3.cell_to_parent(c, 5))
    return sh.dropna(subset=["r7"])


def publish(sh, k, epsilon, clip, rng):
    """One private release per (resolution-7 hexagon, hour) that has a sensor."""
    truth = sh.groupby(["r7", "hour"]).agg(exact=("pm", "mean"), n7=("pm", "size")).reset_index()
    clipped = sh.assign(pm=sh["pm"].clip(0, clip))
    levels = {}
    for res in ("r7", "r6", "r5"):
        g = clipped.groupby([res, "hour"])["pm"].agg(["mean", "size"]).reset_index()
        levels[res] = g.rename(columns={"mean": f"mean_{res}", "size": f"n_{res}"})
    parents = sh.drop_duplicates("r7").set_index("r7")[["r6", "r5"]]
    out = truth.join(parents, on="r7")
    for res in ("r7", "r6", "r5"):
        out = out.merge(levels[res], on=[res, "hour"], how="left")
    value = pd.Series(np.nan, index=out.index)
    n = pd.Series(np.nan, index=out.index)
    level = pd.Series("suppressed", index=out.index)
    for res in ("r5", "r6", "r7"):  # finest level that reaches k wins
        ok = out[f"n_{res}"] >= k
        value[ok], n[ok], level[ok] = out.loc[ok, f"mean_{res}"], out.loc[ok, f"n_{res}"], res
    if np.isfinite(epsilon):
        noisy = value.notna()
        value[noisy] += rng.laplace(0.0, clip / (n[noisy] * epsilon))
        value = value.clip(lower=0)
    out["published"], out["level"] = value, level
    return out


def score(out):
    pub = out[out["published"].notna()]
    truth_alert, pub_alert = pub["exact"] > ELEVATED, pub["published"] > ELEVATED
    tp = int((truth_alert & pub_alert).sum())
    return {"hex_hours": len(out),
            "published_share": round(len(pub) / len(out), 3),
            "at_res7_share": round(float((out["level"] == "r7").mean()), 3),
            "rmse_ugm3": round(float(np.sqrt(((pub["published"] - pub["exact"]) ** 2).mean())), 2),
            "alert_precision": round(tp / max(1, int(pub_alert.sum())), 3),
            "alert_recall": round(tp / max(1, int(truth_alert.sum())), 3)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", default="2020")
    parser.add_argument("--clip", type=float, default=500.0, help="ug/m3; the top of the AQI scale")
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "privacy_tradeoff.csv"))
    args = parser.parse_args()

    sh = sensor_hours(args.run)
    rows = []
    for k in (1, 2, 3, 5):
        for eps in (np.inf, 10.0, 2.0, 1.0, 0.5, 0.1):
            scores = [score(publish(sh, k, eps, args.clip, np.random.default_rng(s)))
                      for s in range(1 if not np.isfinite(eps) else args.seeds)]
            mean = {key: round(float(np.mean([s[key] for s in scores])), 3) for key in scores[0]}
            rows.append({"k": k, "epsilon": "none" if not np.isfinite(eps) else eps, **mean})
            print(rows[-1], flush=True)
    pd.DataFrame(rows).to_csv(args.out, index=False)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
