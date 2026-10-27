"""Ground-truth evaluation against regulatory monitors (EPA AQS / AirNow).

The pipelines never see monitor data, so every number here is a held-out test:
each monitor's hourly PM2.5 is estimated only from the low-cost sensor hexagons
within radius_km of it (inverse-distance weighted), then compared with what the
monitor measured.

Three questions per pipeline output:
  accuracy   RMSE, MAE, bias and R^2 of the hourly estimates vs. the monitors
  alerts     hour by hour, does the stream call "Unhealthy" (PM2.5 > 55.4, AQI > 150)
             when the monitor does? Two versions: on the value alone, and as a smoke
             alert that ignores hours dominated by "local" labels (RQ3: does context
             reduce false alarms?)
  lead time  for each Unhealthy episode at a monitor, how many minutes before the
             official hourly value was published did the stream first raise a smoke
             alert nearby? Official = end of the episode's first hour plus
             publish_delay (AirNow posts hourly values after the hour ends).

Monitor table columns: site_id, latitude, longitude, hour_start (UNIX seconds,
start of the hour, UTC), pm25 (hourly mean, ug/m3).
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import h3
import numpy as np
import pandas as pd

UNHEALTHY_PM25 = 55.4  # top of "Unhealthy for Sensitive Groups"; above it, AQI > 150 (2024 scale)
EARTH_KM = 6371.0


@dataclass(frozen=True)
class EvalConfig:
    radius_km: float = 5.0          # sensors within this distance estimate a monitor
    min_distance_km: float = 0.5    # floor for inverse-distance weights
    publish_delay_s: int = 1800     # assumed delay after the hour before AirNow posts it
    lookback_s: int = 2 * 3600      # alerts this long before an episode's onset still count
    local_share_max: float = 0.5    # smoke alert suppressed when most elevated windows are "local"


def load_monitors(path) -> pd.DataFrame:
    """Monitor CSV or Parquet (file or folder)."""
    path = Path(path)
    files = sorted(path.glob("*.csv")) + sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    frames = [pd.read_csv(f, dtype={"site_id": str}) if f.suffix == ".csv" else pd.read_parquet(f) for f in files]
    if not frames:
        raise FileNotFoundError(f"no monitor files in {path}")
    mon = pd.concat(frames, ignore_index=True)
    if not np.issubdtype(mon["hour_start"].dtype, np.number):
        mon["hour_start"] = pd.to_datetime(mon["hour_start"], utc=True).astype("int64") // 10**9
    mon["hour_start"] = mon["hour_start"].astype("int64")
    mon["site_id"] = mon["site_id"].astype(str)
    return mon[["site_id", "latitude", "longitude", "hour_start", "pm25"]].dropna(subset=["pm25"])


def load_output(path, config_id) -> pd.DataFrame:
    """One pipeline run from a Parquet output folder partitioned by config_id."""
    df = pd.read_parquet(path, filters=[("config_id", "==", config_id)])
    if df.empty:
        raise ValueError(f"no rows for config {config_id} in {path}")
    return prepare_output(df)


def prepare_output(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("window_start", "window_end"):
        df[f"{col}_s"] = pd.to_datetime(df[col], utc=True).astype("int64") // 10**9
    return df


@lru_cache(maxsize=None)
def _center(cell):
    return h3.cell_to_latlng(cell)


def _haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(a))


def site_weights(hexes, monitors: pd.DataFrame, cfg: EvalConfig) -> pd.DataFrame:
    """(site_id, h3, dist_km, w) for every hexagon within radius_km of each monitor."""
    cells = pd.DataFrame({"h3": sorted(set(hexes))})
    if cells.empty:
        return pd.DataFrame(columns=["site_id", "h3", "dist_km", "w"])
    cells[["hex_lat", "hex_lon"]] = [list(_center(c)) for c in cells["h3"]]
    sites = monitors[["site_id", "latitude", "longitude"]].drop_duplicates("site_id")
    pairs = sites.merge(cells, how="cross")
    pairs["dist_km"] = _haversine_km(pairs["latitude"], pairs["longitude"], pairs["hex_lat"], pairs["hex_lon"])
    pairs = pairs[pairs["dist_km"] <= cfg.radius_km].copy()
    pairs["w"] = 1.0 / np.maximum(pairs["dist_km"], cfg.min_distance_km) ** 2
    return pairs[["site_id", "h3", "dist_km", "w"]]


def hourly_estimates(out: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
    """Inverse-distance-weighted PM2.5 per monitor and hour, from the windows inside that hour."""
    win = out[out["pm25"].notna()].merge(weights, on="h3")
    if win.empty:
        return pd.DataFrame(columns=["site_id", "hour_start", "est_pm25", "n_windows", "n_hexes", "local_share"])
    win["hour_start"] = (win["window_start_s"] // 3600) * 3600
    win["wx"] = win["w"] * win["pm25"]
    labeled = win["smoke_label"].notna()
    win["w_labeled"] = np.where(labeled, win["w"], 0.0)
    win["w_local"] = np.where(win["smoke_label"] == "local", win["w"], 0.0)
    g = win.groupby(["site_id", "hour_start"])
    est = g.agg(wx=("wx", "sum"), w=("w", "sum"), n_windows=("pm25", "size"), n_hexes=("h3", "nunique"),
                w_labeled=("w_labeled", "sum"), w_local=("w_local", "sum")).reset_index()
    est["est_pm25"] = est["wx"] / est["w"]
    est["local_share"] = np.where(est["w_labeled"] > 0, est["w_local"] / est["w_labeled"].where(est["w_labeled"] > 0, 1), 0.0)
    return est[["site_id", "hour_start", "est_pm25", "n_windows", "n_hexes", "local_share"]]


def _rates(truth, pred):
    tp = int((truth & pred).sum())
    fp = int((~truth & pred).sum())
    fn = int((truth & ~pred).sum())
    tn = int((~truth & ~pred).sum())
    return {
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "false_alarm_rate": fp / (fp + tn) if fp + tn else None,
    }


def episodes(monitors: pd.DataFrame) -> pd.DataFrame:
    """Runs of consecutive Unhealthy hours per monitor: site_id, onset, end (exclusive), hours."""
    bad = monitors[monitors["pm25"] > UNHEALTHY_PM25].sort_values(["site_id", "hour_start"])
    rows = []
    for site, g in bad.groupby("site_id"):
        hours = g["hour_start"].tolist()
        start = prev = hours[0]
        for h in hours[1:] + [None]:
            if h is not None and h == prev + 3600:
                prev = h
                continue
            rows.append({"site_id": site, "onset": start, "end": prev + 3600, "hours": (prev - start) // 3600 + 1})
            if h is not None:
                start = prev = h
    return pd.DataFrame(rows, columns=["site_id", "onset", "end", "hours"])


def lead_times(out: pd.DataFrame, weights: pd.DataFrame, monitors: pd.DataFrame, cfg: EvalConfig,
               watermark_s: int) -> pd.DataFrame:
    """Per episode: when the stream first raised a nearby smoke alert, and the lead over the official value."""
    eps = episodes(monitors)
    if eps.empty:
        return eps.assign(detected_at=pd.Series(dtype=float), lead_min=pd.Series(dtype=float))
    alerts = out[out["smoke_alert"].fillna(False).astype(bool)].merge(weights[["site_id", "h3"]], on="h3")
    if "emitted_sim_time" in alerts.columns:
        alerts = alerts.assign(decided_at=alerts["emitted_sim_time"].astype(float))
    else:
        alerts = alerts.assign(decided_at=(alerts["window_end_s"] + watermark_s).astype(float))
    merged = eps.merge(alerts[["site_id", "window_start_s", "decided_at"]], on="site_id", how="left")
    in_range = (merged["window_start_s"] >= merged["onset"] - cfg.lookback_s) & (merged["window_start_s"] < merged["end"])
    first = merged[in_range].groupby(["site_id", "onset"])["decided_at"].min().rename("detected_at")
    eps = eps.merge(first, on=["site_id", "onset"], how="left")
    official = eps["onset"] + 3600 + cfg.publish_delay_s
    eps["official_at"] = official
    eps["lead_min"] = (official - eps["detected_at"]) / 60
    return eps


def evaluate(out: pd.DataFrame, monitors: pd.DataFrame, cfg: EvalConfig = EvalConfig(), watermark_s: int = 0):
    """Metrics (prefixed mon_), per-site accuracy, per-episode lead times for one pipeline output."""
    out = out if "window_start_s" in out.columns else prepare_output(out)
    start, end = out["window_start_s"].min(), out["window_end_s"].max()
    mon = monitors[(monitors["hour_start"] >= start) & (monitors["hour_start"] + 3600 <= end)]
    weights = site_weights(out["h3"].unique(), mon, cfg)
    est = hourly_estimates(out, weights)
    hours = mon.merge(est, on=["site_id", "hour_start"], how="left")

    hours["est_pm25"] = pd.to_numeric(hours["est_pm25"], errors="coerce")
    have = hours["est_pm25"].notna()
    err = (hours.loc[have, "est_pm25"] - hours.loc[have, "pm25"]).to_numpy(dtype=float)
    obs = hours.loc[have, "pm25"].to_numpy()
    sst = float(((obs - obs.mean()) ** 2).sum()) if len(obs) else 0.0

    truth = hours["pm25"] > UNHEALTHY_PM25
    est_pm25 = pd.to_numeric(hours["est_pm25"], errors="coerce")
    value_alert = est_pm25.fillna(-1) > UNHEALTHY_PM25  # no estimate = no alert
    smoke_alert = value_alert & (pd.to_numeric(hours["local_share"], errors="coerce").fillna(0) < cfg.local_share_max)
    v, s = _rates(truth, value_alert), _rates(truth, smoke_alert)

    eps = lead_times(out, weights, mon, cfg, watermark_s)
    detected = eps["detected_at"].notna() if not eps.empty else pd.Series(dtype=bool)
    metrics = {
        "mon_sites": int(hours["site_id"].nunique()),
        "mon_hours": int(len(hours)),
        "mon_coverage": float(have.mean()) if len(hours) else None,
        "mon_rmse_ugm3": float(np.sqrt((err ** 2).mean())) if len(err) else None,
        "mon_mae_ugm3": float(np.abs(err).mean()) if len(err) else None,
        "mon_bias_ugm3": float(err.mean()) if len(err) else None,
        "mon_r2": 1 - float((err ** 2).sum()) / sst if sst > 0 else None,
        "mon_value_alert_precision": v["precision"],
        "mon_value_alert_recall": v["recall"],
        "mon_value_false_alarm_rate": v["false_alarm_rate"],
        "mon_smoke_alert_precision": s["precision"],
        "mon_smoke_alert_recall": s["recall"],
        "mon_smoke_false_alarm_rate": s["false_alarm_rate"],
        "mon_episodes": int(len(eps)),
        "mon_detected_share": float(detected.mean()) if len(eps) else None,
        "mon_lead_median_min": float(eps.loc[detected, "lead_min"].median()) if detected.any() else None,
        "mon_early_share": float((eps["lead_min"] > 0).sum() / len(eps)) if len(eps) else None,
    }

    per_site = (hours[have].assign(err=lambda d: d["est_pm25"] - d["pm25"])
                .groupby("site_id")
                .agg(hours=("pm25", "size"), rmse_ugm3=("err", lambda e: float(np.sqrt((e ** 2).mean()))),
                     bias_ugm3=("err", "mean"), mean_hexes=("n_hexes", "mean"))
                .reset_index()) if have.any() else pd.DataFrame()
    return metrics, per_site, eps
