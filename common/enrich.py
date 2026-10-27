"""Steps 4 and 5: sensor quality check, context, smoke labels and alerts.

Input is finalized sensor windows (one row per sensor per window, from
transforms.aggregate_sensors). Output is one row per hexagon per window.
The streaming job, the simulator and the batch baseline all call enrich(), so
they differ only in which readings and which context they had available.

Step 4, neighbour check. A sensor is "deviant" in a window when it is far from
the median of other sensors within qc_ring hexagon rings. It is excluded only
when it was deviant in most of its windows over the last qc_history_minutes.
Short history removes faulty sensors quickly but can also hide real local
spikes and the leading edge of a smoke front; long history is slower to react.
That trade-off is one of the RQ2 knobs.

Step 5, context and labels. Each elevated hexagon window gets:
  extent        share of nearby hexagons that are also elevated (smoke is regional)
  fine_ratio    PM1 / PM2.5 (wildfire smoke is mostly very fine particles)
  in_plume      inside an NOAA smoke plume
  upwind_fire   an active fire upwind (or close by in calm air)
and a label: wildfire, local or uncertain. Context counts only if it had been
published, and picked up by an Airflow refresh, by decision time
(window_end + watermark). That is the context-freshness knob. The hindsight
baseline passes cutoff_offset_s=None and sees all context.
"""

from dataclasses import dataclass, field
from functools import lru_cache

import h3
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.transforms import aqi_category_expr, aqi_expr

EARTH_KM = 6371.0


@dataclass(frozen=True)
class EnrichConfig:
    qc_history_minutes: int = 120          # RQ2 knob
    context_refresh_minutes: int = 60      # RQ2 knob: Airflow refresh interval
    qc_ring: int = 2                       # about 5 km at resolution 7
    qc_min_neighbors: int = 2
    qc_abs_ugm3: float = 10.0              # deviant: more than 10 ug/m3 away ...
    qc_rel: float = 1.0                    # ... and more than 100% away from the median
    qc_sustained_share: float = 0.75
    qc_min_windows: int = 2
    elevated_aqi: int = 100                # above "Moderate"
    alert_aqi: int = 150                   # "Unhealthy" and worse
    extent_ring: int = 3                   # about 7 km at resolution 7
    extent_min_hexes: int = 3
    extent_high: float = 0.5
    extent_low: float = 0.2
    fine_ratio_high: float = 0.7
    fire_lookback_s: int = 24 * 3600
    fire_max_km: float = 150.0
    fire_calm_km: float = 50.0
    calm_wind_ms: float = 1.0
    upwind_half_angle_deg: float = 45.0
    wind_max_age_s: int = 3 * 3600
    wind_max_km: float = 60.0
    # Typical publication delays when a source does not report published_at.
    default_delay_s: dict = field(default_factory=lambda: {"fires": 3 * 3600, "plume_cells": 4 * 3600,
                                                           "wind": 15 * 60})


# ---------- hexagon helpers ----------

@lru_cache(maxsize=None)
def _disk(cell, k):
    return list(h3.grid_disk(cell, k))


@lru_cache(maxsize=None)
def _center(cell):
    return h3.cell_to_latlng(cell)


def _disk_col(col, k):
    return F.udf(lambda c: _disk(c, k) if c else None, T.ArrayType(T.StringType()))(col)


_CENTER = F.udf(lambda c: list(_center(c)) if c else None, T.ArrayType(T.DoubleType()))


def _haversine_km(lat1, lon1, lat2, lon2):
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    return 2 * EARTH_KM * F.asin(F.sqrt(a))


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Compass bearing from point 1 to point 2."""
    p1, p2, dlon = F.radians(lat1), F.radians(lat2), F.radians(lon2 - lon1)
    theta = F.atan2(F.sin(dlon) * F.cos(p2), F.cos(p1) * F.sin(p2) - F.sin(p1) * F.cos(p2) * F.cos(dlon))
    return (F.degrees(theta) + 360) % 360


# ---------- step 4: neighbour quality check ----------

def qc_sensors(sw: DataFrame, cfg: EnrichConfig) -> DataFrame:
    if cfg.qc_history_minutes <= 0:  # check switched off: nobody is excluded
        return (sw.withColumn("neighbor_median", F.lit(None).cast("double"))
                  .withColumn("n_neighbors", F.lit(None).cast("long"))
                  .withColumn("qc_has_info", F.lit(0)).withColumn("qc_deviant", F.lit(0))
                  .withColumn("qc_excluded", F.lit(False)))
    others = sw.select(F.col("h3").alias("nb_h3"), "window_end_s",
                       F.col("sensor_index").alias("nb_sensor"), F.col("pm25").alias("nb_pm25"))
    pairs = (sw.select("sensor_index", "window_end_s", F.explode(_disk_col(F.col("h3"), cfg.qc_ring)).alias("nb_h3"))
               .join(others, ["nb_h3", "window_end_s"])
               .where(F.col("nb_sensor") != F.col("sensor_index")))
    medians = pairs.groupBy("sensor_index", "window_end_s").agg(
        F.median("nb_pm25").alias("neighbor_median"), F.count(F.lit(1)).alias("n_neighbors"))

    out = sw.join(medians, ["sensor_index", "window_end_s"], "left")
    has_info = F.coalesce(F.col("n_neighbors"), F.lit(0)) >= cfg.qc_min_neighbors
    gap = F.abs(F.col("pm25") - F.col("neighbor_median"))
    deviant = has_info & (gap > cfg.qc_abs_ugm3) & (gap / F.greatest(F.col("neighbor_median"), F.lit(5.0)) > cfg.qc_rel)
    out = (out.withColumn("qc_has_info", has_info.cast("int"))
              .withColumn("qc_deviant", F.coalesce(deviant, F.lit(False)).cast("int")))

    history = (Window.partitionBy("sensor_index").orderBy("window_end_s")
                     .rangeBetween(-cfg.qc_history_minutes * 60 + 1, 0))
    informed, deviant_n = F.sum("qc_has_info").over(history), F.sum("qc_deviant").over(history)
    excluded = (informed >= cfg.qc_min_windows) & (deviant_n >= cfg.qc_sustained_share * informed)
    return out.withColumn("qc_excluded", F.coalesce(excluded, F.lit(False)))


def hex_windows(sw: DataFrame) -> DataFrame:
    """Hexagon value = mean of its non-excluded sensors' window means (each sensor counts once)."""
    keep = ~F.col("qc_excluded")
    return (sw.groupBy("h3", "window_start_s", "window_end_s")
              .agg(F.avg(F.when(keep, F.col("pm25"))).alias("pm25"),
                   F.avg(F.when(keep, F.col("pm1"))).alias("pm1"),
                   F.sum("n_readings").alias("n_readings"),
                   F.count(F.lit(1)).alias("n_sensors"),
                   F.sum(F.col("qc_excluded").cast("int")).alias("n_excluded"),
                   F.max("last_arrival_time").alias("last_arrival_time"))
              .withColumn("aqi", aqi_expr(F.col("pm25")))
              .withColumn("aqi_category", aqi_category_expr(F.col("aqi"))))


# ---------- step 5: context ----------

def _available(df, name, time_col, cfg, cutoff_offset_s):
    """Keep context rows usable by decision time; None = hindsight (keep everything)."""
    if cutoff_offset_s is None:
        return df.withColumn("available_at", F.lit(None).cast("double"))
    refresh = cfg.context_refresh_minutes * 60
    published = F.coalesce(F.col("published_at"), F.col(time_col) + cfg.default_delay_s[name])
    return df.withColumn("available_at", F.ceil(published / refresh) * refresh)


def _usable(cutoff_offset_s):
    if cutoff_offset_s is None:
        return F.lit(True)
    return F.col("available_at") <= F.col("window_end_s") + cutoff_offset_s


def add_extent(hw: DataFrame, cfg: EnrichConfig) -> DataFrame:
    elevated = hw.select(F.col("h3").alias("nb_h3"), "window_end_s",
                         (F.col("aqi") > cfg.elevated_aqi).cast("int").alias("nb_elevated"))
    ext = (hw.select("h3", "window_end_s", F.explode(_disk_col(F.col("h3"), cfg.extent_ring)).alias("nb_h3"))
             .join(elevated, ["nb_h3", "window_end_s"])
             .where(F.col("nb_h3") != F.col("h3"))
             .groupBy("h3", "window_end_s")
             .agg(F.count(F.lit(1)).alias("n_nearby_hexes"), F.avg("nb_elevated").alias("extent_raw")))
    return (hw.join(ext, ["h3", "window_end_s"], "left")
              .withColumn("extent", F.when(F.col("n_nearby_hexes") >= cfg.extent_min_hexes, F.col("extent_raw")))
              .drop("extent_raw"))


def add_plumes(hw, plumes, cfg, cutoff_offset_s):
    if plumes is None:
        return hw.withColumn("in_plume", F.lit(False)).withColumn("plume_density", F.lit(None).cast("string"))
    p = _available(plumes, "plume_cells", "valid_start", cfg, cutoff_offset_s)
    rank = F.when(F.col("density") == "Heavy", 3).when(F.col("density") == "Medium", 2).otherwise(1)
    hits = (hw.select("h3", "window_start_s", "window_end_s")
              .join(p, "h3")
              .where((F.col("valid_start") < F.col("window_end_s")) & (F.col("valid_end") > F.col("window_start_s"))
                     & _usable(cutoff_offset_s))
              .groupBy("h3", "window_end_s")
              .agg(F.max(rank).alias("density_rank")))
    density = F.when(F.col("density_rank") == 3, "Heavy").when(F.col("density_rank") == 2, "Medium") \
               .when(F.col("density_rank") == 1, "Light")
    return (hw.join(hits, ["h3", "window_end_s"], "left")
              .withColumn("in_plume", F.col("density_rank").isNotNull())
              .withColumn("plume_density", density)
              .drop("density_rank"))


def add_wind(hw, wind, cfg, cutoff_offset_s):
    """Wind from the nearest station that reported in the last wind_max_age_s (within wind_max_km).

    Using only the single nearest station leaves gaps whenever that station is
    silent, so every station in range is considered and the closest one with a
    usable report wins (its latest report, if it has several).
    """
    if wind is None:
        return hw.withColumn("wind_dir_deg", F.lit(None).cast("double")).withColumn("wind_speed_ms", F.lit(None).cast("double"))
    stations = wind.select("station_id", F.col("latitude").alias("st_lat"), F.col("longitude").alias("st_lon")).distinct()
    cells = hw.select("h3").distinct().withColumn("c", _CENTER("h3"))
    in_range = (cells.crossJoin(F.broadcast(stations))
                     .withColumn("dist_km", _haversine_km(F.col("c")[0], F.col("c")[1], F.col("st_lat"), F.col("st_lon")))
                     .where(F.col("dist_km") <= cfg.wind_max_km)
                     .select("h3", "station_id", "dist_km"))
    obs = _available(wind, "wind", "observed_at", cfg, cutoff_offset_s)
    # Closest station first, then its latest report (struct fields compare in order).
    best = F.struct(-F.col("dist_km"), F.col("observed_at"), F.col("station_id"))
    latest = (hw.select("h3", "window_end_s")
                .join(in_range, "h3")
                .join(obs, "station_id")
                .where((F.col("observed_at") <= F.col("window_end_s"))
                       & (F.col("observed_at") > F.col("window_end_s") - cfg.wind_max_age_s)
                       & _usable(cutoff_offset_s))
                .groupBy("h3", "window_end_s")
                .agg(F.max_by("wind_dir_deg", best).alias("wind_dir_deg"),
                     F.max_by("wind_speed_ms", best).alias("wind_speed_ms")))
    return hw.join(latest, ["h3", "window_end_s"], "left")


def add_fires(hw, fires, cfg, cutoff_offset_s):
    if fires is None:
        return (hw.withColumn("upwind_fire", F.lit(False)).withColumn("nearest_fire_km", F.lit(None).cast("double"))
                  .withColumn("n_fires", F.lit(0).cast("long")))
    f = _available(fires, "fires", "detected_at", cfg, cutoff_offset_s)
    # Only elevated windows need a fire search; it is the expensive join.
    cand = (hw.where(F.col("aqi") > cfg.elevated_aqi)
              .select("h3", "window_end_s", "wind_dir_deg", "wind_speed_ms")
              .withColumn("c", _CENTER("h3")))
    lat, lon = F.col("c")[0], F.col("c")[1]
    pairs = (cand.join(f, (F.col("detected_at") <= F.col("window_end_s"))
                          & (F.col("detected_at") > F.col("window_end_s") - cfg.fire_lookback_s)
                          & (F.abs(F.col("latitude") - lat) < 1.5) & (F.abs(F.col("longitude") - lon) < 2.0))
                 .where(_usable(cutoff_offset_s))
                 .withColumn("km", _haversine_km(F.col("latitude"), F.col("longitude"), lat, lon))
                 .where(F.col("km") <= cfg.fire_max_km))
    # Wind direction is where the wind comes FROM; smoke travels the opposite way.
    downwind = (F.col("wind_dir_deg") + 180) % 360
    bearing = _bearing_deg(F.col("latitude"), F.col("longitude"), lat, lon)
    off_axis = F.abs(((bearing - downwind + 540) % 360) - 180)
    calm = F.col("wind_speed_ms").isNull() | (F.col("wind_speed_ms") < cfg.calm_wind_ms)
    upwind = F.when(calm, F.col("km") <= cfg.fire_calm_km).otherwise(off_axis <= cfg.upwind_half_angle_deg)
    found = (pairs.groupBy("h3", "window_end_s")
                  .agg(F.max(upwind.cast("int")).alias("upwind_int"), F.min("km").alias("nearest_fire_km"),
                       F.count(F.lit(1)).alias("n_fires")))
    return (hw.join(found, ["h3", "window_end_s"], "left")
              .withColumn("upwind_fire", F.coalesce(F.col("upwind_int") == 1, F.lit(False)))
              .withColumn("n_fires", F.coalesce(F.col("n_fires"), F.lit(0).cast("long")))
              .drop("upwind_int"))


def label(hw: DataFrame, cfg: EnrichConfig) -> DataFrame:
    """Baseline rule-based label; RQ3 measures how well it works."""
    elevated = F.col("aqi") > cfg.elevated_aqi
    context = F.col("in_plume") | F.col("upwind_fire")
    ext_low = F.coalesce(F.col("extent") <= cfg.extent_low, F.lit(False))
    ext_high = F.coalesce(F.col("extent") >= cfg.extent_high, F.lit(False))
    fine = F.coalesce(F.col("fine_ratio") >= cfg.fine_ratio_high, F.lit(False))
    smoke_label = (F.when(~elevated | F.col("aqi").isNull(), F.lit(None).cast("string"))
                    .when(context & ~ext_low, "wildfire")
                    .when(~context & ext_low, "local")
                    .when(~context & ext_high & fine, "wildfire")
                    .otherwise("uncertain"))
    return (hw.withColumn("smoke_label", smoke_label)
              .withColumn("smoke_alert", F.coalesce((F.col("aqi") > cfg.alert_aqi)
                                                    & (F.col("smoke_label") != "local"), F.lit(False))))


def materialize(df: DataFrame) -> DataFrame:
    """Compute df once and cut its lineage.

    Each step below joins the table with a subset of itself, so without this Spark
    would recompute everything upstream (the whole simulation, for the sweep) once
    per reference, which is slow and runs out of memory on a full season.
    """
    return df.localCheckpoint(eager=True)


def enrich(sensor_windows: DataFrame, context: dict, cfg: EnrichConfig, cutoff_offset_s) -> DataFrame:
    """Sensor windows -> labeled hexagon windows. cutoff_offset_s=None means hindsight."""
    hw = materialize(hex_windows(qc_sensors(materialize(sensor_windows), cfg)))
    hw = hw.withColumn("fine_ratio", F.when(F.col("pm25") > 0, F.col("pm1") / F.col("pm25")))
    hw = add_extent(hw, cfg)
    hw = materialize(add_plumes(hw, context.get("plume_cells"), cfg, cutoff_offset_s))
    hw = materialize(add_wind(hw, context.get("wind"), cfg, cutoff_offset_s))
    hw = add_fires(hw, context.get("fires"), cfg, cutoff_offset_s)
    return label(hw, cfg)


OUTPUT_COLUMNS = ["window_start", "window_end", "h3", "pm25", "pm1", "aqi", "aqi_category",
                  "n_readings", "n_sensors", "n_excluded", "extent", "fine_ratio", "in_plume", "plume_density",
                  "wind_dir_deg", "wind_speed_ms", "upwind_fire", "nearest_fire_km", "n_fires",
                  "smoke_label", "smoke_alert", "last_arrival_time"]


def to_output(hw: DataFrame, extra=()) -> DataFrame:
    return (hw.withColumn("window_start", F.timestamp_seconds("window_start_s"))
              .withColumn("window_end", F.timestamp_seconds("window_end_s"))
              .select(*OUTPUT_COLUMNS, *extra))
