"""Context lookups for step 5: fire detections, smoke plume cells and wind.

The Airflow DAGs write these tables, the replay experiments read historical
copies, and all pipelines load them through load_context(). All times are UNIX
seconds (UTC). published_at is when the data became available; when a source
does not say, enrich.py assumes a typical delay for that source.

Layout (Parquet or CSV with a header, any number of files per folder):
  <dir>/fires/        detected_at, latitude, longitude, frp_mw, published_at
  <dir>/plume_cells/  h3, valid_start, valid_end, density, published_at
  <dir>/wind/         station_id, latitude, longitude, observed_at, wind_dir_deg, wind_speed_ms, published_at

Plumes are stored as the H3 cells they cover (at the pipeline's resolution), so
Spark joins on cell ids instead of doing polygon geometry. plume_to_cells()
does that conversion for the Airflow task.
"""

from pathlib import Path

import h3
from pyspark.sql import types as T

D = T.DoubleType()
S = T.StringType()

SCHEMAS = {
    "fires": T.StructType([T.StructField(n, t) for n, t in [
        ("detected_at", D), ("latitude", D), ("longitude", D), ("frp_mw", D), ("published_at", D)]]),
    "plume_cells": T.StructType([T.StructField(n, t) for n, t in [
        ("h3", S), ("valid_start", D), ("valid_end", D), ("density", S), ("published_at", D)]]),
    "wind": T.StructType([T.StructField(n, t) for n, t in [
        ("station_id", S), ("latitude", D), ("longitude", D), ("observed_at", D),
        ("wind_dir_deg", D), ("wind_speed_ms", D), ("published_at", D)]]),
}


def load_context(spark, context_dir):
    """{name: DataFrame} for each context folder that exists and has files; others are omitted."""
    tables = {}
    if not context_dir:
        return tables
    for name, schema in SCHEMAS.items():
        folder = Path(context_dir) / name
        if list(folder.glob("*.parquet")):
            tables[name] = spark.read.schema(schema).parquet(str(folder))
        elif list(folder.glob("*.csv")):
            tables[name] = spark.read.schema(schema).option("header", True).csv(str(folder))
    return tables


def plume_to_cells(outer_ring, res, holes=()):
    """H3 cells covered by one plume polygon. Rings are lists of (lat, lon) points."""
    return sorted(h3.polygon_to_cells(h3.LatLngPoly(outer_ring, *holes), res))
