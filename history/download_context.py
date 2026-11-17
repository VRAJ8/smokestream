#!/usr/bin/env python3
"""Download and convert the free public data for a fire season.

No API keys needed. Raw files are cached under data/raw/, so reruns are fast.
Output goes to data/history/<season>/ in the formats common/context.py and
common/evaluate.py read:

  monitors  EPA AQS hourly PM2.5 (parameter 88101), the ground truth
            -> monitors/monitors.csv   site_id, latitude, longitude, hour_start, pm25
  fires     NASA FIRMS VIIRS (S-NPP) detections, vegetation fires only
            -> context/fires/fires.parquet
  plumes    NOAA HMS smoke polygons, clipped to the region and converted to H3 cells
            -> context/plume_cells/plume_cells.parquet
  wind      Airport (ASOS) wind from the Iowa Environmental Mesonet
            -> context/wind/wind.parquet

Usage:
  python history/download_context.py all --season 2020
  python history/download_context.py monitors --season 2021 --region bayarea

Assumptions worth stating in the paper:
  * HMS analysts draw plumes from daytime satellite images. A plume is treated as
    present from its Start time until 12 hours after its End time (smoke lingers
    overnight), and as published 2 hours after End.
  * FIRMS archive (standard-processing) records do not say when the near-real-time
    version appeared; published_at is left empty, so enrich.py assumes 3 hours.
  * AQS values with several instruments (POCs) at one site are averaged per hour.
"""

import argparse
import csv
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from common.context import plume_to_cells  # noqa: E402
from history.download_purpleair import REGIONS, SEASONS  # noqa: E402

RAW = ROOT / "data" / "raw"
HISTORY = ROOT / "data" / "history"
USER_AGENT = "SmokeStream research project (San Jose State University)"

# Fires and wind matter beyond the study area: smoke travels ~150 km.
CONTEXT_MARGIN_DEG = (2.0, 1.5)  # lon, lat
PLUME_PERSIST_S = 12 * 3600
PLUME_PUBLISH_DELAY_S = 2 * 3600
H3_RES = 7


def fetch(url, dest, pause=0.0, refresh=False):
    """Download url to dest once (cached), or again with refresh (files that grow during the day)."""
    dest = Path(dest)
    if dest.exists() and dest.stat().st_size > 0 and not refresh:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as r:
        data = r.read()
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(data)
    tmp.rename(dest)
    time.sleep(pause)
    return dest


def season_bounds(season):
    start, end = SEASONS[season]
    to_ts = lambda d: int(datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    return to_ts(start), to_ts(end)


def expanded(region):
    west, south, east, north = REGIONS[region]
    dlon, dlat = CONTEXT_MARGIN_DEG
    return west - dlon, south - dlat, east + dlon, north + dlat


def out_dir(season, *parts):
    d = HISTORY.joinpath(season, *parts)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------- monitors (EPA AQS) ----------

def cmd_monitors(season, region):
    start, end = season_bounds(season)
    west, south, east, north = REGIONS[region]
    zpath = fetch(f"https://aqs.epa.gov/aqsweb/airdata/hourly_88101_{season}.zip",
                  RAW / "aqs" / f"hourly_88101_{season}.zip")
    cols = ["State Code", "County Code", "Site Num", "Latitude", "Longitude", "Date GMT", "Time GMT",
            "Sample Measurement"]
    with zipfile.ZipFile(zpath) as z:
        name = next(n for n in z.namelist() if n.endswith(".csv"))
        chunks = []
        with z.open(name) as f:
            for chunk in pd.read_csv(f, usecols=cols, dtype={"State Code": str, "County Code": str, "Site Num": str},
                                     chunksize=500_000):
                chunk = chunk[(chunk["State Code"] == "06")
                              & chunk["Longitude"].between(west, east) & chunk["Latitude"].between(south, north)]
                if not chunk.empty:
                    chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    df["hour_start"] = (pd.to_datetime(df["Date GMT"] + " " + df["Time GMT"], utc=True).astype("int64") // 10**9)
    df = df[(df["hour_start"] >= start) & (df["hour_start"] < end)]
    df["site_id"] = df["State Code"] + df["County Code"].str.zfill(3) + df["Site Num"].str.zfill(4)
    out = (df.groupby(["site_id", "hour_start"])
             .agg(latitude=("Latitude", "first"), longitude=("Longitude", "first"),
                  pm25=("Sample Measurement", "mean"))
             .reset_index()[["site_id", "latitude", "longitude", "hour_start", "pm25"]])
    out["pm25"] = out["pm25"].clip(lower=0).round(2)
    path = out_dir(season, "monitors") / "monitors.csv"
    out.to_csv(path, index=False)
    print(f"monitors: {out['site_id'].nunique()} sites, {len(out):,} site-hours -> {path}")


# ---------- fires (NASA FIRMS) ----------

def cmd_fires(season, region):
    start, end = season_bounds(season)
    west, south, east, north = expanded(region)
    path = fetch(f"https://firms.modaps.eosdis.nasa.gov/data/country/viirs-snpp/{season}/"
                 f"viirs-snpp_{season}_United_States.csv", RAW / "firms" / f"viirs-snpp_{season}_United_States.csv")
    df = pd.read_csv(path, usecols=["latitude", "longitude", "acq_date", "acq_time", "frp", "type"],
                     dtype={"acq_time": str})
    df = df[df["longitude"].between(west, east) & df["latitude"].between(south, north) & (df["type"] == 0)]
    stamp = pd.to_datetime(df["acq_date"] + " " + df["acq_time"].str.zfill(4), format="%Y-%m-%d %H%M", utc=True)
    df = df.assign(detected_at=(stamp.astype("int64") // 10**9).astype(float))
    # Keep a day before the season so the 24-hour fire lookback works from the first window.
    df = df[(df["detected_at"] >= start - 86400) & (df["detected_at"] < end)]
    out = pd.DataFrame({"detected_at": df["detected_at"], "latitude": df["latitude"], "longitude": df["longitude"],
                        "frp_mw": df["frp"].astype(float), "published_at": pd.Series(dtype=float)})
    dest = out_dir(season, "context", "fires") / "fires.parquet"
    out.to_parquet(dest, index=False)
    print(f"fires: {len(out):,} vegetation-fire detections within the region + margin -> {dest}")


# ---------- plumes (NOAA HMS) ----------

def clip_ring(ring, bbox):
    """Sutherland-Hodgman clip of a (lon, lat) ring to a rectangle."""
    west, south, east, north = bbox
    edges = [(lambda p: p[0] >= west, lambda a, b: _cut_x(a, b, west)),
             (lambda p: p[0] <= east, lambda a, b: _cut_x(a, b, east)),
             (lambda p: p[1] >= south, lambda a, b: _cut_y(a, b, south)),
             (lambda p: p[1] <= north, lambda a, b: _cut_y(a, b, north))]
    pts = list(ring)
    for inside, cut in edges:
        if not pts:
            break
        clipped, prev = [], pts[-1]
        for cur in pts:
            if inside(cur):
                if not inside(prev):
                    clipped.append(cut(prev, cur))
                clipped.append(cur)
            elif inside(prev):
                clipped.append(cut(prev, cur))
            prev = cur
        pts = clipped
    return pts


def _cut_x(a, b, x):
    t = (x - a[0]) / (b[0] - a[0])
    return (x, a[1] + t * (b[1] - a[1]))


def _cut_y(a, b, y):
    t = (y - a[1]) / (b[1] - a[1])
    return (a[0] + t * (b[0] - a[0]), y)


def point_in_ring(lon, lat, ring):
    """Ray casting: is (lon, lat) inside the (lon, lat) ring?"""
    inside, j = False, len(ring) - 1
    for i in range(len(ring)):
        (xi, yi), (xj, yj) = ring[i], ring[j]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def cells_in_ring(ring, region_centers):
    """H3 cells whose centers fall inside the ring. polygon_to_cells uses the same rule
    (center containment); this fallback handles the rare rings H3 rejects."""
    try:
        return plume_to_cells([(lat, lon) for lon, lat in ring], H3_RES)
    except Exception:
        return [c for c, (lat, lon) in region_centers.items() if point_in_ring(lon, lat, ring)]


def hms_time(value):
    """'2020253 1300' (year, day of year, HHMM, UTC) -> UNIX seconds."""
    value = value.strip()
    return int(datetime.strptime(value, "%Y%j %H%M").replace(tzinfo=timezone.utc).timestamp())


def plume_cells(days, bbox, refresh=False):
    """HMS smoke polygons for the given UTC days as H3 cells, one row per cell and day:
    h3, valid_start, valid_end, density, published_at. Also returns how many days had no file."""
    import shapefile
    rank = {"Light": 1, "Medium": 2, "Heavy": 3}
    cells = {}  # (cell, day) -> [valid_start, valid_end, density_rank, published_at]
    import h3
    west, south, east, north = bbox
    region = h3.polygon_to_cells(h3.LatLngPoly([(south, west), (south, east), (north, east), (north, west)]), H3_RES)
    region_centers = {c: h3.cell_to_latlng(c) for c in region}
    missing = 0
    for day in days:
        tag = day.strftime("%Y%m%d")
        url = (f"https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/Shapefile/"
               f"{day:%Y}/{day:%m}/hms_smoke{tag}.zip")
        try:
            zpath = fetch(url, RAW / "hms" / f"hms_smoke{tag}.zip", pause=0.2, refresh=refresh)
        except Exception:
            missing += 1
            continue
        with zipfile.ZipFile(zpath) as z:
            base = next(n for n in z.namelist() if n.endswith(".shp"))[:-4]
            reader = shapefile.Reader(shp=io.BytesIO(z.read(base + ".shp")), dbf=io.BytesIO(z.read(base + ".dbf")),
                                      shx=io.BytesIO(z.read(base + ".shx")))
            names = [f[0] for f in reader.fields[1:]]
            for shape, rec in zip(reader.iterShapes(), reader.iterRecords()):
                r = dict(zip(names, rec))
                try:
                    t0, t1 = hms_time(r["Start"]), hms_time(r["End"])
                except ValueError:
                    continue
                density = rank.get(str(r.get("Density", "")).strip(), 1)
                parts = list(shape.parts) + [len(shape.points)]
                for i in range(len(parts) - 1):
                    ring = clip_ring(shape.points[parts[i]:parts[i + 1]], bbox)
                    if len(ring) < 3:
                        continue
                    for cell in cells_in_ring(ring, region_centers):
                        key = (cell, tag)
                        cur = cells.get(key)
                        vs, ve, pub = t0, t1 + PLUME_PERSIST_S, t1 + PLUME_PUBLISH_DELAY_S
                        if cur is None:
                            cells[key] = [vs, ve, density, pub]
                        else:
                            cells[key] = [min(cur[0], vs), max(cur[1], ve), max(cur[2], density), min(cur[3], pub)]
    names = {1: "Light", 2: "Medium", 3: "Heavy"}
    out = pd.DataFrame([(c, float(v[0]), float(v[1]), names[v[2]], float(v[3])) for (c, _), v in cells.items()],
                       columns=["h3", "valid_start", "valid_end", "density", "published_at"])
    return out, missing


def cmd_plumes(season, region):
    start, end = season_bounds(season)
    first = datetime.fromtimestamp(start, timezone.utc) - timedelta(days=1)  # plumes linger into the first day
    out, missing = plume_cells([first + timedelta(days=i) for i in range((end - start) // 86400 + 1)],
                               REGIONS[region])
    dest = out_dir(season, "context", "plume_cells") / "plume_cells.parquet"
    out.to_parquet(dest, index=False)
    print(f"plumes: {len(out):,} cell-days at resolution {H3_RES}"
          + (f" ({missing} days had no HMS file)" if missing else "") + f" -> {dest}")


# ---------- wind (Iowa Environmental Mesonet ASOS) ----------

def asos_wind(s, e, region, dest, refresh=False):
    """Airport wind observations from UTC day s up to day e (exclusive), cached at dest,
    in the context wind columns."""
    west, south, east, north = expanded(region)
    stations_path = fetch("https://mesonet.agron.iastate.edu/geojson/network/CA_ASOS.geojson",
                          RAW / "iem" / "CA_ASOS.geojson")
    features = json.loads(stations_path.read_text())["features"]
    stations = {f["properties"]["sid"]: f["geometry"]["coordinates"] for f in features
                if west <= f["geometry"]["coordinates"][0] <= east and south <= f["geometry"]["coordinates"][1] <= north}
    query = "&".join(f"station={sid}" for sid in sorted(stations))
    url = (f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?{query}&data=drct&data=sknt"
           f"&year1={s:%Y}&month1={s.month}&day1={s.day}&year2={e:%Y}&month2={e.month}&day2={e.day}"
           f"&tz=Etc/UTC&format=onlycomma&latlon=yes&missing=empty&report_type=3")
    path = fetch(url, dest, refresh=refresh)
    df = pd.read_csv(path, na_values=["", "M"])
    df = df.dropna(subset=["drct", "sknt"])
    observed = pd.to_datetime(df["valid"], utc=True).astype("int64") // 10**9
    out = pd.DataFrame({"station_id": df["station"].astype(str), "latitude": df["lat"].astype(float),
                        "longitude": df["lon"].astype(float), "observed_at": observed.astype(float),
                        "wind_dir_deg": df["drct"].astype(float), "wind_speed_ms": (df["sknt"] * 0.514444).round(2),
                        "published_at": pd.Series(dtype=float)})
    return out


def cmd_wind(season, region):
    start, end = season_bounds(season)
    out = asos_wind(datetime.fromtimestamp(start, timezone.utc), datetime.fromtimestamp(end, timezone.utc), region,
                    RAW / "iem" / f"asos_{season}_{region}.csv")
    dest = out_dir(season, "context", "wind") / "wind.parquet"
    out.to_parquet(dest, index=False)
    print(f"wind: {out['station_id'].nunique()} stations, {len(out):,} observations -> {dest}")


COMMANDS = {"monitors": cmd_monitors, "fires": cmd_fires, "plumes": cmd_plumes, "wind": cmd_wind}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("what", choices=[*COMMANDS, "all"])
    parser.add_argument("--season", required=True, choices=sorted(s for s in SEASONS if s.isdigit()))
    parser.add_argument("--region", choices=sorted(REGIONS), default="bayarea")
    args = parser.parse_args()
    for name in (COMMANDS if args.what == "all" else [args.what]):
        COMMANDS[name](args.season, args.region)


if __name__ == "__main__":
    main()
