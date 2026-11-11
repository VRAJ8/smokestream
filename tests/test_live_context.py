"""Live context files: parsing the public feeds and merging repeated fetches."""

import os

import pandas as pd

from history import live_context as lc

BAY = (-123.0, 36.9, -121.2, 38.3)
NOV10 = 1794268800  # 2026-11-10 00:00 UTC


def test_merge_keeps_first_published_at_and_splits_by_day(tmp_path):
    first = pd.DataFrame({"observed_at": [NOV10 - 60, NOV10 + 60], "station_id": ["SJC", "SJC"],
                          "published_at": [100.0, 100.0]})
    again = pd.DataFrame({"observed_at": [NOV10 + 60, NOV10 + 120], "station_id": ["SJC", "SJC"],
                          "published_at": [200.0, 200.0]})
    assert lc.merge_by_day(tmp_path, "wind", first, "observed_at", ["station_id", "observed_at"]) == 2
    assert lc.merge_by_day(tmp_path, "wind", again, "observed_at", ["station_id", "observed_at"]) == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == ["wind_2026-11-09.parquet", "wind_2026-11-10.parquet"]
    day = pd.read_parquet(tmp_path / "wind_2026-11-10.parquet").set_index("observed_at")["published_at"]
    assert day.to_dict() == {NOV10 + 60: 100.0, NOV10 + 120: 200.0}  # re-fetching never makes old data look new


def test_monitor_revisions_keep_the_latest_value(tmp_path):
    keys = ["site_id", "hour_start"]
    lc.merge_by_day(tmp_path, "monitors", pd.DataFrame({"site_id": ["a"], "hour_start": [NOV10], "pm25": [5.0]}),
                    "hour_start", keys, keep="last")
    lc.merge_by_day(tmp_path, "monitors", pd.DataFrame({"site_id": ["a"], "hour_start": [NOV10], "pm25": [7.0]}),
                    "hour_start", keys, keep="last")
    assert pd.read_parquet(tmp_path / "monitors_2026-11-10.parquet")["pm25"].tolist() == [7.0]


def test_firms_keeps_region_and_drops_low_confidence(tmp_path):
    path = tmp_path / "firms.csv"
    path.write_text(
        "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight\n"
        "37.5,-122.0,330,0.5,0.4,2026-11-10,905,N20,nominal,2.0NRT,280,4.5,N\n"
        "37.6,-122.1,330,0.5,0.4,2026-11-10,1010,N20,low,2.0NRT,280,1.0,N\n"
        "45.0,-100.0,330,0.5,0.4,2026-11-10,1010,N20,high,2.0NRT,280,9.0,N\n")
    fires = lc.parse_firms_nrt(path, BAY, published_at=NOV10 + 7200)
    assert fires["detected_at"].tolist() == [NOV10 + 9 * 3600 + 5 * 60]
    assert fires["published_at"].tolist() == [NOV10 + 7200]


def test_airnow_hour_joins_site_locations(tmp_path):
    sites = tmp_path / "sites.dat"
    sites.write_text(
        "StationID|AQSID|FullAQSID|Parameter|MonitorType|SiteCode|SiteName|Status|AgencyID|AgencyName|EPARegion|"
        "Latitude|Longitude|Elevation|GMTOffset|CountryFIPS|CBSA_ID|CBSA_Name|StateAQSCode|StateAbbreviation|"
        "CountyAQSCode|CountyName\n"
        "1|060850005|840060850005|PM2.5|Permanent|0005|San Jose|Active|CA1|BAAQMD|R9|37.348497|-121.894898|"
        "|-8.00|US|||06|CA|085|Santa Clara\n"
        "2|060850005|840060850005|O3|Permanent|0005|San Jose|Active|CA1|BAAQMD|R9|37.348497|-121.894898|"
        "|-8.00|US|||06|CA|085|Santa Clara\n"
        "3|060371103|840060371103|PM2.5|Permanent|1103|Los Angeles|Active|CA1|SCAQMD|R9|34.06659|-118.22688|"
        "|-8.00|US|||06|CA|037|Los Angeles\n")
    hour = tmp_path / "hour.dat"
    hour.write_text("11/10/26|15:00|060850005|San Jose|-8|PM2.5|UG/M3|12.4|BAAQMD\n"
                    "11/10/26|15:00|060850005|San Jose|-8|OZONE|PPB|30|BAAQMD\n"
                    "11/10/26|15:00|060371103|Los Angeles|-8|PM2.5|UG/M3|20.0|SCAQMD\n")
    mon = lc.parse_airnow_hour(hour, lc.parse_airnow_sites(sites, BAY))
    assert mon.to_dict("records") == [{"site_id": "060850005", "latitude": 37.348497, "longitude": -121.894898,
                                       "hour_start": NOV10 + 15 * 3600, "pm25": 12.4}]


def test_prune_removes_only_old_day_files(tmp_path):
    for name in ("fires_2020-01-01.parquet", "fires_2999-01-01.parquet"):
        (tmp_path / name).write_bytes(b"")
    lc.prune(tmp_path, keep_days=14)
    assert os.listdir(tmp_path) == ["fires_2999-01-01.parquet"]
