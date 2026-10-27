"""History downloader against a fake PurpleAir API: selection, budget, resume, merge."""

import csv
import random
import sys

import pytest

from common import purpleair as pa
from common.history import resolve_columns
from history import download_purpleair as dl

START_2020, END_2020 = dl.ts("2020-08-01"), dl.ts("2020-10-01")


@pytest.fixture
def fake_api(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "HISTORY_DIR", tmp_path)
    monkeypatch.setattr(dl, "REQUEST_GAP_S", 0)
    monkeypatch.setenv("PURPLEAIR_READ_KEY", "test")
    calls = []

    def get(path, params, retries=5):
        calls.append((path, params))
        if path == "/sensors":
            fields = ["sensor_index", "latitude", "longitude", "date_created", "last_seen"]
            data = [
                [1, 37.3, -121.9, START_2020 - 10**7, END_2020 + 10**7],   # active throughout
                [2, 37.4, -122.0, START_2020 - 10**7, START_2020 + 86400],  # went offline early in season
                [3, 37.5, -122.1, END_2020 + 1, END_2020 + 10**7],          # created after the season
                [4, 37.6, -122.2, START_2020 - 10**8, START_2020 - 1],      # dead before the season
                [5, 37.7, -122.3, START_2020 + 86400, END_2020 + 10**7],    # created mid-season
            ]
            return {"fields": fields, "data": data}
        sensor = int(path.split("/")[2])
        if sensor == 2 and params["start_timestamp"] > START_2020 + 86400:
            return {"fields": ["time_stamp", *dl.FIELDS], "data": []}       # offline: no rows
        times = list(range(params["start_timestamp"], params["end_timestamp"], params["average"] * 60))
        random.Random(sensor).shuffle(times)  # the API returns rows unordered
        return {"fields": ["time_stamp", *dl.FIELDS], "data": [[t, 10.0, 11.0, 7.0, 50.0] for t in times]}

    monkeypatch.setattr(pa, "get", get)
    return calls


def run(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["download_purpleair.py", *argv])
    dl.main()


def test_sensor_selection_keeps_only_sensors_active_in_season(fake_api, monkeypatch, tmp_path):
    run(monkeypatch, "sensors", "--season", "2020", "--sample", "0")
    assert dl.load_sensor_ids("2020") == [1, 2, 5]
    assert fake_api[0][1]["max_age"] == 0 and fake_api[0][1]["location_type"] == 0


def test_estimate_counts_rows_requests_and_points(fake_api, monkeypatch):
    run(monkeypatch, "sensors", "--season", "2020", "--sample", "0")
    rows, requests, points = dl.estimate(dl.load_meta("2020"), 3, 10, 6.0)
    days = (END_2020 - START_2020) // 86400            # 61
    assert rows == 3 * days * 144
    assert requests == 3 * 21                          # 61 days in 3-day chunks
    assert points == rows * 6.0 + requests * dl.BASE_POINTS


def test_download_resume_budget_and_merge(fake_api, monkeypatch, tmp_path):
    run(monkeypatch, "sensors", "--season", "2020", "--sample", "0")
    n_before = len(fake_api)

    # A small budget stops early without overspending.
    run(monkeypatch, "download", "--season", "2020", "--max-points", "10000")
    first_batch = len(fake_api) - n_before
    assert 0 < first_batch < 63

    # Rerunning with a bigger budget resumes instead of starting over.
    run(monkeypatch, "download", "--season", "2020", "--max-points", "10000000")
    assert len(fake_api) - n_before == 3 * 21

    # A third run has nothing left to fetch.
    run(monkeypatch, "download", "--season", "2020", "--max-points", "10000000")
    assert len(fake_api) - n_before == 3 * 21

    run(monkeypatch, "merge", "--season", "2020")
    with (tmp_path / "2020" / "history_avg10.csv").open() as f:
        reader = csv.DictReader(f)
        resolve_columns(reader.fieldnames)  # the replay tool and batch job can read it
        rows = [(int(r["time_stamp"]), int(r["sensor_index"])) for r in reader]
    assert rows == sorted(rows)
    assert len(rows) == len(set(rows))
    days = (END_2020 - START_2020) // 86400
    assert sum(1 for _, s in rows if s == 1) == days * 144      # full season
    assert sum(1 for _, s in rows if s == 2) == 3 * 144         # only its first chunk
    assert {s for _, s in rows} == {1, 2, 5}


def test_max_requests_stops_for_a_cost_check(fake_api, monkeypatch):
    run(monkeypatch, "sensors", "--season", "2020", "--sample", "0")
    n_before = len(fake_api)
    run(monkeypatch, "download", "--season", "2020", "--max-points", "10000000", "--max-requests", "1")
    assert len(fake_api) - n_before == 1


def test_iso_timestamps_are_accepted():
    assert dl.to_epoch("2020-08-01T00:00:00Z") == START_2020
    assert dl.to_epoch("1596240000") == START_2020
