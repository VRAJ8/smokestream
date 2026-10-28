from datetime import datetime

import h3

from spark.streaming_job import mongo_upsert


class FakeCollection:
    def __init__(self):
        self.ops = []

    def bulk_write(self, ops, ordered):
        self.ops = ops


CELL = h3.latlng_to_cell(37.3352, -121.8811, 7)


def row(minute, aqi):
    return {"h3": CELL, "window_end": datetime(2020, 9, 11, 9, minute), "aqi": aqi}


def test_only_the_latest_window_per_hexagon_is_written():
    hexes = FakeCollection()
    # Two windows finalized in one batch; the earlier one listed last.
    mongo_upsert([row(20, 180), row(10, 150)], hexes)
    assert len(hexes.ops) == 2  # one conditional update, one insert-if-new
    for op in hexes.ops:
        doc = op._doc.get("$set") or op._doc.get("$setOnInsert")
        assert doc["window_end"] == datetime(2020, 9, 11, 9, 20) and doc["aqi"] == 180


def test_location_is_geojson_lon_lat():
    hexes = FakeCollection()
    mongo_upsert([row(10, 150)], hexes)
    lon, lat = hexes.ops[0]._doc["$set"]["location"]["coordinates"]
    assert -122.0 < lon < -121.7 and 37.2 < lat < 37.5


def test_empty_batch_writes_nothing():
    hexes = FakeCollection()
    mongo_upsert([], hexes)
    assert hexes.ops == []
