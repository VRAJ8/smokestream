"""Converters for the public context and monitor data."""

import pandas as pd
import pytest

from analysis.measure_streaming import measure
from history.download_context import clip_ring, hms_time


def test_clip_ring_keeps_only_the_part_inside_the_box():
    square = [(-1, -1), (2, -1), (2, 2), (-1, 2)]
    clipped = clip_ring(square, (0, 0, 1, 1))
    assert sorted((round(x, 6), round(y, 6)) for x, y in clipped) == [(0, 0), (0, 1), (1, 0), (1, 1)]
    assert clip_ring([(5, 5), (6, 5), (6, 6)], (0, 0, 1, 1)) == []


def test_clip_ring_cuts_a_partly_outside_triangle():
    clipped = clip_ring([(0.5, 0.5), (3, 0.5), (0.5, 3)], (0, 0, 1, 1))
    assert all(0 <= x <= 1 and 0 <= y <= 1 for x, y in clipped)
    assert len(clipped) >= 3


def test_hms_time_is_year_day_of_year_utc():
    assert hms_time("2020253 1300") == pd.Timestamp("2020-09-09 13:00", tz="UTC").timestamp()
    with pytest.raises(ValueError):
        hms_time("not a time")


def test_measure_streaming_latency():
    meta = {"start_wall": 1000.0, "first_arrival": 0.0, "speed": 10.0, "messages": 500, "finished_wall": 1050.0}
    # Last reading of each window sent at wall 1000 + arrival/10; written 2 s and 5 s later.
    out = pd.DataFrame({"last_arrival_time": [100.0, 200.0],
                        "emitted_at": pd.to_datetime([1012.0, 1025.0], unit="s", utc=True)})
    m = measure(out, meta, target_s=3)
    assert m["throughput_msgs_per_s"] == 10.0
    assert m["latency_p50_s"] == pytest.approx(3.5)
    assert m["share_under_target"] == 0.5


def test_cells_in_ring_fallback_matches_h3(monkeypatch):
    import h3
    from history import download_context as dc
    ring = [(-122.1, 37.2), (-121.7, 37.2), (-121.7, 37.5), (-122.1, 37.5)]  # (lon, lat)
    region = h3.polygon_to_cells(h3.LatLngPoly([(37.0, -122.3), (37.0, -121.5), (37.7, -121.5), (37.7, -122.3)]), 7)
    centers = {c: h3.cell_to_latlng(c) for c in region}
    normal = set(dc.cells_in_ring(ring, centers))

    def broken(*args, **kwargs):
        raise RuntimeError("H3 rejected the polygon")
    monkeypatch.setattr(dc, "plume_to_cells", broken)
    fallback = set(dc.cells_in_ring(ring, centers))
    assert normal and fallback == normal
