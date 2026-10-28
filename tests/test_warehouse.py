"""The warehouse SQL, run locally in DuckDB on what the loader would send to Snowflake."""

import io
from contextlib import redirect_stdout
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

from warehouse.load_snowflake import DryRun, knobs, load_run, queries

T0 = datetime(2020, 9, 11, 9, 0)


def windows(pm25, labels, extra=None):
    n = len(pm25)
    frame = pd.DataFrame({
        "window_start": [T0 + timedelta(minutes=10 * i) for i in range(n)],
        "window_end": [T0 + timedelta(minutes=10 * (i + 1)) for i in range(n)],
        "h3": "87283444bffffff", "pm25": pm25, "aqi": 150.0, "aqi_category": "Unhealthy",
        "n_readings": 5, "smoke_label": labels, "smoke_alert": True, "last_arrival_time": 1.6e9})
    return frame.assign(**(extra or {}))


def write_run(root, name, config, frame):
    folder = root / name / f"config_id={config}"
    folder.mkdir(parents=True)
    frame.to_parquet(folder / "part-0.parquet", index=False)
    return root / name


@pytest.fixture
def warehouse(tmp_path):
    db = DryRun()
    batch = windows([10.0, 20.0, 30.0, 40.0], ["wildfire", "local", None, "wildfire"])
    # The stream misses the second window, has no value for the third, and stops before the fourth.
    stream = windows([11.0, np.nan, 33.0], ["wildfire", None, "uncertain"], {"decision_delay_s": 900})
    stream = stream.drop(index=1)
    with redirect_stdout(io.StringIO()):
        load_run(db, write_run(tmp_path, "batch_2020-test", "w10_q120_r7", batch))
        load_run(db, write_run(tmp_path, "stream_2020-test", "w10_wm5_q120_c60_r7", stream))
    con = duckdb.connect()
    con.execute("CREATE MACRO IFF(c, a, b) AS CASE WHEN c THEN a ELSE b END")
    con.execute("CREATE MACRO SQUARE(x) AS x * x")
    for name in ("DIM_PIPELINE_CONFIG", "FACT_HEX_WINDOW"):
        con.register(name, db.tables[name])
    return db, con


def test_rq1_query_matches_hand_computed_metrics(warehouse):
    _, con = warehouse
    row = con.execute(queries()["rq1_stream_vs_hindsight"]).df().iloc[0]
    assert row["STREAM_CONFIG"] == "w10_wm5_q120_c60_r7" and row["BATCH_CONFIG"] == "w10_q120_r7"
    assert row["BASELINE_WINDOWS"] == 3            # the baseline's fourth window is after the stream's last
    assert row["RMSE_UGM3"] == pytest.approx(np.sqrt((1 + 9) / 2))
    assert row["LABEL_MATCH"] == pytest.approx(1 / 2)  # wildfire matches, local was missed, None is skipped


def test_modes_and_knobs(warehouse):
    db, _ = warehouse
    configs = db.tables["DIM_PIPELINE_CONFIG"].set_index("RUN_ID")
    assert configs.loc["batch_2020-test", "MODE"] == "batch"
    assert configs.loc["stream_2020-test", "MODE"] == "simulated"
    assert pd.isna(configs.loc["batch_2020-test", "WATERMARK_MINUTES"])
    assert configs.loc["stream_2020-test", "FIRE_SEASON"] == "2020"


def test_knobs_parse_batch_and_stream_ids():
    assert knobs("w10_wm5_q120_c60_r7") == {"WINDOW_MINUTES": 10, "WATERMARK_MINUTES": 5, "QC_HISTORY_MINUTES": 120,
                                            "CONTEXT_REFRESH_MINUTES": 60, "H3_RESOLUTION": 7}
    assert knobs("w30_q120_r7")["WATERMARK_MINUTES"] is None
