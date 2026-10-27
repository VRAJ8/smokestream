import csv
import subprocess
import sys
from pathlib import Path

from analysis.plot_frontier import pareto, setting_tag

ROOT = Path(__file__).resolve().parent.parent


def test_pareto_keeps_only_settings_not_beaten_on_both_axes():
    pts = [(10, 0.5, "a"), (12, 0.6, "b"), (15, 0.3, "c"), (20, 0.3, "d"), (30, 0.1, "e")]
    assert [p[2] for p in pareto(pts)] == ["a", "c", "e"]


def test_setting_tag_names_only_changed_knobs():
    default = {"window": 10, "watermark": 5, "qc_history": 120, "context_refresh": 60}
    assert setting_tag(default) == "default"
    assert setting_tag({**default, "watermark": 15, "window": 30}) == "w30 wm15"


def test_script_writes_figures_and_table(tmp_path):
    fields = ["season", "window", "watermark", "qc_history", "context_refresh", "stream_config",
              "decision_latency_min", "rmse_ugm3", "mae_ugm3", "missed_readings", "label_match",
              "smoke_alert_recall"]
    rows = [(s, w, wm, 120, 60, f"w{w}_wm{wm}_q120_c60_r7", w + wm + 2, 1 / (1 + wm) + w / 100, 0.1, 0.1, 0.9, 0.9)
            for s in ("2020", "2021") for w in (10, 30) for wm in (0, 5, 15)]
    results = tmp_path / "sweep.csv"
    with results.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(fields)
        w.writerows(rows)
    out = tmp_path / "fig"
    subprocess.run([sys.executable, str(ROOT / "analysis" / "plot_frontier.py"), "--results", str(results),
                    "--out", str(out)], check=True, capture_output=True)
    for name in ("frontier_rmse_ugm3.png", "frontier_rmse_ugm3.pdf", "knob_effects.png", "frontier_points.csv"):
        assert (out / name).stat().st_size > 0
    with (out / "frontier_points.csv").open() as f:
        table = list(csv.DictReader(f))
    assert {r["season"] for r in table} == {"2020", "2021"}
