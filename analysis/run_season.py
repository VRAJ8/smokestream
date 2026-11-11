#!/usr/bin/env python3
"""Run one fire season end to end with real data, one step at a time.

Steps (each skipped if its output already exists, so the command is resumable):
  1. context   public monitors, fires, plumes, wind      (history/download_context.py)
  2. sensors   pick PurpleAir sensors active in the window (history/download_purpleair.py)
  3. estimate  print the points cost and stop, unless --max-points is given
  4. history   download PurpleAir history within --max-points, then merge
  5. replay    replay with the delay profile, one file per simulated minute
  6. sweep     knob sweep with monitor scoring                (analysis/run_sweep.py)
  7. figures   frontier and knob-effect figures, monitor and hindsight versions
  8. labels    blind RQ3 labeling sheets from the hindsight baseline

Delay profile: data/delay_profile.json (built from the live recorder) if it
exists; otherwise one of the built-in sensitivity profiles (--profile fast,
typical or slow), which the outputs are tagged with so they are never mistaken
for a measured profile.

An event window such as 2021-july4 (see history/download_purpleair.py) runs the
same way from its own PurpleAir history, with its season's monitors and context.

Usage:
  python analysis/run_season.py --season 2020 --start 2020-08-16 --end 2020-09-16
  python analysis/run_season.py --season 2020 --start 2020-08-16 --end 2020-09-16 --max-points 900000
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable

# Sensitivity profiles for when no live recording exists. PurpleAir sensors report every
# 2 minutes; these bracket how stale a reading may be when a poller sees it, and how often
# sensors go quiet. They are assumptions, labeled as such in every output.
PROFILES = {
    "fast": {"staleness_quantiles_s": [0, 10, 20, 30, 40, 50, 60, 75, 90, 110, 120],
             "outage_rate_per_s": 1 / 86400, "outage_durations_s": [600, 1800]},
    "typical": {"staleness_quantiles_s": [0, 20, 45, 70, 100, 130, 170, 220, 300, 480, 1200],
                "outage_rate_per_s": 1 / 21600, "outage_durations_s": [600, 900, 1800, 3600, 7200]},
    "slow": {"staleness_quantiles_s": [0, 60, 135, 210, 300, 390, 510, 660, 900, 1440, 3600],
             "outage_rate_per_s": 1 / 7200, "outage_durations_s": [900, 1800, 3600, 7200, 14400]},
}


def run(*args):
    print("+", " ".join(str(a) for a in args), flush=True)
    subprocess.run([PY, *map(str, args)], check=True, cwd=ROOT)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", required=True,
                        choices=["2020", "2021", "2024", "2021-july4", "2021-smoke", "2024-july4"])
    parser.add_argument("--start", required=True, help="YYYY-MM-DD, e.g. the start of the peak smoke weeks")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD, exclusive")
    parser.add_argument("--sample", type=int, default=60, help="PurpleAir sensors")
    parser.add_argument("--average", type=int, default=10, choices=[0, 10, 30, 60], help="history resolution, minutes")
    parser.add_argument("--max-points", type=int, help="PurpleAir budget; without it the run stops after the estimate")
    parser.add_argument("--points-per-row", type=float, default=6.0)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="typical",
                        help="delay profile to use if data/delay_profile.json does not exist")
    parser.add_argument("--delay-profile", type=Path,
                        help="use this delay profile file instead (e.g. one built from a partial recording); "
                             "its file name becomes the run's profile tag")
    parser.add_argument("--grid", default="watermark,qc_history")
    args = parser.parse_args()

    s = args.season
    hist = ROOT / "data" / "history" / s
    season = ROOT / "data" / "history" / s[:4]  # monitors and context
    out = ROOT / "data" / "out" / f"season_{s}"
    out.mkdir(parents=True, exist_ok=True)

    # 1. context and monitors (free)
    if not (season / "monitors" / "monitors.csv").exists():
        run("history/download_context.py", "all", "--season", s[:4])

    # 2-4. PurpleAir
    merged = hist / f"history_avg{args.average}.csv"
    if not merged.exists():
        if not (hist / "sensors.csv").exists():
            run("history/download_purpleair.py", "sensors", "--season", s, "--sample", args.sample,
                "--start", args.start, "--end", args.end)
        run("history/download_purpleair.py", "estimate", "--season", s, "--average", args.average,
            "--points-per-row", args.points_per_row)
        if args.max_points is None:
            print("\nStopped before spending points. Rerun with --max-points N to download.")
            return
        run("history/download_purpleair.py", "download", "--season", s, "--average", args.average,
            "--max-points", args.max_points, "--points-per-row", args.points_per_row)
        run("history/download_purpleair.py", "merge", "--season", s, "--average", args.average)

    # 5. replay with a measured or an assumed delay profile
    measured = ROOT / "data" / "delay_profile.json"
    if args.delay_profile:
        profile_path, profile_tag = args.delay_profile, args.delay_profile.stem.replace("_", "-")
    elif measured.exists():
        profile_path, profile_tag = measured, "measured"
    else:
        profile_path, profile_tag = out / f"profile_{args.profile}.json", f"assumed-{args.profile}"
        profile_path.write_text(json.dumps({"note": f"ASSUMED {args.profile} profile, not measured",
                                            **PROFILES[args.profile]}, indent=2))
        print(f"\nNo measured delay profile yet; using the {args.profile} sensitivity profile.")
    replay = out / f"replay_{profile_tag}"
    if not replay.exists():
        run("replay/replay.py", "--history", merged, "--sensors", hist / "sensors.csv", "--profile", profile_path,
            "--out", replay, "--file-seconds", 60, "--interval-seconds", args.average * 60)

    # 6. sweep, scored against monitors. 10-minute history means the smallest window is 10.
    label = f"{s}-{profile_tag}"
    results = out / "sweep.csv"
    run("analysis/run_sweep.py", "--label", label, "--replay", replay, "--history", merged,
        "--sensors", hist / "sensors.csv", "--context", season / "context", "--monitors", season / "monitors",
        "--grid", args.grid, "--out", out, "--results", results)

    # 7. figures
    figs = out / "figures"
    run("analysis/plot_frontier.py", "--results", results, "--out", figs / "hindsight", "--seasons", label)
    run("analysis/plot_frontier.py", "--results", results, "--out", figs / "monitors", "--seasons", label,
        "--metric", "mon_rmse_ugm3", "--rows", "monitors")

    # 8. labeling sheets from the default hindsight baseline
    labels = ROOT / "data" / "labels" / s
    if not labels.exists():
        run("analysis/label_events.py", "export", "--output", out / f"batch_{label}", "--config", "w10_q120_r7",
            "--out", labels)
    print(f"\nDone. Results: {results}\nFigures: {figs}\nLabeling sheets: {labels}")


if __name__ == "__main__":
    main()
