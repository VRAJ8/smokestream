#!/usr/bin/env python3
"""What each fire season looks like in the real ground-truth and context data.

Reads data/history/<season>/ (written by history/download_context.py) and writes:
  season_overview.csv / .md   per season: monitor sites and hours, Unhealthy hours
                              and episodes, peak PM2.5, smoke-plume days, fire detections
  season_<season>.png / .pdf  timeline: monitor PM2.5 (median and range across sites),
                              share of the region under an HMS plume, daily fire detections

These numbers decide how many evaluation events each season can offer (RQ2, RQ3).

Usage:
  python analysis/season_overview.py --seasons 2020,2021,2024 --plot 2020 --out data/out/overview
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import h3  # noqa: E402
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.ticker  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from analysis.plot_frontier import AXIS, INK, INK_2, MUTED, SERIES, SURFACE, style  # noqa: E402
from common.evaluate import UNHEALTHY_PM25, episodes, load_monitors  # noqa: E402
from history.download_purpleair import REGIONS, SEASONS  # noqa: E402

USG_PM25 = 35.4
PLUME_DAY_SHARE = 0.5  # a "plume day" has at least half the region under smoke


def region_cells(region, res=7):
    west, south, east, north = REGIONS[region]
    ring = [(south, west), (south, east), (north, east), (north, west)]
    return set(h3.polygon_to_cells(h3.LatLngPoly(ring), res))


def daily_plume_share(plumes, cells, start, end):
    """Share of region cells under any plume (and under Heavy smoke), per UTC day."""
    days = pd.date_range(pd.to_datetime(start, unit="s", utc=True), pd.to_datetime(end, unit="s", utc=True),
                         freq="D", inclusive="left")
    p = plumes[plumes["h3"].isin(cells)]
    rows = []
    for d in days:
        noon = d.timestamp() + 12 * 3600  # sample each day at 12:00 UTC (05:00 local)
        hit = p[(p["valid_start"] <= noon + 12 * 3600) & (p["valid_end"] >= noon - 12 * 3600)]
        rows.append({"day": d, "any": hit["h3"].nunique() / len(cells),
                     "heavy": hit.loc[hit["density"] == "Heavy", "h3"].nunique() / len(cells)})
    return pd.DataFrame(rows)


def season_stats(season, region):
    base = ROOT / "data" / "history" / season
    start, end = (pd.Timestamp(d, tz="UTC").timestamp() for d in SEASONS[season])
    mon = load_monitors(base / "monitors")
    plumes = pd.read_parquet(base / "context" / "plume_cells")
    fires = pd.read_parquet(base / "context" / "fires")
    fires = fires[(fires["detected_at"] >= start) & (fires["detected_at"] < end)]
    shares = daily_plume_share(plumes, region_cells(region), start, end)
    eps = episodes(mon)
    peak = mon.loc[mon["pm25"].idxmax()]
    unhealthy = mon[mon["pm25"] > UNHEALTHY_PM25]
    stats = {
        "season": season,
        "monitor_sites": mon["site_id"].nunique(),
        "site_hours": len(mon),
        "usg_or_worse_hours": int((mon["pm25"] > USG_PM25).sum()),
        "unhealthy_hours": len(unhealthy),
        "unhealthy_episodes": len(eps),
        "days_with_unhealthy": unhealthy["hour_start"].floordiv(86400).nunique(),
        "peak_pm25": round(float(peak["pm25"]), 1),
        "peak_day_utc": pd.to_datetime(peak["hour_start"], unit="s").strftime("%Y-%m-%d"),
        "plume_days": int((shares["any"] >= PLUME_DAY_SHARE).sum()),
        "heavy_plume_days": int((shares["heavy"] >= PLUME_DAY_SHARE).sum()),
        "fire_detections": len(fires),
    }
    return stats, mon, shares, fires


def plot_season(season, mon, shares, fires, out, formats, size=(7.2, 5.6), suffix=""):
    hourly = mon.groupby("hour_start")["pm25"].agg(["median", "min", "max"]).reset_index()
    hourly["t"] = pd.to_datetime(hourly["hour_start"], unit="s", utc=True).dt.tz_convert("America/Los_Angeles")
    shares = shares.assign(t=shares["day"].dt.tz_convert("America/Los_Angeles"))
    fdaily = (pd.to_datetime(fires["detected_at"], unit="s", utc=True).dt.tz_convert("America/Los_Angeles")
              .dt.floor("D").value_counts().sort_index())

    fig, axes = plt.subplots(3, 1, figsize=size, sharex=True, gridspec_kw={"height_ratios": [2.2, 1, 1]})
    ax = axes[0]
    ax.fill_between(hourly["t"], hourly["min"], hourly["max"], color=SERIES[0], alpha=0.12, linewidth=0)
    ax.plot(hourly["t"], hourly["median"], color=SERIES[0], linewidth=1.2)
    ax.axhline(UNHEALTHY_PM25, color=INK_2, linewidth=0.8)
    ax.text(hourly["t"].iloc[0], UNHEALTHY_PM25, " Unhealthy (AQI 150)", va="bottom", fontsize=7.5, color=INK_2)
    peak = hourly.loc[hourly["max"].idxmax()]
    ax.annotate(f"peak {peak['max']:.0f} µg/m³, {peak['t']:%b %-d}", (peak["t"], peak["max"]),
                xytext=(8, -4), textcoords="offset points", fontsize=7.5, color=INK_2, va="top")
    ax.set_ylabel("PM2.5 (µg/m³)")
    ax.set_ylim(bottom=0)
    ax.set_title("Regulatory monitors: median across sites, band = lowest to highest site", loc="left",
                 fontsize=8.5, fontweight="normal", color=INK_2)

    ax = axes[1]
    ax.bar(shares["t"], shares["any"] * 100, width=0.8, color=SERIES[0], linewidth=0)
    ax.set_ylabel("% of region")
    ax.set_ylim(0, 100)
    ax.set_title("Area under an NOAA smoke plume (HMS)", loc="left", fontsize=8.5, fontweight="normal", color=INK_2)

    ax = axes[2]
    ax.bar(fdaily.index, fdaily.values, width=0.8, color=SERIES[0], linewidth=0)
    ax.set_ylabel("detections")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_title("VIIRS active-fire detections within ~150 km", loc="left", fontsize=8.5, fontweight="normal",
                 color=INK_2)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %-d", tz=hourly["t"].dt.tz))

    fig.text(0.01, 0.995, f"The {season} fire season in the San Francisco Bay Area", ha="left", va="top",
             fontsize=10.5, fontweight="bold", color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.965), h_pad=0.8)
    for fmt in formats:
        fig.savefig(out / f"season_{season}{suffix}.{fmt}", dpi=300, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seasons", default="2020,2021,2024")
    parser.add_argument("--region", default="bayarea", choices=sorted(REGIONS))
    parser.add_argument("--plot", default="2020", help="season(s) to draw a timeline for, comma-separated")
    parser.add_argument("--out", default=str(ROOT / "data" / "out" / "overview"))
    parser.add_argument("--formats", default="png,pdf")
    parser.add_argument("--compact", action="store_true", help="also draw a shorter timeline for page-limited reports")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    style()
    rows = []
    for season in args.seasons.split(","):
        stats, mon, shares, fires = season_stats(season, args.region)
        rows.append(stats)
        if season in args.plot.split(","):
            plot_season(season, mon, shares, fires, out, args.formats.split(","))
            if args.compact:
                plot_season(season, mon, shares, fires, out, args.formats.split(","), size=(7.2, 3.7), suffix="_compact")
    table = pd.DataFrame(rows)
    table.to_csv(out / "season_overview.csv", index=False)
    (out / "season_overview.md").write_text(table.to_markdown(index=False) + "\n")
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
