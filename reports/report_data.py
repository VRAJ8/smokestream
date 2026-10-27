import json, sys, csv
from pathlib import Path
root = Path(sys.argv[1]); out = Path(sys.argv[2])
rows = {r["season"]: r for r in csv.DictReader(open(root / "data/out/overview/season_overview.csv"))}
seasons = []
for s in ("2020", "2021", "2024"):
    if s in rows:
        r = rows[s]
        seasons.append({k: (float(v) if k not in ("season", "peak_day_utc") else v) for k, v in r.items()})
    else:
        seasons.append({"season": s, "missing": True})
for s in seasons:
    for k in ("monitor_sites", "site_hours", "unhealthy_hours", "unhealthy_episodes", "days_with_unhealthy", "plume_days", "fire_detections"):
        if k in s: s[k] = int(s[k])
have = [s for s in seasons if not s.get("missing")]
note = sys.argv[3] if len(sys.argv) > 3 else ""
json.dump({"seasons": seasons, "kafka": json.load(open(root / "data/out/kafka_measure.json")),
           "capacity": 14500, "tests": int(sys.argv[4]) if len(sys.argv) > 4 else 72, "season_note": note},
          open(out, "w"), indent=1)
print("seasons with data:", [s["season"] for s in have])
