"""Column names shared by the replay tool and the batch (hindsight) job.

Historical PurpleAir downloads use the API's field names. Everything downstream
uses the short internal names below, so this is the only place that maps them.
"""

# internal name -> accepted PurpleAir column names, first match wins
HISTORY_COLUMNS = {
    "event_time": ["time_stamp"],
    "sensor_index": ["sensor_index"],
    "pm25_a": ["pm2.5_cf_1_a"],
    "pm25_b": ["pm2.5_cf_1_b"],
    "pm1": ["pm1.0_cf_1", "pm1.0_cf_1_a"],
    "rh": ["humidity", "humidity_a"],
}

SENSOR_COLUMNS = {"sensor_index": "sensor_index", "lat": "latitude", "lon": "longitude"}

# Fields of every message the replay tool sends and the streaming job reads.
MESSAGE_FIELDS = ["sensor_index", "event_time", "arrival_time", "lat", "lon", "pm25_a", "pm25_b", "pm1", "rh"]


def resolve_columns(header):
    """Map internal names to the actual column names present in a CSV header."""
    resolved = {}
    for name, options in HISTORY_COLUMNS.items():
        match = next((c for c in options if c in header), None)
        if match is None:
            raise ValueError(f"history file has none of {options} (needed for {name})")
        resolved[name] = match
    return resolved
