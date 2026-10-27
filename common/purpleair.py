"""Minimal PurpleAir API client (standard library only).

Used by the history downloader. The live recorder keeps its own copy of this
logic on purpose, so it stays a single self-contained file.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_ROOT = "https://api.purpleair.com/v1"


class PurpleAirError(RuntimeError):
    """An error retrying will not fix: bad key, no points left, bad parameters."""


def load_env(root: Path):
    """Read KEY=VALUE pairs from root/.env into os.environ (existing values win)."""
    env_path = root / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def api_key():
    key = os.environ.get("PURPLEAIR_READ_KEY", "").strip()
    if not key:
        raise PurpleAirError("PURPLEAIR_READ_KEY is missing. Copy .env.example to .env and paste your READ key.")
    return key


def get(path, params, retries=5):
    """GET {API_ROOT}{path}. Retries network errors, rate limits and server errors."""
    url = f"{API_ROOT}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"X-API-Key": api_key()})
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")[:300]
            if err.code != 429 and err.code < 500:
                raise PurpleAirError(f"HTTP {err.code}: {body}")
            if attempt == retries:
                raise RuntimeError(f"HTTP {err.code} after {retries} attempts: {body}")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            if attempt == retries:
                raise RuntimeError(f"network error after {retries} attempts: {err}")
        time.sleep(delay)
        delay = min(delay * 2, 120)


def rows_as_dicts(payload):
    fields = payload.get("fields", [])
    return [dict(zip(fields, row)) for row in payload.get("data", [])]
