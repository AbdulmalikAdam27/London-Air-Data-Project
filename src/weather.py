from __future__ import annotations

import csv
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

import requests


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_COLUMNS = ["site_code", "time_utc", "wind_speed_ms", "wind_direction_deg", "temperature_c"]
BATCH_SIZE = 50
MAX_PAST_DAYS = 92


def _hour_label(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:00:00")


def needed_site_hours(readings: List[dict]) -> Set[Tuple[str, str]]:
    """(site_code, hour) pairs that have at least one reading."""
    out = set()
    for r in readings:
        ts = r.get("data_end_parsed_dt")
        if ts is not None and r.get("site_code"):
            out.add((r["site_code"], _hour_label(ts)))
    return out


def existing_site_hours(csv_path: str) -> Set[Tuple[str, str]]:
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        return {(r["site_code"], r["time_utc"]) for r in csv.DictReader(f)}


def _fetch_batch(batch: List[Tuple[str, float, float]], past_days: int) -> List[dict]:
    params = {
        "latitude": ",".join(str(round(lat, 4)) for _, lat, _ in batch),
        "longitude": ",".join(str(round(lon, 4)) for _, _, lon in batch),
        "hourly": "wind_speed_10m,wind_direction_10m,temperature_2m",
        "wind_speed_unit": "ms",
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    for attempt in range(3):
        try:
            r = requests.get(OPEN_METEO_URL, params=params, timeout=60)
            r.raise_for_status()
            break
        except requests.RequestException:
            if attempt == 2:
                raise
            time.sleep(5 * (attempt + 1))
    payload = r.json()
    if isinstance(payload, dict):
        payload = [payload]

    rows = []
    for (site_code, _, _), loc in zip(batch, payload):
        h = loc.get("hourly", {})
        for t, spd, deg, tmp in zip(
            h.get("time", []),
            h.get("wind_speed_10m", []),
            h.get("wind_direction_10m", []),
            h.get("temperature_2m", []),
        ):
            rows.append(
                {
                    "site_code": site_code,
                    "time_utc": t.replace("T", " ") + ":00",
                    "wind_speed_ms": spd,
                    "wind_direction_deg": deg,
                    "temperature_c": tmp,
                }
            )
    return rows


def update_weather(csv_path: str, readings: List[dict], sites: List[dict]) -> int:
    """
    Appends hourly wind/temperature for every (site, hour) that has an air-quality
    reading but no weather row yet. Returns the number of rows written.
    """
    coords: Dict[str, Tuple[float, float]] = {}
    for s in sites:
        try:
            coords[s["site_code"]] = (float(s["lat"]), float(s["lon"]))
        except (KeyError, TypeError, ValueError):
            continue

    missing = {
        (code, hour)
        for code, hour in needed_site_hours(readings) - existing_site_hours(csv_path)
        if code in coords
    }
    if not missing:
        return 0

    oldest = min(datetime.strptime(h, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) for _, h in missing)
    days = (datetime.now(timezone.utc) - oldest).days + 2
    past_days = max(1, min(MAX_PAST_DAYS, days))

    site_codes = sorted({code for code, _ in missing})
    new_rows: List[dict] = []
    for i in range(0, len(site_codes), BATCH_SIZE):
        batch = [(c, *coords[c]) for c in site_codes[i : i + BATCH_SIZE]]
        for row in _fetch_batch(batch, past_days):
            if (row["site_code"], row["time_utc"]) in missing:
                new_rows.append(row)

    if not new_rows:
        return 0

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=WEATHER_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)
    return len(new_rows)
