from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.transform import READINGS_FIELDS, SITES_FIELDS

READINGS_COLUMNS = READINGS_FIELDS + ["data_end_parsed"]
SITES_COLUMNS = SITES_FIELDS


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    """Parses an ISO-ish timestamp string, assuming UTC when no offset is given."""
    if not s:
        return None
    s = str(s).strip()
    if not s or s.lower() == "nat":
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _read_csv_rows(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_readings(csv_path: str, rows: List[Dict[str, Any]]) -> int:
    """
    Appends new hourly readings to the CSV, skipping rows that already exist
    for the same (site_code, species_code, data_end). Returns the number of
    new rows written.
    """
    if not rows:
        return 0

    existing_keys = {
        (r.get("site_code"), r.get("species_code"), r.get("data_end"))
        for r in _read_csv_rows(csv_path)
    }

    new_rows = []
    for r in rows:
        key = (r.get("site_code"), r.get("species_code"), r.get("data_end"))
        if key in existing_keys:
            continue
        existing_keys.add(key)
        out = dict(r)
        parsed = parse_dt(out.get("data_end"))
        out["data_end_parsed"] = parsed.isoformat() if parsed else ""
        new_rows.append(out)

    if not new_rows:
        return 0

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=READINGS_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    return len(new_rows)


def write_sites(csv_path: str, rows: List[Dict[str, Any]]) -> int:
    """
    Merges freshly fetched site metadata into the sites CSV (upsert by
    site_code) so Power BI always has the latest known coordinates/names.
    Returns the number of sites now on file.
    """
    by_code: Dict[str, Dict[str, Any]] = {
        r["site_code"]: r for r in _read_csv_rows(csv_path) if r.get("site_code")
    }
    for r in rows:
        if r.get("site_code"):
            by_code[r["site_code"]] = r

    if not by_code:
        return 0

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SITES_COLUMNS)
        writer.writeheader()
        for code in sorted(by_code):
            row = by_code[code]
            writer.writerow({k: row.get(k, "") for k in SITES_COLUMNS})

    return len(by_code)


def load_readings(csv_path: str) -> List[Dict[str, Any]]:
    """Loads readings.csv with aq_index coerced to float and data_end parsed to datetime."""
    rows = []
    for r in _read_csv_rows(csv_path):
        out = dict(r)
        try:
            out["aq_index"] = float(out["aq_index"]) if out.get("aq_index") not in (None, "") else None
        except ValueError:
            out["aq_index"] = None
        out["data_end_parsed_dt"] = parse_dt(out.get("data_end_parsed") or out.get("data_end"))
        rows.append(out)
    return rows
