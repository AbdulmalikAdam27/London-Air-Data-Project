"""One-off migration: export the old SQLite readings table into data/readings.csv
so historical data collected before the CSV pipeline isn't lost. Safe to delete
after running once.
"""
import csv
import json
import sqlite3

from src.store_csv import READINGS_COLUMNS, parse_dt

SRC_DB = "data/london_air.db"
DEST_CSV = "data/readings.csv"


def decode_aq_index(x):
    """The old pipeline stored aq_index as an 8-byte little-endian int64 BLOB
    (a numpy int64 written via sqlite3 without an adapter, since the fetched
    values had no NaNs) instead of a plain number — decode it back here."""
    if isinstance(x, (bytes, bytearray)) and len(x) == 8:
        return int.from_bytes(x, byteorder="little", signed=False)
    return x

conn = sqlite3.connect(SRC_DB)
conn.row_factory = sqlite3.Row
cur = conn.execute("SELECT * FROM readings ORDER BY id")
rows = cur.fetchall()
conn.close()

with open(DEST_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=READINGS_COLUMNS)
    writer.writeheader()
    for row in rows:
        parsed = parse_dt(row["data_end"])
        try:
            aq_band = json.loads(row["raw_species_json"]).get("@AirQualityBand")
        except (TypeError, ValueError):
            aq_band = None
        writer.writerow(
            {
                "fetched_at_utc": row["fetched_at_utc"],
                "ttl_minutes": row["ttl_minutes"],
                "local_authority": row["local_authority"],
                "site_code": row["site_code"],
                "site_name": row["site_name"],
                "site_type": row["site_type"],
                "species_code": row["species_code"],
                "species_name": row["species_name"],
                "aq_index": decode_aq_index(row["aq_index"]),
                "aq_band": aq_band,
                "index_source": row["index_source"],
                "data_end": row["data_end"],
                "data_end_parsed": parsed.isoformat() if parsed else "",
            }
        )

print(f"Migrated {len(rows)} rows from {SRC_DB} to {DEST_CSV}")
