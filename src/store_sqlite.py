from __future__ import annotations


import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
def pick(d: dict, *keys):
    """Return the first matching key from d (supports exact + case-insensitive)."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    lower_map = {str(k).lower(): k for k in d.keys()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower_map:
            return d[lower_map[lk]]
    return None

def _as_list(x: Any) -> List[Any]:
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def flatten_hourly_json(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Flattens the ERG Hourly MonitoringIndex JSON into a dataframe.

    Your feed uses attribute-style keys like:
      - "@SiteCode", "@SiteName"
      - "@SpeciesCode", "@SpeciesDescription"
      - "@AirQualityIndex", "@IndexSource"
    so we extract using a robust `pick()` helper.

    Notes:
    - Some responses are wrapped; we search for a dict containing LocalAuthority/localAuthority.
    - The hourly feed may not include a per-species timestamp. We attempt to pull DataEnd/DataDate
      from species/site/root, and if missing we fall back to fetched_at_utc.
    """
    fetched_at_utc = datetime.now(timezone.utc).isoformat()

    def _as_list(x: Any) -> List[Any]:
        if x is None:
            return []
        return x if isinstance(x, list) else [x]

    def pick(d: Any, *keys: str) -> Any:
        """Return the first matching key from dict d (supports exact + case-insensitive)."""
        if not isinstance(d, dict):
            return None
        for k in keys:
            if k in d:
                return d[k]
        lower_map = {str(k).lower(): k for k in d.keys()}
        for k in keys:
            lk = str(k).lower()
            if lk in lower_map:
                return d[lower_map[lk]]
        return None

    def _find_tree(obj: Any) -> Optional[Dict[str, Any]]:
        """Recursively find the dict that contains the LocalAuthority list."""
        if isinstance(obj, dict):
            if ("LocalAuthority" in obj) or ("localAuthority" in obj):
                return obj
            for v in obj.values():
                found = _find_tree(v)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for v in obj:
                found = _find_tree(v)
                if found is not None:
                    return found
        return None

    # Locate the tree root (some responses wrap it under another key)
    root = _find_tree(payload) or payload
    if not isinstance(root, dict):
        return pd.DataFrame([])

    ttl = pick(root, "TTL", "ttl", "@TTL", "@ttl")
    las = pick(root, "LocalAuthority", "localAuthority") or []
    las = _as_list(las)

    rows: List[Dict[str, Any]] = []

    # Optional root-level timestamps (may or may not exist in your feed)
    root_data_end = pick(root, "@DataEnd", "DataEnd", "dataEnd", "@DataDate", "DataDate", "dataDate")

    for la in las:
        la_name = pick(
            la,
            "@LaName", "LaName",
            "@LocalAuthorityName", "LocalAuthorityName",
            "@Name", "Name",
            "laName"
        )

        sites = pick(la, "Site", "site") or []
        for site in _as_list(sites):
            site_code = pick(site, "@SiteCode", "SiteCode", "siteCode")
            site_name = pick(site, "@SiteName", "SiteName", "siteName", "@SiteDescription", "SiteDescription")
            site_type = pick(site, "@SiteType", "SiteType", "siteType")

            site_data_end = pick(site, "@DataEnd", "DataEnd", "dataEnd", "@DataDate", "DataDate", "dataDate") or root_data_end

            species_list = pick(site, "Species", "species") or []
            for sp in _as_list(species_list):
                species_code = pick(sp, "@SpeciesCode", "SpeciesCode", "speciesCode")
                species_name = pick(sp, "@SpeciesDescription", "SpeciesDescription", "@SpeciesName", "SpeciesName", "speciesName")
                aq_index = pick(sp, "@AirQualityIndex", "AirQualityIndex", "@AQIndex", "AQIndex", "aqIndex")
                index_source = pick(sp, "@IndexSource", "IndexSource", "indexSource")

                # Best-effort timestamp
                data_end = (
                    pick(sp, "@DataEnd", "DataEnd", "dataEnd", "@DataDate", "DataDate", "dataDate")
                    or site_data_end
                    or root_data_end
                    or fetched_at_utc
                )

                rows.append(
                    {
                        "fetched_at_utc": fetched_at_utc,
                        "ttl_minutes": ttl,
                        "local_authority": la_name,
                        "site_code": site_code,
                        "site_name": site_name,
                        "site_type": site_type,
                        "species_code": species_code,
                        "species_name": species_name,
                        "aq_index": aq_index,
                        "index_source": index_source,
                        "data_end": data_end,
                        "raw_species_json": json.dumps(sp, ensure_ascii=False),
                    }
                )

    df = pd.DataFrame(rows)

    # Normalise types (best-effort)
    if not df.empty:
        df["aq_index"] = pd.to_numeric(df["aq_index"], errors="coerce")
        df["data_end_parsed"] = pd.to_datetime(df["data_end"], errors="coerce", utc=True)

    return df



def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at_utc TEXT NOT NULL,
            ttl_minutes INTEGER,
            local_authority TEXT,
            site_code TEXT,
            site_name TEXT,
            site_type TEXT,
            species_code TEXT,
            species_name TEXT,
            aq_index REAL,
            index_source TEXT,
            data_end TEXT,
            data_end_parsed TEXT,
            raw_species_json TEXT,
            UNIQUE(site_code, species_code, data_end)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_site_species_time ON readings(site_code, species_code, data_end_parsed);")
    conn.commit()


def upsert_readings(db_path: str, df: pd.DataFrame) -> int:
    """
    Inserts rows; duplicates (same site_code+species_code+data_end) are ignored.
    Returns number of attempted inserts (not necessarily committed rows).
    """
    if df.empty:
        return 0

    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)

        df2 = df.copy()
        df2["data_end_parsed"] = df2["data_end_parsed"].astype(str)

        rows = df2[
            [
                "fetched_at_utc",
                "ttl_minutes",
                "local_authority",
                "site_code",
                "site_name",
                "site_type",
                "species_code",
                "species_name",
                "aq_index",
                "index_source",
                "data_end",
                "data_end_parsed",
                "raw_species_json",
            ]
        ].to_records(index=False)

        conn.executemany(
            """
            INSERT OR IGNORE INTO readings (
                fetched_at_utc, ttl_minutes, local_authority, site_code, site_name, site_type,
                species_code, species_name, aq_index, index_source, data_end, data_end_parsed, raw_species_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            list(rows),
        )
        conn.commit()
        return len(df2)
    finally:
        conn.close()
