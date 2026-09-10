from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

READINGS_FIELDS = [
    "fetched_at_utc",
    "ttl_minutes",
    "local_authority",
    "site_code",
    "site_name",
    "site_type",
    "species_code",
    "species_name",
    "aq_index",
    "aq_band",
    "index_source",
    "data_end",
]

SITES_FIELDS = ["site_code", "site_name", "local_authority", "site_type", "lat", "lon"]


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


def _pick_float(d: Any, *keys: str) -> Optional[float]:
    val = pick(d, *keys)
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
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


def flatten_hourly_json(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flattens the ERG Hourly MonitoringIndex JSON into one row per
    (site, pollutant species) reading.

    The feed uses attribute-style keys like "@SiteCode", "@SpeciesCode",
    "@AirQualityIndex" — the pick() helper handles that plus plain-key
    variants and is case-insensitive.
    """
    fetched_at_utc = datetime.now(timezone.utc).isoformat()

    root = _find_tree(payload) or payload
    if not isinstance(root, dict):
        return []

    ttl = pick(root, "@TimeToLive", "TimeToLive", "TTL", "ttl", "@TTL", "@ttl")
    las = _as_list(pick(root, "LocalAuthority", "localAuthority") or [])
    root_data_end = pick(root, "@DataEnd", "DataEnd", "dataEnd", "@DataDate", "DataDate", "dataDate")

    rows: List[Dict[str, Any]] = []

    for la in las:
        la_name = pick(
            la,
            "@LaName", "LaName",
            "@LocalAuthorityName", "LocalAuthorityName",
            "@Name", "Name",
            "laName",
        )

        for site in _as_list(pick(la, "Site", "site") or []):
            site_code = pick(site, "@SiteCode", "SiteCode", "siteCode")
            site_name = pick(site, "@SiteName", "SiteName", "siteName", "@SiteDescription", "SiteDescription")
            site_type = pick(site, "@SiteType", "SiteType", "siteType")
            site_data_end = (
                pick(site, "@BulletinDate", "BulletinDate", "bulletinDate")
                or pick(site, "@DataEnd", "DataEnd", "dataEnd", "@DataDate", "DataDate", "dataDate")
                or root_data_end
            )

            for sp in _as_list(pick(site, "Species", "species") or []):
                species_code = pick(sp, "@SpeciesCode", "SpeciesCode", "speciesCode")
                species_name = pick(sp, "@SpeciesDescription", "SpeciesDescription", "@SpeciesName", "SpeciesName", "speciesName")
                aq_index = _pick_float(sp, "@AirQualityIndex", "AirQualityIndex", "@AQIndex", "AQIndex", "aqIndex")
                aq_band = pick(sp, "@AirQualityBand", "AirQualityBand", "airQualityBand")
                index_source = pick(sp, "@IndexSource", "IndexSource", "indexSource")

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
                        "aq_band": aq_band,
                        "index_source": index_source,
                        "data_end": data_end,
                    }
                )

    return rows


def find_sites_with_coords(obj: Any) -> List[Dict[str, Any]]:
    """Recursively collect site dicts that carry a site code and coordinates."""
    sites: List[Dict[str, Any]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            has_code = ("@SiteCode" in x) or ("SiteCode" in x) or ("siteCode" in x)
            has_lat = ("@Latitude" in x) or ("Latitude" in x) or ("latitude" in x)
            has_lon = ("@Longitude" in x) or ("Longitude" in x) or ("longitude" in x)
            if has_code and has_lat and has_lon:
                sites.append(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return sites


def flatten_site_metadata(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flattens the ERG MonitoringSites JSON into one row per site with lat/lon."""
    rows: List[Dict[str, Any]] = []
    seen = set()

    for s in find_sites_with_coords(payload):
        site_code = pick(s, "@SiteCode", "SiteCode", "siteCode")
        lat = _pick_float(s, "@Latitude", "Latitude", "latitude")
        lon = _pick_float(s, "@Longitude", "Longitude", "longitude")

        if not site_code or lat is None or lon is None:
            continue

        site_code = str(site_code).strip()
        if site_code in seen:
            continue
        seen.add(site_code)

        rows.append(
            {
                "site_code": site_code,
                "site_name": pick(s, "@SiteName", "SiteName", "siteName", "@SiteDescription", "SiteDescription"),
                "local_authority": pick(s, "@LocalAuthorityName", "LocalAuthorityName", "localAuthorityName"),
                "site_type": pick(s, "@SiteType", "SiteType", "siteType"),
                "lat": lat,
                "lon": lon,
            }
        )

    return rows
