import os
import sqlite3

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

import requests
import pydeck as pdk


def decode_sqlite_number(x):
    """
    Convert SQLite BLOB-encoded numbers (seen as bytes) into Python ints.
    Your aq_index is currently coming back like b'\\x03\\x00...'(8 bytes).
    """
    if isinstance(x, memoryview):
        x = x.tobytes()
    if isinstance(x, (bytes, bytearray)):
        b = bytes(x)
        # Common case: 8-byte little-endian integer
        if len(b) == 8:
            return int.from_bytes(b, byteorder="little", signed=False)
        # Fallback: try little-endian anyway
        return int.from_bytes(b, byteorder="little", signed=False)
    return x


load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./data/london_air.db")

st.title("London Air Quality — Live Hourly Index (LAQN / ERG API)")
st.caption(f"Using database: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
n = pd.read_sql_query("SELECT COUNT(*) AS n FROM readings", conn).iloc[0]["n"]
st.caption(f"Total rows in DB: {int(n)}")

# Latest snapshot
latest_q = """
SELECT *
FROM readings
WHERE data_end_parsed IS NOT NULL
ORDER BY data_end_parsed DESC
LIMIT 2000
"""
latest = pd.read_sql_query(latest_q, conn)
if latest.empty:
    st.warning("No data yet. Run: python run.py")
    st.stop()

latest["data_end_parsed"] = pd.to_datetime(latest["data_end_parsed"], errors="coerce", utc=True)

# ✅ FIX: decode aq_index from SQLite BLOBs for anything that uses `latest`
if "aq_index" in latest.columns:
    latest["aq_index"] = latest["aq_index"].apply(decode_sqlite_number)
    latest["aq_index"] = pd.to_numeric(latest["aq_index"], errors="coerce")


# -------------------------
# MAP SECTION
# -------------------------
st.write("MAP SECTION REACHED ✅")
st.subheader("London AQI map (Green = best, Red = worst)")


def aqi_to_rgba(aqi: float):
    """Map AQI to color: green (low) -> red (high)."""
    try:
        aqi = float(aqi)
    except Exception:
        aqi = 0.0

    # Clamp typical range 1..10; treat 0 as 'no data' (keep grey-ish)
    if aqi <= 0:
        return [120, 120, 120, 120]
    aqi = max(1.0, min(10.0, aqi))
    t = (aqi - 1.0) / 9.0  # 0..1
    r = int(255 * t)
    g = int(255 * (1 - t))
    b = 0
    return [r, g, b, 170]


def find_sites_anywhere(obj):
    """
    Recursively collect site dicts that contain a site code and coordinates.
    This is robust to wrapper differences in ERG JSON output.
    """
    sites = []

    def walk(x):
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


def fetch_site_coords(group_name="London"):
    url = f"https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSites/GroupName={group_name}/Json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    raw_sites = find_sites_anywhere(payload)

    rows = []
    for s in raw_sites:
        site_code = s.get("@SiteCode") or s.get("SiteCode") or s.get("siteCode")
        site_name = s.get("@SiteName") or s.get("SiteName") or s.get("siteName") or s.get("@SiteDescription")
        lat = s.get("@Latitude") or s.get("Latitude") or s.get("latitude")
        lon = s.get("@Longitude") or s.get("Longitude") or s.get("longitude")
        rows.append({"site_code": site_code, "meta_site_name": site_name, "lat": lat, "lon": lon})

    dfm = pd.DataFrame(rows)
    if dfm.empty:
        return dfm

    dfm["lat"] = pd.to_numeric(dfm["lat"], errors="coerce")
    dfm["lon"] = pd.to_numeric(dfm["lon"], errors="coerce")
    dfm = dfm.dropna(subset=["site_code", "lat", "lon"])
    dfm["site_code"] = dfm["site_code"].astype(str).str.strip()
    return dfm


# ✅ FIX: build snapshot list + usability using pandas (not SQL comparisons) because aq_index is BLOB
# ✅ DAILY COMBINED MAP: average overlapping site readings across all snapshots from the same day

# Pull enough recent rows to cover multiple days (increase limit if needed)
snap_raw_q = """
SELECT fetched_at_utc, site_code, site_name, aq_index
FROM readings
WHERE fetched_at_utc IS NOT NULL AND trim(fetched_at_utc) <> ''
  AND site_code IS NOT NULL AND trim(site_code) <> ''
  AND aq_index IS NOT NULL
ORDER BY fetched_at_utc DESC
LIMIT 50000
"""
snap_raw = pd.read_sql_query(snap_raw_q, conn)
if snap_raw.empty:
    st.warning("No snapshots found yet. Run: python run.py and refresh.")
    st.stop()

# Decode AQI (BLOB -> int) then numeric
snap_raw["aq_index"] = snap_raw["aq_index"].apply(decode_sqlite_number)
snap_raw["aq_index"] = pd.to_numeric(snap_raw["aq_index"], errors="coerce")
snap_raw = snap_raw.dropna(subset=["aq_index"])

# Parse fetched_at_utc and create a date column
snap_raw["fetched_at_utc_dt"] = pd.to_datetime(snap_raw["fetched_at_utc"], errors="coerce", utc=True)
snap_raw = snap_raw.dropna(subset=["fetched_at_utc_dt"])
snap_raw["date_utc"] = snap_raw["fetched_at_utc_dt"].dt.date.astype(str)

# Show available days
days = (
    snap_raw.groupby("date_utc", as_index=False)
    .agg(
        usable_points=("aq_index", lambda s: int((s > 0).sum())),
        total_points=("aq_index", "size"),
        max_aq=("aq_index", "max"),
    )
    .sort_values("date_utc", ascending=False)
)

st.write("Days found:", len(days))

if days.empty:
    st.warning("No usable days found yet. Run python run.py and refresh.")
    st.stop()

# Prefer days with AQI > 0 (usable)
usable_days = days[days["usable_points"] > 0].copy()

if usable_days.empty:
    st.warning(
        "Your data exists, but it contains no usable AQI values (>0) on any day "
        "(likely 'No data' returns from the feed)."
    )
    day_choice = st.selectbox("Select day (UTC)", days["date_utc"].tolist(), key="map_day_all")
else:
    usable_days["label"] = (
        usable_days["date_utc"].astype(str)
        + " | usable=" + usable_days["usable_points"].astype(int).astype(str)
        + " | maxAQI=" + usable_days["max_aq"].astype(int).astype(str)
    )
    label = st.selectbox("Select day (UTC) — combined snapshots", usable_days["label"].tolist(), key="map_day_usable")
    day_choice = label.split(" | ")[0].strip()

# Filter to that day
day_df = snap_raw[snap_raw["date_utc"] == day_choice].copy()
day_df["site_code"] = day_df["site_code"].astype(str).str.strip()
day_df["site_name"] = day_df["site_name"].fillna("(Unknown site name)")

# Optionally ignore AQI=0 ("No data") in the average
day_df_nonzero = day_df[day_df["aq_index"] > 0].copy()

st.write("Rows for selected day:", len(day_df), "| Nonzero AQI rows:", len(day_df_nonzero))

if day_df_nonzero.empty:
    st.info("Selected day contains only AQI=0 ('No data'). Showing grey points.")
    site_aqi = (
        day_df.groupby(["site_code", "site_name"], as_index=False)["aq_index"]
        .mean()
        .rename(columns={"aq_index": "avg_aq_index"})
    )
else:
    # ✅ Combine overlapping readings by averaging across all snapshots that day (per site)
    site_aqi = (
        day_df_nonzero.groupby(["site_code", "site_name"], as_index=False)["aq_index"]
        .mean()
        .rename(columns={"aq_index": "avg_aq_index"})
    )

# Fetch coords + join
try:
    meta = fetch_site_coords("London")
    st.write("Sites with coordinates fetched:", len(meta))
except Exception as e:
    st.error(f"Failed to fetch site coordinates: {e}")
    meta = pd.DataFrame([])

if meta.empty:
    st.warning("No coordinates could be extracted from MonitoringSites JSON, so the map cannot be drawn.")
else:
    map_df = site_aqi.merge(meta[["site_code", "lat", "lon"]], on="site_code", how="inner")
    st.write("Map points after join:", len(map_df))

    if map_df.empty:
        st.warning("No AQI sites matched the coordinate list. (site_code mismatch)")
        st.dataframe(site_aqi.head(10))
        st.dataframe(meta.head(10))
    else:
        # Color + radius based on daily average AQI
        map_df["color"] = map_df["avg_aq_index"].apply(aqi_to_rgba)
        map_df["radius"] = (map_df["avg_aq_index"].clip(0, 10) * 220).astype(int)

        st.caption(
            "Daily combined view: each dot is a monitoring site. "
            "Color shows the **average AQI across all snapshots that day** (overlapping readings are averaged)."
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position="[lon, lat]",
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
        )

        view_state = pdk.ViewState(
            longitude=float(map_df["lon"].mean()),
            latitude=float(map_df["lat"].mean()),
            zoom=9.5,
            pitch=0,
        )
        tooltip = {
            "html": "<b>{site_name}</b><br/>Avg AQI (day): {avg_aq_index}<br/>Site code: {site_code}",
            "style": {"backgroundColor": "black", "color": "white"},
        }

        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))



# -------------------------
# WORST SITES (DO NOT CHANGE)
# -------------------------
st.subheader("Worst sites right now (highest AQIndex)")
worst = (
    latest.dropna(subset=["aq_index", "data_end_parsed"])
          .sort_values("data_end_parsed")
          .groupby(["site_code", "species_code"], as_index=False)
          .tail(1)
          .sort_values("aq_index", ascending=False)
          .head(10)
)
st.dataframe(worst[["site_name", "species_name", "aq_index", "data_end_parsed"]])


# -------------------------
# EXPLORE TREND
# -------------------------
st.subheader("Explore a site + pollutant trend")

sites_q = """
SELECT DISTINCT site_code, site_name
FROM readings
WHERE site_code IS NOT NULL
  AND trim(site_code) <> ''
  AND species_code IS NOT NULL
  AND trim(species_code) <> ''
"""
sites = pd.read_sql_query(sites_q, conn)

if sites.empty:
    st.warning("No site/species data found yet. Run: python run.py and refresh.")
    st.stop()

sites["site_name"] = sites["site_name"].fillna("(Unknown site name)")
sites = sites.sort_values(["site_name", "site_code"])
sites["label"] = sites["site_name"] + " (" + sites["site_code"].astype(str) + ")"

site_label = st.selectbox("Site", sites["label"].tolist(), key="site_select")
site_code = site_label.split("(")[-1].replace(")", "").strip()

species_q = """
SELECT DISTINCT species_code, species_name
FROM readings
WHERE site_code = ?
  AND species_code IS NOT NULL
  AND trim(species_code) <> ''
"""
species = pd.read_sql_query(species_q, conn, params=(site_code,))
species["species_name"] = species["species_name"].fillna("(Unknown species)")
species = species.sort_values(["species_name", "species_code"])
species["label"] = species["species_name"] + " (" + species["species_code"].astype(str) + ")"

if species.empty:
    st.warning("No species found for this site yet. Run: python run.py and refresh.")
    st.stop()

species_label = st.selectbox("Species (pollutant)", species["label"].tolist(), key="species_select")
species_code = species_label.split("(")[-1].replace(")", "").strip()

lookback_days = st.slider("History window (days)", min_value=1, max_value=30, value=7, step=1)

hist_q = """
SELECT
    COALESCE(NULLIF(data_end_parsed, 'NaT'), data_end_parsed, fetched_at_utc) AS ts,
    aq_index
FROM readings
WHERE site_code = ?
  AND species_code = ?
ORDER BY ts ASC
"""
hist = pd.read_sql_query(hist_q, conn, params=(site_code, species_code))

# ✅ FIX: decode aq_index BLOBs, then numeric conversion
hist["aq_index"] = hist["aq_index"].apply(decode_sqlite_number)
hist["aq_index"] = pd.to_numeric(hist["aq_index"], errors="coerce")

hist["ts"] = pd.to_datetime(hist["ts"], errors="coerce", utc=True)
hist = hist.dropna(subset=["ts", "aq_index"])

if hist.empty:
    st.info("No usable history for this site/species yet. Run python run.py a few more times and refresh.")
    st.stop()

cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=lookback_days)
hist = hist[hist["ts"] >= cutoff]

if hist.empty:
    st.info("No data in the selected time window. Increase the history window or run python run.py again.")
    st.stop()

latest_point = hist.iloc[-1]
st.caption(f"Latest AQ index: **{latest_point['aq_index']}** at **{latest_point['ts']}**")

st.line_chart(hist.set_index("ts")["aq_index"])

conn.close()
