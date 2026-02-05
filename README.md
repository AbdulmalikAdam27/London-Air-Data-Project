# London Air Mini  
**Live London air-quality monitoring mini-project (Python + SQLite + Streamlit)**

A lightweight, beginner-friendly data project that fetches live London air quality data from the ERG/LAQN API, stores it in SQLite, and visualises:

- **Worst sites right now** (highest AQI)
- **Site + pollutant trends over time**
- **Colour-coded London map** (green = better AQI, red = worse AQI)

---

## Project Goal

Build an end-to-end mini data pipeline that demonstrates practical skills relevant to **Data Analyst / Data / PM / Quant-adjacent** roles:

- API ingestion
- Data cleaning + transformation
- Local persistence (SQLite)
- Dashboarding (Streamlit)
- Reproducible repo structure

---

## Data Source

This project uses the **London Air Quality Network / ERG API** documentation and endpoints:
- ERG Air Quality API Help: `https://api.erg.ic.ac.uk/AirQuality/help`

## Features

Live ingestion from London feed

Flattening nested JSON into tabular records

SQLite storage with dedupe-friendly structure

Trend exploration by site + pollutant

Map visualisation by site with AQI colour scale

## Repository Structure 

```text
london-air-mini/
├─ data/
│  └─ london_air.db                 # SQLite database (generated locally)
├─ src/
│  ├─ fetch_hourly.py               # API request logic
│  ├─ store_sqlite.py               # flatten + save to SQLite
│  ├─ dashboard.py                  # Streamlit dashboard
├─ run.py                           # one-shot pipeline runner (fetch -> store)
├─ requirements.txt                 # Python dependencies
├─ .env.example                     # template environment variables
└─ README.md




Works locally on Windows 11 + Conda + PyCharm or terminal
