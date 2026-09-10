# London Air Data Project

**Live London air-quality monitoring pipeline (Python → CSV → Power BI)**

A lightweight data project that fetches live London air quality data from the
ERG/LAQN API, appends it to plain CSV files, and visualises it in a Power BI
dashboard:

- **Worst sites right now** (highest AQI)
- **Site + pollutant trends over time**
- **Colour-coded London map** (green = better AQI, red = worse AQI)
- **Spike alerts** (readings that break out of a site's normal range)

---

## Project Goal

An end-to-end mini data pipeline that demonstrates practical skills relevant
to **Data Analyst / Data / PM / Quant-adjacent** roles:

- API ingestion
- Data cleaning + transformation
- CSV-based storage, dedupe-safe on rerun
- BI dashboarding (Power BI)
- Reproducible repo structure

## Data Source

This project uses the **London Air Quality Network / ERG API**:
- ERG Air Quality API Help: `https://api.erg.ic.ac.uk/AirQuality/help`

## Repository Structure

```text
London-Air-Data-Project/
├─ .github/workflows/
│  └─ collect.yml            # scheduled cloud run: fetches data, commits the CSVs
├─ data/
│  ├─ readings.csv           # fact table: hourly readings (tracked — see Automation below)
│  ├─ sites.csv              # dimension table: site name + lat/lon
│  └─ spike_alerts.csv       # log of detected spikes
├─ powerbi/
│  ├─ README.md              # step-by-step Power BI Desktop setup
│  └─ measures.dax           # DAX measures to paste in
├─ src/
│  ├─ fetch_hourly.py        # API request logic (readings + site metadata)
│  ├─ transform.py           # flatten nested JSON into row dicts
│  ├─ store_csv.py           # append/dedupe/upsert into the CSVs
│  └─ alert_spikes.py        # spike detection over the CSV history
├─ run.py                    # one-shot pipeline runner (fetch -> store -> alert)
├─ run_pipeline.bat          # entry point for local Windows Task Scheduler
├─ migrate_sqlite_to_csv.py  # one-off: import old SQLite-era history
├─ requirements.txt          # Python dependencies (stdlib-heavy, no pandas)
├─ .env                      # local config (paths, group name, alert tuning)
└─ README.md
```

## Setup

```bash
pip install -r requirements.txt
python run.py
```

`run.py`:
1. Fetches the latest hourly monitoring index and appends new readings to `data/readings.csv` (skips anything already on file, keyed by site + pollutant + the API's own bulletin timestamp).
2. Refreshes `data/sites.csv` with the latest site names and coordinates.
3. Scans the full history for statistical outliers and logs them to `data/spike_alerts.csv`.

Config lives in `.env` (already present with sane defaults):

```env
READINGS_CSV=./data/readings.csv
SITES_CSV=./data/sites.csv
SPIKES_CSV=./data/spike_alerts.csv
GROUP_NAME=London

LOOKBACK_HOURS=24
Z_THRESHOLD=2.0
MIN_AQINDEX=4
```

## Building the dashboard

See [powerbi/README.md](powerbi/README.md) for the full walkthrough —
importing the CSVs into Power BI Desktop, setting up the relationship, and
which visuals/measures to build.

## Scheduling / Automation

Two ways to keep the CSVs growing, and you can use either or both:

**Cloud (recommended) — GitHub Actions.** [`.github/workflows/collect.yml`](.github/workflows/collect.yml)
runs `python run.py` every hour on GitHub's infrastructure and commits the
updated CSVs back to this repo — no local machine needs to be on. It also
runs on demand from the **Actions** tab (`Run workflow`). Because this repo
is public, this costs nothing (public repos get unlimited Actions minutes).
Power BI then reads the CSVs straight from GitHub instead of a local file —
see [powerbi/README.md](powerbi/README.md#cloud-data-via-github-actions).

**Local — Windows Task Scheduler.** Point a scheduled task at
`run_pipeline.bat` in this folder. It runs `run.py` using the project's own
`.venv` and appends output to `logs/pipeline.log`. Useful if you want a local
copy of the data or want to run more often than hourly. If you also use
GitHub Actions, `git pull` before running locally to avoid diverging from
what the workflow has already committed.

Either way, open Power BI Desktop and hit **Refresh** whenever you want the
dashboard to pick up the latest rows (or set up scheduled refresh in the
Power BI Service — also covered in the Power BI guide).

## Migrating from the old SQLite version

Earlier versions of this project stored data in `data/london_air.db` via
SQLite and shipped a Streamlit dashboard. That's been replaced by the CSV +
Power BI pipeline above. If you still have the old `.db` file, run:

```bash
python migrate_sqlite_to_csv.py
```

to carry its history into `data/readings.csv` before you start collecting
new data.

Works locally on Windows + PyCharm or terminal.
