# LAQN Power BI dashboard

The dashboard is the Power BI Project in this folder. Open `LAQN Dashboard.pbip`
in Power BI Desktop and click **Refresh**. It reads the CSVs this repo's pipeline
publishes, directly from GitHub:

| File | What it is | Grain |
|---|---|---|
| `readings.csv` | Fact table: every bulletin reading collected | 1 row per site, pollutant, bulletin hour |
| `sites.csv` | Site name, borough, type, lat/lon | 1 row per site |
| `spike_alerts.csv` | Spikes and near-misses (`kind`, `z_score`) | 1 row per flagged reading |
| `weather.csv` | Open-Meteo wind and temperature | 1 row per site-hour that has a reading |

```text
https://raw.githubusercontent.com/AbdulmalikAdam27/London-Air-Data-Project/main/data/<file>.csv
```

If `weather.csv` is ever missing, the `weather` query returns an empty table, so
refresh still succeeds.

The project's machine-specific settings (`.pbi/localSettings.json`) and data
cache (`.pbi/cache.abf`) are not committed, so the first open shows empty visuals
until you click **Refresh**.

## Model

```text
sites 1 ──* readings *── 1 Calendar
  1            *
  │            └───────── 1 weather        (Weather Key: site + hour)
  *
spikes_alerts

Site Species          one row per site + pollutant, with an Is Active flag
Exceedance Threshold  drives the threshold slicer (default AQI ≥ 2)
```

Power Query converts timestamps to true UTC and drops rows before 1 September
2026, which came from the old SQLite pipeline with unreliable timestamps. All
measures are in [`measures.dax`](measures.dax).

## Pages

| Page | Question | Audience |
|---|---|---|
| London now | Is the air OK today? | Public |
| Trends: when | When is it worst? | Analysts |
| Trends: where & why | Where is it worst, and why? | Analysts |
| Alerts | What has spiked, and where? | Analysts |
| Data quality | Can I trust the data? | Pipeline owner |
| About & method | How was this built? | Reviewers |
| Overview | Pipeline overview; also the drill-through target: right-click any site, then **Drill through → Overview** | Analysts |
| Map tooltip (hidden) | Shown when you hover a site on the Where & why map | Everyone |

## Definitions

- **Exceedance hours**: site-pollutant-hours at or above the threshold slicer.
  London sits close to index 1, so averages hide the variation.
- **Active pair**: a site and pollutant with a valid reading in the 7 days before
  the latest bulletin. No-data and completeness figures only use active pairs.
- **Completeness**: valid readings ÷ (active pairs × bulletin hours captured).
  It separates a site that stopped reporting from hours the pipeline never captured.
- **Spike**: latest reading ≥ 4 and above mean + 2 × std over the previous 24 hours,
  with at least 6 readings in that window.
- **Near-miss**: latest reading ≥ 2 and above mean + 1.5 × std.

## Refreshing

- **Desktop**: open the project and click **Refresh**.
- **Power BI Service**: publish, then under **Settings → Scheduled refresh** set the
  Web source credentials to **Anonymous**. No gateway is needed because the URLs
  are public. Pro allows up to 8 refreshes a day.

GitHub caches raw files for a few minutes, so a refresh straight after a workflow
run can show the previous version.

## Known limitations

- **Coverage**: the feed only serves the latest bulletin, and GitHub delays
  scheduled runs, so the pipeline captures about 4–5 of 24 hourly bulletins a day.
  The counts are samples, not complete totals. The Data quality and Overview
  pages track this.
- **Alerts**: the 6-readings-in-24-hours requirement is rarely met at current
  coverage, so spikes and near-misses seldom fire.
- **Index resolution**: DAQI values are coarse integers and NO2 is almost always 1,
  so the roadside gap is small for NO2 (+0.01). It would need concentrations
  (µg/m³), which this feed doesn't provide.
- **Wind**: wind and temperature are in the model (`weather`, with a compass
  `Wind Sector` column) but no page uses them yet. Wind comes from Open-Meteo's
  model grid, so nearby sites share similar values.
- **Boroughs**: 17 of London's 33 boroughs have sites returning valid values in
  this feed.
