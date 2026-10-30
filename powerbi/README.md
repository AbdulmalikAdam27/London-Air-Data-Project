# LAQN Power BI dashboard

The dashboard is a Power BI Project (`LAQN Dashboard.pbip`). It reads the CSVs
this repo's pipeline publishes, directly from GitHub:

| File | What it is | Grain |
|---|---|---|
| `readings.csv` | Fact table: every bulletin reading collected | 1 row per site, pollutant, bulletin hour |
| `sites.csv` | Site name, borough, type, lat/lon | 1 row per site |
| `spike_alerts.csv` | Spikes and near-misses (`kind`, `z_score`) | 1 row per flagged reading |
| `weather.csv` | Open-Meteo wind and temperature | 1 row per site-hour that has a reading |

```text
https://raw.githubusercontent.com/AbdulmalikAdam27/London-Air-Data-Project/main/data/<file>.csv
```

If `weather.csv` isn't on GitHub yet, the `weather` query returns an empty table
so refresh still succeeds. Only the wind chart is empty until then.

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
| Alerts | What has spiked? | Analysts |
| Data quality | Can I trust this? | Pipeline owner |
| About & method | How was this built? | Reviewers |
| Site detail (hidden) | Drill-through: right-click any site, then **Drill through** | Analysts |
| Map tooltip (hidden) | Shown when you hover a site on the Where & why map | Everyone |

## Definitions

- **Exceedance hours**: site-pollutant-hours at or above the threshold slicer.
  London sits close to index 1, so averages hide the variation.
- **Active pair**: a site and pollutant with a valid reading in the 7 days before
  the latest bulletin. No-data and completeness figures only use active pairs.
- **Completeness**: valid readings ÷ (active pairs × bulletin hours captured that
  day). A low cell means a site problem. A blank column means no bulletin was
  captured at all, which points at the API or the pipeline.
- **Spike**: latest reading ≥ 4 and above mean + 2 × std over the previous 24 hours.
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
  scheduled runs, so the pipeline captures about 5 of 24 hourly bulletins a day.
  The counts are samples, not complete totals. The Data quality page tracks this.
- **Index resolution**: DAQI values are coarse integers and NO2 is almost always 1,
  so the roadside gap is small for NO2 (+0.01). It would need concentrations
  (µg/m³), which this feed doesn't provide.
- **Wind**: Power BI has no built-in polar chart, so the pollution rose is a bar
  chart by compass sector. Wind comes from Open-Meteo's model grid, so nearby
  sites share similar values.
- **Boroughs**: 18 of London's 33 boroughs have sites reporting in this feed.
