# Building the Power BI dashboard

The Python pipeline (`run.py`) only produces data — three CSVs in `data/`:

| File | What it is | Grain |
|---|---|---|
| `readings.csv` | Fact table: every hourly pollutant reading ever collected | 1 row per site + pollutant + hour |
| `sites.csv` | Dimension table: site name, borough, lat/lon | 1 row per site |
| `spike_alerts.csv` | Log of unusual readings the alert rule has flagged | 1 row per detected spike, per run |

Everything visual lives in Power BI Desktop. Steps below assume you've run
`python run.py` at least once so the CSVs exist.

## 1. Import the data

1. Open Power BI Desktop → **Get Data → Text/CSV**.
2. Select `data\readings.csv` → **Transform Data** (not Load, so you can fix types first).
3. In Power Query Editor, set column types:
   - `data_end_parsed` → **Date/Time** (this is the real observation timestamp — use it for all time-based visuals, not `fetched_at_utc`)
   - `aq_index` → **Decimal Number**
   - `ttl_minutes` → **Whole Number**
   - everything else → **Text**
4. Rename the query to `readings`. Click **Close & Apply** (or repeat steps 5-6 first, then apply once).
5. **Get Data → Text/CSV** again, select `data\sites.csv`.
6. Set `lat` and `lon` to **Decimal Number**. Rename the query to `sites`. **Close & Apply**.
7. Optionally repeat for `data\spike_alerts.csv` → rename to `spike_alerts`, set `aq_index`/`mean`/`std`/`threshold` to Decimal Number and `checked_at_utc`/`data_end_parsed` to Date/Time.

## 2. Set up the relationship

Go to the **Model** view:

- Drag `sites[site_code]` onto `readings[site_code]` to create a relationship.
- Direction: **sites → readings** (one-to-many), cross-filter **single**.
- Leave `spike_alerts` unrelated — it's small and self-contained; filter it directly by date/site in its own visual instead.

## 3. Add the DAX measures

Open `powerbi/measures.dax` in this repo and paste each measure into
**Modeling → New Measure** on the `readings` table (or `spike_alerts` for
`Spike Count (All Time)`).

## 4. Build the pages

**Overview page**
- Map visual (or **Azure Maps**): `sites[lat]` / `sites[lon]`, size or color by `[Latest AQI (Selected Context)]`, tooltip on `site_name`.
- Card visuals: `[Worst AQI Today]`, `[Sites Reporting Today]`, `[Spike Count (All Time)]`.
- Table: worst sites right now — `site_name`, `species_name`, `aq_index`, `data_end_parsed`, sorted descending by `aq_index`, filtered to the latest `data_end_parsed`.

**Trends page**
- Slicers: `sites[site_name]`, `readings[species_name]`.
- Line chart: `data_end_parsed` (axis) vs `aq_index` (value), filtered by the slicers.

**Alerts page**
- Table bound to `spike_alerts`: `checked_at_utc`, `site_name`, `species_name`, `aq_index`, `threshold`.
- Conditional formatting on `aq_index` (red scale) makes spikes pop visually.

## 5. Keep it refreshing

- **Manual**: after each `python run.py`, click **Refresh** in Power BI Desktop (Home ribbon) — it re-reads the CSVs.
- **Scheduled on this machine**: use `run_pipeline.bat` (see the main [README](../README.md#scheduling)) with Windows Task Scheduler to keep the CSVs growing hourly, then hit Refresh in Desktop whenever you open it.
- **Scheduled + published**: if you publish the report to the Power BI Service, you can point a **Gateway** at this folder and set a scheduled refresh there too — but for a portfolio/local project, Desktop refresh is usually enough.
