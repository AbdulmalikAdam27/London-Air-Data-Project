# Building the Power BI dashboard

The Python pipeline (`run.py`) only produces data — three CSVs in `data/`:

| File | What it is | Grain |
|---|---|---|
| `readings.csv` | Fact table: every hourly pollutant reading ever collected | 1 row per site + pollutant + hour |
| `sites.csv` | Dimension table: site name, borough, lat/lon | 1 row per site |
| `spike_alerts.csv` | Log of unusual readings the alert rule has flagged | 1 row per detected spike, per run |

Everything visual lives in Power BI Desktop. You can point it at either a
local copy of the CSVs or the copy that GitHub Actions keeps updated in the
cloud (see [Cloud data via GitHub Actions](#cloud-data-via-github-actions)
below) — the import steps are almost identical, just a different source URL.

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
- **Scheduled on this machine**: use `run_pipeline.bat` (see the main [README](../README.md#scheduling--automation)) with Windows Task Scheduler to keep the CSVs growing hourly, then hit Refresh in Desktop whenever you open it. This only requires a **Gateway** if you later publish to the Power BI Service and want that local file kept fresh there too.
- **Cloud, no machine required**: use the GitHub Actions source below — since it's a public HTTPS URL, the Power BI **Service** can refresh it on a schedule with no gateway at all.

## Cloud data via GitHub Actions

[`.github/workflows/collect.yml`](../.github/workflows/collect.yml) runs the
pipeline every hour on GitHub's servers and commits the updated CSVs to this
repo. That means the data updates even if your laptop is off, and Power BI
can read it from a plain URL instead of a local file:

```
https://raw.githubusercontent.com/AbdulmalikAdam27/London-Air-Data-Project/main/data/readings.csv
https://raw.githubusercontent.com/AbdulmalikAdam27/London-Air-Data-Project/main/data/sites.csv
https://raw.githubusercontent.com/AbdulmalikAdam27/London-Air-Data-Project/main/data/spike_alerts.csv
```

**In Power BI Desktop**: use **Get Data → Web** instead of **Text/CSV**,
paste one of the URLs above, and continue exactly as in step 1 — Power BI
detects the CSV and lets you set column types the same way.

**In the Power BI Service (scheduled refresh without a gateway)**:
1. Publish the report (Home → Publish, or File → Publish → Publish to Power BI).
2. In the workspace, open the dataset's **Settings → Scheduled refresh**.
3. Under **Data source credentials**, set the Web source to **Anonymous** — no gateway needed since it's a public URL.
4. Turn on **Keep your data up to date** and pick a refresh time (Pro workspaces allow up to 8 refreshes/day).

Two things worth knowing:
- GitHub's raw-content CDN caches responses for a few minutes, so a refresh right after a commit lands may occasionally still show the previous version — refreshing again a few minutes later picks it up.
- The workflow's schedule can lag by several minutes at busy times (a GitHub Actions quirk, not something in this repo) — it's still "every hour," just not to the second.
