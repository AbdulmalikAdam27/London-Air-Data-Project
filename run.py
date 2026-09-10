import os
from dotenv import load_dotenv

from src.fetch_hourly import fetch_hourly_monitoring_index, fetch_site_metadata
from src.transform import flatten_hourly_json, flatten_site_metadata
from src.store_csv import append_readings, write_sites, load_readings
from src.alert_spikes import AlertConfig, find_spikes, print_spike_report, append_spike_log

load_dotenv()

READINGS_CSV = os.getenv("READINGS_CSV", "./data/readings.csv")
SITES_CSV = os.getenv("SITES_CSV", "./data/sites.csv")
SPIKES_CSV = os.getenv("SPIKES_CSV", "./data/spike_alerts.csv")
GROUP_NAME = os.getenv("GROUP_NAME", "London")

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))
MIN_AQINDEX = float(os.getenv("MIN_AQINDEX", "4"))


def main() -> None:
    # 1) Fetch + append the latest hourly readings
    payload = fetch_hourly_monitoring_index(GROUP_NAME)
    df = flatten_hourly_json(payload)
    new_rows = append_readings(READINGS_CSV, df)
    print(f"Fetched rows: {len(df)} | New rows appended: {new_rows}")

    # 2) Refresh site metadata (names/coords) used for the Power BI map
    try:
        site_payload = fetch_site_metadata(GROUP_NAME)
        site_df = flatten_site_metadata(site_payload)
        total_sites = write_sites(SITES_CSV, site_df)
        print(f"Site metadata refreshed: {total_sites} sites on file")
    except Exception as e:
        print(f"Warning: could not refresh site metadata: {e}")

    # 3) Spike detection over full history, logged for Power BI to surface
    readings = load_readings(READINGS_CSV)
    cfg = AlertConfig(
        lookback_hours=LOOKBACK_HOURS,
        z_threshold=Z_THRESHOLD,
        min_aqindex=MIN_AQINDEX,
    )
    spikes = find_spikes(readings, cfg)
    print_spike_report(spikes)
    append_spike_log(SPIKES_CSV, spikes)


if __name__ == "__main__":
    main()
