import os
from dotenv import load_dotenv

from src.fetch_hourly import fetch_hourly_monitoring_index
from src.store_sqlite import flatten_hourly_json, upsert_readings
from src.alert_spikes import AlertConfig, find_spikes, print_spike_report

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "./data/london_air.db")
GROUP_NAME = os.getenv("GROUP_NAME", "London")

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))
MIN_AQINDEX = float(os.getenv("MIN_AQINDEX", "4"))

def main() -> None:
    payload = fetch_hourly_monitoring_index(GROUP_NAME)
    df = flatten_hourly_json(payload)
    inserted = upsert_readings(DB_PATH, df)
    print(f"Fetched rows: {len(df)} | Attempted inserts: {inserted}")

    cfg = AlertConfig(
        lookback_hours=LOOKBACK_HOURS,
        z_threshold=Z_THRESHOLD,
        min_aqindex=MIN_AQINDEX,
    )
    spikes = find_spikes(DB_PATH, cfg)
    print_spike_report(spikes)

if __name__ == "__main__":
    main()
