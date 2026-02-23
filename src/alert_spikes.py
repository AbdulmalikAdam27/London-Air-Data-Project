from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd


@dataclass
class AlertConfig:
    lookback_hours: int = 24
    z_threshold: float = 2.0
    min_aqindex: float = 4.0


def find_spikes(db_path: str, cfg: AlertConfig) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        q = f"""
        SELECT
            site_code, site_name, species_code, species_name,
            aq_index, data_end_parsed
        FROM readings
        WHERE data_end_parsed IS NOT NULL
          AND data_end_parsed >= datetime('now', '-{cfg.lookback_hours} hours')
        """
        df = pd.read_sql_query(q, conn)

        if df.empty:
            return df

        df["aq_index"] = pd.to_numeric(df["aq_index"], errors="coerce")
        df["data_end_parsed"] = pd.to_datetime(df["data_end_parsed"], errors="coerce", utc=True)
        df = df.dropna(subset=["aq_index", "data_end_parsed"])

        # Get latest row per site+species
        latest = (
            df.sort_values("data_end_parsed")
              .groupby(["site_code", "species_code"], as_index=False)
              .tail(1)
        )

        # Stats over lookback window
        stats = (
            df.groupby(["site_code", "species_code"], as_index=False)["aq_index"]
              .agg(mean="mean", std="std", count="count")
        )

        merged = latest.merge(stats, on=["site_code", "species_code"], how="left")
        merged["std"] = merged["std"].fillna(0.0)

        # Spike rule
        merged["threshold"] = merged["mean"] + cfg.z_threshold * merged["std"]
        spikes = merged[
            (merged["aq_index"] >= cfg.min_aqindex)
            & (merged["count"] >= 6)  # require enough points
            & (merged["aq_index"] > merged["threshold"])
        ].copy()

        spikes = spikes.sort_values("aq_index", ascending=False)
        return spikes
    finally:
        conn.close()


def print_spike_report(spikes: pd.DataFrame) -> None:
    if spikes.empty:
        print("No spikes detected.")
        return

    print("\nSPIKE ALERTS (latest > mean + z*std)\n")
    cols = ["site_name", "site_code", "species_name", "species_code", "aq_index", "mean", "std", "threshold", "data_end_parsed"]
    print(spikes[cols].to_string(index=False))
