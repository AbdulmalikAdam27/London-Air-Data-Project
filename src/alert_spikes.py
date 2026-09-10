from __future__ import annotations

import csv
import os
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


@dataclass
class AlertConfig:
    lookback_hours: int = 24
    z_threshold: float = 2.0
    min_aqindex: float = 4.0


SPIKE_COLUMNS = [
    "checked_at_utc",
    "site_code",
    "site_name",
    "species_code",
    "species_name",
    "aq_index",
    "mean",
    "std",
    "threshold",
    "data_end_parsed",
]


def find_spikes(readings: List[Dict[str, Any]], cfg: AlertConfig) -> List[Dict[str, Any]]:
    """
    Given the full readings history (as loaded by store_csv.load_readings),
    flags site+species readings whose latest value is an unusually high
    outlier (latest > mean + z*std) over the lookback window.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg.lookback_hours)

    by_key: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in readings:
        ts = r.get("data_end_parsed_dt")
        aqi = r.get("aq_index")
        if ts is None or aqi is None or ts < cutoff:
            continue
        key = (r.get("site_code"), r.get("species_code"))
        by_key.setdefault(key, []).append(r)

    spikes = []
    for (site_code, species_code), rows in by_key.items():
        rows.sort(key=lambda r: r["data_end_parsed_dt"])
        values = [r["aq_index"] for r in rows]
        if len(values) < 6:
            continue

        latest = rows[-1]
        mean = statistics.mean(values)
        std = statistics.pstdev(values) if len(values) > 1 else 0.0
        threshold = mean + cfg.z_threshold * std

        if latest["aq_index"] >= cfg.min_aqindex and latest["aq_index"] > threshold:
            spikes.append(
                {
                    "site_code": site_code,
                    "site_name": latest.get("site_name"),
                    "species_code": species_code,
                    "species_name": latest.get("species_name"),
                    "aq_index": latest["aq_index"],
                    "mean": mean,
                    "std": std,
                    "threshold": threshold,
                    "data_end_parsed": latest["data_end_parsed_dt"].isoformat(),
                }
            )

    spikes.sort(key=lambda s: s["aq_index"], reverse=True)
    return spikes


def print_spike_report(spikes: List[Dict[str, Any]]) -> None:
    if not spikes:
        print("No spikes detected.")
        return

    print("\nSPIKE ALERTS (latest > mean + z*std)\n")
    for s in spikes:
        print(
            f"{s['site_name']} ({s['site_code']}) | {s['species_name']} ({s['species_code']}) | "
            f"aq_index={s['aq_index']:.1f} mean={s['mean']:.2f} std={s['std']:.2f} "
            f"threshold={s['threshold']:.2f} at {s['data_end_parsed']}"
        )


def append_spike_log(csv_path: str, spikes: List[Dict[str, Any]]) -> int:
    """Appends detected spikes to a running log CSV, tagged with when the check ran."""
    if not spikes:
        return 0

    checked_at_utc = datetime.now(timezone.utc).isoformat()
    rows = [{**s, "checked_at_utc": checked_at_utc} for s in spikes]

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SPIKE_COLUMNS)
        if write_header:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in SPIKE_COLUMNS})

    return len(rows)
