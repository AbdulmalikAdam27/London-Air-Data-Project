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
    near_miss_z: float = 1.5
    near_miss_min_aqindex: float = 2.0


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
    "z_score",
    "kind",
]


def find_spikes(readings: List[Dict[str, Any]], cfg: AlertConfig) -> List[Dict[str, Any]]:
    """
    Given the full readings history (as loaded by store_csv.load_readings),
    flags site+species readings whose latest value is an unusually high
    outlier over the lookback window.

    kind="spike":     latest >= min_aqindex and latest > mean + z_threshold * std
    kind="near_miss": not a spike, but latest >= near_miss_min_aqindex and
                      latest > mean + near_miss_z * std
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

    found = []
    for (site_code, species_code), rows in by_key.items():
        rows.sort(key=lambda r: r["data_end_parsed_dt"])
        values = [r["aq_index"] for r in rows]
        if len(values) < 6:
            continue

        latest = rows[-1]
        mean = statistics.mean(values)
        std = statistics.pstdev(values) if len(values) > 1 else 0.0
        threshold = mean + cfg.z_threshold * std
        z = (latest["aq_index"] - mean) / std if std > 0 else 0.0

        if latest["aq_index"] >= cfg.min_aqindex and latest["aq_index"] > threshold:
            kind = "spike"
        elif (
            std > 0
            and latest["aq_index"] >= cfg.near_miss_min_aqindex
            and latest["aq_index"] > mean + cfg.near_miss_z * std
        ):
            kind = "near_miss"
        else:
            continue

        found.append(
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
                "z_score": z,
                "kind": kind,
            }
        )

    found.sort(key=lambda s: (s["kind"] != "spike", -s["aq_index"]))
    return found


def print_spike_report(spikes: List[Dict[str, Any]]) -> None:
    if not spikes:
        print("No spikes or near-misses detected.")
        return

    print("\nSPIKE ALERTS (spike: latest > mean + z*std | near_miss: relaxed rule)\n")
    for s in spikes:
        print(
            f"[{s['kind']}] {s['site_name']} ({s['site_code']}) | {s['species_name']} ({s['species_code']}) | "
            f"aq_index={s['aq_index']:.1f} mean={s['mean']:.2f} std={s['std']:.2f} "
            f"z={s['z_score']:.2f} at {s['data_end_parsed']}"
        )


def _read_log(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_spike_log(csv_path: str, spikes: List[Dict[str, Any]]) -> int:
    """
    Appends newly detected spikes/near-misses to a running log CSV, tagged with
    when the check ran. A reading already logged (same site, species, reading
    time and kind) is not logged again. Logs written by older versions (without
    z_score/kind) are upgraded in place; their rows are all spikes.
    """
    existing = _read_log(csv_path)
    if existing and list(existing[0].keys()) != SPIKE_COLUMNS:
        for r in existing:
            r.setdefault("z_score", "")
            r.setdefault("kind", "spike")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SPIKE_COLUMNS)
            writer.writeheader()
            for r in existing:
                writer.writerow({k: r.get(k, "") for k in SPIKE_COLUMNS})

    seen = {
        (r.get("site_code"), r.get("species_code"), r.get("data_end_parsed"), r.get("kind") or "spike")
        for r in existing
    }
    checked_at_utc = datetime.now(timezone.utc).isoformat()
    rows = [
        {**s, "checked_at_utc": checked_at_utc}
        for s in spikes
        if (s["site_code"], s["species_code"], s["data_end_parsed"], s["kind"]) not in seen
    ]
    if not rows:
        return 0

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    fresh = not existing
    with open(csv_path, "w" if fresh else "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SPIKE_COLUMNS)
        if fresh:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in SPIKE_COLUMNS})

    return len(rows)
