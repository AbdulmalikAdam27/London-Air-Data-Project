from __future__ import annotations
import requests

BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"


def fetch_hourly_monitoring_index(group_name: str = "London") -> dict:
    """
    Fetch latest hourly monitoring index for a group in JSON.
    Endpoint:
    /Hourly/MonitoringIndex/GroupName={GroupName}/Json
    """
    url = f"{BASE_URL}/Hourly/MonitoringIndex/GroupName={group_name}/Json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()