"""
Create UptimeRobot monitors for the Forced Action platform.

Run once after deployment:
    python scripts/setup_uptimerobot.py

Requires UPTIMEROBOT_API_KEY in .env.
Idempotent — skips monitors that already exist by friendly name.
"""
import sys
sys.path.insert(0, ".")

import requests
from config.settings import get_settings

API = "https://api.uptimerobot.com/v2"
TIMEOUT = 15

MONITORS = [
    {
        "friendly_name": "Forced Action — API Health",
        "url": "https://forcedactionleads.com/health",
        "type": 1,          # HTTP(S)
        "interval": 300,    # 5 minutes
        "http_method": 2,   # GET (type=1 values: 1=HEAD 2=GET 3=POST)
    },
    {
        "friendly_name": "Forced Action — Health Detailed",
        "url": "https://forcedactionleads.com/health/detailed",
        "type": 1,          # HTTP(S) — returns 503 on DB failure, 200 on ok/degraded
        "interval": 300,
        "http_method": 2,   # GET
    },
]


def get_api_key() -> str:
    s = get_settings()
    if not s.uptimerobot_api_key:
        print("ERROR: UPTIMEROBOT_API_KEY not set in .env")
        sys.exit(1)
    return s.uptimerobot_api_key.get_secret_value()


def get_existing_monitors(api_key: str) -> dict:
    """Return {friendly_name: monitor_id} for all existing monitors."""
    resp = requests.post(
        f"{API}/getMonitors",
        data={"api_key": api_key, "format": "json"},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("stat") != "ok":
        print(f"ERROR fetching monitors: {data}")
        sys.exit(1)
    return {m["friendly_name"]: m["id"] for m in data.get("monitors", [])}


def delete_monitor(api_key: str, monitor_id: int, name: str) -> None:
    resp = requests.post(
        f"{API}/deleteMonitor",
        data={"api_key": api_key, "format": "json", "id": monitor_id},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("stat") == "ok":
        print(f"  Deleted: {name} (id={monitor_id})")
    else:
        print(f"  FAILED delete: {name} — {data.get('error', data)}")


def create_monitor(api_key: str, monitor: dict) -> None:
    payload = {
        "api_key": api_key,
        "format": "json",
        **monitor,
    }
    resp = requests.post(f"{API}/newMonitor", data=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("stat") == "ok":
        mid = data.get("monitor", {}).get("id")
        print(f"  Created: {monitor['friendly_name']} (id={mid})")
    else:
        print(f"  FAILED:  {monitor['friendly_name']} — {data.get('error', data)}")


def main():
    api_key = get_api_key()
    print("Fetching existing monitors...")
    existing = get_existing_monitors(api_key)
    print(f"Found {len(existing)} existing monitor(s).\n")

    # Delete the Stripe Webhook monitor if it exists (it always 405s — not useful)
    stripe_monitor = "Forced Action — Stripe Webhook"
    if stripe_monitor in existing:
        delete_monitor(api_key, existing[stripe_monitor], stripe_monitor)

    for monitor in MONITORS:
        name = monitor["friendly_name"]
        if name in existing:
            print(f"  Skipped (exists): {name} (id={existing[name]})")
        else:
            create_monitor(api_key, monitor)

    print("\nDone. Verify at https://uptimerobot.com/dashboard")


if __name__ == "__main__":
    main()
