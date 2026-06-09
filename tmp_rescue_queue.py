"""
Fetches queue 98799 from Tracerfy and saves results to a JSON file.
Run this now as a backup — if the main process fails before committing,
run tmp_apply_rescue.py to write the data to DB directly.
"""
import json
import requests
from config.settings import get_settings

settings = get_settings()
api_key = settings.tracerfy_api_key.get_secret_value()

print("Fetching queue 98799 from Tracerfy...")
resp = requests.get(
    "https://tracerfy.com/v1/api/queue/98799",
    headers={"Authorization": f"Bearer {api_key}"},
    timeout=60,
)
print(f"HTTP {resp.status_code}")
results = resp.json()
print(f"Got {len(results)} rows")

with open("rescue_queue_98799.json", "w") as f:
    json.dump(results, f)

print(f"Saved to rescue_queue_98799.json")
hits = sum(1 for r in results if r.get("primary_phone") or r.get("mobile_1") or r.get("email_1"))
print(f"Rows with contact data: {hits}")
