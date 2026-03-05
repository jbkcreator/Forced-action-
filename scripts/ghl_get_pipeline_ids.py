"""
Fetch GHL pipeline and stage IDs for .env configuration.
Usage: python scripts/ghl_get_pipeline_ids.py
"""

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

API_KEY     = os.getenv("GHL_API_KEY")
LOCATION_ID = os.getenv("GHL_LOCATION_ID")

if not API_KEY or not LOCATION_ID:
    print("ERROR: GHL_API_KEY and GHL_LOCATION_ID must be set in .env")
    sys.exit(1)

resp = requests.get(
    "https://services.leadconnectorhq.com/opportunities/pipelines",
    headers={
        "Authorization": f"Bearer {API_KEY}",
        "Version": "2021-07-28",
        "Accept": "application/json",
    },
    params={"locationId": LOCATION_ID},
    timeout=10,
)

if not resp.ok:
    print(f"ERROR: {resp.status_code} — {resp.text}")
    sys.exit(1)

pipelines = resp.json().get("pipelines", [])

if not pipelines:
    print("No pipelines found for this location.")
    sys.exit(0)

print("\n=== GHL PIPELINES & STAGE IDs ===\n")
for pl in pipelines:
    print(f"Pipeline: {pl['name']}")
    print(f"  GHL_PIPELINE_ID={pl['id']}")
    print(f"  Stages:")
    for stage in pl.get("stages", []):
        print(f"    {stage['name']:30s}  id={stage['id']}")
    print()

print("=== ADD TO .env ===")
print("GHL_PIPELINE_ID=<pipeline_id_above>")
print("GHL_STAGE_IMMEDIATE=<stage_id_for_immediate>")
print("GHL_STAGE_HIGH=<stage_id_for_high>")
print("GHL_STAGE_MEDIUM=<stage_id_for_medium>")
