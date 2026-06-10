import requests
import json

BASE_URL = "https://api.weather.gov/alerts"

headers = {
    "User-Agent": "fire-alert-monitor heu.solutions@gmail.com",
    "Accept": "application/geo+json",
}

TARGET_COUNTIES = {
    "Hillsborough": {
        "county_ugc": "FLC057",
        "same": "012057",
        "keywords": ["hillsborough"],
    },
    "Pinellas": {
        "county_ugc": "FLC103",
        "same": "012103",
        "keywords": ["pinellas"],
    },
}

START_DATE = "2026-06-01T00:00:00Z"
END_DATE = "2026-06-05T23:59:59Z"

params = {
    "area": "FL",
    "start": START_DATE,
    "end": END_DATE,
    "limit": 500,
}

response = requests.get(
    BASE_URL,
    headers=headers,
    params=params,
    timeout=20,
)

response.raise_for_status()
data = response.json()

result = {
    "Hillsborough": {
        "alert_count": 0,
        "alerts": [],
    },
    "Pinellas": {
        "alert_count": 0,
        "alerts": [],
    },
}

for feature in data.get("features", []):
    props = feature.get("properties", {})

    area_desc = (props.get("areaDesc") or "").lower()
    geocode = props.get("geocode") or {}

    ugc_codes = geocode.get("UGC", [])
    same_codes = geocode.get("SAME", [])

    for county_name, rules in TARGET_COUNTIES.items():
        name_match = any(keyword in area_desc for keyword in rules["keywords"])
        ugc_match = rules["county_ugc"] in ugc_codes
        same_match = rules["same"] in same_codes

        if name_match or ugc_match or same_match:
            alert = {
                "event": props.get("event"),
                "severity": props.get("severity"),
                "certainty": props.get("certainty"),
                "urgency": props.get("urgency"),
                "headline": props.get("headline"),
                "area": props.get("areaDesc"),
                "sent": props.get("sent"),
                "effective": props.get("effective"),
                "onset": props.get("onset"),
                "expires": props.get("expires"),
                "ends": props.get("ends"),
                "description": props.get("description"),
                "instruction": props.get("instruction"),
                "status": props.get("status"),
                "message_type": props.get("messageType"),
                "sender_name": props.get("senderName"),
                "ugc_codes": ugc_codes,
                "same_codes": same_codes,
            }

            result[county_name]["alerts"].append(alert)

for county_name in result:
    result[county_name]["alert_count"] = len(result[county_name]["alerts"])

print(json.dumps(result, indent=2))