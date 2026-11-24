"""External watchdog settings that are not env-driven: the public doors it knocks on."""
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Door:
    name: str
    method: Literal["POST", "GET"]
    path: str
    healthy: frozenset[int]


# Each route checks the secret first and writes nothing, so a knock with no secret is refused.
DOORS: tuple[Door, ...] = (
    Door("ghl-stage", "POST", "/webhooks/lending/ghl-stage", frozenset({401})),
    Door("ghl-disposition", "POST", "/webhooks/lending/ghl-disposition", frozenset({401})),
    Door("lendingflow", "POST", "/api/lending/lendingflow", frozenset({401})),
    Door("instantly", "POST", "/webhooks/lending/instantly", frozenset({403})),
    Door("website", "GET", "/api/lending/web-leads", frozenset({405})),
)


ALERT_LABELS: dict[str, str] = {
    "unit:fa-lending-cora": "Cora",
    "unit:lending-api": "Lending API",
    "health:lending-api": "Lending API health",
    "database": "Database",
    "door:ghl-stage": "GHL stage webhook",
    "door:ghl-disposition": "GHL outcome webhook",
    "door:lendingflow": "LendingFlow webhook",
    "door:instantly": "Instantly webhook",
    "door:website": "Website lead form",
    "ghl_outcomes": "GHL call outcomes",
    "cards": "Approval cards",
    "replies": "Reply backlog",
    "failures": "Webhook failures",
}

ALERT_TEMPLATES: dict[str, str] = {
    "down": "NDL watchdog: {label} DOWN since {since} ET. {detail}",
    "still_down": "NDL watchdog: {label} STILL DOWN since {since} ET. {detail}",
    "recovered": "NDL watchdog: {label} recovered after {minutes} min.",
}

SMS_MAX_CHARS = 160
