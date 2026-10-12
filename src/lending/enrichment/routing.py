"""Dynamic pipeline routing (SPEC §4.4): the one function that decides FULL_MACHINE or NURTURE.

Address present AND target close date within 30 days (inclusive, Eastern calendar days) ->
FULL_MACHINE. A missing address, a missing date, or a date already in the past -> NURTURE. A past
date is not a target; treating it as one would send a stale lead to the closer first (default approved
2026-10-09, not yet confirmed by Josh).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

from config.lending_enrichment import FULL_MACHINE_MAX_DAYS, ROUTING_TZ
from src.lending.contracts import RoutingTag


@dataclass(frozen=True)
class RoutingDecision:
    tag: RoutingTag
    reason: str


def today_et(now: Optional[datetime] = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(ROUTING_TZ).date()


def route_lead(address: Optional[str], target_close_date: Optional[date], *, today: date) -> RoutingDecision:
    if not (address and address.strip()):
        return RoutingDecision(RoutingTag.NURTURE, "missing_address")
    if target_close_date is None:
        return RoutingDecision(RoutingTag.NURTURE, "missing_close_date")
    days = (target_close_date - today).days
    if days < 0:
        return RoutingDecision(RoutingTag.NURTURE, "close_date_in_past")
    if days > FULL_MACHINE_MAX_DAYS:
        return RoutingDecision(RoutingTag.NURTURE, "close_date_beyond_window")
    return RoutingDecision(RoutingTag.FULL_MACHINE, "address_and_close_date_within_window")
