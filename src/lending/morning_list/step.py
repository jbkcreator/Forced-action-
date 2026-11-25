"""R1 1.3 sweep wiring: the morning list's digest settings and drafts, and the 6:00-7:40 AM ET Clay pass.

Both read their switches from settings once, at Cora's start (restart ``fa-lending-cora`` to change them):
- ``make_morning_list``: None while ``LENDING_MORNING_LIST_ENABLED`` is off (the digest stays as it was). With drafts on
  (``LENDING_MORNING_DRAFTS_ENABLED``) and a gate, ``on_listed`` holds each eligible item's draft for approval.
- ``make_morning_enrichment``: None unless the list is on, ``CLAY_API_KEY`` is set and ``LENDING_CLAY_DAILY_CAP`` > 0.
  It runs every ``ENRICH_EVERY`` from 6:00 to 7:40 AM ET: it builds the same list the digest will post and sends that
  list's gaps to Clay, so results that land by 7:40 are in the 7:47 post and later ones count the next day.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.settings import AppSettings
from src.lending.consent import text_consented_phones
from src.lending.cora_morning_digest import DIGEST_TIME, MorningListConfig
from src.lending.morning_list.clay import ClaySettings, LiveClayRoutines, enrich_gaps
from src.lending.morning_list.drafts import (
    DraftContext, anthropic_writer, morning_drafts, submit_all, touched_phones, voice_examples,
)
from src.lending.morning_list.items import ListItem, build
from src.lending.morning_list.read import LinkConfig, read_candidates
from src.lending.morning_list.time import ET

logger = logging.getLogger(__name__)

ENRICH_START = time(6, 0)
ENRICH_END = time(7, 40)
ENRICH_EVERY = timedelta(minutes=5)


@dataclass(frozen=True)
class MorningList:
    config: MorningListConfig
    on_listed: Optional[Callable[[list[ListItem]], Any]]


def parse_time(value: str, default: time = DIGEST_TIME) -> time:
    try:
        hours, minutes = (int(part) for part in value.strip().split(":"))
        return time(hours, minutes)
    except (ValueError, AttributeError):
        logger.error("[morning-list] LENDING_MORNING_DIGEST_TIME %r is not HH:MM; using %s", value, default)
        return default


def _stuck_deals(db: Session, now: datetime):
    from src.lending.closing.blockers import stuck_deals

    return stuck_deals(db, now)


def list_config(settings: AppSettings) -> MorningListConfig:
    return MorningListConfig(size=settings.lending_morning_list_size, lead_days=settings.lending_morning_list_lead_days,
                             david_user_id=settings.lending_david_slack_user_id, stuck_deals=_stuck_deals,
                             links=LinkConfig(ghl_app_url=settings.lending_ghl_app_url,
                                              ghl_location_id=settings.lending_ghl_location_id or ""))


def make_morning_list(engine: Engine, settings: AppSettings, *, gate=None, messages_client=None,
                      approvals_channel: str = "") -> Optional[MorningList]:
    if not settings.lending_morning_list_enabled:
        return None
    on_listed = None
    if settings.lending_morning_drafts_enabled and gate is not None and messages_client is not None:
        writer = anthropic_writer(messages_client, settings.lending_cora_model)

        def on_listed(items: list[ListItem]) -> int:
            now = datetime.now(timezone.utc)
            phones = sorted({i.phone for i in items if i.phone})
            with Session(engine) as db, db.begin():
                recent, open_drafts = touched_phones(db, phones, now)
                consented = text_consented_phones(db, phones)
                examples = voice_examples(db)
            drafts = morning_drafts(items, writer, DraftContext(
                now=now, text_consent=consented.__contains__, recently_contacted=recent, open_drafts=open_drafts,
                examples=examples))
            queued = submit_all(gate, drafts, approvals_channel=approvals_channel)
            logger.info("[morning-list] %s morning draft(s) held for approval", queued)
            return queued
    elif settings.lending_morning_drafts_enabled:
        logger.error("[morning-list] drafts are on but Cora has no send gate or model client; no drafts")
    return MorningList(config=list_config(settings), on_listed=on_listed)


def clay_settings(settings: AppSettings) -> ClaySettings:
    return ClaySettings(email_routine=settings.lending_clay_email_routine,
                        llc_routine=settings.lending_clay_llc_routine,
                        email_enabled=settings.lending_clay_email_enabled,
                        llc_enabled=settings.lending_clay_llc_enabled,
                        daily_cap=settings.lending_clay_daily_cap, ttl_days=settings.lending_clay_ttl_days)


def make_morning_enrichment(engine: Engine, settings, *, port=None,
                            clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
                            ) -> Optional[Callable[[], Any]]:
    key = settings.clay_api_key.get_secret_value() if settings.clay_api_key else ""
    if not (settings.lending_morning_list_enabled and settings.lending_clay_daily_cap > 0 and (key or port)):
        return None
    port = port or LiveClayRoutines(key, settings.lending_clay_api_base)
    config = list_config(settings)
    clay = clay_settings(settings)
    last_run: list[datetime] = []

    def enrich() -> Any:
        now = clock()
        if not ENRICH_START <= now.astimezone(ET).time() < ENRICH_END:
            return None
        if last_run and now - last_run[0] < ENRICH_EVERY:
            return None
        last_run[:] = [now]
        with Session(engine) as db, db.begin():
            candidates, _ = read_candidates(db, now, lead_days=config.lead_days, size=config.size,
                                            stuck_deals=config.stuck_deals)
            return enrich_gaps(db, build(candidates, size=config.size), port, clay, now=now)

    return enrich
