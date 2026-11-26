"""R1 1.3 sweep wiring: the morning list's digest settings and drafts, and the 6:00-7:40 AM ET Clay pass.

All read their switches from settings once, at Cora's start (restart ``fa-lending-cora`` to change them):
- ``list_config``: the digest's list settings; the digest posts it while ``LENDING_MORNING_LIST_ENABLED`` is on.
- ``make_morning_drafts``: None unless the list and ``LENDING_MORNING_DRAFTS_ENABLED`` are on. Its own sweep step,
  every ``DRAFT_EVERY`` from the digest time to noon ET once today's digest is posted: it rebuilds the list and holds
  a draft for each eligible item that has none for today yet (key ``morning:{ET day}:{ref}``). A failure or a restart
  is picked up by the next pass, and nothing is drafted twice.
- ``make_morning_enrichment``: None unless the list is on, ``CLAY_API_KEY`` is set and ``LENDING_CLAY_DAILY_CAP`` > 0.
  It runs every ``ENRICH_EVERY`` from 6:00 to 7:40 AM ET: it builds the same list the digest will post and sends that
  list's gaps to Clay, so results that land by 7:40 are in the 7:47 post and later ones count the next day.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.settings import AppSettings
from src.lending.consent import text_consented_phones
from src.lending.cora_morning_digest import DIGEST_TIME, MorningListConfig
from src.lending.morning_list.clay import ClaySettings, LiveClayRoutines, enrich_gaps
from src.lending.morning_list.drafts import (
    DraftContext, DraftWriter, already_drafted, anthropic_writer, draft_key, morning_drafts, submit_all,
    touched_phones, voice_examples,
)
from src.lending.morning_list.items import build
from src.lending.morning_list.read import LinkConfig, read_candidates
from src.lending.morning_list.time import ET

logger = logging.getLogger(__name__)

ENRICH_START = time(6, 0)
ENRICH_END = time(7, 40)
ENRICH_EVERY = timedelta(minutes=5)
DRAFT_EVERY = timedelta(minutes=15)
DRAFT_END = time(12, 0)
_POSTED = text("SELECT 1 FROM lending.cora_morning_digests WHERE digest_date = :day AND posted_at IS NOT NULL")


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


def make_morning_drafts(engine: Engine, settings: AppSettings, *, gate=None, messages_client=None,
                        approvals_channel: str = "", writer: Optional[DraftWriter] = None,
                        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
                        ) -> Optional[Callable[[], Any]]:
    if not (settings.lending_morning_list_enabled and settings.lending_morning_drafts_enabled):
        return None
    if gate is None or (writer is None and messages_client is None):
        logger.error("[morning-list] drafts are on but Cora has no send gate or model client; no drafts")
        return None
    writer = writer or anthropic_writer(messages_client, settings.lending_cora_model)
    config = list_config(settings)
    start = parse_time(settings.lending_morning_digest_time)
    last_run: list[datetime] = []

    def drafts_step() -> Optional[int]:
        now = clock()
        local = now.astimezone(ET)
        if not start <= local.time() < DRAFT_END or (last_run and now - last_run[0] < DRAFT_EVERY):
            return None
        last_run[:] = [now]
        with Session(engine) as db, db.begin():
            if db.execute(_POSTED, {"day": local.date()}).first() is None:
                return None
            candidates, _ = read_candidates(db, now, lead_days=config.lead_days, size=config.size,
                                            stuck_deals=config.stuck_deals)
            items = build(candidates, size=config.size)
            done = already_drafted(db, [draft_key(now, i) for i in items])
            items = [i for i in items if draft_key(now, i) not in done]
            phones = sorted({i.phone for i in items if i.phone})
            recent, open_drafts = touched_phones(db, phones, now)
            consented = text_consented_phones(db, phones)
            examples = voice_examples(db)
        drafts = morning_drafts(items, writer, DraftContext(
            now=now, text_consent=consented.__contains__, recently_contacted=recent, open_drafts=open_drafts,
            examples=examples))
        queued = submit_all(gate, drafts, approvals_channel=approvals_channel)
        if queued:
            logger.info("[morning-list] %s morning draft(s) held for approval", queued)
        return queued

    return drafts_step


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
