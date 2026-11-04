"""Inbound leads into their BatchDialer campaign, and out again once they are handled in GHL.

Two sources share this code (``LeadSource``): LendingFlow leads -> campaign "LendingFlow" (no consent check, no Tracerfy
scrub: client decision), and website leads -> campaign "Website" (only leads who ticked the call-consent box, with the
Tracerfy DNC scrub like every other list). What follows is written for LendingFlow and applies to both.

Loading runs from the 5-minute LendingFlow sweep, never inside the GHL delivery. A lead is loaded once it is in GHL,
is not suppressed, is older than LENDING_LENDINGFLOW_DIALER_DELAY_MINUTES (default 0: the next sweep) and has not been
handled in GHL. Client decision: LendingFlow leads need no consent certificate and get no Tracerfy DNC scrub
(``dnc_scrub=False``; the dialer sweep and the weekly rescrub skip them too, ``compliance.scrub_exempt_phones``).
Each lead goes through ``run_dialer_load`` on its own, so every other gate still applies (opt-out / suppression,
Georgia, Backflip, the attempt caps, the calling window), and a refused or shared-phone batch can never hold back
another lead. Right before the load the
lead's GHL calls are re-read live; if that read fails the lead is not loaded (fail closed).

``dialer_status``: pending -> loading -> loaded -> remove_pending -> removed. Off that path: blocked (a permanent
reason, in ``dialer_block_reason``), duplicate (another lead holds the phone, ``duplicate_of``), unconfirmed (the
create outcome is unknown: never pushed again until it is reconciled), expired (not loaded within
DIALER_MAX_LEAD_AGE_DAYS), failed (DIALER_MAX_FAILURES push errors) and preexisting (stored before this feature). A
temporary reason (calling hours, attempt cap, scrub, halt) keeps the lead pending and retries it after
DIALER_RETRY_MINUTES, so a lead that arrives at 8:30pm is loaded the next morning.

Handled in GHL = booked (``POST /webhooks/lending/ghl-appointment`` / ``booking-confirmed``) or a connected GHL call
(GHL_CONNECTED_OUTCOMES with at least HANDLED_MIN_TALK_SECONDS of talk). The BatchDialer contact is deleted
(RemovalReason.HANDLED_IN_GHL) and its load row deactivated. No ``dialer_holds`` row is opened, so the dialer sweep
never restores it. The loader and the webhook share no lock, and both orders end with the contact removed: the loader
commits the load row and only then re-reads ``handled_in_ghl_at``; the webhook commits the stamp and only then looks
for active load rows, so at least one of them sees the other. A failed removal stays ``remove_pending`` and every
sweep retries it.

One dialer step at a time: a transaction advisory lock (DIALER_LOCK_KEY) is held for the whole run.
"""
from __future__ import annotations

import logging
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_compliance import RemovalReason
from config.lending_dialer import CAMPAIGN_HOOKS, POOL_CAMPAIGN_TAGS
from config.lending_followup import CALL_SOURCE_GHL
from config.lending_lendingflow import (
    DIALER_BATCH_SIZE,
    DIALER_GHL_RECHECK_BATCH,
    DIALER_LOCK_KEY,
    DIALER_MAX_FAILURES,
    DIALER_MAX_LEAD_AGE_DAYS,
    DIALER_PERMANENT_REASONS,
    DIALER_POOL,
    DIALER_REF_PREFIX,
    DIALER_RETRY_MINUTES,
    DIALER_TEMPORARY_REASONS,
    GHL_CONNECTED_OUTCOMES,
    HANDLED_MIN_TALK_SECONDS,
    WEB_DIALER_LOCK_KEY,
    WEB_DIALER_POOL,
    WEB_DIALER_REF_PREFIX,
)
from config.settings import get_settings
from src.lending.compliance import Scrubber, phone_hash
from src.lending.dialer_display import NOT_AVAILABLE
from src.lending.dialer_load import CARD_TIMEZONE, DialerContacts, LoadRefused, LoadReport, run_dialer_load
from src.lending.ghl_call_import import CALL_DIRECTIONS, CallReader, ImportStats, _store_contact
from src.lending.global_halt import is_globally_halted
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

PENDING = "pending"
LOADING = "loading"
LOADED = "loaded"
BLOCKED = "blocked"
UNCONFIRMED = "unconfirmed"
REMOVE_PENDING = "remove_pending"
REMOVED = "removed"
FAILED = "failed"
_PUSH_FAILED = "push_failed"  # an outcome, not a status: pending again, or failed after DIALER_MAX_FAILURES

HANDLED_BOOKED = "booked"
HANDLED_CONNECTED = "ghl_call_connected"
CAMPAIGN = POOL_CAMPAIGN_TAGS[DIALER_POOL]
WEB_CAMPAIGN = POOL_CAMPAIGN_TAGS[WEB_DIALER_POOL]

SessionFactory = Callable[[], AbstractContextManager[Session]]


@dataclass(frozen=True)
class LeadSource:
    """One inbound lead table loaded into its own BatchDialer campaign."""
    label: str
    table: str                      # lending.<table>, with the dialer_* / handled_in_ghl_* columns
    ref_prefix: str                 # dialer_load_records.source_record_ref = prefix + the lead's ref key
    ref_key: str                    # lead column that makes the ref (lead_uuid / id)
    pool: str
    campaign: str
    lock_key: int
    dnc_scrub: bool                 # Tracerfy DNC / litigator scrub at load
    claim_filter: str               # extra SQL on the claim (e.g. the call-consent tick)
    blocked_filter: str             # SQL for a pending lead that can never load (reason in blocked_reason)
    blocked_reason: str
    returning: str                  # claim RETURNING columns
    enabled: Callable[[], bool]
    record: Callable[[Mapping[str, Any]], dict[str, Any]]


@dataclass
class DialerStepStats:
    skipped_locked: bool = False
    claimed: int = 0
    loaded: int = 0
    retry_later: int = 0
    blocked: int = 0
    unconfirmed: int = 0
    failed: int = 0
    handled: int = 0
    removed: int = 0
    remove_failed: int = 0
    ghl: ImportStats = field(default_factory=ImportStats)


def _default_sessions() -> AbstractContextManager[Session]:
    from src.lending.db import lending_session

    return lending_session()


def _ref(lead: Mapping[str, Any], source: Optional[LeadSource] = None) -> str:
    source = source or LENDINGFLOW
    return f"{source.ref_prefix}{lead[source.ref_key]}"


# --------------------------------------------------------------------------- the record the loader sees

def _card_text(value: Any) -> str:
    cleaned = " ".join(str(value).split()) if value is not None else ""
    return cleaned or NOT_AVAILABLE


def lendingflow_card(lead: Mapping[str, Any]) -> dict[str, str]:
    """The ISA's call card: who asked, for what, and when. ``prior_contact`` is added by the loader."""
    amount = lead.get("loan_amount_range") or (f"${lead['loan_amount']:,}" if lead.get("loan_amount") else None)
    requested: datetime = lead.get("submitted_at") or lead["received_at"]
    return {
        "first_name": _card_text(lead.get("first_name")),
        "campaign": CAMPAIGN,
        "hook": _card_text(CAMPAIGN_HOOKS.get(CAMPAIGN)),
        "loan": _card_text(amount),
        "loan_type": _card_text((lead.get("loan_type") or "").replace("_", " ")),
        "state": _card_text(lead.get("property_state")),
        "requested": f"{requested.astimezone(CARD_TIMEZONE):%Y-%m-%d}",
    }


def lendingflow_pool_record(lead: Mapping[str, Any]) -> dict[str, Any]:
    """A stored lead as a ``run_dialer_load`` record in the ``lendingflow`` pool."""
    name = " ".join(p for p in (lead.get("first_name"), lead.get("last_name")) if p) or None
    return {
        "source_record_ref": _ref(lead),
        "pool": DIALER_POOL,
        "phone": lead["phone"],
        "email": lead.get("email"),
        "borrower_name": name,
        "property_address": lead.get("property_address"),
        "estimated_loan_value": lead.get("loan_amount"),
        "state": lead.get("property_state"),
        "zip": lead.get("property_zip"),
        "card": lendingflow_card(lead),
    }


def web_lead_card(lead: Mapping[str, Any]) -> dict[str, str]:
    """The ISA's call card for a website lead: who asked, for what, and when."""
    name = (lead.get("name") or "").split()
    amount = f"${lead['loan_amount']:,}" if lead.get("loan_amount") else None
    return {
        "first_name": _card_text(name[0] if name else None),
        "campaign": WEB_CAMPAIGN,
        "hook": _card_text(CAMPAIGN_HOOKS.get(WEB_CAMPAIGN)),
        "loan": _card_text(amount),
        "loan_type": _card_text(lead.get("deal_type")),
        "state": _card_text(lead.get("property_state")),
        "requested": f"{lead['received_at'].astimezone(CARD_TIMEZONE):%Y-%m-%d}",
    }


def web_lead_pool_record(lead: Mapping[str, Any]) -> dict[str, Any]:
    """A stored website lead as a ``run_dialer_load`` record in the ``website`` pool."""
    return {
        "source_record_ref": _ref(lead, WEBSITE),
        "pool": WEB_DIALER_POOL,
        "phone": lead["phone"],
        "email": lead.get("email"),
        "borrower_name": lead.get("name"),
        "property_address": lead.get("property_address"),
        "estimated_loan_value": lead.get("loan_amount"),
        "state": lead.get("property_state"),
        "card": web_lead_card(lead),
    }


LENDINGFLOW = LeadSource(
    label="lendingflow-dialer", table="lending.lendingflow_leads", ref_prefix=DIALER_REF_PREFIX, ref_key="lead_uuid",
    pool=DIALER_POOL, campaign=CAMPAIGN, lock_key=DIALER_LOCK_KEY, dnc_scrub=False, claim_filter="",
    blocked_filter="suppressed", blocked_reason="SUPPRESSED",
    returning=("id, lead_uuid, phone, email, first_name, last_name, loan_amount, loan_amount_range, loan_type, "
               "property_state, property_zip, property_address, submitted_at, received_at, ghl_contact_id, "
               "dialer_failures"),
    enabled=lambda: get_settings().lending_lendingflow_dialer_enabled,
    record=lendingflow_pool_record,
)
WEBSITE = LeadSource(
    label="web-lead-dialer", table="lending.web_leads", ref_prefix=WEB_DIALER_REF_PREFIX, ref_key="id",
    pool=WEB_DIALER_POOL, campaign=WEB_CAMPAIGN, lock_key=WEB_DIALER_LOCK_KEY, dnc_scrub=True,
    claim_filter="AND call_consent", blocked_filter="(suppressed OR NOT call_consent)",
    blocked_reason="NO_CALL_CONSENT",
    returning=("id, phone, email, name, loan_amount, deal_type, property_state, property_address, received_at, "
               "ghl_contact_id, dialer_failures"),
    enabled=lambda: get_settings().lending_web_lead_dialer_enabled,
    record=web_lead_pool_record,
)
SOURCES: tuple[LeadSource, ...] = (LENDINGFLOW, WEBSITE)


# --------------------------------------------------------------------------- handled in GHL

_STAMP_HANDLED = """
    UPDATE {t}
       SET handled_in_ghl_at = :now, handled_in_ghl_reason = :why,
           dialer_status = CASE WHEN dialer_status = 'pending' THEN 'blocked'
                                WHEN dialer_status IN ('loaded', 'unconfirmed') THEN 'remove_pending'
                                ELSE dialer_status END,
           dialer_block_reason = CASE WHEN dialer_status = 'pending' THEN 'HANDLED_IN_GHL' ELSE dialer_block_reason END
     WHERE phone = :phone AND handled_in_ghl_at IS NULL
"""

_CONNECTED_IN_GHL = """
    SELECT DISTINCT l.phone FROM {t} l
     WHERE l.handled_in_ghl_at IS NULL AND l.dialer_status IN ('pending', 'loading', 'loaded', 'unconfirmed')
       {phone_filter}
       AND EXISTS (SELECT 1 FROM lending.call_dispositions d
                    WHERE d.phone = l.phone AND d.call_source = :ghl AND d.disposition_raw = ANY(:connected)
                      AND COALESCE(d.talk_duration_sec, 0) >= :min_talk AND d.call_started_at >= l.received_at)
"""

_ACTIVE_SOURCE_ROW = text(
    "SELECT EXISTS (SELECT 1 FROM lending.dialer_load_records "
    "WHERE active AND phone = :phone AND source_record_ref LIKE :prefix)"
)


def mark_handled_in_ghl(db: Session, phone: str, *, reason: str, now: Optional[datetime] = None,
                        sources: Optional[tuple[LeadSource, ...]] = None) -> int:
    """Stamp every unstamped lead on this phone (every source) as handled in GHL. A pending lead is blocked; a loaded
    (or unconfirmed) one becomes remove_pending; a lead being loaded keeps ``loading`` (its loader re-reads the stamp).
    Returns how many leads were stamped. Does not commit."""
    now = now or datetime.now(timezone.utc)
    return sum(db.execute(text(_STAMP_HANDLED.format(t=source.table)),
                          {"phone": phone, "why": reason, "now": now}).rowcount for source in sources or SOURCES)


def _connected_phones(db: Session, source: LeadSource, phone: Optional[str] = None) -> list[str]:
    sql = _CONNECTED_IN_GHL.format(t=source.table, phone_filter="AND l.phone = :phone" if phone else "")
    params: dict[str, Any] = {"ghl": CALL_SOURCE_GHL, "connected": list(GHL_CONNECTED_OUTCOMES),
                              "min_talk": HANDLED_MIN_TALK_SECONDS}
    if phone:
        params["phone"] = phone
    return [row[0] for row in db.execute(text(sql), params)]


def stamp_handled_from_calls(db: Session, now: datetime, *, phone: Optional[str] = None,
                             source: Optional[LeadSource] = None) -> int:
    """Leads with a connected GHL call stored since they arrived (by the 15-minute import or this module's live
    re-read) are stamped handled. Returns the number of leads stamped. Does not commit."""
    return sum(mark_handled_in_ghl(db, p, reason=HANDLED_CONNECTED, now=now)
               for p in _connected_phones(db, source or LENDINGFLOW, phone))


def remove_handled_phone(db: Session, dialer: DialerContacts, phone: str, now: Optional[datetime] = None) -> bool:
    """Delete the phone's BatchDialer contacts, then deactivate its load rows and mark its handled leads removed.
    On failure the leads stay remove_pending for the next sweep. Does not commit."""
    now = now or datetime.now(timezone.utc)
    try:
        dialer.remove(phone, reason=RemovalReason.HANDLED_IN_GHL.value)
    except Exception as exc:  # class only: dialer errors can carry request detail
        for source in SOURCES:
            db.execute(text(f"UPDATE {source.table} SET dialer_last_error = :err "
                            "WHERE phone = :phone AND dialer_status = 'remove_pending'"),
                       {"err": f"remove: {type(exc).__name__}", "phone": phone})
        logger.error("[lendingflow-dialer] handled-in-GHL removal failed lead_phone_hash=%s: %s",
                     _hash(phone), type(exc).__name__)
        return False
    db.execute(text("UPDATE lending.dialer_load_records SET active = false, deactivated_at = :now, "
                    "deactivation_reason = :reason WHERE active AND phone = :phone"),
               {"now": now, "reason": RemovalReason.HANDLED_IN_GHL.value, "phone": phone})
    for source in SOURCES:
        db.execute(text(f"UPDATE {source.table} SET dialer_status = 'removed', dialer_removed_at = :now, "
                        "dialer_last_error = NULL WHERE phone = :phone AND handled_in_ghl_at IS NOT NULL "
                        "AND dialer_status IN ('remove_pending', 'loaded')"),
                   {"now": now, "phone": phone})
    logger.info("[lendingflow-dialer] removed a lead handled in GHL from the dialer phone_hash=%s", _hash(phone))
    return True


def _needs_removal(db: Session, phone: str) -> bool:
    for source in SOURCES:
        if db.execute(_ACTIVE_SOURCE_ROW, {"phone": phone, "prefix": f"{source.ref_prefix}%"}).scalar():
            return True
        if db.execute(text(f"SELECT EXISTS (SELECT 1 FROM {source.table} "
                           "WHERE phone = :phone AND dialer_status = 'remove_pending')"), {"phone": phone}).scalar():
            return True
    return False


def handle_handled_in_ghl(phone: Optional[str], *, reason: str,
                          sessions: SessionFactory = _default_sessions,
                          dialer_factory: Optional[Callable[[], Optional[DialerContacts]]] = None) -> None:
    """Background task for the GHL webhooks: stamp the phone's leads (committed first), then delete the contact from
    BatchDialer when one is loaded. Never raises; whatever fails here the sweep finishes."""
    normalized = normalize(phone) if phone else None
    if not normalized:
        return
    try:
        with sessions() as db:
            mark_handled_in_ghl(db, normalized, reason=reason)
            db.commit()  # the stamp is visible before we look for load rows (see the module docstring)
            if not _needs_removal(db, normalized):
                return
            dialer = (dialer_factory or _live_dialer)()
            if dialer is None:
                logger.warning("[lendingflow-dialer] lead handled in GHL is still in the dialer: dialer not configured")
                return
            remove_handled_phone(db, dialer, normalized)
            db.commit()
    except Exception as exc:  # class only: SQL errors embed bound parameters (phones)
        logger.error("[lendingflow-dialer] handled-in-GHL processing failed: %s", type(exc).__name__)


def _remove_all_handled(db: Session, dialer: Optional[DialerContacts], now: datetime, stats: DialerStepStats,
                        source: LeadSource) -> None:
    phones = [row[0] for row in db.execute(text(f"""
        SELECT DISTINCT l.phone FROM {source.table} l
         WHERE l.handled_in_ghl_at IS NOT NULL
           AND (l.dialer_status = 'remove_pending'
                OR EXISTS (SELECT 1 FROM lending.dialer_load_records r
                            WHERE r.active AND r.phone = l.phone AND r.source_record_ref LIKE :prefix))
    """), {"prefix": f"{source.ref_prefix}%"})]
    if phones and dialer is None:
        logger.warning("[lendingflow-dialer] %d lead(s) handled in GHL wait for removal: dialer not configured",
                       len(phones))
        return
    for phone in phones:
        if remove_handled_phone(db, dialer, phone, now):
            stats.removed += 1
        else:
            stats.remove_failed += 1
        db.commit()


# --------------------------------------------------------------------------- housekeeping

def _housekeeping(db: Session, now: datetime, source: Optional[LeadSource] = None) -> None:
    """Recover leads a crashed run left ``loading`` (no run is live: we hold the lock), release unconfirmed leads whose
    create was reconciled, block leads that can never load, expire stale ones, then mark same-phone duplicates."""
    source = source or LENDINGFLOW
    t, ref, prefix = source.table, f"l.{source.ref_key}::text", source.ref_prefix
    db.execute(text(f"""
        UPDATE {t} l
           SET dialer_status = CASE
                 WHEN EXISTS (SELECT 1 FROM lending.dialer_unconfirmed_creates u
                               WHERE u.source_record_ref = :prefix || {ref} AND u.resolved_at IS NULL)
                   THEN 'unconfirmed'
                 WHEN EXISTS (SELECT 1 FROM lending.dialer_load_records r
                               WHERE r.active AND r.source_record_ref = :prefix || {ref}) THEN 'loaded'
                 ELSE 'pending' END
         WHERE dialer_status = 'loading'
    """), {"prefix": prefix})
    db.execute(text(f"""
        UPDATE {t} l SET dialer_status = 'pending'
         WHERE dialer_status = 'unconfirmed' AND NOT EXISTS (
               SELECT 1 FROM lending.dialer_unconfirmed_creates u
                WHERE u.source_record_ref = :prefix || {ref} AND u.resolved_at IS NULL)
    """), {"prefix": prefix})
    db.execute(text(f"""
        UPDATE {t}
           SET dialer_status = CASE WHEN received_at < :oldest THEN 'expired' ELSE 'blocked' END,
               dialer_block_reason = CASE WHEN received_at < :oldest THEN dialer_block_reason
                                          WHEN suppressed THEN 'SUPPRESSED' ELSE :reason END
         WHERE dialer_status = 'pending' AND ({source.blocked_filter} OR received_at < :oldest)
    """), {"oldest": now - timedelta(days=DIALER_MAX_LEAD_AGE_DAYS), "reason": source.blocked_reason})
    # One BatchDialer contact per phone: a pending lead is a duplicate when another lead on the phone is already in the
    # dialer path, or is an older loadable pending lead.
    db.execute(text(f"""
        UPDATE {t} l SET dialer_status = 'duplicate', duplicate_of = o.id
          FROM {t} o
         WHERE l.dialer_status = 'pending' AND o.phone = l.phone AND o.id <> l.id
           AND (o.dialer_status IN ('loading', 'loaded', 'unconfirmed', 'remove_pending', 'removed')
                OR (o.dialer_status = 'pending' AND (o.received_at, o.id) < (l.received_at, l.id)))
    """))


# --------------------------------------------------------------------------- loading

_CLAIM = """
    UPDATE {t} SET dialer_status = 'loading', dialer_claimed_at = :now,
           dialer_attempts = dialer_attempts + 1
     WHERE id IN (SELECT id FROM {t}
                   WHERE dialer_status = 'pending' AND handled_in_ghl_at IS NULL AND NOT suppressed
                     AND ghl_status IN ('synced', 'contact_only') AND ghl_contact_id IS NOT NULL {claim_filter}
                     AND received_at <= :ready_before
                     AND (dialer_next_attempt_at IS NULL OR dialer_next_attempt_at <= :now)
                   ORDER BY received_at LIMIT :limit FOR UPDATE SKIP LOCKED)
    RETURNING {returning}
"""


def _claim(db: Session, now: datetime, source: Optional[LeadSource] = None) -> list[dict[str, Any]]:
    source = source or LENDINGFLOW
    delay = timedelta(minutes=get_settings().lending_lendingflow_dialer_delay_minutes)
    sql = text(_CLAIM.format(t=source.table, claim_filter=source.claim_filter, returning=source.returning))
    rows = db.execute(sql, {"now": now, "ready_before": now - delay, "limit": DIALER_BATCH_SIZE}).mappings().all()
    return [dict(row) for row in rows]


def _classify(reason: str) -> str:
    if reason in DIALER_PERMANENT_REASONS:
        return BLOCKED
    if reason not in DIALER_TEMPORARY_REASONS:
        logger.warning("[lendingflow-dialer] unknown loader reason %s: kept pending", reason)
    return PENDING


def _unconfirmed(db: Session, ref: str) -> bool:
    return bool(db.execute(text("SELECT EXISTS (SELECT 1 FROM lending.dialer_unconfirmed_creates "
                                "WHERE source_record_ref = :ref AND resolved_at IS NULL)"), {"ref": ref}).scalar())


def _outcome(db: Session, report: LoadReport, ref: str) -> tuple[str, Optional[str]]:
    """(status, reason) for the single lead of a live ``run_dialer_load`` report."""
    if ref in report.loaded_refs:
        return LOADED, None
    if report.failed:
        error = str(report.failed[0].get("error") or "unknown")
        if error == "ContactFieldsNotSet":  # the contact is live in the dialer and tracked: dialable, card not set
            return LOADED, None
        if _unconfirmed(db, ref):
            return UNCONFIRMED, "CREATE_OUTCOME_UNKNOWN"
        return _PUSH_FAILED, error
    if report.halted_mid_run:
        return PENDING, "HALTED"
    if report.suppressed_mid_run:
        return BLOCKED, "SUPPRESSED"
    if report.excluded_by_reason:
        reason = next(iter(report.excluded_by_reason))
        return _classify(reason), reason
    return _classify("NOT_LOADED"), "NOT_LOADED"


def _finish(db: Session, lead: Mapping[str, Any], status: str, reason: Optional[str], now: datetime,
            source: Optional[LeadSource] = None) -> str:
    """Write the lead's outcome (only while it is still ``loading``, ours) and commit. Returns the stored status."""
    t = (source or LENDINGFLOW).table
    params: dict[str, Any] = {"id": lead["id"], "now": now, "reason": reason}
    if status == _PUSH_FAILED:
        status = FAILED if int(lead["dialer_failures"]) + 1 >= DIALER_MAX_FAILURES else PENDING
        params["err"] = (reason or "")[:200]
        db.execute(text(f"UPDATE {t} SET dialer_status = :status, dialer_failures = dialer_failures + 1, "
                        "dialer_last_error = :err, dialer_next_attempt_at = :retry WHERE id = :id AND dialer_status = 'loading'"),
                   {**params, "status": status, "retry": now + timedelta(minutes=DIALER_RETRY_MINUTES)})
    elif status == LOADED:
        db.execute(text(f"UPDATE {t} SET dialer_status = 'loaded', dialer_loaded_at = :now, "
                        "dialer_block_reason = NULL, dialer_last_error = NULL WHERE id = :id AND dialer_status = 'loading'"),
                   params)
    elif status == PENDING:
        db.execute(text(f"UPDATE {t} SET dialer_status = 'pending', dialer_block_reason = :reason, "
                        "dialer_next_attempt_at = :retry WHERE id = :id AND dialer_status = 'loading'"),
                   {**params, "retry": now + timedelta(minutes=DIALER_RETRY_MINUTES)})
    else:  # BLOCKED / UNCONFIRMED
        db.execute(text(f"UPDATE {t} SET dialer_status = :status, dialer_block_reason = :reason "
                        "WHERE id = :id AND dialer_status = 'loading'"), {**params, "status": status})
    db.commit()
    return status


def _refresh_ghl_calls(db: Session, read_calls: CallReader, lead: Mapping[str, Any], stats: DialerStepStats) -> bool:
    """Store the lead's GHL calls read live now. False when the read failed (the caller must not trust the DB)."""
    before = stats.ghl.lookup_failures
    _store_contact(db, read_calls, lead["ghl_contact_id"], lead["phone"], lead["received_at"], stats.ghl,
                   CALL_DIRECTIONS)
    db.commit()
    return stats.ghl.lookup_failures == before


def _in_other_campaign(db: Session, phone: str, source: LeadSource) -> bool:
    return bool(db.execute(text("SELECT EXISTS (SELECT 1 FROM lending.dialer_load_records "
                                "WHERE active AND phone = :phone AND campaign_tag IS DISTINCT FROM :campaign)"),
                           {"phone": phone, "campaign": source.campaign}).scalar())


def _load_one(db: Session, lead: dict[str, Any], *, dialer: DialerContacts, scrubber: Scrubber,
              read_calls: CallReader, now: datetime, stats: DialerStepStats, source: LeadSource) -> None:
    ref = _ref(lead, source)

    def finish(status: str, reason: Optional[str]) -> None:
        _count(stats, _finish(db, lead, status, reason, now, source))

    if not _refresh_ghl_calls(db, read_calls, lead, stats):
        finish(PENDING, "GHL_CHECK_FAILED")
        return
    if stamp_handled_from_calls(db, now, phone=lead["phone"], source=source):
        stats.handled += 1
        finish(BLOCKED, "HANDLED_IN_GHL")
        return
    if _in_other_campaign(db, lead["phone"], source):
        finish(BLOCKED, "IN_OTHER_CAMPAIGN")
        return
    if _unconfirmed(db, ref):
        finish(UNCONFIRMED, "CREATE_OUTCOME_UNKNOWN")
        return
    run_id = f"{source.pool}-{lead['id']}-{now:%Y%m%dT%H%M%S}"
    try:
        report = run_dialer_load([source.record(lead)], db, run_id=run_id, dry_run=False, scrubber=scrubber,
                                 dialer=dialer, commit=db.commit, now=now, dnc_scrub=source.dnc_scrub)
    except LoadRefused:
        db.rollback()
        finish(PENDING, "LOAD_REFUSED")
        return
    except Exception as exc:  # class only: SQL / HTTP errors can carry phone numbers
        db.rollback()
        logger.error("[%s] load crashed lead=%s: %s", source.label, lead["id"], type(exc).__name__)
        finish(_PUSH_FAILED, type(exc).__name__)
        return
    status, reason = _outcome(db, report, ref)
    stored = _finish(db, lead, status, reason, now, source)
    _count(stats, stored)
    if stored == LOADED:
        _remove_if_handled_meanwhile(db, dialer, lead, now, stats, source)


def _remove_if_handled_meanwhile(db: Session, dialer: DialerContacts, lead: Mapping[str, Any], now: datetime,
                                 stats: DialerStepStats, source: LeadSource) -> None:
    """The load row is committed; a stamp written while we were pushing is visible now (module docstring)."""
    handled = db.execute(text(f"SELECT handled_in_ghl_at FROM {source.table} WHERE id = :id"),
                         {"id": lead["id"]}).scalar()
    if handled is None:
        return
    db.execute(text(f"UPDATE {source.table} SET dialer_status = 'remove_pending' "
                    "WHERE id = :id AND dialer_status = 'loaded'"), {"id": lead["id"]})
    db.commit()
    if remove_handled_phone(db, dialer, lead["phone"], now):
        stats.removed += 1
    else:
        stats.remove_failed += 1
    db.commit()


def _count(stats: DialerStepStats, status: str) -> None:
    if status == LOADED:
        stats.loaded += 1
    elif status == PENDING:
        stats.retry_later += 1
    elif status == BLOCKED:
        stats.blocked += 1
    elif status == UNCONFIRMED:
        stats.unconfirmed += 1
    elif status == FAILED:
        stats.failed += 1


def _recheck_loaded_in_ghl(db: Session, read_calls: CallReader, now: datetime, stats: DialerStepStats,
                           source: LeadSource) -> None:
    """Re-read the GHL calls of loaded leads (least recently checked first), so a connected call reaches us within
    one sweep instead of the 15-minute import."""
    leads = db.execute(text(f"""
        SELECT id, phone, ghl_contact_id, received_at FROM {source.table}
         WHERE dialer_status = 'loaded' AND handled_in_ghl_at IS NULL AND ghl_contact_id IS NOT NULL
         ORDER BY dialer_ghl_checked_at NULLS FIRST, id LIMIT :limit
    """), {"limit": DIALER_GHL_RECHECK_BATCH}).mappings().all()
    for lead in leads:
        _store_contact(db, read_calls, lead["ghl_contact_id"], lead["phone"], lead["received_at"], stats.ghl,
                       CALL_DIRECTIONS)
        db.execute(text(f"UPDATE {source.table} SET dialer_ghl_checked_at = :now WHERE id = :id"),
                   {"now": now, "id": lead["id"]})
        db.commit()


# --------------------------------------------------------------------------- the sweep step

@contextmanager
def _single_run(sessions: SessionFactory, lock_key: int = DIALER_LOCK_KEY) -> Iterator[bool]:
    """Yields whether this run holds the dialer-step lock. The lock lives in its own never-committed transaction, so
    the work session's commits do not release it; leaving rolls it back and frees the lock."""
    with sessions() as lock_db:
        try:
            yield bool(lock_db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": lock_key}).scalar())
        finally:
            lock_db.rollback()


def run_dialer_step(*, dialer: Optional[DialerContacts], scrubber: Optional[Scrubber],
                    read_calls: Optional[CallReader], now: Optional[datetime] = None,
                    sessions: SessionFactory = _default_sessions,
                    single_run: Optional[Callable[[], AbstractContextManager[bool]]] = None,
                    source: Optional[LeadSource] = None) -> DialerStepStats:
    """One pass: housekeeping, handled-in-GHL detection and removals (always, so turning loading off never strands a
    lead that Josh has since handled), then loading (only with LENDING_LENDINGFLOW_DIALER_ENABLED, a dialer, a
    scrubber and a GHL call reader, and not while the emergency halt is on). Commits as it goes."""
    now = now or datetime.now(timezone.utc)
    source = source or LENDINGFLOW
    stats = DialerStepStats()
    with (single_run or (lambda: _single_run(sessions, source.lock_key)))() as holds_lock:
        if not holds_lock:
            stats.skipped_locked = True
            return stats
        with sessions() as db:
            _housekeeping(db, now, source)
            db.commit()
            if read_calls is not None:
                _recheck_loaded_in_ghl(db, read_calls, now, stats, source)
            stats.handled += stamp_handled_from_calls(db, now, source=source)
            db.commit()
            _remove_all_handled(db, dialer, now, stats, source)
            if not _may_load(dialer, scrubber, read_calls, source):
                return stats
            claimed = _claim(db, now, source)
            db.commit()
            stats.claimed = len(claimed)
            for lead in claimed:
                _load_one(db, lead, dialer=dialer, scrubber=scrubber, read_calls=read_calls, now=now, stats=stats,
                          source=source)
    return stats


def _may_load(dialer: Optional[DialerContacts], scrubber: Optional[Scrubber], read_calls: Optional[CallReader],
              source: Optional[LeadSource] = None) -> bool:
    if not (source or LENDINGFLOW).enabled():
        return False
    if dialer is None or scrubber is None or read_calls is None:
        logger.warning("[lendingflow-dialer] loading is on but the dialer, the scrubber or the GHL call reader is not "
                       "configured: nothing loaded")
        return False
    if is_globally_halted():
        logger.info("[lendingflow-dialer] emergency halt is on: nothing loaded")
        return False
    return True


def _live_dialer() -> Optional[DialerContacts]:
    from src.lending.dialer_port import get_dialer

    return get_dialer()


def run_from_settings(source: Optional[LeadSource] = None) -> DialerStepStats:
    """Sweep entry point with the live dialer, Tracerfy and GHL reader. Never raises."""
    source = source or LENDINGFLOW
    from src.lending.compliance import tracerfy_scrub
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_calls import call_messages_reader

    try:
        account = lending_ghl_account()
        stats = run_dialer_step(dialer=_live_dialer(), scrubber=tracerfy_scrub,
                                read_calls=call_messages_reader(account) if account is not None else None,
                                source=source)
    except Exception as exc:  # class only
        logger.error("[%s] step crashed: %s", source.label, type(exc).__name__)
        return DialerStepStats()
    logger.info("[%s] claimed=%d loaded=%d retry_later=%d blocked=%d unconfirmed=%d failed=%d "
                "handled=%d removed=%d remove_failed=%d ghl_lookup_failures=%d skipped_locked=%s",
                source.label, stats.claimed, stats.loaded, stats.retry_later, stats.blocked, stats.unconfirmed, stats.failed,
                stats.handled, stats.removed, stats.remove_failed, stats.ghl.lookup_failures, stats.skipped_locked)
    return stats


def _hash(phone: str) -> str:
    return phone_hash(phone)[:12]
