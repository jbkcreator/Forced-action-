"""Lending Wave 0 compliance floor (WP-W0-2 / WP-W0-3 / WP-W0-8).

- ``filter_loadable`` — list-build gate: "is this number clean to load?"
  Order (spec §3 pipeline): phone valid → Georgia entity stop → internal
  suppression → fresh Tracerfy scrub (national DNC, state DNC, litigator).
- ``can_dial_now`` — call-time gate: calling window + rolling attempt cap.
- ``propagate_opt_out`` / ``mirror_fa_opt_out`` — global stop-propagation.
- ``reconcile_suppression`` — idempotent catch-up of FA opt-outs into the
  lending suppression list (backfill + retry path for a failed mirror).

Batch only: one query per gate for the whole list. Never commits — the caller
owns the transaction.
"""
from __future__ import annotations

import hashlib
import json
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import (
    AREA_CODE_TZ,
    ATTEMPT_PERIOD_HOURS,
    CALL_WINDOW_END,
    CALL_WINDOW_START,
    DEFAULT_TZ,
    DNC_SCRUB_MAX_AGE_DAYS,
    GEORGIA_ALLOWED_ENTITY_TYPES,
    MAX_ATTEMPTS_PER_PERIOD,
    NON_OPT_OUT_SOURCES,
    SMS_OPT_OUT_SOURCES,
    STOP_PROPAGATION_SLA_SECONDS,
    TRACERFY_DNC_SOURCE,
    OptOutChannel,
    OptOutStatus,
    ReasonCode,
    SuppressionReason,
)
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import get_logger

logger = get_logger(__name__)

Scrubber = Callable[[list[str]], list[dict]]
DialerRemover = Callable[[str], None]


@dataclass(frozen=True)
class GateResult:
    phone: str
    allowed: bool
    reason: Optional[ReasonCode] = None


@dataclass(frozen=True)
class ScrubResult:
    national_dnc: bool
    litigator: bool
    state_dnc: bool
    checked_at: datetime
    line_type: Optional[str] = None


def _blocked(phone: str, reason: ReasonCode) -> GateResult:
    return GateResult(phone=phone, allowed=False, reason=reason)


def _is_yes(value: Optional[str]) -> bool:
    return (value or "").strip().upper() in ("Y", "YES", "TRUE", "1")


def phone_hash(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()


# ── List-build filter (WP-W0-2 + Georgia stop) ───────────────────────────────


def _georgia_blocked(record: dict) -> bool:
    if (record.get("state") or "").strip().upper() != "GA":
        return False
    return (record.get("entity_status") or "").strip().upper() not in GEORGIA_ALLOWED_ENTITY_TYPES


def _suppressed_phones(db, phones: list[str]) -> set[str]:
    rows = db.execute(
        text(
            "SELECT phone FROM lending.suppression_list WHERE phone = ANY(:phones) "
            "UNION "
            "SELECT phone FROM sms_opt_outs WHERE phone = ANY(:phones) AND source <> :tracerfy"
        ),
        {"phones": phones, "tracerfy": TRACERFY_DNC_SOURCE},
    ).fetchall()
    return {r[0] for r in rows}


def _stored_scrubs(db, phones: list[str]) -> dict[str, ScrubResult]:
    rows = db.execute(
        text(
            "SELECT phone, national_dnc, litigator, checked_at, "
            "raw_result->>'state_dnc', raw_result->>'phone_type' "
            "FROM dnc_phone_checks WHERE phone = ANY(:phones)"
        ),
        {"phones": phones},
    ).fetchall()
    return {r[0]: ScrubResult(r[1], r[2], _is_yes(r[4]), r[3], r[5]) for r in rows}


def _verdict(phone: str, scrub: ScrubResult) -> GateResult:
    if scrub.litigator:
        return _blocked(phone, ReasonCode.LITIGATOR)
    if scrub.national_dnc:
        return _blocked(phone, ReasonCode.NATIONAL_DNC)
    if scrub.state_dnc:
        return _blocked(phone, ReasonCode.STATE_DNC)
    return GateResult(phone=phone, allowed=True)


def filter_loadable(
    records: list[dict],
    db,
    *,
    now: Optional[datetime] = None,
    scrubber: Scrubber,
    run_id: Optional[str] = None,
) -> list[GateResult]:
    """One result per input record, same order. With ``run_id``, every block is
    written to lending.load_exclusions (A2 evidence)."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)

    early: dict[int, GateResult] = {}
    phones: list[Optional[str]] = []
    for i, record in enumerate(records):
        raw = record.get("normalized_phone") or record.get("phone") or ""
        phone = normalize_phone(raw)
        phones.append(phone)
        if not phone:
            early[i] = _blocked(raw, ReasonCode.INVALID_PHONE)
        elif _georgia_blocked(record):
            early[i] = _blocked(phone, ReasonCode.GA_NATURAL_PERSON)

    candidates = sorted({p for i, p in enumerate(phones) if p and i not in early})
    by_phone: dict[str, GateResult] = {}

    suppressed = _suppressed_phones(db, candidates) if candidates else set()
    by_phone.update({p: _blocked(p, ReasonCode.SUPPRESSED) for p in suppressed})
    candidates = [p for p in candidates if p not in suppressed]

    scrubs = _stored_scrubs(db, candidates) if candidates else {}
    fresh = {p: s for p, s in scrubs.items() if s.checked_at >= cutoff}
    stale = [p for p in candidates if p not in fresh]
    if stale:
        fresh.update(_scrub(db, stale, scrubber))
    for phone in candidates:
        scrub = fresh.get(phone)
        by_phone[phone] = _verdict(phone, scrub) if scrub else _blocked(phone, ReasonCode.SCRUB_FAILED)

    _stamp_contacts(db, fresh)
    results = [early.get(i) or by_phone[phones[i]] for i in range(len(records))]
    if run_id:
        _record_exclusions(db, run_id, results)
    return results


def _scrub(db, phones: list[str], scrubber: Scrubber) -> dict[str, ScrubResult]:
    """One Tracerfy batch for every stale/missing phone. Unreturned phones are absent."""
    try:
        rows = scrubber(phones)
    except Exception as exc:
        logger.error("[lending-compliance] Tracerfy scrub failed for %d phones: %s", len(phones), exc)
        return {}

    wanted = set(phones)
    checked_at = datetime.now(timezone.utc)
    scrubbed: dict[str, ScrubResult] = {}
    raw_by_phone: dict[str, dict] = {}
    for row in rows:
        phone = normalize_phone(str(row.get("phone") or "").strip())
        if phone not in wanted:
            continue
        scrubbed[phone] = ScrubResult(
            national_dnc=_is_yes(row.get("national_dnc")),
            litigator=_is_yes(row.get("litigator")),
            state_dnc=_is_yes(row.get("state_dnc")),
            checked_at=checked_at,
            line_type=row.get("phone_type") or None,
        )
        raw_by_phone[phone] = row
    if not scrubbed:
        return {}

    db.execute(
        text(
            "INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source, raw_result) "
            "VALUES (:phone, :national_dnc, :litigator, :checked_at, :source, CAST(:raw_result AS jsonb)) "
            "ON CONFLICT (phone) DO UPDATE SET national_dnc = EXCLUDED.national_dnc, "
            "litigator = EXCLUDED.litigator, checked_at = EXCLUDED.checked_at, "
            "source = EXCLUDED.source, raw_result = EXCLUDED.raw_result"
        ),
        [
            {
                "phone": p,
                "national_dnc": s.national_dnc,
                "litigator": s.litigator,
                "checked_at": s.checked_at,
                "source": TRACERFY_DNC_SOURCE,
                "raw_result": json.dumps(raw_by_phone[p]),
            }
            for p, s in scrubbed.items()
        ],
    )
    litigators = [p for p, s in scrubbed.items() if s.litigator]
    if litigators:
        db.execute(
            text(
                "INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                "SELECT unnest(CAST(:phones AS varchar[])), :reason, 'tracerfy_scrub' "
                "ON CONFLICT (phone) DO NOTHING"
            ),
            {"phones": litigators, "reason": SuppressionReason.LITIGATOR.value},
        )
    return scrubbed


def _stamp_contacts(db, scrubs: dict[str, ScrubResult]) -> None:
    """Every scrubbed number carries its scrub time (spec §4.2) and line type."""
    if not scrubs:
        return
    phones = list(scrubs)
    db.execute(
        text(
            "INSERT INTO lending.contacts (phone, last_dnc_scrub, line_type) "
            "SELECT * FROM unnest(CAST(:phones AS varchar[]), CAST(:checked AS timestamptz[]), "
            "CAST(:line_types AS varchar[])) "
            "ON CONFLICT (phone) DO UPDATE SET last_dnc_scrub = EXCLUDED.last_dnc_scrub, "
            "line_type = COALESCE(EXCLUDED.line_type, lending.contacts.line_type)"
        ),
        {
            "phones": phones,
            "checked": [scrubs[p].checked_at for p in phones],
            "line_types": [scrubs[p].line_type for p in phones],
        },
    )


def _record_exclusions(db, run_id: str, results: list[GateResult]) -> None:
    blocked = [r for r in results if not r.allowed]
    if not blocked:
        return
    db.execute(
        text(
            "INSERT INTO lending.load_exclusions (run_id, phone_hash, reason) "
            "VALUES (:run_id, :phone_hash, :reason)"
        ),
        [{"run_id": run_id, "phone_hash": phone_hash(r.phone), "reason": r.reason.value} for r in blocked],
    )


def tracerfy_scrub(phones: list[str]) -> list[dict]:
    """Live Tracerfy DNC batch via FA's existing submit/poll helpers (ADR 0002)."""
    from config.settings import get_settings
    from src.tasks.dnc_refresh import _poll_queue, _submit_scrub_batch

    key = get_settings().tracerfy_api_key
    if not key:
        raise RuntimeError("TRACERFY_API_KEY not configured")
    api_key = key.get_secret_value()
    return _poll_queue(_submit_scrub_batch(phones, api_key), api_key)


# ── Call-time gate (WP-W0-3) ─────────────────────────────────────────────────


def recipient_timezone(phone: str, zip_code: Optional[str] = None) -> ZoneInfo:
    """Wave 0 pools are Hillsborough/Pinellas: a ZIP in either is Eastern.
    Otherwise area code, defaulting to Eastern."""
    if zip_code:
        from src.utils.zip_centroids import get_zip_centroid

        if get_zip_centroid(zip_code, "hillsborough") or get_zip_centroid(zip_code, "pinellas"):
            return ZoneInfo(DEFAULT_TZ)
    digits = "".join(c for c in phone if c.isdigit())
    if digits.startswith("1"):
        digits = digits[1:]
    return ZoneInfo(AREA_CODE_TZ.get(digits[:3], DEFAULT_TZ))


def can_dial_now(
    phone: str,
    db,
    *,
    now: Optional[datetime] = None,
    zip_code: Optional[str] = None,
) -> GateResult:
    """Recipient-local calling window, then the rolling attempt cap.

    Attempts are every row in lending.call_dispositions (one per Aircall call.ended,
    with or without a disposition) — owned by WP-W0-6.
    """
    now = now or datetime.now(timezone.utc)
    normalized = normalize_phone(phone)
    if not normalized:
        return _blocked(phone, ReasonCode.INVALID_PHONE)

    local = now.astimezone(recipient_timezone(normalized, zip_code)).time()
    if not (CALL_WINDOW_START <= local < CALL_WINDOW_END):
        return _blocked(normalized, ReasonCode.OUTSIDE_CALL_WINDOW)

    attempts = db.execute(
        text(
            "SELECT count(*) FROM lending.call_dispositions "
            "WHERE phone = :phone AND call_ended_at > :since AND call_ended_at <= :now"
        ),
        {"phone": normalized, "since": now - timedelta(hours=ATTEMPT_PERIOD_HOURS), "now": now},
    ).scalar()
    if attempts >= MAX_ATTEMPTS_PER_PERIOD:
        return _blocked(normalized, ReasonCode.ATTEMPT_CAP_REACHED)
    return GateResult(phone=normalized, allowed=True)


# ── Global stop-propagation (WP-W0-8, spec §3.2) ─────────────────────────────
# Every FA opt-out path (SMS STOP, email unsubscribe, Cora/concierge replies, IVR)
# ends in email_suppression.suppress_contact, which calls mirror_fa_opt_out once the
# FA stores are written. The dialer's verbal decline enters via propagate_opt_out,
# which passes its call context down through _DIALER_CTX (suppress_contact's
# signature is shared FA code and stays unchanged).


@dataclass
class _DialerContext:
    source_ref: Optional[str]
    actor: Optional[str]
    dialer_remover: Optional[DialerRemover]
    event_id: Optional[int] = None


_DIALER_CTX: ContextVar[Optional[_DialerContext]] = ContextVar("lending_dialer_ctx", default=None)


def _default_dialer_remover() -> Optional[DialerRemover]:
    """Aircall pool removal is provided by the WP-W0-5 client once it exists."""
    from src.services import aircall_client

    return getattr(aircall_client, "remove_contact_from_pool", None)


def _channel_for(source: str) -> OptOutChannel:
    return OptOutChannel.SMS if source in SMS_OPT_OUT_SOURCES else OptOutChannel.EMAIL


def propagate_opt_out(
    db,
    *,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    source_ref: Optional[str] = None,
    actor: Optional[str] = None,
    dialer_remover: Optional[DialerRemover] = None,
) -> Optional[int]:
    """Verbal decline (DNC_REQUEST) entry point. Returns the opt_out_events id. Does not commit."""
    from src.services.email_suppression import suppress_contact

    ctx = _DialerContext(source_ref=source_ref, actor=actor, dialer_remover=dialer_remover)
    token = _DIALER_CTX.set(ctx)
    try:
        suppress_contact(db, email=email, phone=phone, source="lending_dialer")
    finally:
        _DIALER_CTX.reset(token)
    return ctx.event_id


def _already_suppressed(db, phone: Optional[str], email: Optional[str]) -> bool:
    return db.execute(
        text(
            "SELECT count(*) FROM lending.suppression_list "
            "WHERE (CAST(:phone AS varchar) IS NULL OR phone = :phone) "
            "AND (CAST(:email AS varchar) IS NULL OR email = :email) "
            "AND (phone IS NOT NULL OR email IS NOT NULL)"
        ),
        {"phone": phone, "email": email},
    ).scalar() > 0


def mirror_fa_opt_out(db, *, phone: Optional[str], email: Optional[str], source: str) -> Optional[int]:
    """Called by suppress_contact after the FA SMS/email stores are written."""
    if source in NON_OPT_OUT_SOURCES:
        return None
    phone = normalize_phone(phone) if phone else None
    email = email.strip().lower() if email else None
    if not phone and not email:
        return None

    ctx = _DIALER_CTX.get()
    if ctx is None and _already_suppressed(db, phone, email):
        return None  # repeat FA opt-out (e.g. second STOP): already propagated

    channel = OptOutChannel.DIALER if ctx else _channel_for(source)
    source_ref = ctx.source_ref if ctx else source
    received_at = datetime.now(timezone.utc)

    event_id = db.execute(
        text(
            "INSERT INTO lending.opt_out_events (channel, source_ref, phone_hash, actor, received_at) "
            "VALUES (:channel, :source_ref, :phash, :actor, :received_at) RETURNING id"
        ),
        {
            "channel": channel.value,
            "source_ref": source_ref,
            "phash": phone_hash(phone) if phone else None,
            "actor": ctx.actor if ctx else None,
            "received_at": received_at,
        },
    ).scalar()
    if ctx:
        ctx.event_id = event_id

    db.execute(
        text(
            "INSERT INTO lending.suppression_list (phone, email, reason, source_channel, source_ref) "
            "SELECT p, e, :reason, :channel, :source_ref FROM (VALUES "
            "(CAST(:phone AS varchar), CAST(NULL AS varchar)), (NULL, CAST(:email AS varchar))) v(p, e) "
            "WHERE p IS NOT NULL OR e IS NOT NULL "
            "ON CONFLICT DO NOTHING"
        ),
        {
            "reason": SuppressionReason.OPT_OUT.value,
            "channel": channel.value,
            "source_ref": source_ref,
            "phone": phone,
            "email": email,
        },
    )
    if phone:
        db.execute(
            text(
                "INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:phone, true) "
                "ON CONFLICT (phone) DO UPDATE SET do_not_contact = true"
            ),
            {"phone": phone},
        )
    stores_written_at = datetime.now(timezone.utc)

    dialer_removed_at = _remove_from_dialer(phone, ctx, event_id) if phone else None
    status = OptOutStatus.COMPLETE if (phone is None or dialer_removed_at) else OptOutStatus.DIALER_PENDING

    db.execute(
        text(
            "UPDATE lending.opt_out_events SET suppression_at = :stores_at, sms_at = :sms_at, "
            "email_at = :email_at, dialer_removed_at = :dialer_at, status = :status WHERE id = :id"
        ),
        {
            "stores_at": stores_written_at,
            # suppress_contact writes sms_opt_outs whenever it holds a phone and
            # email_opt_outs whenever it holds an email, before calling us.
            "sms_at": stores_written_at if phone else None,
            "email_at": stores_written_at if email else None,
            "dialer_at": dialer_removed_at,
            "status": status.value,
            "id": event_id,
        },
    )
    _log_propagation(event_id, channel, received_at, dialer_removed_at or stores_written_at, status)
    return event_id


def _remove_from_dialer(phone: str, ctx: Optional[_DialerContext], event_id: int) -> Optional[datetime]:
    remover = (ctx.dialer_remover if ctx else None) or _default_dialer_remover()
    if remover is None:
        return None
    try:
        remover(phone)
    except Exception as exc:
        logger.error("[lending-compliance] dialer removal failed event=%s: %s", event_id, exc)
        return None
    return datetime.now(timezone.utc)


def _log_propagation(event_id, channel, received_at, finished_at, status) -> None:
    seconds = (finished_at - received_at).total_seconds()
    level = logger.warning if seconds > STOP_PROPAGATION_SLA_SECONDS else logger.info
    level(
        "[lending-compliance] opt-out event=%s channel=%s status=%s propagated_in=%.3fs (sla=%ss)",
        event_id, channel.value, status.value, seconds, STOP_PROPAGATION_SLA_SECONDS,
    )


def reconcile_suppression(db, source_schema: str = "public", target_schema: str = "lending") -> int:
    """Copy FA opt-outs (and Tracerfy litigators) missing from lending.suppression_list.

    Idempotent. Used by the migration backfill and as the retry path when
    mirror_fa_opt_out fails inside suppress_contact's savepoint. Phones are
    normalized in Python (phone_utils rule) before insert. Returns rows added.
    """
    s, t = source_schema, target_schema
    excluded = sorted(NON_OPT_OUT_SOURCES | {TRACERFY_DNC_SOURCE})
    sms = db.execute(
        text(f'SELECT phone FROM "{s}".sms_opt_outs WHERE source <> ALL(:excluded)'),
        {"excluded": excluded},
    ).fetchall()
    emails = db.execute(
        text(f'SELECT email FROM "{s}".email_opt_outs WHERE source <> ALL(:excluded)'),
        {"excluded": excluded},
    ).fetchall()
    litigators = db.execute(text(f'SELECT phone FROM "{s}".dnc_phone_checks WHERE litigator')).fetchall()

    rows: list[dict] = []
    seen_phones: set[str] = set()
    for phones, reason, channel in (
        (sms, SuppressionReason.OPT_OUT, "backfill:sms_opt_outs"),
        (litigators, SuppressionReason.LITIGATOR, "backfill:dnc_phone_checks"),
    ):
        for (raw,) in phones:
            phone = normalize_phone(raw)
            if phone and phone not in seen_phones:
                seen_phones.add(phone)
                rows.append({"phone": phone, "email": None, "reason": reason.value, "channel": channel})
    for email in {(e or "").strip().lower() for (e,) in emails} - {""}:
        rows.append({"phone": None, "email": email, "reason": SuppressionReason.OPT_OUT.value,
                     "channel": "backfill:email_opt_outs"})
    if not rows:
        return 0

    before = db.execute(text(f'SELECT count(*) FROM "{t}".suppression_list')).scalar()
    db.execute(
        text(
            f'INSERT INTO "{t}".suppression_list (phone, email, reason, source_channel) '
            "VALUES (:phone, :email, :reason, :channel) ON CONFLICT DO NOTHING"
        ),
        rows,
    )
    added = db.execute(text(f'SELECT count(*) FROM "{t}".suppression_list')).scalar() - before
    logger.info("[lending-compliance] reconcile_suppression added=%d", added)
    return added
