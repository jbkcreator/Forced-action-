"""Lending Wave 0 list-build filter — "is this number clean to load?" (WP-W0-2).

Gate order (spec §3 pipeline): phone valid → Georgia entity stop → internal
suppression → fresh Tracerfy scrub. Batch only: one query per gate for the whole
list. Never commits — the caller owns the transaction.
"""
from __future__ import annotations

import hashlib
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from sqlalchemy import text

from config.lending_compliance import (
    ATTEMPT_PERIOD_HOURS,
    CALL_WINDOW_END,
    CALL_WINDOW_START,
    DNC_SCRUB_MAX_AGE_DAYS,
    GEORGIA_ALLOWED_ENTITY_TYPES,
    MAX_ATTEMPTS_PER_PERIOD,
    ReasonCode,
)
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import get_logger

logger = get_logger(__name__)

Scrubber = Callable[[list[str]], list[dict]]


@dataclass(frozen=True)
class GateResult:
    phone: str
    allowed: bool
    reason: Optional[ReasonCode] = None


def _blocked(phone: str, reason: ReasonCode) -> GateResult:
    return GateResult(phone=phone, allowed=False, reason=reason)


def _georgia_blocked(record: dict) -> bool:
    if (record.get("state") or "").strip().upper() != "GA":
        return False
    return (record.get("entity_status") or "").strip().upper() not in GEORGIA_ALLOWED_ENTITY_TYPES


def _suppressed_phones(db, phones: list[str]) -> set[str]:
    rows = db.execute(
        text(
            "SELECT phone FROM lending.suppression_list WHERE phone = ANY(:phones) "
            "UNION "
            "SELECT phone FROM sms_opt_outs "
            "WHERE phone = ANY(:phones) AND source <> 'tracerfy_dnc_refresh'"
        ),
        {"phones": phones},
    ).fetchall()
    return {r[0] for r in rows}


def _dnc_checks(db, phones: list[str]) -> dict[str, dict]:
    rows = db.execute(
        text("SELECT phone, national_dnc, litigator, checked_at, raw_result->>'state_dnc' "
            "FROM dnc_phone_checks WHERE phone = ANY(:phones)"),
        {"phones": phones},
    ).fetchall()
    return {r[0]: {"national_dnc": r[1], "litigator": r[2], "checked_at": r[3], "state_dnc": _is_yes(r[4])} for r in rows}


def _verdict(phone: str, check: dict) -> GateResult:
    if check["litigator"]:
        return _blocked(phone, ReasonCode.LITIGATOR)
    if check["national_dnc"]:
        return _blocked(phone, ReasonCode.NATIONAL_DNC)
    if check.get("state_dnc"):
        return _blocked(phone, ReasonCode.STATE_DNC)
    return GateResult(phone=phone, allowed=True)


def filter_loadable(
    records: list[dict],
    db,
    *,
    now: Optional[datetime] = None,
    scrubber: Scrubber,
) -> list[GateResult]:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)

    results: dict[str, GateResult] = {}
    candidates: list[str] = []
    for record in records:
        raw = record.get("phone") or ""
        phone = normalize_phone(raw)
        if not phone:
            results[raw] = _blocked(raw, ReasonCode.INVALID_PHONE)
        elif _georgia_blocked(record):
            results[phone] = _blocked(phone, ReasonCode.GA_NATURAL_PERSON)
        else:
            candidates.append(phone)

    suppressed = _suppressed_phones(db, candidates) if candidates else set()
    for phone in candidates:
        if phone in suppressed:
            results[phone] = _blocked(phone, ReasonCode.SUPPRESSED)
    candidates = [p for p in candidates if p not in suppressed]

    checks = _dnc_checks(db, candidates) if candidates else {}
    stale: list[str] = []
    for phone in candidates:
        check = checks.get(phone)
        if check and check["checked_at"] >= cutoff:
            results[phone] = _verdict(phone, check)
        else:
            stale.append(phone)

    if stale:
        results.update(_scrub_and_judge(db, stale, scrubber))

    return list(results.values())


def _is_yes(value: Optional[str]) -> bool:
    return (value or "").strip().upper() in ("Y", "YES", "TRUE", "1")


def _scrub_and_judge(db, phones: list[str], scrubber: Scrubber) -> dict[str, GateResult]:
    """One Tracerfy batch for every stale/missing phone; a phone with no result is never loadable."""
    from src.tasks.dnc_refresh import _upsert_dnc_phone_check

    try:
        rows = scrubber(phones)
    except Exception as exc:
        logger.error("[lending-compliance] Tracerfy scrub failed for %d phones: %s", len(phones), exc)
        return {p: _blocked(p, ReasonCode.SCRUB_FAILED) for p in phones}

    judged: dict[str, GateResult] = {}
    scrubbed: list[str] = []
    for row in rows:
        phone = normalize_phone(str(row.get("phone") or "").strip())
        if phone not in phones:
            continue
        national_dnc, litigator = _is_yes(row.get("national_dnc")), _is_yes(row.get("litigator"))
        _upsert_dnc_phone_check(db, phone, national_dnc, litigator, row)
        scrubbed.append(phone)
        if litigator:
            db.execute(
                text(
                    "INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:phone, 'LITIGATOR', 'tracerfy_scrub') ON CONFLICT (phone) DO NOTHING"
                ),
                {"phone": phone},
            )
        judged[phone] = _verdict(
            phone,
            {"national_dnc": national_dnc, "litigator": litigator, "state_dnc": _is_yes(row.get("state_dnc"))},
        )

    if scrubbed:
        db.execute(
            text(
                "INSERT INTO lending.contacts (phone, last_dnc_scrub) "
                "SELECT unnest(:phones), now() "
                "ON CONFLICT (phone) DO UPDATE SET last_dnc_scrub = EXCLUDED.last_dnc_scrub"
            ),
            {"phones": scrubbed},
        )

    for phone in phones:
        judged.setdefault(phone, _blocked(phone, ReasonCode.SCRUB_FAILED))
    return judged


def tracerfy_scrub(phones: list[str]) -> list[dict]:
    """Live Tracerfy DNC batch via FA's existing submit/poll helpers (ADR 0002)."""
    from config.settings import get_settings
    from src.tasks.dnc_refresh import _poll_queue, _submit_scrub_batch

    key = get_settings().tracerfy_api_key
    if not key:
        raise RuntimeError("TRACERFY_API_KEY not configured")
    api_key = key.get_secret_value()
    return _poll_queue(_submit_scrub_batch(phones, api_key), api_key)


def can_dial_now(
    phone: str,
    db,
    *,
    now: Optional[datetime] = None,
    zip_code: Optional[str] = None,
) -> GateResult:
    """Call-time gate: recipient-local calling window, then the rolling attempt cap.

    Attempts are every row in lending.call_dispositions (one per Aircall call.ended,
    with or without a disposition) — owned by WP-W0-6.
    """
    from src.services.compliance_gator import _resolve_timezone

    now = now or datetime.now(timezone.utc)
    normalized = normalize_phone(phone)
    if not normalized:
        return _blocked(phone, ReasonCode.INVALID_PHONE)

    local = now.astimezone(_resolve_timezone(normalized, zip_code)).time()
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
# FA stores are written. The dialer's verbal decline enters via propagate_opt_out.

DialerRemover = Callable[[str], None]

_OPT_OUT_CTX: ContextVar[Optional[dict]] = ContextVar("lending_opt_out_ctx", default=None)
_SMS_SOURCES = ("inbound_sms", "twilio_inbound", "cascaded_from_sms", "synthflow_ivr_optout")


def phone_hash(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()


def _default_dialer_remover() -> Optional[DialerRemover]:
    """Aircall pool removal is provided by the WP-W0-5 client once it exists."""
    from src.services import aircall_client

    return getattr(aircall_client, "remove_contact_from_pool", None)


def _infer_channel(source: str) -> str:
    return "sms" if source in _SMS_SOURCES or "sms" in source else "email"


def propagate_opt_out(
    db,
    *,
    channel: str,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    source_ref: Optional[str] = None,
    actor: Optional[str] = None,
    dialer_remover: Optional[DialerRemover] = None,
) -> Optional[int]:
    """Entry point for opt-outs the FA handlers do not see (e.g. DNC_REQUEST). Does not commit."""
    from src.services.email_suppression import suppress_contact

    ctx = {"channel": channel, "source_ref": source_ref, "actor": actor, "dialer_remover": dialer_remover}
    token = _OPT_OUT_CTX.set(ctx)
    try:
        suppress_contact(db, email=email, phone=phone, source=f"lending_{channel}"[:30])
    finally:
        _OPT_OUT_CTX.reset(token)
    return ctx.get("event_id")


def mirror_fa_opt_out(db, *, phone: Optional[str], email: Optional[str], source: str) -> None:
    ctx = _OPT_OUT_CTX.get() or {}
    channel = ctx.get("channel") or _infer_channel(source)
    received_at = datetime.now(timezone.utc)
    params = {"phone": phone, "email": email, "channel": channel}

    event_id = db.execute(
        text(
            "INSERT INTO lending.opt_out_events (channel, source_ref, phone_hash, actor, received_at) "
            "VALUES (:channel, :source_ref, :phash, :actor, :received_at) RETURNING id"
        ),
        {
            "channel": channel,
            "source_ref": ctx.get("source_ref"),
            "phash": phone_hash(phone) if phone else None,
            "actor": ctx.get("actor"),
            "received_at": received_at,
        },
    ).scalar()
    ctx["event_id"] = event_id

    for column, value in (("phone", phone), ("email", email)):
        if value:
            db.execute(
                text(
                    f"INSERT INTO lending.suppression_list ({column}, reason, source_channel, source_ref) "
                    f"VALUES (:v, 'OPT_OUT', :channel, :source_ref) ON CONFLICT ({column}) DO NOTHING"
                ),
                {"v": value, "channel": channel, "source_ref": ctx.get("source_ref")},
            )
    if phone:
        db.execute(
            text(
                "INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:phone, true) "
                "ON CONFLICT (phone) DO UPDATE SET do_not_contact = true"
            ),
            {"phone": phone},
        )

    stamps = {"suppression_at": datetime.now(timezone.utc)}
    if phone:
        stamps["sms_at"] = stamps["suppression_at"]
    if email:
        stamps["email_at"] = stamps["suppression_at"]

    dialer_done = phone is None
    if phone:
        remover = ctx.get("dialer_remover") or _default_dialer_remover()
        if remover is not None:
            try:
                remover(phone)
                stamps["dialer_removed_at"] = datetime.now(timezone.utc)
                dialer_done = True
            except Exception as exc:
                logger.error("[lending-compliance] dialer removal failed event=%s: %s", event_id, exc)

    db.execute(
        text(
            "UPDATE lending.opt_out_events SET suppression_at = :suppression_at, sms_at = :sms_at, "
            "email_at = :email_at, dialer_removed_at = :dialer_removed_at, status = :status WHERE id = :id"
        ),
        {
            "suppression_at": stamps["suppression_at"],
            "sms_at": stamps.get("sms_at"),
            "email_at": stamps.get("email_at"),
            "dialer_removed_at": stamps.get("dialer_removed_at"),
            "status": "complete" if dialer_done else "dialer_pending",
            "id": event_id,
        },
    )
