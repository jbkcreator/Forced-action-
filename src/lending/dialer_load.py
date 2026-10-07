"""Load gated pool records into the dialer dialer.

Pipeline (spec §3): list-build compliance filter (phone, Georgia stop,
suppression, Tracerfy DNC) -> per-borrower Backflip conflict check ->
dialer contact upsert -> lending.dialer_load_records.

Blocks apply per phone, not per record: every record sharing a phone with a
blocked record is blocked too, because the same person would be dialled.

Open client decisions are never defaulted. A live load refuses to run while a
loadable pool has no campaign tag, or while a phone would load for more than
one record; the dry run reports both.

Dry run: nothing is sent to dialer or Tracerfy, and the caller rolls back.
Live: records reach dialer one at a time (paced by the client) and each load row is
committed right after its push, so a stop part-way leaves every loaded contact
tracked (an opt-out can only delete contacts we hold an id for); a re-run updates
contacts instead of duplicating them. A contact created but left without its fields
is tracked too (see ``ContactFieldsNotSet``) and finished by the next run.
"""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from sqlalchemy import text

from config.lending_compliance import RemovalReason
from config.lending_dialer import POOL_CAMPAIGN_TAGS
from config.settings import get_settings
from src.lending.backflip_conflict import (
    BorrowerRecord,
    find_borrower_conflicts,
    load_backflip_identifier_index,
)
from src.lending.compliance import (
    GateResult,
    Scrubber,
    _suppressed_phones,
    dial_blocks,
    filter_loadable,
    phone_hash,
)
from src.lending.dialer_contact import DialerDisplay, dialer_fields, display_from_record
from src.lending.dialer_port import (
    ContactFieldsNotSet,
    ContactUpsertResult,
    DialerContactFields,
    DialerRequestError,
)
from src.lending.dialer_removal import DialerRemovalUndecided
from src.lending.context_card import NO_PRIOR_CONTACT, build_context_card
from src.lending.lead_facts import LeadFacts, load_lead_facts
from src.lending.lead_scoring import LeadSignals, score_lead
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

COMMIT_CHUNK_SIZE = 1  # the client paces pushes one at a time, so a commit per push costs nothing
SUPERSEDED = "superseded"
REASON_SCRUB_FAILED = "SCRUB_FAILED"
REASON_SUPPRESSED = "SUPPRESSED"
REASON_NEEDS_SCRUB = "NEEDS_SCRUB"


class DialerContacts(Protocol):
    def upsert_contact(self, phone: str, fields: DialerContactFields, *, campaign: Optional[str] = None,
                       vendor_contact_id: Optional[str] = None) -> ContactUpsertResult: ...
    def update_contact(self, contact_id: Any, fields: DialerContactFields, *,
                       phone: Optional[str] = None, vendor_contact_id: Optional[str] = None) -> dict: ...
    def remove(self, phone: str, *, reason: str) -> None: ...


class LoadRefused(RuntimeError):
    """A live load cannot run until an open decision is made."""


class LoadAborted(RuntimeError):
    """A live load stopped after too many consecutive dialer failures (dialer likely down)."""


def no_scrub(phones: list[str]) -> list[dict]:
    """Dry-run scrubber: scrubs nothing, so stale numbers show as needing a scrub."""
    return []


@dataclass(frozen=True)
class _Loadable:
    record: Mapping[str, Any]
    record_ref: str
    pool: str
    phone: str
    display: DialerDisplay


@dataclass
class LoadReport:
    run_id: str
    dry_run: bool
    total: int = 0
    loadable: int = 0
    loaded: int = 0
    created: int = 0
    updated: int = 0
    excluded_by_reason: Counter = field(default_factory=Counter)
    loadable_by_pool: Counter = field(default_factory=Counter)
    duplicate_phones: int = 0
    distinct_phones: int = 0
    needs_scrub: int = 0  # dry run: numbers with no fresh scrub, whatever blocked them afterwards
    needs_scrub_backflip_blocked: int = 0  # of those, how many the Backflip check also blocked
    unmapped_pools: list[str] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)
    active_not_in_run: int = 0
    backflip_check: bool = True
    suppressed_mid_run: int = 0  # opted out after the start-of-run gate; skipped before the push

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "total": self.total,
            "loadable": self.loadable,
            "loaded": self.loaded,
            "created": self.created,
            "updated": self.updated,
            "excluded_by_reason": dict(self.excluded_by_reason),
            "loadable_by_pool": dict(self.loadable_by_pool),
            "duplicate_phones": self.duplicate_phones,
            "distinct_phones": self.distinct_phones,
            "needs_scrub": self.needs_scrub,
            "unmapped_pools": self.unmapped_pools,
            "failed": self.failed,
            "active_not_in_run": self.active_not_in_run,
            "backflip_check": self.backflip_check,
            "suppressed_mid_run": self.suppressed_mid_run,
        }


def _record_ref(record: Mapping[str, Any]) -> str:
    return str(record.get("source_record_ref") or record.get("record_ref") or "")


def _gate_blocks(records: Sequence[Mapping[str, Any]], results: Sequence[GateResult]) -> dict[int, str]:
    return {i: result.reason.value for i, result in enumerate(results) if not result.allowed}


def _conflict_blocks(db, records: Sequence[Mapping[str, Any]], candidates: Sequence[int],
                     phones: Sequence[Optional[str]]) -> tuple[dict[int, str], dict[int, tuple[str, ...]]]:
    index = load_backflip_identifier_index(db)
    borrowers = [
        BorrowerRecord(
            record_ref=_record_ref(records[i]),
            phone=phones[i],
            email=records[i].get("email"),
            entity_name=records[i].get("entity_name"),
            parcel_id=records[i].get("parcel_id"),
        )
        for i in candidates
    ]
    blocks: dict[int, str] = {}
    criteria: dict[int, tuple[str, ...]] = {}
    for i, decision in zip(candidates, find_borrower_conflicts(borrowers, index)):
        if decision.blocked:
            blocks[i] = decision.reason
            criteria[i] = decision.matched_criteria
    return blocks, criteria


def _propagate_by_phone(phones: Sequence[Optional[str]], blocks: dict[int, str]) -> dict[int, tuple[str, int]]:
    """Records not blocked themselves but sharing a phone with a blocked record."""
    first_block_by_phone: dict[str, int] = {}
    for i in sorted(blocks):
        if phones[i]:
            first_block_by_phone.setdefault(phones[i], i)
    return {
        i: (blocks[first_block_by_phone[phone]], first_block_by_phone[phone])
        for i, phone in enumerate(phones)
        if phone and i not in blocks and phone in first_block_by_phone
    }


def _write_exclusions(db, run_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    db.execute(
        text(
            "INSERT INTO lending.load_exclusions (run_id, phone_hash, reason, detail) "
            "VALUES (:run_id, :phone_hash, :reason, CAST(:detail AS jsonb))"
        ),
        [{**row, "run_id": run_id} for row in rows],
    )


def _active_rows(db, phones: list[str]) -> dict[str, tuple[int, Optional[int], Optional[str]]]:
    if not phones:
        return {}
    rows = db.execute(
        text(
            "SELECT phone, id, dialer_contact_id, campaign_tag FROM lending.dialer_load_records "
            "WHERE active AND phone = ANY(CAST(:phones AS varchar[]))"
        ),
        {"phones": phones},
    ).fetchall()
    return {row.phone: (row.id, row.dialer_contact_id, row.campaign_tag) for row in rows}


def _count_active_not_in(db, phones: list[str]) -> int:
    return db.execute(
        text(
            "SELECT count(*) FROM lending.dialer_load_records "
            "WHERE active AND NOT (phone = ANY(CAST(:phones AS varchar[])))"
        ),
        {"phones": phones},
    ).scalar_one()


CAMPAIGN_CHANGED = "pool_changed"


def _push_contact(dialer: DialerContacts, item: _Loadable, known_contact_id: Optional[str],
                  known_campaign_tag: Optional[str], fields: DialerContactFields) -> ContactUpsertResult:
    """Update by the stored contact id when known (search can lag); else upsert by phone.

    A phone whose pool changed between staging runs must move dialer campaigns, not
    just get its card fields updated in place — a bare update would leave BatchDialer
    dialing the old campaign forever while the database says a different one (finding
    #10). If the removal from the old campaign can't be confirmed, this raises
    DialerRequestError so the caller flags the record instead of silently writing the
    new campaign_tag as if the move had actually happened.
    """
    new_campaign = item.display.campaign_tag
    if known_contact_id is not None and known_campaign_tag is not None and known_campaign_tag != new_campaign:
        try:
            dialer.remove(item.phone, reason=CAMPAIGN_CHANGED)
        except DialerRemovalUndecided as exc:
            raise DialerRequestError(f"campaign move unconfirmed: {exc}") from exc
        return dialer.upsert_contact(item.phone, fields, campaign=new_campaign,
                                     vendor_contact_id=item.record_ref or None)
    if known_contact_id is not None:
        try:
            # full PUT: resend our record id or BatchDialer clears it
            dialer.update_contact(known_contact_id, fields, phone=item.phone,
                                  vendor_contact_id=item.record_ref or None)
            return ContactUpsertResult(contact_id=known_contact_id, created=False)
        except DialerRequestError as exc:
            if exc.status != 404:
                raise
    return dialer.upsert_contact(item.phone, fields, campaign=new_campaign,
                                     vendor_contact_id=item.record_ref or None)


def _create_outcome_unknown(exc: DialerRequestError) -> bool:
    return exc.maybe_created and not isinstance(exc, ContactFieldsNotSet)


def _record_unconfirmed_create(db, run_id: str, item: _Loadable, status: Optional[int]) -> None:
    db.execute(
        text(
            "INSERT INTO lending.dialer_unconfirmed_creates "
            "(run_id, phone, phone_hash, source_record_ref, error_status) "
            "VALUES (:run_id, :phone, :phone_hash, :ref, :status)"
        ),
        {"run_id": run_id, "phone": item.phone, "phone_hash": phone_hash(item.phone),
         "ref": item.record_ref, "status": status},
    )


def _pull_if_suppressed_after_push(db, dialer: DialerContacts, chunk: list[tuple[_Loadable, int]],
                                   commit: Callable[[], None]) -> int:
    """A STOP that landed while a contact was being pushed: the opt-out poller found no load row
    and completed the event, so the contact must be pulled here, now that its row exists."""
    suppressed = _suppressed_phones(db, [item.phone for item, _ in chunk])
    pulled = 0
    for item, _ in chunk:
        if item.phone not in suppressed:
            continue
        try:
            dialer.remove(item.phone, reason=RemovalReason.OPT_OUT.value)
        except Exception as exc:
            logger.error("[dialer-load] phone_hash=%s opted out during its push and could not be removed "
                         "(%s); remove it from the dialer by hand", phone_hash(item.phone)[:12], type(exc).__name__)
            continue
        db.execute(
            text("UPDATE lending.dialer_load_records SET active = false, deactivated_at = now(), "
                 "deactivation_reason = :reason WHERE phone = :phone AND active"),
            {"reason": "opted_out_during_push", "phone": item.phone},
        )
        commit()
        pulled += 1
    return pulled


def _record_pushed_before_abort(db, run_id: str, chunk: list[tuple[_Loadable, int]],
                                active: dict[str, tuple[int, Optional[int], Optional[str]]],
                                commit: Callable[[], None]) -> None:
    if not chunk:
        return
    try:
        _store_chunk(db, run_id, chunk, active)
        commit()
    except Exception as exc:
        logger.error("[dialer-load] run %s aborted with %d pushed contact(s) not recorded (%s); "
                     "reconcile them against the dialer", run_id, len(chunk), type(exc).__name__)


def _store_chunk(db, run_id: str, loaded: list[tuple[_Loadable, int]],
                 active: dict[str, tuple[int, Optional[int], Optional[str]]]) -> None:
    superseded = [active[item.phone][0] for item, _ in loaded if item.phone in active]
    if superseded:
        db.execute(
            text(
                "UPDATE lending.dialer_load_records SET active = false, deactivated_at = now(), "
                "deactivation_reason = :reason WHERE id = ANY(CAST(:ids AS bigint[]))"
            ),
            {"reason": SUPERSEDED, "ids": superseded},
        )
    db.execute(
        text(
            "INSERT INTO lending.dialer_load_records "
            "(run_id, pool, source_record_ref, phone, phone_hash, campaign_tag, borrower_name, "
            " entity_name, property_address, estimated_loan_value, recent_permit_details, "
            " dialer_contact_id) "
            "VALUES (:run_id, :pool, :ref, :phone, :phone_hash, :campaign_tag, :borrower_name, "
            " :entity_name, :property_address, :estimated_loan_value, :recent_permit_details, "
            " :contact_id)"
        ),
        [
            {
                "run_id": run_id,
                "pool": item.pool,
                "ref": item.record_ref,
                "phone": item.phone,
                "phone_hash": phone_hash(item.phone),
                "campaign_tag": item.display.campaign_tag,
                "borrower_name": item.display.borrower_name,
                "entity_name": item.display.entity_name,
                "property_address": item.display.property_address,
                "estimated_loan_value": item.display.estimated_loan_value,
                "recent_permit_details": item.display.recent_permit_details,
                "contact_id": str(contact_id) if contact_id is not None else None,
            }
            for item, contact_id in loaded
        ],
    )


CARD_TIMEZONE = ZoneInfo("America/New_York")


def _first_name(borrower_name: Optional[str]) -> Optional[str]:
    parts = (borrower_name or "").split()
    return parts[0] if parts else None


def _prior_contact_line(calls: int, last_ended: datetime, last_disposition: Optional[str]) -> str:
    line = f"{calls} call{'s' if calls != 1 else ''}, last {last_ended.astimezone(CARD_TIMEZONE):%Y-%m-%d}"
    if last_disposition:
        line += f"; last outcome: {last_disposition.replace('_', ' ')}"
    return line


def _prior_contacts(db, phones: Sequence[str]) -> Optional[dict[str, str]]:
    """Prior-contact line per phone with logged calls, from one query; None when unreadable.

    Counts every logged call to or from the number. The outcome is the latest call
    that has a disposition, since the newest call may not be dispositioned yet.
    """
    if not phones:
        return {}
    try:
        with db.begin_nested():
            rows = db.execute(
                text(
                    "SELECT phone, count(*) AS calls, max(call_ended_at) AS last_ended, "
                    "(array_agg(disposition ORDER BY call_ended_at DESC) "
                    " FILTER (WHERE disposition IS NOT NULL))[1] AS last_disposition "
                    "FROM lending.call_dispositions WHERE phone = ANY(:phones) GROUP BY phone"
                ),
                {"phones": list(phones)},
            ).mappings().all()
    except Exception as exc:
        logger.warning("[dialer-load] call history unavailable (%s); prior contact left unknown",
                       type(exc).__name__)
        return None
    return {
        row["phone"]: _prior_contact_line(int(row["calls"]), row["last_ended"], row["last_disposition"])
        for row in rows if row["last_ended"] is not None
    }


def _cards(db, loadable: Sequence["_Loadable"]) -> dict[str, dict[str, str]]:
    """Context-card custom fields per record ref, from one batched facts query.

    The card is extra context for the caller, never a reason to skip a load: if
    the facts query fails, every record loads without a card (inside a savepoint,
    so the load's transaction stays usable). Prior contact comes from the logged
    calls; if they can't be read, it shows as not available.
    """
    today = datetime.now(CARD_TIMEZONE).date()
    property_ids = sorted({int(item.record["property_id"]) for item in loadable
                           if item.record.get("property_id") is not None})
    facts: dict[int, LeadFacts] = {}
    if property_ids:
        try:
            with db.begin_nested():
                facts = load_lead_facts(db, property_ids, today=today)
        except Exception as exc:
            logger.warning("[dialer-load] card facts unavailable (%s); loading without cards", type(exc).__name__)
            return {}
    history = _prior_contacts(db, sorted({item.phone for item in loadable}))
    cards: dict[str, dict[str, str]] = {}
    for item in loadable:
        property_id = item.record.get("property_id")
        lead = facts.get(int(property_id)) if property_id is not None else None
        lead = lead or LeadFacts(property_id=int(property_id or 0), property_address=item.display.property_address,
                                 lender_name=None, latest_permit=item.display.recent_permit_details,
                                 signals=LeadSignals())
        card = build_context_card(
            lead, score_lead(lead.signals, today=today),
            first_name=_first_name(item.display.borrower_name), county=item.display.county,
            campaign=item.display.campaign_tag, hook=item.display.hook,
            prior_contact=None if history is None else history.get(item.phone, NO_PRIOR_CONTACT),
        )
        cards[item.record_ref] = card.custom_fields
    return cards


def _raise_if_refused(unmapped_pools: Sequence[str], duplicate_phones: int) -> None:
    if unmapped_pools:
        raise LoadRefused(f"no campaign tag for pool(s): {', '.join(unmapped_pools)}")
    if duplicate_phones:
        raise LoadRefused(f"{duplicate_phones} phone(s) would load for more than one record")


def _refuse_before_scrub(records: Sequence[Mapping[str, Any]], phones: Sequence[Optional[str]], db,
                         campaign_tags: Mapping[str, str], backflip_check: bool) -> None:
    """Refusal checks that must run before the paid Tracerfy scrub, which a refused run would
    otherwise pay for and then roll back. A number the scrub could still clear counts as
    loadable, so this can only over-refuse, never under-refuse."""
    results = filter_loadable(list(records), db, scrubber=no_scrub)
    blocks = {i: r for i, r in _gate_blocks(records, results).items() if r != REASON_SCRUB_FAILED}
    if backflip_check:
        candidates = [i for i in range(len(records)) if i not in blocks]
        blocks.update(_conflict_blocks(db, records, candidates, phones)[0])
    blocked = set(blocks) | set(_propagate_by_phone(phones, blocks))
    possible = [i for i in range(len(records)) if i not in blocked]
    pools = sorted({str(records[i].get("pool") or "") for i in possible
                    if not campaign_tags.get(str(records[i].get("pool") or ""))})
    per_phone = Counter(phones[i] for i in possible if phones[i])
    _raise_if_refused(pools, sum(1 for n in per_phone.values() if n > 1))


def run_dialer_load(
    records: Sequence[Mapping[str, Any]],
    db,
    *,
    run_id: str,
    dry_run: bool,
    scrubber: Optional[Scrubber] = None,
    dialer: Optional[DialerContacts] = None,
    campaign_tags: Mapping[str, str] = POOL_CAMPAIGN_TAGS,
    commit: Optional[Callable[[], None]] = None,
    now: Optional[datetime] = None,
    backflip_check: Optional[bool] = None,
    max_consecutive_failures: Optional[int] = None,
) -> LoadReport:
    """Gate every record, then (live only) load the survivors into dialer.

    Dry run never calls dialer or Tracerfy and never commits; roll ``db``
    back afterwards. Live requires ``scrubber`` and ``dialer`` and commits
    through ``commit`` (default ``db.commit``) after each chunk of loads.
    ``backflip_check`` defaults to the ``LENDING_BACKFLIP_CHECK_ENABLED`` setting.
    ``max_consecutive_failures`` raises ``LoadAborted`` after that many dialer
    failures in a row; pushed contacts are recorded first. None = never abort.
    """
    if backflip_check is None:
        backflip_check = get_settings().lending_backflip_check_enabled
    report = LoadReport(run_id=run_id, dry_run=dry_run, total=len(records), backflip_check=backflip_check)
    if not dry_run and (scrubber is None or dialer is None):
        raise ValueError("a live load needs a scrubber and a dialer")
    phones = [normalize_phone(r.get("normalized_phone") or r.get("phone") or "") for r in records]
    if not dry_run:
        _refuse_before_scrub(records, phones, db, campaign_tags, backflip_check)

    gate_results = filter_loadable(
        list(records), db, scrubber=no_scrub if dry_run else scrubber,
        run_id=None if dry_run else run_id,
    )
    blocks = _gate_blocks(records, gate_results)
    # A dry run scrubs nothing, so unscrubbed numbers still go through the
    # Backflip check; those that pass are reported as needing a scrub.
    pending_scrub = {i for i, reason in blocks.items() if dry_run and reason == REASON_SCRUB_FAILED}
    report.needs_scrub = len(pending_scrub)
    for i in pending_scrub:
        del blocks[i]
    candidates = [i for i in range(len(records)) if i not in blocks]
    conflict_blocks, criteria = (_conflict_blocks(db, records, candidates, phones)
                                 if backflip_check else ({}, {}))
    blocks.update(conflict_blocks)
    # Call-time rail in the dial path: a live load never loads a number that cannot be
    # dialed right now (attempt cap, 09:00-19:15 ET / 8-20 local). Dry runs report
    # eligibility, not dial time, so they skip it.
    if not dry_run:
        rail_candidates = [i for i in range(len(records)) if i not in blocks and phones[i]]
        rail = dial_blocks(db, sorted({phones[i] for i in rail_candidates}), now=now)
        blocks.update({i: rail[phones[i]].value for i in rail_candidates if phones[i] in rail})
    blocks.update({i: REASON_NEEDS_SCRUB for i in pending_scrub if i not in conflict_blocks})
    report.needs_scrub_backflip_blocked = sum(1 for i in pending_scrub if i in conflict_blocks)
    propagated = _propagate_by_phone(phones, blocks)

    exclusion_rows = [
        {"phone_hash": phone_hash(phones[i]), "reason": reason,
         "detail": json.dumps({"matched_criteria": list(criteria[i])})}
        for i, reason in conflict_blocks.items()
    ] + [
        {"phone_hash": phone_hash(phones[i]), "reason": reason,
         "detail": json.dumps({"blocked_via": _record_ref(records[via])})}
        for i, (reason, via) in propagated.items()
    ]
    for i, reason in blocks.items():
        report.excluded_by_reason[reason] += 1
    for reason, _ in propagated.values():
        report.excluded_by_reason[reason] += 1

    loadable = [
        _Loadable(
            record=records[i],
            record_ref=_record_ref(records[i]),
            pool=str(records[i].get("pool") or ""),
            phone=phones[i],
            display=display_from_record(records[i], campaign_tags.get(str(records[i].get("pool") or ""))),
        )
        for i in range(len(records))
        if i not in blocks and i not in propagated
    ]
    report.loadable = len(loadable)
    report.loadable_by_pool.update(item.pool for item in loadable)
    records_per_phone = Counter(item.phone for item in loadable)
    report.duplicate_phones = sum(1 for n in records_per_phone.values() if n > 1)
    report.distinct_phones = len(records_per_phone)
    report.unmapped_pools = sorted({item.pool for item in loadable if item.display.campaign_tag is None})
    loadable_phones = sorted(records_per_phone)
    report.active_not_in_run = _count_active_not_in(db, loadable_phones)

    if dry_run:
        logger.info("[dialer-load] dry run %s: %s", run_id, json.dumps(report.as_dict()))
        return report

    _raise_if_refused(report.unmapped_pools, report.duplicate_phones)

    _write_exclusions(db, run_id, exclusion_rows)
    commit = commit or db.commit
    commit()
    active = _active_rows(db, loadable_phones)
    chunk: list[tuple[_Loadable, int]] = []
    cards = _cards(db, loadable)
    consecutive_failures = 0
    try:
        for item in loadable:
            if _suppressed_phones(db, [item.phone]):  # one indexed lookup per push (a push is 3 HTTP calls)
                report.suppressed_mid_run += 1
                _write_exclusions(db, run_id, [{
                    "phone_hash": phone_hash(item.phone), "reason": REASON_SUPPRESSED,
                    "detail": json.dumps({"stage": "recheck_before_push", "record_ref": item.record_ref})}])
                continue
            fields = replace(dialer_fields(item.display, email=item.record.get("email")),
                             card=cards.get(item.record_ref, {}))
            try:
                _, known_contact_id, known_campaign_tag = active.get(item.phone, (None, None, None))
                result = _push_contact(dialer, item, known_contact_id, known_campaign_tag, fields)
            except DialerRequestError as exc:
                report.failed.append({"record_ref": item.record_ref, "error": type(exc).__name__,
                                      "status": getattr(exc, "status", None)})
                if _create_outcome_unknown(exc):
                    _record_unconfirmed_create(db, run_id, item, exc.status)
                    commit()
                consecutive_failures += 1
                if max_consecutive_failures and consecutive_failures >= max_consecutive_failures:
                    raise LoadAborted(f"{consecutive_failures} consecutive dialer failures")
                if not isinstance(exc, ContactFieldsNotSet):
                    continue
                contact_id = exc.contact_id  # live in the dialer: track it so opt-out can delete it
            else:
                consecutive_failures = 0
                report.loaded += 1
                report.created += int(result.created)
                report.updated += int(not result.created)
                contact_id = result.contact_id
            chunk.append((item, contact_id))
            if len(chunk) >= COMMIT_CHUNK_SIZE:
                _store_chunk(db, run_id, chunk, active)
                commit()
                report.suppressed_mid_run += _pull_if_suppressed_after_push(db, dialer, chunk, commit)
                chunk = []
    except BaseException:
        # SIGTERM, a crash or an unexpected error mid-run: contacts already pushed to the
        # dialer but not yet recorded would be live with no load row, invisible to a later
        # opt-out removal. Record them before the error propagates.
        _record_pushed_before_abort(db, run_id, chunk, active, commit)
        raise
    if chunk:
        _store_chunk(db, run_id, chunk, active)
        commit()
    logger.info("[dialer-load] live run %s: %s", run_id, json.dumps(report.as_dict()))
    return report
