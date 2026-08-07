"""
Whale contact tracing — Tracerfy skip-trace for buyer_entities.is_whale rows
that have zero phone AND zero email on file today.

Population: a whale (buyer_entities.is_whale=true) with no contact info anywhere
across its linked `owners` rows. Whales resolved only from a `deeds`/`sunbiz_snapshots`
mention (no `owners` link at all) have no address to trace and are out of scope —
there is nothing for Tracerfy to submit.

For non-Individual owner rows (LLC/Trust/Corporate/Estate), name resolution reuses
tracerfy_fallback._resolve_trace_subject unchanged: managing_members first, then
registered_agent_name/registered_agent_address. That is the existing, tested path
for "use the registered agent as the traceable person" — nothing new to build there.

Two Tracerfy modes, run as two strictly sequential, ledger-gated stages:
  Stage 1 (normal)   — name + address, 1 credit/hit ($0.02). Entities with no
                       resolvable person name (no managing member, no registered
                       agent) still get submitted here with blank first/last —
                       Tracerfy still has the address to match against, and this
                       is what lets stage 2 unlock for them (see note below).
  Stage 2 (advanced) — address only, 2 credits/hit ($0.04). Only owners that are
                       STILL contact-less after stage 1 are re-submitted here.

Idempotency note: skip_trace_ledger.should_submit() only allows an ADVANCED
submission for a key that already has a NORMAL submission on record — it is the
guardrail against skipping straight to the pricier tier. Submitting nameless
entities in stage 1 too (rather than skipping them straight to stage 2, which is
what tracerfy_fallback.py's own auto-cascade does for entity-skipped owners) is
what makes them eligible for stage 2 in the first place. Skipping stage 1 for
them would leave stage 2 silently blocked by the ledger on every future run.

Every submission — both stages — goes through the same shared ledger
(skip_trace_ledger.already_traced/should_submit, source="tracerfy") that every
other Tracerfy consumer in this codebase uses, so re-running this script never
re-pays for an address already traced by this script, by tracerfy_fallback.py,
or by the Lead Pack hot-enrichment path.

Usage:
  python -m src.services.whale_contact_trace --dry-run
  python -m src.services.whale_contact_trace --limit 200
  python -m src.services.whale_contact_trace --balance
"""

import time
import traceback
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional

from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import EnrichedContact, EnrichmentUsageLog, SmsOptOut
from src.services.email import send_alert
from src.services.enrichment_log import log_usage
from src.services.skip_trace_ledger import ADVANCED, NORMAL, BillingModel, RunSpendCap, already_traced, should_submit, trace_key
from src.services.tracerfy_fallback import (
    _headers, _parse_trace_row, _poll_trace_queue, _resolve_trace_subject, _submit_trace_batch, get_tracerfy_balance,
)
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_BATCH_SIZE  = 3000  # matches tracerfy_fallback.py — whale volume is far under this in one run
_BATCH_DELAY = 31    # seconds between batch POSTs (rate limit: 10/5min)

_CANDIDATE_SQL = """
    WITH whale_owner AS (
        SELECT DISTINCT
            o.id AS owner_id, be.id AS buyer_entity_id,
            (o.phone_1 IS NOT NULL AND length(trim(o.phone_1)) > 0) AS row_has_phone,
            (
                (o.email_1 IS NOT NULL AND length(trim(o.email_1)) > 0) OR
                (o.email_2 IS NOT NULL AND length(trim(o.email_2)) > 0)
            ) AS row_has_email
        FROM buyer_entities be
        JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id AND bel.source_table = 'owners'
        JOIN owners o ON o.id = bel.source_id
        WHERE be.is_whale
    ),
    entity_contact AS (
        SELECT buyer_entity_id, bool_or(row_has_phone OR row_has_email) AS has_any_contact
        FROM whale_owner
        GROUP BY buyer_entity_id
    )
    SELECT DISTINCT
        o.id AS owner_id, o.property_id, o.county_id AS owner_county_id,
        o.owner_name, o.owner_type, o.phone_1, o.email_1,
        o.phone_metadata, o.managing_members,
        o.registered_agent_name, o.registered_agent_address,
        p.id AS prop_id, p.address, p.city, p.state, p.zip
    FROM whale_owner wo
    JOIN entity_contact ec ON ec.buyer_entity_id = wo.buyer_entity_id AND ec.has_any_contact = false
    JOIN owners o ON o.id = wo.owner_id
    JOIN properties p ON p.id = o.property_id
    WHERE p.address IS NOT NULL AND p.address != ''
      AND p.zip IS NOT NULL AND p.zip != ''
    ORDER BY o.id
    {limit_clause}
"""


def _select_candidates(session, limit: Optional[int] = None) -> list[tuple]:
    """Whale-linked owner rows with zero contact info on their whale, and an address to submit."""
    sql = _CANDIDATE_SQL.format(limit_clause="LIMIT :limit" if limit else "")
    params = {"limit": limit} if limit else {}
    raw_rows = session.execute(sa_text(sql), params).mappings().all()
    return [
        (
            SimpleNamespace(
                id=r["owner_id"], property_id=r["property_id"], county_id=r["owner_county_id"],
                owner_name=r["owner_name"], owner_type=r["owner_type"],
                phone_1=r["phone_1"], email_1=r["email_1"], phone_metadata=r["phone_metadata"],
                managing_members=r["managing_members"],
                registered_agent_name=r["registered_agent_name"],
                registered_agent_address=r["registered_agent_address"],
            ),
            SimpleNamespace(id=r["prop_id"], address=r["address"], city=r["city"], state=r["state"], zip=r["zip"]),
        )
        for r in raw_rows
    ]


def _reselect_still_missing(session, owner_ids: list[int]) -> set[int]:
    """After stage 1, which of these owners' whales still have zero contact?"""
    if not owner_ids:
        return set()
    rows = session.execute(sa_text("""
        WITH whale_owner AS (
            SELECT DISTINCT
                o.id AS owner_id, be.id AS buyer_entity_id,
                (o.phone_1 IS NOT NULL AND length(trim(o.phone_1)) > 0) AS row_has_phone,
                (
                    (o.email_1 IS NOT NULL AND length(trim(o.email_1)) > 0) OR
                    (o.email_2 IS NOT NULL AND length(trim(o.email_2)) > 0)
                ) AS row_has_email
            FROM buyer_entities be
            JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id AND bel.source_table = 'owners'
            JOIN owners o ON o.id = bel.source_id
            WHERE be.is_whale
        ),
        entity_contact AS (
            SELECT buyer_entity_id, bool_or(row_has_phone OR row_has_email) AS has_any_contact
            FROM whale_owner
            GROUP BY buyer_entity_id
        )
        SELECT DISTINCT wo.owner_id
        FROM whale_owner wo
        JOIN entity_contact ec ON ec.buyer_entity_id = wo.buyer_entity_id AND ec.has_any_contact = false
        WHERE wo.owner_id = ANY(:owner_ids)
    """), {"owner_ids": owner_ids}).fetchall()
    return {r.owner_id for r in rows}


def _run_stage(
    rows: list[tuple],
    api_key: str,
    address_only: bool,
    cost_per_hit: int,
    cap: RunSpendCap,
    dry_run: bool,
    force_retrace: bool,
) -> dict:
    """
    One ledger-gated submit/poll/persist pass, either mode. Mirrors
    tracerfy_fallback.run_tracerfy_fallback's per-batch flow, scoped to an
    explicit row list rather than a SQL candidate query (that part is
    _select_candidates above).
    """
    submit_mode = ADVANCED if address_only else NORMAL
    stats = {"total": len(rows), "success": 0, "failed": 0, "no_address": 0,
              "skipped_already_traced": 0, "aborted_cost_cap": False}

    resolved: list[tuple] = []  # (owner, prop, first, last, addr, key)
    for owner, prop in rows:
        if not prop.address or not prop.zip:
            stats["no_address"] += 1
            continue
        if address_only:
            first, last = "", ""
            addr = {"address": prop.address, "city": prop.city or "Tampa",
                    "state": prop.state or "FL", "zip": (prop.zip or "")[:5]}
        else:
            first, last, addr_override, is_traceable = _resolve_trace_subject(owner)
            # Unlike tracerfy_fallback.py's cascade, nameless entities are NOT
            # skipped here — see module docstring on why that would permanently
            # block their stage-2 (advanced) submission via the shared ledger.
            addr = addr_override or {"address": prop.address, "city": prop.city or "Tampa",
                                      "state": prop.state or "FL", "zip": (prop.zip or "")[:5]}
        resolved.append((owner, prop, first, last, addr, trace_key(addr["address"], addr["zip"])))

    with get_db_context() as session:
        ledger = already_traced(session, "tracerfy", [r[5] for r in resolved if r[5]])

    records: list[dict] = []
    record_keys: list[str] = []
    key_map: dict[str, list[tuple]] = {}
    for owner, prop, first, last, addr, key in resolved:
        if not key:
            stats["no_address"] += 1
            continue
        if not force_retrace and not should_submit(key, submit_mode, ledger, BillingModel.PER_HIT):
            stats["skipped_already_traced"] += 1
            continue
        if key in key_map:
            key_map[key].append((owner, prop))
            continue
        key_map[key] = [(owner, prop)]
        rec = {"label": str(owner.id), "address": addr["address"], "city": addr["city"],
               "state": addr["state"], "zip": addr["zip"]}
        if not address_only:
            rec["first_name"], rec["last_name"] = first, last
        records.append(rec)
        record_keys.append(key)

    logger.info(
        "[WhaleTrace/%s] %d unique addresses to submit (skipped_already_traced=%d no_address=%d)",
        submit_mode, len(records), stats["skipped_already_traced"], stats["no_address"],
    )

    if dry_run:
        for rec in records[:5]:
            logger.info("[WhaleTrace/%s DRY RUN] Would trace: %s, %s %s %s",
                        submit_mode, rec["address"], rec.get("city"), rec.get("state"), rec.get("zip"))
        logger.info("[WhaleTrace/%s DRY RUN] Would submit %d addresses. No API call made.", submit_mode, len(records))
        return stats

    if not records:
        return stats

    for batch_start in range(0, len(records), _BATCH_SIZE):
        batch_records = records[batch_start: batch_start + _BATCH_SIZE]
        batch_keys = set(record_keys[batch_start: batch_start + _BATCH_SIZE])
        batch_num = batch_start // _BATCH_SIZE + 1

        projected_cents = len(batch_records) * cost_per_hit
        if cap.would_exceed(projected_cents):
            logger.error(
                "[WhaleTrace/%s] Per-run spend cap %d¢ would be exceeded by batch %d "
                "(worst-case +%d¢, already %d¢) — stopping.",
                submit_mode, cap.ceiling_cents, batch_num, projected_cents, cap.projected_cents,
            )
            send_alert(
                subject="[Forced Action] Whale trace — Tracerfy spend cap hit",
                body=(
                    f"Whale contact trace stopped before {submit_mode} batch {batch_num} "
                    f"({len(batch_records)} addresses, worst-case {projected_cents}¢): would exceed "
                    f"the per-run ceiling of {cap.ceiling_cents}¢ (SKIP_TRACE_MAX_RUN_COST_CENTS).\n\n"
                    f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ),
            )
            stats["aborted_cost_cap"] = True
            break

        logger.info("[WhaleTrace/%s] Batch %d: submitting %d records...", submit_mode, batch_num, len(batch_records))
        try:
            queue_id, estimated_wait = _submit_trace_batch(batch_records, api_key, address_only=address_only)
        except RuntimeError as e:
            logger.error("[WhaleTrace/%s] Batch %d submit failed: %s", submit_mode, batch_num, e)
            stats["failed"] += len(batch_records)
            continue

        cap.add(projected_cents)
        logger.info("[WhaleTrace/%s] Batch %d queued — queue_id=%s", submit_mode, batch_num, queue_id)

        try:
            results = _poll_trace_queue(
                queue_id, api_key, estimated_wait,
                stable_rounds_required=8 if address_only else 4,
                max_empty_attempts=36 if address_only else 120,
                min_settle_seconds=30,
            )
        except RuntimeError as e:
            logger.error(
                "[WhaleTrace/%s] Batch %d: BILLED queue=%s failed to drain (%s) — hits may be uningested.",
                submit_mode, batch_num, queue_id, e,
            )
            results = []

        hit_keys: set[str] = set()
        now = datetime.now(timezone.utc)
        with get_db_context() as session:
            for row in results:
                rkey = trace_key(row.get("address"), row.get("zip"))
                targets = key_map.get(rkey)
                if not targets or rkey not in batch_keys:
                    continue
                hit_keys.add(rkey)
                parsed = _parse_trace_row(row)

                for owner_snap, prop_snap in targets:
                    try:
                        existing = (
                            session.query(EnrichedContact)
                            .filter(EnrichedContact.property_id == owner_snap.property_id,
                                     EnrichedContact.source == "tracerfy")
                            .first()
                        )
                        owner = session.execute(
                            sa_text("SELECT id, property_id, county_id, phone_1, email_1, phone_metadata "
                                    "FROM owners WHERE id = :id"),
                            {"id": owner_snap.id},
                        ).mappings().first()
                        if owner is None:
                            continue

                        log_usage(
                            session, vendor="tracerfy", purpose="whale_contact_trace",
                            success=parsed["match_success"],
                            cost_cents=cost_per_hit if parsed["match_success"] else 0,
                            property_id=owner_snap.property_id, target_address=rkey,
                            request_ref=queue_id,
                        )

                        if existing:
                            existing.mobile_phone    = parsed["mobile_phone"] or existing.mobile_phone
                            existing.landline        = parsed["landline"] or existing.landline
                            existing.email           = parsed["email"] or existing.email
                            existing.mailing_address = parsed["mailing_address"] or existing.mailing_address
                            existing.match_success   = parsed["match_success"] or existing.match_success
                            existing.raw_response    = dict(row)
                            existing.enriched_at     = now
                            existing.trace_type      = submit_mode
                        else:
                            session.add(EnrichedContact(
                                property_id=owner_snap.property_id, county_id=owner["county_id"],
                                mobile_phone=parsed["mobile_phone"], landline=parsed["landline"],
                                email=parsed["email"], mailing_address=parsed["mailing_address"],
                                source="tracerfy", trace_type=submit_mode,
                                match_success=parsed["match_success"], raw_response=row, enriched_at=now,
                            ))

                        phone_1, email_1 = owner["phone_1"], owner["email_1"]
                        if parsed["mobile_phone"] and not phone_1:
                            phone_1 = parsed["mobile_phone"]
                        elif parsed["landline"] and not phone_1:
                            phone_1 = parsed["landline"]
                        if parsed["email"] and not email_1:
                            email_1 = parsed["email"]
                        if phone_1 != owner["phone_1"] or email_1 != owner["email_1"]:
                            session.execute(
                                sa_text("UPDATE owners SET phone_1 = :phone_1, email_1 = :email_1, "
                                        "skip_trace_success = TRUE WHERE id = :id"),
                                {"phone_1": phone_1, "email_1": email_1, "id": owner_snap.id},
                            )

                        for dnc_num in parsed.get("all_dnc_phones") or []:
                            already = session.query(SmsOptOut).filter_by(phone=dnc_num).first()
                            if not already:
                                session.add(SmsOptOut(phone=dnc_num, keyword_used="DNC",
                                                       source="tracerfy_dnc", opted_out_at=now))

                        if parsed["match_success"]:
                            stats["success"] += 1
                        else:
                            stats["failed"] += 1
                    except Exception as e:
                        logger.error("[WhaleTrace/%s] Persist error owner_id=%d: %s", submit_mode, owner_snap.id, e)
                        logger.debug(traceback.format_exc())
                        stats["failed"] += 1

            # Misses: keys with no returned row at all — still record a $0 usage
            # row + a miss EnrichedContact so the ledger reflects the submission.
            for k in batch_keys:
                if k in hit_keys:
                    continue
                for owner_snap, prop_snap in key_map[k]:
                    existing = (
                        session.query(EnrichedContact)
                        .filter(EnrichedContact.property_id == owner_snap.property_id,
                                 EnrichedContact.source == "tracerfy")
                        .first()
                    )
                    log_usage(
                        session, vendor="tracerfy", purpose="whale_contact_trace",
                        success=False, cost_cents=0,
                        property_id=owner_snap.property_id, target_address=k, request_ref=queue_id,
                    )
                    if not existing:
                        session.add(EnrichedContact(
                            property_id=owner_snap.property_id, county_id=owner_snap.county_id,
                            source="tracerfy", trace_type=submit_mode, match_success=False, enriched_at=now,
                        ))
                    else:
                        existing.trace_type = existing.trace_type or submit_mode
                    stats["failed"] += 1

            session.commit()

        if batch_start + _BATCH_SIZE < len(records):
            logger.info("[WhaleTrace/%s] Waiting %ds before next batch (rate limit)...", submit_mode, _BATCH_DELAY)
            time.sleep(_BATCH_DELAY)

    return stats


def run_whale_trace(limit: Optional[int] = None, dry_run: bool = False, force_retrace: bool = False) -> dict:
    """
    Two-stage Tracerfy trace for whales with zero contact info: normal (name+address,
    $0.02) first, then advanced (address-only, $0.04) for whoever is still contact-less.
    Both stages gated by the shared skip_trace_ledger — safe to re-run at any time.
    """
    settings = get_settings()
    if not settings.tracerfy_api_key:
        logger.warning("TRACERFY_API_KEY not set — whale contact trace skipped")
        return {"skipped": True, "reason": "TRACERFY_API_KEY not configured"}
    api_key = settings.tracerfy_api_key.get_secret_value()
    cap = RunSpendCap(settings.skip_trace_max_run_cost_cents)

    with get_db_context() as session:
        rows = _select_candidates(session, limit)

    if not rows:
        logger.info("[WhaleTrace] No candidates found — every owners-linked whale already has contact info.")
        return {"stage_normal": {}, "stage_advanced": {}}

    logger.info("[WhaleTrace] %d owner row(s) to trace across contact-less whales", len(rows))

    stage_normal = _run_stage(
        rows, api_key, address_only=False, cost_per_hit=settings.tracerfy_cost_cents,
        cap=cap, dry_run=dry_run, force_retrace=force_retrace,
    )

    if dry_run:
        return {"stage_normal": stage_normal, "stage_advanced": {}}

    with get_db_context() as session:
        still_missing = _reselect_still_missing(session, [o.id for o, _ in rows])
    rows_stage2 = [pair for pair in rows if pair[0].id in still_missing]

    logger.info("[WhaleTrace] %d owner row(s) still contact-less after normal — running advanced fallback",
                len(rows_stage2))

    stage_advanced = _run_stage(
        rows_stage2, api_key, address_only=True, cost_per_hit=4,
        cap=cap, dry_run=dry_run, force_retrace=force_retrace,
    ) if rows_stage2 else {"total": 0, "success": 0, "failed": 0, "no_address": 0,
                            "skipped_already_traced": 0, "aborted_cost_cap": False}

    logger.info("=" * 60)
    logger.info("WHALE CONTACT TRACE COMPLETE")
    logger.info("  Stage 1 (normal)   : total=%d success=%d failed=%d skipped_already_traced=%d",
                stage_normal.get("total", 0), stage_normal.get("success", 0),
                stage_normal.get("failed", 0), stage_normal.get("skipped_already_traced", 0))
    logger.info("  Stage 2 (advanced) : total=%d success=%d failed=%d skipped_already_traced=%d",
                stage_advanced.get("total", 0), stage_advanced.get("success", 0),
                stage_advanced.get("failed", 0), stage_advanced.get("skipped_already_traced", 0))
    logger.info("=" * 60)

    return {"stage_normal": stage_normal, "stage_advanced": stage_advanced}


if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(description="Whale contact trace — Tracerfy normal + advanced fallback")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--balance", action="store_true", help="Print account credit balance and exit")
    parser.add_argument("--force-retrace", dest="force_retrace", action="store_true",
                        help="Bypass the address dedup ledger (deliberate re-verification — may re-charge)")
    args = parser.parse_args()

    try:
        if args.balance:
            print(json.dumps(get_tracerfy_balance(), indent=2))
            sys.exit(0)

        run_whale_trace(limit=args.limit, dry_run=args.dry_run, force_retrace=args.force_retrace)
        sys.exit(0)
    except Exception as e:
        logger.error("Whale contact trace failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
