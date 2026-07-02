"""
Enrichment Background Loop (Sprint 4.5).

Polls for un-enriched Gold+ properties, seeds free contact data from the
voter registry, DNC-filters seeded phones, then routes remaining leads
through the paid skip-trace cascade (Tracerfy → BatchData → PDL).

Single-pass design: runs once and exits. Scheduled via cron every 10 minutes.

Cron: */10 7-23 * * 1-6  (Mon-Sat, 07:00-23:59 UTC, after scoring opens at 07:00)
"""
import logging
import sys
import uuid

from sqlalchemy import text

from src.core.database import get_db_context
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ── Self-heal: reset owners stuck in 'queued' from a prior crashed run ────────

def _reset_stuck_queued(session, county_id: str) -> int:
    """
    Reset owners whose contact_refresh_status has been 'queued' past their
    contact_next_refresh_at expiry (set to NOW()+30min when claimed). Prevents
    owners from being silently skipped forever after a crashed run.
    """
    result = session.execute(text("""
        UPDATE owners
           SET contact_refresh_status  = NULL,
               contact_next_refresh_at = NULL
         WHERE county_id               = :county_id
           AND contact_refresh_status  = 'queued'
           AND contact_next_refresh_at IS NOT NULL
           AND contact_next_refresh_at < NOW()
    """), {"county_id": county_id})
    count = result.rowcount
    if count:
        logger.warning(
            "[EnrichmentLoop] Reset %d stuck-queued owners for county=%s "
            "(likely from a prior crashed run)",
            count, county_id,
        )
    return count


# ── Candidate selection ───────────────────────────────────────────────────────

# Shared WHERE predicate used by both the read-only (dry-run) and atomic
# fetch-and-claim paths so the eligibility conditions stay in one place.
_CANDIDATE_WHERE = """
        WHERE o.county_id = :county_id
          AND (o.phone_1 IS NULL OR trim(o.phone_1) = '')
          AND (o.contact_refresh_status IS NULL
               OR o.contact_refresh_status = 'due')
          AND p.address IS NOT NULL AND trim(p.address) != ''
          AND p.zip     IS NOT NULL AND trim(p.zip)     != ''
          AND EXISTS (
              SELECT 1 FROM distress_scores ds
              WHERE ds.property_id  = o.property_id
                AND ds.lead_tier    IN ('Gold', 'Platinum', 'Ultra Platinum')
                AND ds.is_guess_lead = FALSE
          )
          AND NOT EXISTS (
              SELECT 1 FROM enriched_contacts ec
              WHERE ec.property_id = o.property_id
                AND ec.source IN ('tracerfy', 'batch_skip_tracing')
          )
"""


def _fetch_candidates(session, county_id: str, limit: int) -> list:
    """
    Read-only candidate fetch — used only for dry-run mode.

    Returns eligible Gold+ owners without claiming (writing) anything.
    """
    rows = session.execute(text(f"""
        SELECT DISTINCT ON (o.id)
            o.id          AS owner_id,
            o.property_id,
            o.county_id
        FROM owners o
        JOIN properties p ON p.id = o.property_id
        {_CANDIDATE_WHERE}
        ORDER BY o.id
        LIMIT :limit
    """), {"county_id": county_id, "limit": limit}).fetchall()
    return rows


def _fetch_and_claim_candidates(session, county_id: str, limit: int) -> list:
    """
    Atomically select and claim up to `limit` eligible Gold+ owners.

    Uses FOR UPDATE SKIP LOCKED inside the subquery so that two concurrent loop
    invocations can never read the same candidate set — the first writer's lock
    is visible to the second before either transaction commits.  Returns the
    claimed rows (already written; caller must commit).
    """
    rows = session.execute(text(f"""
        UPDATE owners
           SET contact_refresh_status  = 'queued',
               contact_next_refresh_at = NOW() + INTERVAL '30 minutes'
         WHERE id IN (
             SELECT o.id
             FROM owners o
             JOIN properties p ON p.id = o.property_id
             {_CANDIDATE_WHERE}
             ORDER BY o.id
             LIMIT :limit
             FOR UPDATE OF o SKIP LOCKED
         )
        RETURNING id AS owner_id, property_id, county_id
    """), {"county_id": county_id, "limit": limit}).fetchall()
    return rows


# ── Free voter-phone seeding ──────────────────────────────────────────────────

def _seed_voters(session, rows: list) -> set:
    """
    Seed owner.phone_1 from the voters table for owners with no current phone.

    Writes a normalized E.164 phone to owner.phone_1 and upserts an
    EnrichedContact(source='voters') row. Free — no API call. Returns the set
    of owner_ids that were successfully seeded.
    """
    from src.services.phone_utils import normalize as normalize_phone

    if not rows:
        return set()

    property_ids = [r.property_id for r in rows]
    owner_by_property = {r.property_id: r.owner_id for r in rows}

    # Pick the most recently updated voter phone per property
    voter_rows = session.execute(text("""
        SELECT DISTINCT ON (v.property_id)
            v.property_id,
            v.phone_1 AS voter_phone
        FROM voters v
        WHERE v.property_id = ANY(:pids)
          AND v.phone_1 IS NOT NULL
          AND trim(v.phone_1) != ''
        ORDER BY v.property_id, v.updated_at DESC
    """), {"pids": property_ids}).fetchall()

    seeded_ids: set = set()

    for vrow in voter_rows:
        normalized = normalize_phone(vrow.voter_phone)
        if not normalized:
            continue

        owner_id = owner_by_property[vrow.property_id]

        # Write normalized phone to owner
        session.execute(text("""
            UPDATE owners
               SET phone_1 = :phone
             WHERE id = :owner_id
               AND (phone_1 IS NULL OR trim(phone_1) = '')
        """), {"phone": normalized, "owner_id": owner_id})

        # Insert or update EnrichedContact(source='voters').
        # No unique constraint on (property_id, source), so check first.
        county_id_val = next(
            r.county_id for r in rows if r.property_id == vrow.property_id
        )
        existing = session.execute(text("""
            SELECT id FROM enriched_contacts
             WHERE property_id = :property_id AND source = 'voters'
             LIMIT 1
        """), {"property_id": vrow.property_id}).fetchone()

        if existing:
            session.execute(text("""
                UPDATE enriched_contacts
                   SET mobile_phone  = :phone,
                       match_success = TRUE,
                       enriched_at   = NOW()
                 WHERE id = :ec_id
            """), {"phone": normalized, "ec_id": existing.id})
        else:
            session.execute(text("""
                INSERT INTO enriched_contacts
                    (property_id, county_id, source, mobile_phone,
                     match_success, enriched_at)
                VALUES
                    (:property_id, :county_id, 'voters', :phone,
                     TRUE, NOW())
            """), {
                "property_id": vrow.property_id,
                "county_id":   county_id_val,
                "phone":       normalized,
            })

        seeded_ids.add(owner_id)

    if seeded_ids:
        logger.info(
            "[EnrichmentLoop] Voter-seeded %d owners (free)", len(seeded_ids)
        )
    return seeded_ids


# ── DNC pre-filter ────────────────────────────────────────────────────────────

def _filter_dnc(session, seeded_ids: set) -> set:
    """
    For voter-seeded owners (who now have a phone), check sms_opt_outs.
    Owners with a DNC-flagged phone are blocked from further use.

    Owners with no phone yet (going to cascade) are not checked here — DNC
    is handled inline by Tracerfy when it returns a number.

    Returns blocked_ids: set of owner_ids whose voter-seeded phone is DNC-flagged.
    """
    if not seeded_ids:
        return set()

    blocked_rows = session.execute(text("""
        SELECT o.id AS owner_id
          FROM owners o
          JOIN sms_opt_outs soo ON soo.phone = o.phone_1
         WHERE o.id = ANY(:owner_ids)
    """), {"owner_ids": list(seeded_ids)}).fetchall()

    blocked_ids = {r.owner_id for r in blocked_rows}
    if blocked_ids:
        logger.info(
            "[EnrichmentLoop] DNC-blocked %d voter-seeded owners", len(blocked_ids)
        )
    return blocked_ids


# ── Voter-lead completion (M2 equivalent without cascade) ─────────────────────

def _complete_voter_lead(session, owner_id: int, property_id: int) -> None:
    """
    For voter-seeded owners that skip the paid cascade, emit the same
    enrichment.completed event and prospect update that run_cascade()'s M2
    block emits — so Cora and the lead pack surface see the contact as resolved.
    """
    from src.services.prospect_service import (
        best_ec as _best_ec,
        get_or_create_prospect,
    )
    from src.services.event_bus import emit_event

    try:
        prospect_id_str = get_or_create_prospect(session, property_id)
        ec = _best_ec(session, property_id)

        session.execute(text("""
            UPDATE prospects
               SET contactability_state = 'contactable',
                   updated_at           = NOW()
             WHERE prospect_id = :pid
        """), {"pid": prospect_id_str})

        emit_event(
            session,
            event_type="enrichment.completed",
            actor="enrichment_loop",
            source_component="enrichment_background_loop",
            prospect_id=uuid.UUID(prospect_id_str),
            payload={
                "property_id":           property_id,
                "contactability_state":  "contactable",
                "enriched_contact_id":   ec.id if ec else None,
                "source":                "voters",
                "confidence":            float(ec.confidence) if ec and ec.confidence else 0.6,
                "total_cost_cents":      0,
            },
        )
    except Exception:
        logger.warning(
            "[EnrichmentLoop] Voter-lead M2 failed for owner_id=%d property_id=%d",
            owner_id, property_id, exc_info=True,
        )


# ── Main orchestration ────────────────────────────────────────────────────────

def run_once(
    county_id: str = "hillsborough",
    limit: int = 50,
    dry_run: bool = False,
) -> dict:
    """
    Single enrichment pass for one county. Called by cron every 10 minutes.

    Flow:
      1. Reset any owners stuck in 'queued' from a prior crashed run (skipped in dry-run)
      2+3. Atomically fetch + claim up to `limit` eligible Gold+ owners via
           FOR UPDATE SKIP LOCKED (dry-run uses read-only fetch, no claim)
      4. Seed phones from voters table (free)
      5. DNC-filter voter-seeded phones
      6. Voter-seeded + DNC-cleared  → emit enrichment.completed, mark 'fresh'
      7. Voter-seeded + DNC-blocked  → undo phone write, mark 'failed'
      8. No voter phone              → paid cascade (Tracerfy → BatchData → PDL)
      9. Update contact_refresh_status for cascade results
    """
    from src.services.skip_trace_waterfall import WaterfallStats
    from src.services.enrichment_router import EnrichmentRouter, LeadRecord
    from src.services.prospect_service import dedupe_after_cascade

    stats = {
        "county_id":         county_id,
        "candidates":        0,
        "voter_seeded":      0,
        "dnc_blocked":       0,
        "cascade_attempted": 0,
        "cascade_hits":      0,
        "cascade_misses":    0,
        "cascade_cost_cents": 0,
        "dry_run":           dry_run,
        "errors":            [],
    }

    owner_ids_claimed: list = []

    try:
        # ── Step 1: self-heal (skipped in dry-run — no writes allowed) ────────
        if not dry_run:
            with get_db_context() as session:
                _reset_stuck_queued(session, county_id)
                session.commit()

        # ── Steps 2+3: fetch candidates and claim atomically ──────────────────
        # dry-run uses the read-only path; normal mode atomically selects and
        # marks rows 'queued' in one statement (FOR UPDATE SKIP LOCKED) so two
        # concurrent invocations can never claim the same owner.
        with get_db_context() as session:
            if dry_run:
                rows = _fetch_candidates(session, county_id, limit)
            else:
                rows = _fetch_and_claim_candidates(session, county_id, limit)
                session.commit()

        if not rows:
            logger.info("[EnrichmentLoop] No candidates for county=%s", county_id)
            return stats

        stats["candidates"] = len(rows)
        owner_ids_claimed   = [] if dry_run else [r.owner_id for r in rows]

        logger.info(
            "[EnrichmentLoop] %d candidates in county=%s", len(rows), county_id
        )

        # ── Step 4: voter seeding (free) ──────────────────────────────────────
        seeded_ids: set = set()
        if not dry_run:
            with get_db_context() as session:
                seeded_ids = _seed_voters(session, rows)
                session.commit()

        # ── Step 5: DNC pre-filter on voter-seeded phones ─────────────────────
        with get_db_context() as session:
            dnc_blocked_ids = _filter_dnc(session, seeded_ids)

        stats["voter_seeded"] = len(seeded_ids - dnc_blocked_ids)
        stats["dnc_blocked"]  = len(dnc_blocked_ids)

        # ── Step 6: DNC-blocked voter-seeded owners — undo write, mark failed ─
        if dnc_blocked_ids and not dry_run:
            blocked_property_ids = [
                r.property_id for r in rows if r.owner_id in dnc_blocked_ids
            ]
            with get_db_context() as session:
                session.execute(text("""
                    UPDATE owners
                       SET phone_1                 = NULL,
                           contact_refresh_status  = 'failed',
                           contact_next_refresh_at = NULL
                     WHERE id = ANY(:ids)
                """), {"ids": list(dnc_blocked_ids)})
                # Supersede the EC row written during voter seeding
                session.execute(text("""
                    UPDATE enriched_contacts
                       SET superseded_at = NOW()
                     WHERE property_id = ANY(:pids)
                       AND source = 'voters'
                """), {"pids": blocked_property_ids})
                session.commit()

        # ── Step 7: voter-seeded + DNC-cleared — emit completion, mark fresh ──
        voter_cleared_ids = seeded_ids - dnc_blocked_ids
        if voter_cleared_ids and not dry_run:
            voter_cleared_property_ids = [
                r.property_id for r in rows if r.owner_id in voter_cleared_ids
            ]
            with get_db_context() as session:
                for row in rows:
                    if row.owner_id in voter_cleared_ids:
                        _complete_voter_lead(session, row.owner_id, row.property_id)
                session.execute(text("""
                    UPDATE owners
                       SET contact_refresh_status  = 'fresh',
                           contact_next_refresh_at = NULL,
                           contact_last_verified_at = NOW()
                     WHERE id = ANY(:ids)
                """), {"ids": list(voter_cleared_ids)})
                dedupe_after_cascade(session, voter_cleared_property_ids)
                session.commit()

        # ── Step 8: remaining owners (no voter phone) → budget-gated cascade ──
        # Task 6.2: routed through EnrichmentRouter instead of calling
        # run_cascade() directly, so paid-provider spend is checked against
        # the rolling spend/revenue ratio first. On the allowed path this
        # calls run_cascade() with the exact same arguments as before (a
        # thin wrapper, not a behavior change); on the blocked path it falls
        # back to the free voter cross-match per lead instead.
        cascade_owner_ids = [
            r.owner_id for r in rows if r.owner_id not in seeded_ids
        ]
        stats["cascade_attempted"] = len(cascade_owner_ids)

        cascade_stats = WaterfallStats()
        free_fallback_results: dict = {}
        if cascade_owner_ids and not dry_run:
            lead_records = [
                LeadRecord(property_id=r.property_id, owner_id=r.owner_id, county_id=county_id)
                for r in rows if r.owner_id in cascade_owner_ids
            ]
            with get_db_context() as session:
                batch_result = EnrichmentRouter().fetch_contact_profiles_batch(lead_records, session)
                session.commit()
            if batch_result["cascade_stats"] is not None:
                cascade_stats = batch_result["cascade_stats"]
            free_fallback_results = batch_result["free_results"]

        stats["cascade_hits"]       = cascade_stats.hits
        stats["cascade_misses"]     = cascade_stats.misses
        stats["cascade_cost_cents"] = cascade_stats.total_cost_cents
        stats["free_fallback_hits"] = sum(1 for r in free_fallback_results.values() if r["found"])

        # ── Step 9: mark cascade owners done (run_cascade doesn't touch ───────
        #            contact_refresh_status)
        if cascade_owner_ids and not dry_run:
            cascade_property_ids = [
                r.property_id for r in rows if r.owner_id in cascade_owner_ids
            ]
            with get_db_context() as session:
                hit_property_ids = {
                    row[0] for row in session.execute(text("""
                        SELECT DISTINCT property_id
                          FROM enriched_contacts
                         WHERE property_id = ANY(:pids)
                           AND match_success = TRUE
                           AND superseded_at IS NULL
                           AND source IN (
                               'tracerfy', 'batch_skip_tracing', 'idi', 'pdl'
                           )
                    """), {"pids": cascade_property_ids}).fetchall()
                }

                fresh_ids, failed_ids = [], []
                for row in rows:
                    if row.owner_id not in cascade_owner_ids:
                        continue
                    if row.property_id in hit_property_ids:
                        fresh_ids.append(row.owner_id)
                    else:
                        failed_ids.append(row.owner_id)

                if fresh_ids:
                    session.execute(text("""
                        UPDATE owners
                           SET contact_refresh_status   = 'fresh',
                               contact_next_refresh_at  = NULL,
                               contact_last_verified_at = NOW()
                         WHERE id = ANY(:ids)
                    """), {"ids": fresh_ids})
                if failed_ids:
                    session.execute(text("""
                        UPDATE owners
                           SET contact_refresh_status  = 'failed',
                               contact_next_refresh_at = NULL
                         WHERE id = ANY(:ids)
                    """), {"ids": failed_ids})
                session.commit()

        logger.info(
            "[EnrichmentLoop] DONE county=%s candidates=%d voter_seeded=%d "
            "dnc_blocked=%d cascade_hits=%d cascade_misses=%d cost_cents=%d",
            county_id,
            stats["candidates"],
            stats["voter_seeded"],
            stats["dnc_blocked"],
            stats["cascade_hits"],
            stats["cascade_misses"],
            stats["cascade_cost_cents"],
        )

    except Exception as exc:
        logger.error(
            "[EnrichmentLoop] Crashed for county=%s: %s", county_id, exc,
            exc_info=True,
        )
        stats["errors"].append(str(exc))

        # Reset all owners claimed in Step 3 back to NULL so the next run
        # or the nightly backstop can re-attempt them.
        if owner_ids_claimed:
            try:
                with get_db_context() as session:
                    session.execute(text("""
                        UPDATE owners
                           SET contact_refresh_status  = NULL,
                               contact_next_refresh_at = NULL
                         WHERE id = ANY(:ids)
                           AND contact_refresh_status = 'queued'
                    """), {"ids": owner_ids_claimed})
                    session.commit()
                logger.info(
                    "[EnrichmentLoop] Reset %d claimed owners after crash",
                    len(owner_ids_claimed),
                )
            except Exception as reset_exc:
                logger.error(
                    "[EnrichmentLoop] Failed to reset claimed owners: %s", reset_exc
                )

    return stats


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Enrichment Background Loop — voter seed + DNC filter + cascade"
    )
    parser.add_argument("county_id", nargs="?", default="hillsborough",
                        help="County to process (default: hillsborough)")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max owners per run (default: 50)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Select candidates and log without making any API calls or DB writes")
    args = parser.parse_args()

    result = run_once(
        county_id=args.county_id,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"  Candidates    : {result['candidates']}")
    print(f"  Voter seeded  : {result['voter_seeded']}")
    print(f"  DNC blocked   : {result['dnc_blocked']}")
    print(f"  Cascade tried : {result['cascade_attempted']}")
    print(f"  Cascade hits  : {result['cascade_hits']}")
    print(f"  Cost          : ${result['cascade_cost_cents'] / 100:.2f}")
    if result["errors"]:
        print(f"  Errors        : {result['errors']}")
        sys.exit(1)
    sys.exit(0)
