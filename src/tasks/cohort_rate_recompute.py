"""
M6 — Cohort contactability-rate recompute (spec §12.1 cohort fallback).

Run nightly via cron, before the Truth Engine verdict batch:

    45 7 * * * python -m src.tasks.cohort_rate_recompute

Aggregates rolling-30d contact attempts/successes per cohort
(`{cds_lead_tier}|{county_id}|{enrichment_source}`) into the cohort_rates table.
The cohort key is computed in-query — this job performs ZERO writes to prospects.

Pre-launch there are no contact attempts, so every contactability_rate is NULL;
the machinery is live and the numbers fill in once outbound contact data exists.
"""
import argparse

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Cohort key matches config.grading.compute_cohort_key: NULL/'' → 'unknown'.
_COHORT_SELECT = """
    WITH latest_ds AS (
        SELECT DISTINCT ON (property_id) property_id, lead_tier, county_id
        FROM distress_scores
        ORDER BY property_id, score_date DESC
    ),
    best_ec AS (
        SELECT DISTINCT ON (property_id) property_id, source
        FROM enriched_contacts
        WHERE match_success = TRUE AND superseded_at IS NULL
        ORDER BY property_id, enriched_at DESC
    ),
    prospect_cohort AS (
        SELECT
            p.contact_attempts,
            p.successful_contacts,
            COALESCE(NULLIF(ds.lead_tier, ''), 'unknown') || '|' ||
            COALESCE(NULLIF(ds.county_id, ''), 'unknown') || '|' ||
            COALESCE(NULLIF(ec.source, ''), 'unknown') AS cohort_key
        FROM prospects p
        LEFT JOIN latest_ds ds ON ds.property_id = p.property_id
        LEFT JOIN best_ec ec ON ec.property_id = p.property_id
        WHERE p.merged_into_id IS NULL
    )
    SELECT
        cohort_key,
        SUM(contact_attempts)                                                AS contact_attempts,
        SUM(successful_contacts)                                             AS successful_contacts,
        CASE WHEN SUM(contact_attempts) > 0
             THEN ROUND(SUM(successful_contacts)::numeric / SUM(contact_attempts), 4)
             ELSE NULL END                                                   AS contactability_rate,
        COUNT(*)                                                             AS sample_size
    FROM prospect_cohort
    GROUP BY cohort_key
"""


def run_cohort_rate_recompute(dry_run: bool = False) -> dict:
    """Recompute cohort_rates from current prospect attempt counts."""
    results = {"cohorts": 0, "dry_run": dry_run}
    with get_db_context() as db:
        if dry_run:
            count = db.execute(sa_text(f"SELECT COUNT(*) FROM ({_COHORT_SELECT}) q")).scalar() or 0
            results["cohorts"] = int(count)
        else:
            db.execute(sa_text(f"""
                INSERT INTO cohort_rates
                    (cohort_key, contact_attempts, successful_contacts,
                     contactability_rate, sample_size, computed_at)
                {_COHORT_SELECT}
                ON CONFLICT (cohort_key) DO UPDATE SET
                    contact_attempts    = EXCLUDED.contact_attempts,
                    successful_contacts = EXCLUDED.successful_contacts,
                    contactability_rate = EXCLUDED.contactability_rate,
                    sample_size         = EXCLUDED.sample_size,
                    computed_at         = NOW()
            """))
            results["cohorts"] = db.execute(sa_text("SELECT COUNT(*) FROM cohort_rates")).scalar() or 0

    logger.info("[CohortRates] recompute complete cohorts=%d dry_run=%s", results["cohorts"], dry_run)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recompute contactability cohort rates")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run_cohort_rate_recompute(dry_run=args.dry_run))
