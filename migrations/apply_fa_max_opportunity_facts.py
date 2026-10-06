"""
FA Max Opportunity Facts + Qualification Decisions (WP-T3-7).

Creates:
  fa_max_opportunity_facts      — one typed-column row per opportunity; tracks
                                   property/project facts and their provenance.
  fa_max_qualification_decisions — append-only audit row per evaluation run.

Also widens fa_max_exceptions_alert_queue.status CHECK to include 'cancelled',
enabling cancel_pending_alert() in src/services/relay/exceptions_alert_queue.py.
This widening was a cross-team dependency (WP-T2-1 relay infra owner) that
T3-7 delivers to unblock gap-hash supersession — see that module's docstring.

Idempotent: safe to re-run.

Run:
  PYTHONPATH=. python migrations/apply_fa_max_opportunity_facts.py
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context

_DDL = [
    # ------------------------------------------------------------------
    # 1. fa_max_opportunity_facts — one row per opportunity (1:1)
    #
    # Typed columns only — no generic key/value store. Every known fact gets
    # its own constrained column. facts_provenance JSONB holds only metadata:
    # {fact_name: {source, set_by, set_at}}. Client overrides always win over
    # enrichment; only effective changes (new value != stored value) bump
    # facts_revision. See src/services/fa_max_qualification.py for the setter
    # contract.
    #
    # COMPLIANCE BOUNDARY: no borrower financial data columns exist or may be
    # added here. Specifically forbidden: credit_score, income, bank_statement,
    # tax_return, ssn. A schema change adding any of those violates SOT.md
    # Part 1 and is rejected at code review regardless of source.
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_opportunity_facts (
        opportunity_id   UUID        PRIMARY KEY
                         REFERENCES fa_max_opportunities(opportunity_id),

        -- Revision counter: bumped by 1 on every effective fact change.
        -- CAS-keyed by the qualification worker on the claimed revision value.
        facts_revision   INTEGER     NOT NULL DEFAULT 0,

        -- Property link (nullable: not all scenarios reference a parceled property)
        property_id      INTEGER     REFERENCES properties(id),

        -- ── Purchase basis — QuoteReadyInput fallback chain ────────────────
        purchase_price       NUMERIC(14,2),
        estimated_value      NUMERIC(14,2),
        assessed_value_mkt   NUMERIC(14,2),
        last_sale_price      NUMERIC(14,2),

        -- ── Rehab ───────────────────────────────────────────────────────────
        rehab_estimate   NUMERIC(14,2),
        rehab_source     VARCHAR(30)
            CHECK (rehab_source IS NULL OR rehab_source IN ('job_estimator', 'override')),
        rehab_confidence VARCHAR(10)
            CHECK (rehab_confidence IS NULL OR rehab_confidence IN ('high', 'medium', 'low')),

        -- ── ARV ─────────────────────────────────────────────────────────────
        arv              NUMERIC(14,2),
        -- Structured identifier, not free prose — lowercase identifier
        -- characters only (matches QuoteReadyInput's own arv_source shape:
        -- "legacy_financial.arv", a WP-8B comp-range/manual-override tag).
        -- A plain free-text column here was another unrestricted-text path
        -- around the schema boundary alongside current_use (code-review
        -- finding, third round, 2026-09).
        arv_source       VARCHAR(80)
            CHECK (arv_source IS NULL OR (
                arv_source ~ '^[a-z0-9_.:-]+$'
                AND arv_source !~* '(credit_score|income|bank_statement|tax_return|ssn|fico|dti|debt_to_income)'
            )),
        arv_confidence   VARCHAR(10)
            CHECK (arv_confidence IS NULL OR arv_confidence IN ('high', 'medium', 'low')),

        -- ── Scenario-type-specific facts ────────────────────────────────────
        -- Populated per opportunity_type; checklist governs which are required.
        -- Checklist content requires Dev 4 (WP-8A/8B) sign-off — see
        -- config/fa_max_qualification.py comments.
        expected_exit_strategy VARCHAR(30)
            CHECK (expected_exit_strategy IS NULL OR expected_exit_strategy IN
                   ('sale','rent','dscr','refinance','unknown')),
        -- Fixed vocabulary, not free text: current_use is not consumed by
        -- src/services/quote_ready (QuoteReadyInput has no such field) and
        -- a free-text column here was an unrestricted-text path around the
        -- "property/project facts only" schema boundary — extra='forbid' on
        -- the admin request model blocks unknown FIELD NAMES but says
        -- nothing about what goes inside an allowed field's VALUE
        -- (code-review finding, second round, 2026-09). See
        -- config.fa_max_qualification.CurrentUse for the same set enforced
        -- at the Pydantic layer.
        current_use VARCHAR(60)
            CHECK (current_use IS NULL OR current_use IN
                   ('single_family','multi_family_2_4','multi_family_5plus',
                    'condo','townhouse','vacant_land','commercial','mixed_use','other')),
        existing_sqft          INTEGER CHECK (existing_sqft IS NULL OR existing_sqft >= 0),

        -- ── Provenance metadata (not fact values) ───────────────────────────
        -- {fact_name: {source: 'client'|'enrichment'|'auto', set_by, set_at}}
        facts_provenance JSONB NOT NULL DEFAULT '{}'::jsonb,

        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_opp_facts_rev ON fa_max_opportunity_facts (opportunity_id, facts_revision)",

    # Idempotent narrow+constrain for a table created before current_use
    # became a fixed vocabulary (code-review finding, second round, 2026-09).
    # Safe to re-run: DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT in a DO
    # block, same pattern as the status-widening block below. VARCHAR(60)
    # already accommodates every value in the vocabulary, so no data-loss
    # risk narrowing the declared max length would carry; the column stays
    # VARCHAR(60) and this only adds the CHECK.
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_opportunity_facts
            DROP CONSTRAINT IF EXISTS fa_max_opportunity_facts_current_use_check;
    EXCEPTION WHEN undefined_object THEN NULL;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_opportunity_facts
            ADD CONSTRAINT fa_max_opportunity_facts_current_use_check
            CHECK (current_use IS NULL OR current_use IN
                   ('single_family','multi_family_2_4','multi_family_5plus',
                    'condo','townhouse','vacant_land','commercial','mixed_use','other'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    $$
    """,

    # Same idempotent narrow+constrain for arv_source, added a round after
    # current_use (code-review finding, third round, 2026-09). Widened a
    # round later to also reject forbidden-financial-term substrings
    # (code-review finding, fifth round, 2026-09) — the character-shape
    # check alone accepted an identifier-shaped value like
    # "borrower_income:123". Mirrors config.FORBIDDEN_FINANCIAL_TERMS;
    # if that set changes, this constraint must be updated to match.
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_opportunity_facts
            DROP CONSTRAINT IF EXISTS fa_max_opportunity_facts_arv_source_check;
    EXCEPTION WHEN undefined_object THEN NULL;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_opportunity_facts
            ADD CONSTRAINT fa_max_opportunity_facts_arv_source_check
            CHECK (arv_source IS NULL OR (
                arv_source ~ '^[a-z0-9_.:-]+$'
                AND arv_source !~* '(credit_score|income|bank_statement|tax_return|ssn|fico|dti|debt_to_income)'
            ));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    $$
    """,

    # ------------------------------------------------------------------
    # 2. fa_max_qualification_decisions — append-only per evaluation
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS fa_max_qualification_decisions (
        decision_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
        opportunity_id    UUID        NOT NULL
                          REFERENCES fa_max_opportunities(opportunity_id),
        facts_revision    INTEGER     NOT NULL,
        checklist_version VARCHAR(40) NOT NULL,
        -- 'sufficient_pending_contract' added a round after initial creation
        -- (code-review finding, fourth round, 2026-09) — see
        -- config.PENDING_CONTRACT_APPROVAL_TYPES.
        verdict           VARCHAR(30) NOT NULL
            CHECK (verdict IN ('sufficient', 'insufficient', 'pending_enrichment',
                                'sufficient_pending_contract')),
        gaps              JSONB       NOT NULL DEFAULT '[]'::jsonb,
        -- sha256 of sorted (fact_key, reason) tuples; used to dedup EXCEPTIONS alerts
        gap_content_hash  VARCHAR(64),
        decided_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        decided_by        VARCHAR(80)  NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_fa_max_qd_opp_decided ON fa_max_qualification_decisions (opportunity_id, decided_at DESC)",
    # Partial index for fast latest-decision lookup
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_qd_opp_rev ON fa_max_qualification_decisions
        (opportunity_id, facts_revision, checklist_version)
    """,

    # Idempotent widen for a table created before 'sufficient_pending_contract'
    # existed (code-review finding, fourth round, 2026-09). MUST drop both
    # possible constraint names: the original inline CHECK on the very first
    # CREATE TABLE had no explicit name, so Postgres auto-named it
    # '{table}_{column}_check'; only a table created AFTER this migration
    # already had the explicit 'ck_fa_max_qual_dec_verdict' name. Dropping
    # only the explicit name left the auto-named original constraint (still
    # 3 values) silently active alongside the new 4-value one, blocking
    # every insert of the new verdict (code-review finding, SIXTH round,
    # 2026-09 — caught only by a live DB test actually inserting the new
    # value, not by the migration "applying successfully").
    "ALTER TABLE fa_max_qualification_decisions ALTER COLUMN verdict TYPE VARCHAR(30)",
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_qualification_decisions
            DROP CONSTRAINT IF EXISTS ck_fa_max_qual_dec_verdict;
    EXCEPTION WHEN undefined_object THEN NULL;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_qualification_decisions
            DROP CONSTRAINT IF EXISTS fa_max_qualification_decisions_verdict_check;
    EXCEPTION WHEN undefined_object THEN NULL;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_qualification_decisions
            ADD CONSTRAINT ck_fa_max_qual_dec_verdict
            CHECK (verdict IN ('sufficient', 'insufficient', 'pending_enrichment',
                                'sufficient_pending_contract'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    $$
    """,

    # ------------------------------------------------------------------
    # 3. Widen fa_max_exceptions_alert_queue.status to allow 'cancelled'
    #    so cancel_pending_alert() in exceptions_alert_queue.py can mark
    #    superseded gap-hash alerts rather than leaving them pending.
    # ------------------------------------------------------------------
    # Drop the old named constraint and add the widened one. The DROP is
    # idempotent via DO $$ ... EXCEPTION ... $$ (the constraint may not
    # exist on databases created from the initial migration before this
    # revision, where the CHECK was an inline column-level constraint with
    # a system-generated name — we catch and ignore both forms).
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_exceptions_alert_queue
            DROP CONSTRAINT IF EXISTS fa_max_exceptions_alert_queue_status_check;
    EXCEPTION WHEN undefined_object THEN NULL;
    END;
    $$
    """,
    """
    DO $$
    BEGIN
        ALTER TABLE fa_max_exceptions_alert_queue
            ADD CONSTRAINT fa_max_exceptions_alert_queue_status_check
            CHECK (status IN ('pending', 'sent', 'cancelled'));
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    $$
    """,
]


def main() -> int:
    with get_db_context() as db:
        for stmt in _DDL:
            db.execute(text(stmt))
        db.commit()

        tables = [
            "fa_max_opportunity_facts",
            "fa_max_qualification_decisions",
        ]
        for t in tables:
            count = db.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t}: {count} rows")

        # Verify cancelled status is now accepted
        db.execute(text(
            "SELECT 1 FROM fa_max_exceptions_alert_queue "
            "WHERE status = 'pending' LIMIT 0"  # just validates column exists
        ))
        print("  fa_max_exceptions_alert_queue.status widened to include 'cancelled'")

    print("Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
