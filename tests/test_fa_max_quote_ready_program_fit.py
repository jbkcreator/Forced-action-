"""WP-8A Quote Ready — a scenario is persisted and carries a Lender Box program fit.

Client Q17 (2026-09-16): the scenario assembles the full picture including
"likely Backflip program fit". Verification for GRILL-DECISIONS.md G7
(built by PR #299; this test locks it in).
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import pytest
from sqlalchemy import text


def test_scoping_trigger_persists_scenario_with_program_fit(fresh_db, monkeypatch):
    from src.services.quote_ready import dossier

    property_id = fresh_db.execute(text("""
        SELECT f.property_id FROM financials f
        WHERE f.assessed_value_mkt IS NOT NULL
        LIMIT 1
    """)).scalar()
    if property_id is None:
        pytest.skip("no property with financials in this database")

    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'engaged', 'test') RETURNING person_id::text
    """)).scalar()
    opportunity_id = fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities (person_id, opportunity_type, current_stage, source)
            VALUES (:pid ::uuid, 'acquisition', 'new', 'test') RETURNING opportunity_id::text
        """),
        {"pid": person_id},
    ).scalar()
    fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunity_properties (opportunity_id, property_id, role, source)
            VALUES (:oid ::uuid, :prop, 'subject', 'test')
        """),
        {"oid": opportunity_id, "prop": property_id},
    )
    posted = []
    monkeypatch.setattr(dossier, "post_quote_ready_dossier", lambda session, rid: posted.append(rid))

    dossier.maybe_trigger_quote_ready_review(fresh_db, opportunity_id=opportunity_id)

    result_id = fresh_db.execute(
        text("SELECT result_id::text FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"),
        {"oid": opportunity_id},
    ).scalar()
    assert result_id is not None
    assert posted == [result_id]


def test_dossier_carries_program_fit_when_loan_is_known(fresh_db):
    from src.services.quote_ready import dossier

    person_id = fresh_db.execute(text("""
        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
        VALUES (gen_random_uuid(), 'engaged', 'test') RETURNING person_id::text
    """)).scalar()
    opportunity_id = fresh_db.execute(
        text("""
            INSERT INTO fa_max_opportunities (person_id, opportunity_type, current_stage, source)
            VALUES (:pid ::uuid, 'acquisition', 'new', 'test') RETURNING opportunity_id::text
        """),
        {"pid": person_id},
    ).scalar()
    result_id = fresh_db.execute(
        text("""
            INSERT INTO fa_max_quote_ready_results
                (opportunity_id, calculation_version, input_hash, status,
                 inputs, outputs, provenance, confidence, computed_by)
            VALUES (:oid ::uuid, 'v1', 'test-hash', 'computed',
                    '{"purchase_price": "250000", "rehab_estimate": "50000"}'::jsonb,
                    '{"proposed_loan": {"raw": "225000"}}'::jsonb,
                    '{}'::jsonb, '{}'::jsonb, 'test')
            RETURNING result_id::text
        """),
        {"oid": opportunity_id},
    ).scalar()

    row = dossier.assemble_dossier_row(fresh_db, result_id)

    assert row["lender_box"] is not None
    assert row["lender_box"].status in ("in_box", "out_of_box", "uncertain")
