"""
CDE-10 label layer tests.

Unit tests (pure): event mapping coverage + source_ref determinism.
Integration tests (fresh_db, real Postgres): promotion into deal_outcomes,
non-terminal skip, idempotency, county scoping, consumed_at stamping.

fresh_db wraps the real shared DB in a rollback transaction, so real
unconsumed candidates may be visible to promote_candidates — every assertion
here scopes to this file's own rows (large synthetic source_ids, own
source_ref / candidate id), never to aggregate ConnectorRunResult counts.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import text

from src.connectors.label_layer import (
    NON_TERMINAL_EVENTS,
    PIPELINE_STAGE_BY_EVENT,
    promote_candidates,
    source_ref_for,
)
from src.connectors.outcomes import (
    EVENT_TYPE_AUCTION_CANCELLED,
    EVENT_TYPE_AUCTION_REVERTED_TO_LENDER,
    EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
    EVENT_TYPE_QUALIFIED_SALE,
    EVENT_TYPE_TAX_DEED_REDEEMED,
    EVENT_TYPE_TAX_DEED_SOLD,
    OUTCOME_EVENT_TYPES,
    OutcomeCandidateData,
    upsert_outcome_candidate,
)
from src.core.models import Property


# ---------------------------------------------------------------------------
# Unit tests — no DB
# ---------------------------------------------------------------------------

class TestEventMapping:
    def test_won_events(self):
        for event in (EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY, EVENT_TYPE_TAX_DEED_SOLD,
                      EVENT_TYPE_QUALIFIED_SALE):
            assert PIPELINE_STAGE_BY_EVENT[event] == "closed_won"

    def test_lost_events(self):
        for event in (EVENT_TYPE_AUCTION_REVERTED_TO_LENDER, EVENT_TYPE_TAX_DEED_REDEEMED):
            assert PIPELINE_STAGE_BY_EVENT[event] == "closed_lost"

    def test_every_registered_event_type_is_accounted_for(self):
        # A promoted set and a skip set that together cover the whole event
        # vocabulary, with no overlap. Widening OUTCOME_EVENT_TYPES without
        # teaching the label layer the new event MUST fail here loudly.
        promoted = set(PIPELINE_STAGE_BY_EVENT)
        skipped = set(NON_TERMINAL_EVENTS)
        assert promoted | skipped == set(OUTCOME_EVENT_TYPES)
        assert not (promoted & skipped)


class TestSourceRef:
    def test_deterministic_32_char_hex(self):
        ref = source_ref_for("foreclosure_outcomes", "foreclosures", 42, date(2026, 6, 15))
        again = source_ref_for("foreclosure_outcomes", "foreclosures", 42, date(2026, 6, 15))
        assert ref == again
        assert len(ref) == 32
        int(ref, 16)  # valid hex

    def test_distinct_per_natural_key_component(self):
        base = source_ref_for("foreclosure_outcomes", "foreclosures", 42, date(2026, 6, 15))
        assert source_ref_for("tax_deed_outcomes", "foreclosures", 42, date(2026, 6, 15)) != base
        assert source_ref_for("foreclosure_outcomes", "tax_deed_auctions", 42, date(2026, 6, 15)) != base
        assert source_ref_for("foreclosure_outcomes", "foreclosures", 43, date(2026, 6, 15)) != base
        assert source_ref_for("foreclosure_outcomes", "foreclosures", 42, date(2026, 6, 16)) != base


# ---------------------------------------------------------------------------
# Integration tests — require real Postgres (fresh_db)
# ---------------------------------------------------------------------------

def _mk_property(session, parcel: str, *, county_id: str = "hillsborough") -> Property:
    p = Property(parcel_id=parcel, address=f"{parcel} TEST ST", county_id=county_id)
    session.add(p)
    session.flush()
    return p


def _stage(session, prop, *, event_type, source_id, county_id="hillsborough",
           source_type="foreclosure_outcomes", source_table="foreclosures",
           event_date=date(2026, 6, 15), amount=None):
    upsert_outcome_candidate(session, OutcomeCandidateData(
        property_id=prop.id,
        county_id=county_id,
        source_type=source_type,
        source_table=source_table,
        source_id=source_id,
        event_type=event_type,
        event_date=event_date,
        amount=amount,
    ))
    return session.execute(
        text("SELECT id FROM outcome_candidates WHERE source_type = :st "
             "AND source_table = :tbl AND source_id = :sid AND event_date = :ed"),
        {"st": source_type, "tbl": source_table, "sid": source_id, "ed": event_date},
    ).scalar_one()


def _outcome_rows(session, sref):
    return session.execute(
        text("SELECT subscriber_id, property_id, deal_size_bucket, deal_amount, "
             "deal_date, pipeline_stage, county_id, confidence_tier, outcome_source "
             "FROM deal_outcomes WHERE source_ref = :sref"),
        {"sref": sref},
    ).fetchall()


def _candidate_consumed(session, candidate_id):
    return session.execute(
        text("SELECT consumed_at FROM outcome_candidates WHERE id = :id"),
        {"id": candidate_id},
    ).scalar() is not None


class TestPGPromotion:
    def test_won_candidate_promotes_full_shape(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE10-LL-001")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                     source_id=9_000_101, amount=Decimal("230100.00"))

        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("foreclosure_outcomes", "foreclosures", 9_000_101, date(2026, 6, 15))
        rows = _outcome_rows(fresh_db, sref)
        assert len(rows) == 1
        row = rows[0]
        assert row.subscriber_id is None
        assert row.property_id == prop.id
        assert row.deal_size_bucket is None          # sale price is not a profit bucket
        assert row.deal_amount == Decimal("230100.00")
        assert row.deal_date == date(2026, 6, 15)
        assert row.pipeline_stage == "closed_won"
        assert row.county_id == "hillsborough"
        assert row.confidence_tier == "public_record_inferred"
        assert row.outcome_source == "foreclosure_outcomes"
        assert _candidate_consumed(fresh_db, cid)

    def test_lost_candidate_gets_skip_bucket(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE10-LL-002")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_TAX_DEED_REDEEMED,
                     source_id=9_000_102, source_type="tax_deed_outcomes",
                     source_table="tax_deed_auctions")

        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("tax_deed_outcomes", "tax_deed_auctions", 9_000_102, date(2026, 6, 15))
        rows = _outcome_rows(fresh_db, sref)
        assert len(rows) == 1
        assert rows[0].pipeline_stage == "closed_lost"
        assert rows[0].deal_size_bucket == "skip"    # established loss sentinel (B0-01)
        assert _candidate_consumed(fresh_db, cid)

    def test_non_terminal_consumed_without_promotion(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE10-LL-003")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_CANCELLED,
                     source_id=9_000_103)

        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("foreclosure_outcomes", "foreclosures", 9_000_103, date(2026, 6, 15))
        assert _outcome_rows(fresh_db, sref) == []
        assert _candidate_consumed(fresh_db, cid)

    def test_rerun_is_idempotent(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE10-LL-004")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_TAX_DEED_SOLD,
                     source_id=9_000_104, source_type="tax_deed_outcomes",
                     source_table="tax_deed_auctions", amount=Decimal("50000.00"))

        promote_candidates(fresh_db, "hillsborough")
        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("tax_deed_outcomes", "tax_deed_auctions", 9_000_104, date(2026, 6, 15))
        assert len(_outcome_rows(fresh_db, sref)) == 1
        assert _candidate_consumed(fresh_db, cid)

    def test_reconsumed_candidate_updates_in_place(self, fresh_db):
        # A connector correction (e.g. revised winning bid) re-stages the same
        # natural key; clearing consumed_at re-promotes and must UPDATE the
        # existing deal_outcomes row, never duplicate it.
        prop = _mk_property(fresh_db, "CDE10-LL-005")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                     source_id=9_000_105, amount=Decimal("100000.00"))
        promote_candidates(fresh_db, "hillsborough")

        fresh_db.execute(
            text("UPDATE outcome_candidates SET consumed_at = NULL, amount = :amt WHERE id = :id"),
            {"amt": Decimal("181100.00"), "id": cid},
        )
        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("foreclosure_outcomes", "foreclosures", 9_000_105, date(2026, 6, 15))
        rows = _outcome_rows(fresh_db, sref)
        assert len(rows) == 1
        assert rows[0].deal_amount == Decimal("181100.00")

    def test_corrected_amount_via_real_upsert_re_promotes(self, fresh_db):
        # Same scenario as test_reconsumed_candidate_updates_in_place, but
        # through the real upsert_outcome_candidate() path a connector
        # actually calls on re-run, not a manual consumed_at=NULL UPDATE.
        # Regression for PR #150 review finding #1: the conflict-update path
        # must itself clear consumed_at when a promoted field changes.
        prop = _mk_property(fresh_db, "CDE10-LL-007")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                     source_id=9_000_107, amount=Decimal("100000.00"))
        promote_candidates(fresh_db, "hillsborough")
        assert _candidate_consumed(fresh_db, cid)

        # Re-run through the real helper with a corrected amount — the source
        # connector re-staging its own row, not a test-only DB poke.
        _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
               source_id=9_000_107, amount=Decimal("181100.00"))
        assert not _candidate_consumed(fresh_db, cid)

        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("foreclosure_outcomes", "foreclosures", 9_000_107, date(2026, 6, 15))
        rows = _outcome_rows(fresh_db, sref)
        assert len(rows) == 1
        assert rows[0].deal_amount == Decimal("181100.00")
        assert _candidate_consumed(fresh_db, cid)

    def test_audit_only_change_via_real_upsert_does_not_reconsume(self, fresh_db):
        # counterparty/raw_status are audit-only and don't feed the promoted
        # DealOutcome shape -- a re-stage that only changes those must NOT
        # clear consumed_at, or the label layer would reprocess every row on
        # every connector re-run forever.
        prop = _mk_property(fresh_db, "CDE10-LL-008")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                     source_id=9_000_108, amount=Decimal("100000.00"))
        promote_candidates(fresh_db, "hillsborough")
        assert _candidate_consumed(fresh_db, cid)

        upsert_outcome_candidate(fresh_db, OutcomeCandidateData(
            property_id=prop.id, county_id="hillsborough",
            source_type="foreclosure_outcomes", source_table="foreclosures",
            source_id=9_000_108, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
            event_date=date(2026, 6, 15), amount=Decimal("100000.00"),
            counterparty="CHANGED THIRD PARTY LLC",
        ))
        assert _candidate_consumed(fresh_db, cid)

    def test_other_county_left_untouched(self, fresh_db):
        prop = _mk_property(fresh_db, "CDE10-LL-006", county_id="pinellas")
        cid = _stage(fresh_db, prop, event_type=EVENT_TYPE_QUALIFIED_SALE,
                     source_id=9_000_106, county_id="pinellas",
                     source_type="appraiser_sale_outcomes", source_table="financials")

        promote_candidates(fresh_db, "hillsborough")

        sref = source_ref_for("appraiser_sale_outcomes", "financials", 9_000_106, date(2026, 6, 15))
        assert _outcome_rows(fresh_db, sref) == []
        assert not _candidate_consumed(fresh_db, cid)

    def test_bad_row_does_not_poison_later_rows_in_same_run(self, fresh_db):
        """A DB-level error on one row (numeric overflow) must not abort the
        whole session — the per-row SAVEPOINT keeps a later good row
        promotable in the same run, and leaves the bad row unconsumed for
        retry rather than silently losing an already-logged promotion."""
        prop_bad = _mk_property(fresh_db, "CDE10-LL-007A")
        prop_good = _mk_property(fresh_db, "CDE10-LL-007B")

        # deal_outcomes.deal_amount is NUMERIC(12,2) (max ~9,999,999,999.99);
        # this amount overflows it at INSERT time — a genuine DB-level error,
        # not something the per-row try/except alone can isolate without a
        # SAVEPOINT around it.
        bad_cid = _stage(fresh_db, prop_bad, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                         source_id=9_000_107, amount=Decimal("99999999999.99"))
        good_cid = _stage(fresh_db, prop_good, event_type=EVENT_TYPE_AUCTION_SOLD_THIRD_PARTY,
                          source_id=9_000_108, amount=Decimal("50000.00"))

        promote_candidates(fresh_db, "hillsborough")

        good_sref = source_ref_for("foreclosure_outcomes", "foreclosures", 9_000_108, date(2026, 6, 15))
        assert len(_outcome_rows(fresh_db, good_sref)) == 1
        assert _candidate_consumed(fresh_db, good_cid)
        assert not _candidate_consumed(fresh_db, bad_cid)
