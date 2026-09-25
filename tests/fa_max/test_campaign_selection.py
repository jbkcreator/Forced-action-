"""WP-T3-4 — Campaign Selection Agent integration tests.

Real-Postgres tests via the `fresh_db` fixture (tests/conftest.py) — skipped
automatically when DATABASE_URL is not reachable. Requires
migrations/apply_fa_max_wp_t3_4_campaign_selection.py to have already been
applied to that database (idempotent, safe to re-run).
"""
from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from config import fa_max_campaigns as cfg
from src.services.fa_max_campaigns import selection
from src.services.fa_max_campaigns.blocks import enrollment_block_reason


@pytest.fixture(autouse=True)
def _fresh_backflip_feed(fresh_db):
    """enrollment_block_reason() now checks is_backflip_suppressed(), which
    fails closed ("backflip_feed_unavailable") when fa_max_backflip_campaign_
    feed has no row — exactly like every other FA Max send-gate test already
    seeds (see tests/test_fa_max_wp2.py). Autouse so every test below starts
    from a "feed is current, nobody flagged" baseline unless it says
    otherwise."""
    fresh_db.execute(
        text(
            "INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) VALUES (1, now()) "
            "ON CONFLICT (id) DO UPDATE SET last_success_at = now()"
        )
    )
    fresh_db.flush()
    yield


# ── Fixture helpers (mirrors tests/test_fa_max_wp5b_profile.py's style) ────

def _fresh_person(session, *, email: str | None = None, phone: str | None = None) -> str:
    row = session.execute(
        text(
            "INSERT INTO fa_max_persons (lifecycle_state, source, source_reference, email, phone) "
            "VALUES ('identified', 'test', :ref, :email, :phone) "
            "RETURNING person_id::text"
        ),
        {"ref": f"Test Person {uuid.uuid4().hex[:8]}", "email": email, "phone": phone},
    ).scalar()
    session.flush()
    return row


def _fresh_buyer_entity(session, name: str = "Test Buyer LLC") -> int:
    eid = session.execute(
        text(
            "INSERT INTO buyer_entities (canonical_name, entity_type, confidence_score, "
            "verification_status, total_purchase_count, total_cash_volume) "
            "VALUES (:name, 'LLC', 80, 'verified', 0, 0) RETURNING id"
        ),
        {"name": name},
    ).scalar()
    session.flush()
    return eid


def _link_person_to_entity(session, person_id: str, buyer_entity_id: int) -> None:
    session.execute(
        text(
            "INSERT INTO fa_max_person_profiles (person_id, buyer_entity_id) "
            "VALUES (CAST(:pid AS uuid), :beid)"
        ),
        {"pid": person_id, "beid": buyer_entity_id},
    )
    session.flush()


def _insert_property(session, county_id: str = "hillsborough") -> int:
    parcel_id = f"TEST-{uuid.uuid4().hex[:8]}"
    pid = session.execute(
        text(
            "INSERT INTO properties (parcel_id, source_row_hash, needs_rescore, county_id, "
            "address, city, created_at, updated_at) "
            "VALUES (:pid, :hash, false, :county, '123 Test St', 'Tampa', NOW(), NOW()) "
            "RETURNING id"
        ),
        {"pid": parcel_id, "hash": uuid.uuid4().hex, "county": county_id},
    ).scalar()
    session.flush()
    return pid


def _insert_deed(
    session, *, property_id: int, grantee: str, sale_price: float,
    mortgage_amount: float | None = None, record_date: date | None = None,
) -> int:
    record_date = record_date or date.today()
    deed_id = session.execute(
        text(
            "INSERT INTO deeds (property_id, instrument_number, grantee, record_date, "
            "sale_price, mortgage_amount, deed_type, sale_qualified) "
            "VALUES (:pid, :instr, :grantee, :rdate, :price, :mortgage, 'Warranty Deed', true) "
            "RETURNING id"
        ),
        {
            "pid": property_id, "instr": f"INST-{uuid.uuid4().hex[:8]}", "grantee": grantee,
            "rdate": record_date, "price": sale_price, "mortgage": mortgage_amount,
        },
    ).scalar()
    session.flush()
    return deed_id


def _link_deed_to_entity(session, buyer_entity_id: int, deed_id: int) -> None:
    session.execute(
        text(
            "INSERT INTO buyer_entity_links (buyer_entity_id, source_table, source_id, "
            "match_confidence, match_method) "
            "VALUES (:eid, 'deeds', :deed_id, 90, 'exact_name_address') "
            "ON CONFLICT (source_table, source_id) DO NOTHING"
        ),
        {"eid": buyer_entity_id, "deed_id": deed_id},
    )
    session.flush()


def _seed_exit_desk_bought_row(
    session, *, property_id: int, entity_name: str, months_old: int = 10,
) -> None:
    """Exit Desk reads config.fa_max_campaigns.LENDING_MORTGAGE_RECORDS_TABLE
    (the bought-data feed, not yet integrated — plan Gap C/D-1). A session-
    scoped temp table with the agreed shape stands in for it in tests."""
    session.execute(
        text(
            "CREATE TEMP TABLE IF NOT EXISTS lending_mortgage_records ("
            "property_id INT, borrower_entity TEXT, lender_name TEXT, lender_type TEXT, "
            "loan_amount NUMERIC, recording_date DATE, satisfied_bool BOOLEAN, "
            "county_id TEXT, state TEXT)"
        )
    )
    session.execute(
        text(
            "INSERT INTO lending_mortgage_records "
            "(property_id, borrower_entity, lender_name, lender_type, loan_amount, "
            " recording_date, satisfied_bool, county_id, state) "
            "VALUES (:pid, :entity, 'Test Lender', 'hard_money', 350000, "
            " :rdate, false, 'hillsborough', 'FL')"
        ),
        {"pid": property_id, "entity": entity_name, "rdate": date.today() - timedelta(days=months_old * 30)},
    )
    session.flush()


def _cash_buyer_person(session, *, email: str = "buyer@example.com") -> tuple[str, int]:
    """A person resolved to a buyer_entity with one clean cash-purchase deed
    (entity grantee, no mortgage, no later mortgage recorded) — should
    qualify for Capital Desk Loop's recent-cash-buyer rule."""
    person_id = _fresh_person(session, email=email)
    entity_id = _fresh_buyer_entity(session)
    _link_person_to_entity(session, person_id, entity_id)
    prop_id = _insert_property(session)
    deed_id = _insert_deed(
        session, property_id=prop_id, grantee="Test Buyer LLC", sale_price=250000,
        mortgage_amount=None, record_date=date.today() - timedelta(days=5),
    )
    _link_deed_to_entity(session, entity_id, deed_id)
    return person_id, prop_id


def _load_minimal_sequence(session, campaign_key: str, *, channel: str = "email") -> None:
    session.execute(
        text(
            "INSERT INTO fa_max_campaign_sequence_steps "
            "(campaign_key, sequence_version, step, days_after_previous, channel, subject, body_template, loaded_by) "
            "VALUES (:ck, 1, 1, 0, :channel, 'Subject', 'Body {{first_name|there}}', 'test')"
        ),
        {"ck": campaign_key, "channel": channel},
    )
    session.flush()


# ── Eligibility + enrollment ─────────────────────────────────────────────────

class TestCapitalDeskLoopEligibility:
    def test_cash_buyer_becomes_eligible_candidate(self, fresh_db):
        from src.services.fa_max_campaigns import eligibility

        person_id, _ = _cash_buyer_person(fresh_db)
        result = eligibility.capital_desk_loop_candidates(fresh_db)
        assert any(c.person_id == person_id for c in result.candidates)

    def test_no_mortgage_within_72h_blocks_cash_buyer(self, fresh_db):
        from src.services.fa_max_campaigns import eligibility

        person_id = _fresh_person(fresh_db, email="notcash@example.com")
        entity_id = _fresh_buyer_entity(fresh_db)
        _link_person_to_entity(fresh_db, person_id, entity_id)
        prop_id = _insert_property(fresh_db)
        purchase_date = date.today() - timedelta(days=5)
        deed_id = _insert_deed(
            fresh_db, property_id=prop_id, grantee="Test Buyer LLC", sale_price=250000,
            mortgage_amount=None, record_date=purchase_date,
        )
        _link_deed_to_entity(fresh_db, entity_id, deed_id)
        # A second deed on the same property with a mortgage, recorded the
        # next day — disqualifies the "recent cash buyer" rule.
        _insert_deed(
            fresh_db, property_id=prop_id, grantee="Test Buyer LLC", sale_price=250000,
            mortgage_amount=200000, record_date=purchase_date + timedelta(days=1),
        )
        result = eligibility.capital_desk_loop_candidates(fresh_db)
        assert not any(c.person_id == person_id for c in result.candidates)

    def test_unresolved_property_counted_not_enrolled(self, fresh_db):
        """A deed with no linked fa_max_person_profiles row (identity
        resolution hasn't run yet) is counted, never enrolled — plan
        Section 6.3."""
        from src.services.fa_max_campaigns import eligibility

        entity_id = _fresh_buyer_entity(fresh_db, name="Unresolved Buyer LLC")
        prop_id = _insert_property(fresh_db)
        deed_id = _insert_deed(
            fresh_db, property_id=prop_id, grantee="Unresolved Buyer LLC", sale_price=300000,
            mortgage_amount=None, record_date=date.today() - timedelta(days=2),
        )
        _link_deed_to_entity(fresh_db, entity_id, deed_id)
        # No fa_max_person_profiles row links this entity to any person.
        result = eligibility.capital_desk_loop_candidates(fresh_db)
        assert result.unresolved_count >= 1
        assert result.candidates == []

    def test_enrollment_sweep_creates_active_enrollment(self, fresh_db):
        _cash_buyer_person(fresh_db)
        summary = selection.run_enrollment_sweep(fresh_db, dry_run=False)
        assert summary.enrolled.get(cfg.CAMPAIGN_CAPITAL_DESK_LOOP, 0) >= 1

        row = fresh_db.execute(
            text(
                "SELECT status, audience, trigger_type FROM fa_max_campaign_enrollments "
                "WHERE campaign_key = :ck ORDER BY enrolled_at DESC LIMIT 1"
            ),
            {"ck": cfg.CAMPAIGN_CAPITAL_DESK_LOOP},
        ).mappings().fetchone()
        assert row["status"] == "active"
        assert row["audience"] == "investor"
        assert row["trigger_type"] == "cash_purchase"

    def test_dry_run_writes_nothing(self, fresh_db):
        _cash_buyer_person(fresh_db)
        selection.run_enrollment_sweep(fresh_db, dry_run=True)
        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_campaign_enrollments")
        ).scalar_one()
        assert count == 0

    def test_fully_suppressed_contact_never_enrolled(self, fresh_db):
        """Audit fix: a candidate whose only channel is already suppressed
        (email-only person, hard-bounced) must never be freshly enrolled —
        they would just skip-advance through the whole sequence with zero
        real touches and burn a cooldown slot for nothing (plan Section
        6.4, point 2 — checked at enrollment time, not just per-touch)."""
        person_id, _ = _cash_buyer_person(fresh_db, email="bounced@example.com")
        fresh_db.execute(
            text("INSERT INTO email_opt_outs (email, source, opted_out_at) VALUES ('bounced@example.com', 'hard_bounce', NOW())")
        )
        fresh_db.flush()

        summary = selection.run_enrollment_sweep(fresh_db)
        assert summary.enrolled.get(cfg.CAMPAIGN_CAPITAL_DESK_LOOP, 0) == 0

        count = fresh_db.execute(
            text("SELECT COUNT(*) FROM fa_max_campaign_enrollments WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        assert count == 0

    def test_partially_suppressed_contact_still_enrolls_on_clean_channel(self, fresh_db):
        """Control case: a person with BOTH email (suppressed) and phone
        (clean) must still enroll — the per-touch gate correctly skips the
        suppressed channel rather than the enrollment being blocked outright."""
        person_id = _fresh_person(fresh_db, email="dual@example.com", phone="+15557654321")
        entity_id = _fresh_buyer_entity(fresh_db, name="Dual Channel LLC")
        _link_person_to_entity(fresh_db, person_id, entity_id)
        prop_id = _insert_property(fresh_db)
        deed_id = _insert_deed(
            fresh_db, property_id=prop_id, grantee="Dual Channel LLC", sale_price=250000,
            mortgage_amount=None, record_date=date.today() - timedelta(days=5),
        )
        _link_deed_to_entity(fresh_db, entity_id, deed_id)
        fresh_db.execute(
            text("INSERT INTO email_opt_outs (email, source, opted_out_at) VALUES ('dual@example.com', 'hard_bounce', NOW())")
        )
        fresh_db.flush()

        summary = selection.run_enrollment_sweep(fresh_db)
        assert summary.enrolled.get(cfg.CAMPAIGN_CAPITAL_DESK_LOOP, 0) == 1

        status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_enrollments WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        assert status == "active"


class TestOneEnrollmentPerPerson:
    def test_second_active_enrollment_rejected_by_db(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="dup@example.com")
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), 'capital_desk_loop', 'investor', 1, 'active', 'cash_purchase', 'test')"
            ),
            {"pid": person_id},
        )
        fresh_db.flush()
        with pytest.raises(IntegrityError):
            fresh_db.execute(
                text(
                    "INSERT INTO fa_max_campaign_enrollments "
                    "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                    "VALUES (CAST(:pid AS uuid), 'rescue_circuit', 'partner', 1, 'active', 'rescue_circuit_partner', 'test')"
                ),
                {"pid": person_id},
            )
            fresh_db.flush()


class TestBlocks:
    def test_repeat_borrower_never_enrolled(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="repeat@example.com")
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_opportunities (person_id, opportunity_type, outcome, source) "
                "VALUES (CAST(:pid AS uuid), 'acquisition', 'funded', 'deed')"
            ),
            {"pid": person_id},
        )
        fresh_db.flush()
        block = enrollment_block_reason(fresh_db, person_id=person_id)
        assert block.blocked is True
        assert block.pause is False
        assert block.reason == "repeat_borrower"

    def test_open_deal_pauses_not_blocks(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="open-deal@example.com")
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_opportunities (person_id, opportunity_type, outcome, source) "
                "VALUES (CAST(:pid AS uuid), 'acquisition', 'open', 'deed')"
            ),
            {"pid": person_id},
        )
        fresh_db.flush()
        block = enrollment_block_reason(fresh_db, person_id=person_id)
        assert block.blocked is True
        assert block.pause is True
        assert block.reason == "open_opportunity"

    def test_clean_person_is_not_blocked(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="clean@example.com")
        block = enrollment_block_reason(fresh_db, person_id=person_id)
        assert block.blocked is False


class TestCancelEnrollments:
    def test_cancel_ends_enrollment_and_touches(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="optout@example.com")
        enrollment_id = fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), 'capital_desk_loop', 'investor', 1, 'active', 'cash_purchase', 'test') "
                "RETURNING enrollment_id::text"
            ),
            {"pid": person_id},
        ).scalar()
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_touches "
                "(enrollment_id, step, channel, due_at, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 1, 'email', NOW(), :idem)"
            ),
            {"eid": enrollment_id, "idem": uuid.uuid4().hex},
        )
        fresh_db.flush()

        changed = selection.cancel_enrollments(fresh_db, person_id=person_id, reason="opt_out_email")
        assert changed == 1

        row = fresh_db.execute(
            text("SELECT status, end_reason FROM fa_max_campaign_enrollments WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).mappings().fetchone()
        assert row["status"] == "cancelled"
        assert row["end_reason"] == "opt_out_email"

        touch_status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_touches WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).scalar_one()
        assert touch_status == "cancelled"

        event = fresh_db.execute(
            text("SELECT event FROM fa_max_campaign_enrollment_events WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).scalars().all()
        assert "cancelled" in event

    def test_cancel_with_no_enrollment_is_a_noop(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="never-enrolled@example.com")
        changed = selection.cancel_enrollments(fresh_db, person_id=person_id, reason="opt_out_email")
        assert changed == 0


class TestDueStepSweep:
    def _enroll_with_touch(self, session, *, channel: str, campaign_key: str = "capital_desk_loop", **person_kwargs):
        person_id = _fresh_person(session, **person_kwargs)
        _load_minimal_sequence(session, campaign_key, channel=channel)
        enrollment_id = session.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), :ck, 'investor', 1, 'active', 'cash_purchase', 'test') "
                "RETURNING enrollment_id::text"
            ),
            {"pid": person_id, "ck": campaign_key},
        ).scalar()
        touch_id = session.execute(
            text(
                "INSERT INTO fa_max_campaign_touches "
                "(enrollment_id, step, channel, due_at, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 1, :channel, NOW() - interval '1 minute', :idem) "
                "RETURNING touch_id"
            ),
            {"eid": enrollment_id, "channel": channel, "idem": uuid.uuid4().hex},
        ).scalar()
        session.flush()
        return person_id, enrollment_id, touch_id

    def test_email_step_without_consent_is_held(self, fresh_db):
        person_id, _, touch_id = self._enroll_with_touch(fresh_db, channel="email", email="hold@example.com")
        summary = selection.process_due_touches(fresh_db)
        assert summary["held"] == 1

        status = fresh_db.execute(
            text("SELECT status, status_reason FROM fa_max_campaign_touches WHERE touch_id = :tid"),
            {"tid": touch_id},
        ).mappings().fetchone()
        assert status["status"] == "held"
        assert status["status_reason"] == "no_email_consent"

    def test_sms_step_without_consent_is_skipped_and_never_writes_consent(self, fresh_db):
        person_id, _, touch_id = self._enroll_with_touch(fresh_db, channel="sms", phone="+15551234567")
        before = fresh_db.execute(text("SELECT COUNT(*) FROM fa_max_person_consent")).scalar_one()
        summary = selection.process_due_touches(fresh_db)
        after = fresh_db.execute(text("SELECT COUNT(*) FROM fa_max_person_consent")).scalar_one()

        assert summary["skipped"] == 1
        assert after == before  # never writes consent (plan Section 6.5)

        status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_touches WHERE touch_id = :tid"),
            {"tid": touch_id},
        ).scalar_one()
        assert status == "skipped"

    def test_touch_hands_off_once_consent_exists(self, fresh_db):
        person_id, enrollment_id, touch_id = self._enroll_with_touch(
            fresh_db, channel="email", email="ready@example.com",
        )
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_person_consent (person_id, channel, consented, source) "
                "VALUES (CAST(:pid AS uuid), 'email', true, 'test')"
            ),
            {"pid": person_id},
        )
        fresh_db.flush()

        summary = selection.process_due_touches(fresh_db)
        assert summary["handed_off"] == 1

        touch = fresh_db.execute(
            text("SELECT status, work_item_id FROM fa_max_campaign_touches WHERE touch_id = :tid"),
            {"tid": touch_id},
        ).mappings().fetchone()
        assert touch["status"] == "handed_off"
        assert touch["work_item_id"] is not None

        queued = fresh_db.execute(
            text("SELECT queue_name FROM fa_max_work_queue WHERE work_item_id = :wid"),
            {"wid": touch["work_item_id"]},
        ).scalar_one()
        assert queued == "fa_max_outreach"

    def test_mark_touch_sent_schedules_next_step(self, fresh_db):
        person_id, enrollment_id, touch_id = self._enroll_with_touch(
            fresh_db, channel="email", email="sent@example.com",
        )
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_sequence_steps "
                "(campaign_key, sequence_version, step, days_after_previous, channel, subject, body_template, loaded_by) "
                "VALUES ('capital_desk_loop', 1, 2, 3, 'email', 'Follow up', 'Body 2', 'test')"
            ),
        )
        fresh_db.flush()

        selection.mark_touch_sent(
            fresh_db, touch_id=touch_id, sent_at=datetime.now(timezone.utc), relay_item_id=999,
        )

        next_touch = fresh_db.execute(
            text(
                "SELECT step, due_at FROM fa_max_campaign_touches "
                "WHERE enrollment_id = CAST(:eid AS uuid) AND step = 2"
            ),
            {"eid": enrollment_id},
        ).mappings().fetchone()
        assert next_touch is not None
        assert next_touch["due_at"] > datetime.now(timezone.utc) + timedelta(days=2)

    def test_mark_touch_not_sent_rejected_cancels_enrollment(self, fresh_db):
        person_id, enrollment_id, touch_id = self._enroll_with_touch(
            fresh_db, channel="email", email="rejected@example.com",
        )
        selection.mark_touch_not_sent(fresh_db, touch_id=touch_id, reason="rejected_by_operator")

        status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_enrollments WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).scalar_one()
        assert status == "cancelled"

    def test_missing_identifier_skip_advances_instead_of_stalling(self, fresh_db):
        """Audit fix: a step whose channel this person has no identifier for
        used to mark the touch 'skipped' and stop — the sequence never
        advanced and never ended, a silent stall. It must now advance to the
        next step exactly like any other channel-specific skip."""
        person_id = _fresh_person(fresh_db, email="email-only@example.com")  # no phone
        _load_minimal_sequence(fresh_db, "capital_desk_loop", channel="sms")
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_sequence_steps "
                "(campaign_key, sequence_version, step, days_after_previous, channel, subject, body_template, loaded_by) "
                "VALUES ('capital_desk_loop', 1, 2, 0, 'email', 'Subject', 'Body', 'test')"
            ),
        )
        enrollment_id = fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), 'capital_desk_loop', 'investor', 1, 'active', 'cash_purchase', 'test') "
                "RETURNING enrollment_id::text"
            ),
            {"pid": person_id},
        ).scalar()
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_touches (enrollment_id, step, channel, due_at, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 1, 'sms', NOW() - interval '1 minute', :idem)"
            ),
            {"eid": enrollment_id, "idem": uuid.uuid4().hex},
        )
        fresh_db.flush()

        summary = selection.process_due_touches(fresh_db)
        assert summary["skipped"] == 1

        step1 = fresh_db.execute(
            text("SELECT status, status_reason FROM fa_max_campaign_touches WHERE enrollment_id = CAST(:eid AS uuid) AND step = 1"),
            {"eid": enrollment_id},
        ).mappings().fetchone()
        assert step1["status"] == "skipped"
        assert step1["status_reason"] == "no_sms_identifier"

        step2 = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_touches WHERE enrollment_id = CAST(:eid AS uuid) AND step = 2"),
            {"eid": enrollment_id},
        ).mappings().fetchone()
        assert step2 is not None, "step 2 must be scheduled instead of the enrollment stalling silently"
        assert step2["status"] == "scheduled"

    def test_last_step_skip_completes_enrollment_not_stalls(self, fresh_db):
        """Same fix, one-step sequence: skipping the only step must complete
        the enrollment, not leave it active with zero touches forever."""
        person_id = _fresh_person(fresh_db, email="only-step@example.com")  # no phone
        _load_minimal_sequence(fresh_db, "rescue_circuit", channel="sms")
        enrollment_id = fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), 'rescue_circuit', 'partner', 1, 'active', 'rescue_circuit_partner', 'test') "
                "RETURNING enrollment_id::text"
            ),
            {"pid": person_id},
        ).scalar()
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_touches (enrollment_id, step, channel, due_at, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 1, 'sms', NOW() - interval '1 minute', :idem)"
            ),
            {"eid": enrollment_id, "idem": uuid.uuid4().hex},
        )
        fresh_db.flush()

        selection.process_due_touches(fresh_db)

        status = fresh_db.execute(
            text("SELECT status, end_reason FROM fa_max_campaign_enrollments WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).mappings().fetchone()
        assert status["status"] == "completed"
        assert status["end_reason"] == "sequence_complete"


class TestBackflipActiveTouch:
    def test_active_backflip_contact_pauses_not_cancels(self, fresh_db):
        person_id = _fresh_person(fresh_db, email="backflip-active@example.com")
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_backflip_campaign_contacts (identifier_kind, identifier_value, active) "
                "VALUES ('email', 'backflip-active@example.com', true)"
            ),
        )
        fresh_db.flush()

        block = enrollment_block_reason(fresh_db, person_id=person_id)
        assert block.blocked is True
        assert block.pause is True
        assert block.reason.startswith("backflip_active:")

    def test_stale_feed_fails_closed(self, fresh_db):
        """Overrides the autouse fresh-feed fixture for this one test."""
        person_id = _fresh_person(fresh_db, email="stale-feed@example.com")
        fresh_db.execute(text("DELETE FROM fa_max_backflip_campaign_feed WHERE id = 1"))
        fresh_db.flush()

        block = enrollment_block_reason(fresh_db, person_id=person_id)
        assert block.blocked is True
        assert block.pause is True

    def test_enrollment_sweep_pauses_backflip_active_person_on_housekeeping(self, fresh_db):
        person_id, prop_id = _cash_buyer_person(fresh_db, email="active-then-backflip@example.com")
        selection.run_enrollment_sweep(fresh_db)  # enroll while clean

        fresh_db.execute(
            text(
                "INSERT INTO fa_max_backflip_campaign_contacts (identifier_kind, identifier_value, active) "
                "VALUES ('email', 'active-then-backflip@example.com', true)"
            ),
        )
        fresh_db.flush()

        summary = selection.run_enrollment_sweep(fresh_db)
        assert summary.paused >= 1

        status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_enrollments WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        assert status == "paused"


class TestSwitchDailyCap:
    def test_switch_respects_the_daily_cap(self, fresh_db, monkeypatch):
        """Audit fix: a campaign switch sends a fresh step-1 message and
        must respect the same domain-warmup cap as a brand-new enrollment —
        a person already active in Capital Desk Loop who now also matches
        the higher-priority Exit Desk must NOT switch while Exit Desk's
        daily cap is exhausted."""
        person_id, prop_id = _cash_buyer_person(fresh_db, email="capped-switch@example.com")
        selection.run_enrollment_sweep(fresh_db)
        existing = fresh_db.execute(
            text("SELECT campaign_key FROM fa_max_campaign_enrollments WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        assert existing == cfg.CAMPAIGN_CAPITAL_DESK_LOOP

        entity_id = fresh_db.execute(
            text("SELECT buyer_entity_id FROM fa_max_person_profiles WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        entity_name = fresh_db.execute(
            text("SELECT canonical_name FROM buyer_entities WHERE id = :eid"), {"eid": entity_id},
        ).scalar_one()
        _seed_exit_desk_bought_row(fresh_db, property_id=prop_id, entity_name=entity_name, months_old=10)

        monkeypatch.setitem(cfg.MAX_NEW_ENROLLMENTS_PER_DAY, cfg.CAMPAIGN_EXIT_DESK, 0)
        cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = True
        try:
            summary = selection.run_enrollment_sweep(fresh_db)
        finally:
            cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = False

        assert summary.switched == 0
        still = fresh_db.execute(
            text(
                "SELECT campaign_key FROM fa_max_campaign_enrollments "
                "WHERE person_id = CAST(:pid AS uuid) AND status = 'active'"
            ),
            {"pid": person_id},
        ).scalar_one()
        assert still == cfg.CAMPAIGN_CAPITAL_DESK_LOOP, "switch must not happen while the target campaign's daily cap is 0"

    def test_switch_happens_when_cap_allows(self, fresh_db):
        """Control case for the test above — proves the cap, not something
        else, was what blocked the switch."""
        person_id, prop_id = _cash_buyer_person(fresh_db, email="uncapped-switch@example.com")
        selection.run_enrollment_sweep(fresh_db)

        entity_id = fresh_db.execute(
            text("SELECT buyer_entity_id FROM fa_max_person_profiles WHERE person_id = CAST(:pid AS uuid)"),
            {"pid": person_id},
        ).scalar_one()
        entity_name = fresh_db.execute(
            text("SELECT canonical_name FROM buyer_entities WHERE id = :eid"), {"eid": entity_id},
        ).scalar_one()
        _seed_exit_desk_bought_row(fresh_db, property_id=prop_id, entity_name=entity_name, months_old=10)

        cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = True
        try:
            summary = selection.run_enrollment_sweep(fresh_db)
        finally:
            cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = False

        assert summary.switched == 1
        active = fresh_db.execute(
            text(
                "SELECT campaign_key FROM fa_max_campaign_enrollments "
                "WHERE person_id = CAST(:pid AS uuid) AND status = 'active'"
            ),
            {"pid": person_id},
        ).scalar_one()
        assert active == cfg.CAMPAIGN_EXIT_DESK


class TestResumeAfterSkip:
    def test_resume_continues_past_a_skipped_step_not_re_processes_it(self, fresh_db):
        """Audit fix: resuming a paused enrollment used to look only at
        MAX(step) WHERE status='sent', so a step that was correctly SKIPPED
        (not sent) would be re-targeted and re-processed on resume."""
        person_id = _fresh_person(fresh_db, email="resume-skip@example.com")  # no phone
        for step, channel in ((1, "email"), (2, "sms"), (3, "email")):
            fresh_db.execute(
                text(
                    "INSERT INTO fa_max_campaign_sequence_steps "
                    "(campaign_key, sequence_version, step, days_after_previous, channel, subject, body_template, loaded_by) "
                    "VALUES ('capital_desk_loop', 1, :step, 0, :channel, 'Subject', 'Body', 'test')"
                ),
                {"step": step, "channel": channel},
            )
        enrollment_id = fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_enrollments "
                "(person_id, campaign_key, audience, sequence_version, status, trigger_type, source) "
                "VALUES (CAST(:pid AS uuid), 'capital_desk_loop', 'investor', 1, 'paused', 'cash_purchase', 'test') "
                "RETURNING enrollment_id::text"
            ),
            {"pid": person_id},
        ).scalar()
        # step 1: sent. step 2 (sms, no phone): skipped. step 3: never
        # reached — the pause happened before it could be scheduled.
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_touches (enrollment_id, step, channel, due_at, status, sent_at, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 1, 'email', NOW() - interval '2 days', 'sent', NOW() - interval '2 days', :idem)"
            ),
            {"eid": enrollment_id, "idem": uuid.uuid4().hex},
        )
        fresh_db.execute(
            text(
                "INSERT INTO fa_max_campaign_touches (enrollment_id, step, channel, due_at, status, status_reason, idempotency_key) "
                "VALUES (CAST(:eid AS uuid), 2, 'sms', NOW() - interval '1 day', 'skipped', 'no_sms_identifier', :idem)"
            ),
            {"eid": enrollment_id, "idem": uuid.uuid4().hex},
        )
        fresh_db.flush()

        selection.run_enrollment_sweep(fresh_db)  # housekeeping resumes it (no block conditions)

        status = fresh_db.execute(
            text("SELECT status FROM fa_max_campaign_enrollments WHERE enrollment_id = CAST(:eid AS uuid)"),
            {"eid": enrollment_id},
        ).scalar_one()
        assert status == "active"

        scheduled = fresh_db.execute(
            text(
                "SELECT step FROM fa_max_campaign_touches "
                "WHERE enrollment_id = CAST(:eid AS uuid) AND status = 'scheduled'"
            ),
            {"eid": enrollment_id},
        ).scalar_one()
        assert scheduled == 3, "resume must target step 3 (past the already-skipped step 2), not re-target step 2"


class TestExitDeskLoanAgeMonths:
    def test_loan_age_months_is_total_months_not_wrapped(self, fresh_db):
        """Audit fix: EXTRACT(MONTH FROM AGE(...)) alone only returns 0-11
        and wraps every 12 months — a 14-month-old loan must report 14, not 2."""
        person_id, entity_id = _fresh_person(fresh_db, email="exitdesk@example.com"), _fresh_buyer_entity(fresh_db, name="Exit Desk LLC")
        _link_person_to_entity(fresh_db, person_id, entity_id)
        prop_id = _insert_property(fresh_db)
        deed_id = _insert_deed(
            fresh_db, property_id=prop_id, grantee="Exit Desk LLC", sale_price=400000,
            mortgage_amount=350000, record_date=date.today() - timedelta(days=14 * 30),
        )
        _link_deed_to_entity(fresh_db, entity_id, deed_id)

        _seed_exit_desk_bought_row(fresh_db, property_id=prop_id, entity_name="Exit Desk LLC", months_old=14)

        from src.services.fa_max_campaigns import eligibility

        cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = True
        try:
            result = eligibility.exit_desk_candidates(fresh_db)
        finally:
            cfg.CAMPAIGN_ENABLED[cfg.CAMPAIGN_EXIT_DESK] = False

        matches = [c for c in result.candidates if c.person_id == person_id]
        assert matches, "the fixture mortgage should have matched the 8-15 month window"
        assert matches[0].extra["loan_age_months"] >= 13, (
            f"expected ~14 total months, got {matches[0].extra['loan_age_months']} "
            "(EXTRACT(MONTH FROM AGE(...)) alone would wrap to ~2)"
        )
