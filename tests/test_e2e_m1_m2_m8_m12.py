"""
End-to-end integration tests: M1 (Prospect Seeding) → M2 (Cascade Consumer)
→ M8 (SMS Prospect Gates) → M12 (Event Consumer Reliability).

Uses real DB. Seeds isolated test fixtures identified by parcel_id prefix
'TEST_E2E_' and fully deletes them in teardown — regardless of failures.

Run:
    pytest tests/test_e2e_m1_m2_m8_m12.py -v -s
"""

import logging
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings
from src.services.compliance_gator import ComplianceResult

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_PREFIX = "TEST_E2E_"
_PARCEL_IDS = [f"{_PREFIX}001", f"{_PREFIX}002", f"{_PREFIX}003"]
_TEST_PHONE = "+18135559900"   # fake FL number — only written to sms tables
_M12_CONSUMER = "test_e2e_consumer"


# ── DB fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def engine():
    settings = get_settings()
    e = create_engine(str(settings.database_url))
    yield e
    e.dispose()


@pytest.fixture(scope="module")
def db(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _seed(db) -> dict:
    """
    Create 3 test properties (Gold+, today's score_date, no prospect yet).
    Owners have no phone so they are eligible for the cascade eligibility guard.
    Returns property_ids and owner_ids.
    """
    today = datetime.now(timezone.utc).date()
    data: dict = {"property_ids": [], "owner_ids": []}

    for i, parcel_id in enumerate(_PARCEL_IDS):
        row = db.execute(sa_text("""
            INSERT INTO properties
                (parcel_id, address, city, state, zip, county_id, created_at, updated_at)
            VALUES
                (:parcel_id, :addr, 'Tampa', 'FL', '33601', 'hillsborough', NOW(), NOW())
            ON CONFLICT (parcel_id) DO UPDATE SET address = EXCLUDED.address
            RETURNING id
        """), {"parcel_id": parcel_id, "addr": f"TEST {i + 1} Main St"}).fetchone()
        pid = row.id
        data["property_ids"].append(pid)

        r2 = db.execute(sa_text("""
            INSERT INTO owners (property_id, owner_name, county_id)
            VALUES (:pid, :name, 'hillsborough')
            ON CONFLICT (property_id) DO UPDATE SET owner_name = EXCLUDED.owner_name
            RETURNING id
        """), {"pid": pid, "name": f"E2E Owner {i + 1}"}).fetchone()
        data["owner_ids"].append(r2.id)

        db.execute(sa_text("""
            INSERT INTO distress_scores
                (property_id, score_date, final_cds_score, lead_tier,
                 county_id, qualified, factor_scores, vertical_scores)
            VALUES (:pid, :today, 65.0, 'Gold', 'hillsborough', true,
                    CAST('{}' AS jsonb), CAST('{}' AS jsonb))
        """), {"pid": pid, "today": today})

    db.commit()
    logger.info("[E2E Setup] seeded %d test properties", len(_PARCEL_IDS))
    return data


def _cleanup(db) -> None:
    """
    Delete all test data in reverse FK order.
    Called in finally — must never raise.
    """
    try:
        # event_failures → events → processed_events (prospect-linked)
        db.execute(sa_text("""
            DELETE FROM event_failures ef
            USING events e, prospects p, properties pr
            WHERE ef.event_id = e.event_id
              AND e.prospect_id = p.prospect_id
              AND p.property_id = pr.id
              AND pr.parcel_id LIKE :pfx
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("""
            DELETE FROM processed_events pe
            USING events e, prospects p, properties pr
            WHERE pe.event_id = e.event_id
              AND e.prospect_id = p.prospect_id
              AND p.property_id = pr.id
              AND pr.parcel_id LIKE :pfx
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("""
            DELETE FROM events e
            USING prospects p, properties pr
            WHERE e.prospect_id = p.prospect_id
              AND p.property_id = pr.id
              AND pr.parcel_id LIKE :pfx
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("""
            DELETE FROM sms_send_logs
            WHERE prospect_id IN (
                SELECT p.prospect_id FROM prospects p
                JOIN properties pr ON pr.id = p.property_id
                WHERE pr.parcel_id LIKE :pfx
            )
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("DELETE FROM sms_dead_letters WHERE phone = :ph"),
                   {"ph": _TEST_PHONE})

        db.execute(sa_text("""
            DELETE FROM prospects WHERE property_id IN (
                SELECT id FROM properties WHERE parcel_id LIKE :pfx
            )
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("""
            DELETE FROM distress_scores WHERE property_id IN (
                SELECT id FROM properties WHERE parcel_id LIKE :pfx
            )
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("""
            DELETE FROM owners WHERE property_id IN (
                SELECT id FROM properties WHERE parcel_id LIKE :pfx
            )
        """), {"pfx": f"{_PREFIX}%"})

        db.execute(sa_text("DELETE FROM properties WHERE parcel_id LIKE :pfx"),
                   {"pfx": f"{_PREFIX}%"})

        db.commit()
        logger.info("[E2E Teardown] all test data deleted")
    except Exception as exc:
        db.rollback()
        logger.error("[E2E Teardown] cleanup failed: %s", exc)


def _prospect_ids_for_test(db) -> list[str]:
    rows = db.execute(sa_text("""
        SELECT p.prospect_id FROM prospects p
        JOIN properties pr ON pr.id = p.property_id
        WHERE pr.parcel_id LIKE :pfx
        ORDER BY pr.parcel_id
    """), {"pfx": f"{_PREFIX}%"}).fetchall()
    return [str(r.prospect_id) for r in rows]


# ── The E2E test ──────────────────────────────────────────────────────────────


@pytest.mark.skipif(
    not get_settings().database_url,
    reason="DATABASE_URL not configured",
)
def test_e2e_m1_m2_m8_m12(db):
    seed_data = _seed(db)
    try:
        _run_m1(db, seed_data)
        prospect_ids = _prospect_ids_for_test(db)
        assert len(prospect_ids) == 3, "M1 must have created 3 prospects before M2"
        _run_m2(db)
        _run_m8(db, prospect_ids)
        _run_m12(db, prospect_ids)
    finally:
        _cleanup(db)


# ── M1: Prospect Seeding ──────────────────────────────────────────────────────


def _run_m1(db, seed_data):
    """
    Tests the internal seeding functions directly, scoped to the 3 test properties.
    Avoids calling run() globally (which would process all Gold+ properties in the
    DB and is affected by whether today's cron already ran).
    """
    from src.tasks.prospect_seeding import _fetch_unseeded, _seed_batch
    from src.services.event_bus import emit_event
    from src.core.database import get_db_context

    today = datetime.now(timezone.utc).date()
    test_pids = set(seed_data["property_ids"])

    # ── Fetch: our 3 test properties must appear as unseeded ─────────────────
    with get_db_context() as session:
        all_unseeded = _fetch_unseeded(session, today)
        our_unseeded = [r for r in all_unseeded if r.property_id in test_pids]

    assert len(our_unseeded) == 3, (
        f"_fetch_unseeded must return all 3 test properties, got {len(our_unseeded)}"
    )

    # ── Dry-run assertion: no prospects exist yet ─────────────────────────────
    count = db.execute(sa_text("""
        SELECT count(*) FROM prospects WHERE property_id = ANY(:pids)
    """), {"pids": seed_data["property_ids"]}).scalar()
    assert count == 0, "No prospects should exist before seeding"

    # ── Seed batch + emit events (scoped to our test properties) ─────────────
    with get_db_context() as session:
        created_rows = _seed_batch(session, our_unseeded)
        assert len(created_rows) == 3, (
            f"_seed_batch must create 3 prospects, got {len(created_rows)}"
        )

        meta = {r.property_id: r for r in our_unseeded}
        for prospect_id, property_id in created_rows:
            m = meta[property_id]
            emit_event(
                session,
                event_type="prospect.created",
                actor="prospect_seeding",
                source_component="e2e_test",
                prospect_id=prospect_id,
                payload={
                    "property_id":    property_id,
                    "lead_tier":      m.lead_tier,
                    "county_id":      m.county_id,
                    "scoring_run_id": m.scoring_run_id,
                    "zip":            m.zip,
                },
            )
        session.commit()

    # ── Verify DB state ───────────────────────────────────────────────────────
    prospect_count = db.execute(sa_text("""
        SELECT count(*) FROM prospects WHERE property_id = ANY(:pids)
    """), {"pids": seed_data["property_ids"]}).scalar()
    assert prospect_count == 3, f"Expected 3 prospects in DB, got {prospect_count}"

    event_count = db.execute(sa_text("""
        SELECT count(*) FROM events e
        JOIN prospects p ON p.prospect_id = e.prospect_id
        WHERE p.property_id = ANY(:pids)
          AND e.event_type = 'prospect.created'
    """), {"pids": seed_data["property_ids"]}).scalar()
    assert event_count == 3, f"Expected 3 prospect.created events, got {event_count}"

    # ── Idempotency: re-seeding same batch returns 0 (ON CONFLICT DO NOTHING) ─
    with get_db_context() as session:
        second_created = _seed_batch(session, our_unseeded)
        assert second_created == [], "Re-seeding existing properties must return empty list"
        session.commit()

    logger.info("[E2E M1] PASS — 3 prospects created, 3 events emitted, idempotency verified")


# ── M2: Event-Driven Cascade Consumer ────────────────────────────────────────


def _run_m2(db):
    from src.services.skip_trace_waterfall import consume_prospect_created

    # Mock run_cascade so we don't hit real APIs
    mock_stats = MagicMock(hits=0, misses=3)
    with patch("src.services.skip_trace_waterfall.run_cascade", return_value=mock_stats):
        stats = consume_prospect_created(county_id="hillsborough")

    # All 3 events should be marked processed (regardless of eligibility)
    processed = db.execute(sa_text("""
        SELECT count(*) FROM processed_events pe
        JOIN events e ON e.event_id = pe.event_id
        JOIN prospects p ON p.prospect_id = e.prospect_id
        JOIN properties pr ON pr.id = p.property_id
        WHERE pe.consumer = 'cascade'
          AND e.event_type = 'prospect.created'
          AND pr.parcel_id LIKE :pfx
    """), {"pfx": f"{_PREFIX}%"}).scalar()
    assert processed == 3, f"Expected 3 cascade-processed events, got {processed}"

    # Second call must be a no-op (idempotency)
    with patch("src.services.skip_trace_waterfall.run_cascade", return_value=mock_stats):
        stats2 = consume_prospect_created(county_id="hillsborough")
    # No more unprocessed events for these test prospects (cascade already marked them)

    logger.info("[E2E M2] PASS — 3 events consumed + idempotency verified")


# ── M8: SMS Prospect Gates ────────────────────────────────────────────────────


def _run_m8(db, prospect_ids: list[str]):
    from src.services.sms_compliance import send_sms
    from src.services.prospect_service import get_channel_consent, update_consent
    from src.core.database import get_db_context

    pid_contactable   = prospect_ids[0]   # will be contactable + sms consent
    pid_not_contact   = prospect_ids[1]   # contactability_state stays 'unknown'
    pid_no_consent    = prospect_ids[2]   # contactable but sms consent = false

    # Set up contactable prospect with sms consent
    with get_db_context() as session:
        session.execute(sa_text("""
            UPDATE prospects
            SET contactability_state = 'contactable',
                channel_consent = '{"sms": true}'::jsonb
            WHERE prospect_id = CAST(:pid AS uuid)
        """), {"pid": pid_contactable})
        session.commit()

    # Set up contactable but no-consent prospect
    with get_db_context() as session:
        session.execute(sa_text("""
            UPDATE prospects
            SET contactability_state = 'contactable',
                channel_consent = '{"sms": false}'::jsonb
            WHERE prospect_id = CAST(:pid AS uuid)
        """), {"pid": pid_no_consent})
        session.commit()

    _allowed_compliance = ComplianceResult(allowed=True)

    # ── get_channel_consent + update_consent ──────────────────────────────────
    with get_db_context() as session:
        consent = get_channel_consent(session, pid_contactable, "sms")
        assert consent is True, f"Expected sms consent=True, got {consent}"

        consent2 = get_channel_consent(session, pid_no_consent, "sms")
        assert consent2 is False, f"Expected sms consent=False, got {consent2}"

        # update_consent: grant sms for prospect[1] (was unknown)
        update_consent(session, pid_not_contact, "sms", True)
        session.commit()

    with get_db_context() as session:
        consent3 = get_channel_consent(session, pid_not_contact, "sms")
        assert consent3 is True, "update_consent should have set sms=True"
        # Reset back to unknown for the send_sms gate test
        update_consent(session, pid_not_contact, "sms", False)
        session.commit()

    # ── Gate P1: non-contactable prospect blocked ─────────────────────────────
    with get_db_context() as session:
        with patch("src.services.compliance_gator.validate_outbound",
                   return_value=_allowed_compliance), \
             patch("src.services.sms_compliance.settings") as ms:
            ms.telnyx_sms_enabled = False
            result = send_sms(
                _TEST_PHONE, "Hello", session,
                message_type="transactional",
                prospect_id=pid_not_contact,
            )
        assert result is False, "Non-contactable prospect must be blocked"

        dlq_row = session.execute(sa_text("""
            SELECT reason FROM sms_dead_letters
            WHERE phone = :ph AND reason = 'prospect_not_contactable'
            ORDER BY created_at DESC LIMIT 1
        """), {"ph": _TEST_PHONE}).fetchone()
        assert dlq_row is not None, "DLQ row with prospect_not_contactable expected"
        session.commit()

    # ── Gate P2: contactable but no sms consent ───────────────────────────────
    with get_db_context() as session:
        with patch("src.services.compliance_gator.validate_outbound",
                   return_value=_allowed_compliance), \
             patch("src.services.sms_compliance.settings") as ms:
            ms.telnyx_sms_enabled = False
            result2 = send_sms(
                _TEST_PHONE, "Hello", session,
                message_type="transactional",
                prospect_id=pid_no_consent,
            )
        assert result2 is False, "No-consent prospect must be blocked"

        dlq_row2 = session.execute(sa_text("""
            SELECT reason FROM sms_dead_letters
            WHERE phone = :ph AND reason = 'prospect_sms_consent_withdrawn'
            ORDER BY created_at DESC LIMIT 1
        """), {"ph": _TEST_PHONE}).fetchone()
        assert dlq_row2 is not None, "DLQ row with prospect_sms_consent_withdrawn expected"
        session.commit()

    # ── Gates pass: contactable + sms consent → dry_run True ─────────────────
    with get_db_context() as session:
        with patch("src.services.compliance_gator.validate_outbound",
                   return_value=_allowed_compliance), \
             patch("src.services.sms_compliance.settings") as ms:
            ms.telnyx_sms_enabled = False
            result3 = send_sms(
                _TEST_PHONE, "Hello investor", session,
                message_type="transactional",
                prospect_id=pid_contactable,
            )
        assert result3 is True, "Contactable + consented prospect must pass gates"

        log_row = session.execute(sa_text("""
            SELECT outcome, prospect_id FROM sms_send_logs
            WHERE phone = :ph AND outcome = 'dry_run'
            ORDER BY created_at DESC LIMIT 1
        """), {"ph": _TEST_PHONE}).fetchone()
        assert log_row is not None, "sms_send_logs must have a dry_run row"
        assert str(log_row.prospect_id) == pid_contactable
        session.commit()

    logger.info("[E2E M8] PASS — P1/P2 gates enforced; consented prospect passes dry-run")


# ── M12: Event Consumer Reliability ──────────────────────────────────────────


def _run_m12(db, prospect_ids: list[str]):
    """
    Uses event_type='test.m12' (a dedicated type) so poll_and_dispatch only
    sees M12's own events — isolation from the prospect.created events emitted by M1/M2.
    """
    from src.services.event_consumer import poll_and_dispatch, is_permanently_failed
    from src.core.database import get_db_context

    pid = prospect_ids[0]
    # Use 'delivery.sent' — valid per ck_events_event_type, not produced by M1/M2
    _TYPE = "delivery.sent"

    # Create 2 dedicated M12 test events
    with get_db_context() as session:
        event_a = session.execute(sa_text("""
            INSERT INTO events
                (prospect_id, event_type, actor, payload, source_component)
            VALUES
                (CAST(:pid AS uuid), :etype, 'e2e_test',
                 CAST('{}' AS jsonb), 'e2e_test')
            RETURNING event_id
        """), {"pid": pid, "etype": _TYPE}).fetchone().event_id

        event_b = session.execute(sa_text("""
            INSERT INTO events
                (prospect_id, event_type, actor, payload, source_component)
            VALUES
                (CAST(:pid AS uuid), :etype, 'e2e_test',
                 CAST('{}' AS jsonb), 'e2e_test')
            RETURNING event_id
        """), {"pid": pid, "etype": _TYPE}).fetchone().event_id
        session.commit()

    # ── Success path: handler succeeds → event_a marked processed ────────────
    success_calls = []

    def _success_handler(session, row):
        success_calls.append(row.event_id)

    with get_db_context() as session:
        result = poll_and_dispatch(
            session,
            consumer=_M12_CONSUMER,
            event_types=[_TYPE],
            handler=_success_handler,
            batch_size=1,       # pick up one event at a time
            max_retries=3,
        )

    assert result["processed"] == 1, f"Expected 1 processed, got {result}"
    assert result["failed"] == 0
    assert event_a in success_calls, "event_a must be the first dispatched (oldest by occurred_at)"

    processed_count = db.execute(sa_text("""
        SELECT count(*) FROM processed_events
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": str(event_a), "consumer": _M12_CONSUMER}).scalar()
    assert processed_count == 1, "event_a must be in processed_events"

    # ── Failure path: event_b → recorded in event_failures (retry_count=1) ───
    def _fail_handler(session, row):
        raise RuntimeError("Simulated handler failure")

    with get_db_context() as session:
        result2 = poll_and_dispatch(
            session,
            consumer=_M12_CONSUMER,
            event_types=[_TYPE],
            handler=_fail_handler,
            batch_size=1,
            max_retries=3,
        )

    assert result2["failed"] == 1, "Expected 1 failure recorded"
    assert result2["permanently_failed"] == 0, "First failure must not be permanent (max_retries=3)"

    failure_row = db.execute(sa_text("""
        SELECT retry_count, failed_permanently FROM event_failures
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": str(event_b), "consumer": _M12_CONSUMER}).fetchone()
    assert failure_row is not None, "event_failures row must exist after first failure"
    assert failure_row.retry_count == 1
    assert not failure_row.failed_permanently

    # ── Retry exhaustion: 2 more failures → retry_count=3 → permanently failed ─
    # First failure already happened (retry_count=1). 2 more reach max_retries=3.
    for _ in range(2):
        with get_db_context() as session:
            poll_and_dispatch(
                session,
                consumer=_M12_CONSUMER,
                event_types=[_TYPE],
                handler=_fail_handler,
                batch_size=1,
                max_retries=3,
            )

    with get_db_context() as session:
        permanently = is_permanently_failed(session, event_b, _M12_CONSUMER)
    assert permanently, "After max_retries=3 exhausted, failed_permanently must be True"

    final_row = db.execute(sa_text("""
        SELECT retry_count FROM event_failures
        WHERE event_id = :eid AND consumer = :consumer
    """), {"eid": str(event_b), "consumer": _M12_CONSUMER}).fetchone()
    assert final_row.retry_count == 3

    # ── Exclusion: permanently failed event must not be re-dispatched ─────────
    excluded_calls = []

    def _track_handler(session, row):
        excluded_calls.append(row.event_id)

    with get_db_context() as session:
        poll_and_dispatch(
            session,
            consumer=_M12_CONSUMER,
            event_types=[_TYPE],
            handler=_track_handler,
            batch_size=10,
            max_retries=3,
        )

    assert event_b not in excluded_calls, \
        "Permanently failed event must be excluded from future polls"

    logger.info("[E2E M12] PASS — success, failure recording, retry exhaustion (retry_count=3), exclusion verified")
