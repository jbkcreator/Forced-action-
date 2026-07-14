"""
Stage 12 — End-to-End: Bankruptcy Filing Alert product.

pytest marker: scenario (requires DATABASE_URL with fa059 applied)

Scenarios (all against real Postgres, rolled back per test):

  A. Subscription creation via checkout webhook → row created with access_token,
     correct status/trial, jurisdictions/chapters parsed from metadata.

  B. Filing ingestion (mocked CourtListener) → bankruptcy_filings rows upserted,
     re-ingest is idempotent (case_number dedup).

  C. Alert dispatch → eligible subscriber matched to filing, email logged,
     dedup prevents repeat on second dispatch.

  D. Jurisdiction/chapter filtering → subscriber only alerted for matching filings.

  E. Lifecycle: payment_failed → past_due (not alerted); subscription.deleted →
     canceled (not alerted).

  F. Status summary endpoint data shape.
"""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.scenario


# ── helpers ───────────────────────────────────────────────────────────────────

def _checkout_event(email, customer_id, subscription_id, *, jurisdictions="flmb-tampa", chapters="7,13"):
    return {
        "metadata": {"product": "bankruptcy_alerts", "jurisdictions": jurisdictions, "chapters": chapters},
        "customer": customer_id,
        "subscription": subscription_id,
        "customer_details": {"email": email, "phone": None, "name": "Test Atty"},
        "customer_email": email,
        "id": "cs_" + uuid.uuid4().hex[:8],
    }


def _insert_filing(db, *, case_number, jurisdiction="flmb-tampa", chapter="7", filer="Debtor X"):
    from sqlalchemy import text as sa_text
    row = db.execute(sa_text("""
        INSERT INTO bankruptcy_filings
            (case_number, chapter, court, jurisdiction, filer, date_filed, created_at)
        VALUES (:cn, :ch, 'flmb', :j, :f, :d, NOW())
        ON CONFLICT (case_number) DO NOTHING
        RETURNING id
    """), {"cn": case_number, "ch": chapter, "j": jurisdiction, "f": filer, "d": date(2026, 5, 30)}).first()
    db.flush()
    return row.id if row else None


def _get_sub(db, email):
    from sqlalchemy import text as sa_text
    return db.execute(sa_text("""
        SELECT * FROM bankruptcy_alert_subscriptions WHERE email = :e LIMIT 1
    """), {"e": email}).first()


# ── Scenario A: subscription creation ─────────────────────────────────────────

def test_scenario_a_subscription_created_from_checkout(fresh_db):
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed

    email = f"atty_{uuid.uuid4().hex[:6]}@example.com"
    cust = "cus_" + uuid.uuid4().hex[:8]
    sub_id = "sub_" + uuid.uuid4().hex[:8]

    _on_checkout_completed(_checkout_event(email, cust, sub_id), fresh_db)
    fresh_db.flush()

    row = _get_sub(fresh_db, email)
    assert row is not None
    assert row.stripe_customer_id == cust
    assert row.stripe_subscription_id == sub_id
    assert row.status in ("trialing", "active")
    assert row.access_token  # uuid generated
    assert row.jurisdictions == ["flmb-tampa"]
    assert row.chapters == ["7", "13"]
    assert row.channel_email is True


# ── Scenario B: ingestion idempotency ─────────────────────────────────────────

def test_scenario_b_ingestion_idempotent(fresh_db):
    from src.services.bankruptcy_alert.ingest import ingest_filings

    cn = "8:26-bk-" + uuid.uuid4().hex[:6]
    dockets = [{
        "id": 9001, "docket_number": cn, "case_name": "In re: Acme Co",
        "federal_dn_case_type": "bk", "court": "flmb", "chapter": 11,
        "date_filed": "2026-05-30",
    }]

    with patch("src.services.bankruptcy_alert.ingest._fetch_dockets", return_value=(dockets, 1)):
        r1 = ingest_filings(fresh_db, lookback_days=1)
    fresh_db.flush()
    assert r1.inserted == 1

    # Re-ingest same docket → deduped, nothing new.
    with patch("src.services.bankruptcy_alert.ingest._fetch_dockets", return_value=(dockets, 1)):
        r2 = ingest_filings(fresh_db, lookback_days=1)
    fresh_db.flush()
    assert r2.inserted == 0
    assert r2.duplicates == 1


# ── Scenario C: alert dispatch + dedup ────────────────────────────────────────

def test_scenario_c_dispatch_and_dedup(fresh_db):
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed
    from src.services.bankruptcy_alert.alerts import dispatch_alerts

    email = f"lender_{uuid.uuid4().hex[:6]}@example.com"
    _on_checkout_completed(
        _checkout_event(email, "cus_" + uuid.uuid4().hex[:8], "sub_" + uuid.uuid4().hex[:8],
                        jurisdictions="flmb-tampa", chapters="7,11,13"),
        fresh_db,
    )
    fresh_db.flush()
    # Force status active (trial also eligible, but be explicit).
    fresh_db.execute(sa_text("""
        UPDATE bankruptcy_alert_subscriptions SET status='active' WHERE email=:e
    """), {"e": email})

    _insert_filing(fresh_db, case_number="8:26-bk-" + uuid.uuid4().hex[:6], chapter="7")

    # Patch email sender to succeed without SMTP.
    with patch("src.services.email.send_email", return_value=True):
        r1 = dispatch_alerts(fresh_db)
    fresh_db.flush()
    assert r1.emails_sent >= 1

    # Second dispatch → deduped, no new email.
    with patch("src.services.email.send_email", return_value=True) as send2:
        r2 = dispatch_alerts(fresh_db)
    fresh_db.flush()
    assert r2.emails_sent == 0
    send2.assert_not_called()

    # Audit row exists with status 'sent'.
    cnt = fresh_db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM bankruptcy_filing_alerts a
        JOIN bankruptcy_alert_subscriptions s ON s.id = a.subscription_id
        WHERE s.email = :e AND a.status = 'sent' AND a.channel = 'email'
    """), {"e": email}).first()
    assert int(cnt.c) >= 1


# ── Scenario D: filtering ──────────────────────────────────────────────────────

def test_scenario_d_chapter_filter_excludes_nonmatching(fresh_db):
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed
    from src.services.bankruptcy_alert.alerts import dispatch_alerts

    email = f"inv_{uuid.uuid4().hex[:6]}@example.com"
    # Subscriber only wants chapter 7.
    _on_checkout_completed(
        _checkout_event(email, "cus_" + uuid.uuid4().hex[:8], "sub_" + uuid.uuid4().hex[:8],
                        jurisdictions="flmb-tampa", chapters="7"),
        fresh_db,
    )
    fresh_db.execute(sa_text("UPDATE bankruptcy_alert_subscriptions SET status='active' WHERE email=:e"),
                     {"e": email})

    # A chapter-13 filing — should NOT match this chapter-7-only subscriber.
    _insert_filing(fresh_db, case_number="8:26-bk-" + uuid.uuid4().hex[:6], chapter="13")
    fresh_db.flush()

    with patch("src.services.email.send_email", return_value=True) as send:
        result = dispatch_alerts(fresh_db)
    fresh_db.flush()

    # No email for this subscriber's chapter filter.
    sent_to_email = fresh_db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM bankruptcy_filing_alerts a
        JOIN bankruptcy_alert_subscriptions s ON s.id = a.subscription_id
        WHERE s.email = :e
    """), {"e": email}).first()
    assert int(sent_to_email.c) == 0


def test_scenario_d_jurisdiction_filter(fresh_db):
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed
    from src.services.bankruptcy_alert.alerts import dispatch_alerts

    email = f"orl_{uuid.uuid4().hex[:6]}@example.com"
    # Subscriber only wants Orlando.
    _on_checkout_completed(
        _checkout_event(email, "cus_" + uuid.uuid4().hex[:8], "sub_" + uuid.uuid4().hex[:8],
                        jurisdictions="flmb-orlando", chapters="7,11,13"),
        fresh_db,
    )
    fresh_db.execute(sa_text("UPDATE bankruptcy_alert_subscriptions SET status='active' WHERE email=:e"),
                     {"e": email})
    # A Tampa filing — should not match Orlando-only subscriber.
    _insert_filing(fresh_db, case_number="8:26-bk-" + uuid.uuid4().hex[:6],
                   jurisdiction="flmb-tampa", chapter="7")
    fresh_db.flush()

    with patch("src.services.email.send_email", return_value=True):
        dispatch_alerts(fresh_db)
    fresh_db.flush()

    cnt = fresh_db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM bankruptcy_filing_alerts a
        JOIN bankruptcy_alert_subscriptions s ON s.id = a.subscription_id
        WHERE s.email = :e
    """), {"e": email}).first()
    assert int(cnt.c) == 0


# ── Scenario E: lifecycle ──────────────────────────────────────────────────────

def test_scenario_e_past_due_not_alerted(fresh_db):
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.subscription import (
        _on_checkout_completed, _on_payment_failed,
    )
    from src.services.bankruptcy_alert.alerts import dispatch_alerts

    email = f"pd_{uuid.uuid4().hex[:6]}@example.com"
    sub_id = "sub_" + uuid.uuid4().hex[:8]
    _on_checkout_completed(_checkout_event(email, "cus_" + uuid.uuid4().hex[:8], sub_id), fresh_db)
    fresh_db.flush()

    # Payment fails → past_due.
    _on_payment_failed({"subscription": sub_id}, fresh_db)
    fresh_db.flush()
    row = _get_sub(fresh_db, email)
    assert row.status == "past_due"

    _insert_filing(fresh_db, case_number="8:26-bk-" + uuid.uuid4().hex[:6], chapter="7")
    fresh_db.flush()

    with patch("src.services.email.send_email", return_value=True):
        dispatch_alerts(fresh_db)
    fresh_db.flush()

    # past_due is NOT in ALERT_ELIGIBLE_STATUSES → no alert.
    cnt = fresh_db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM bankruptcy_filing_alerts a
        JOIN bankruptcy_alert_subscriptions s ON s.id = a.subscription_id
        WHERE s.email = :e
    """), {"e": email}).first()
    assert int(cnt.c) == 0


def test_scenario_e_canceled_via_deletion(fresh_db):
    from src.services.bankruptcy_alert.subscription import (
        _on_checkout_completed, _on_subscription_deleted,
    )
    email = f"cx_{uuid.uuid4().hex[:6]}@example.com"
    sub_id = "sub_" + uuid.uuid4().hex[:8]
    _on_checkout_completed(_checkout_event(email, "cus_" + uuid.uuid4().hex[:8], sub_id), fresh_db)
    fresh_db.flush()

    _on_subscription_deleted({"id": sub_id}, fresh_db)
    fresh_db.flush()
    row = _get_sub(fresh_db, email)
    assert row.status == "canceled"
    assert row.canceled_at is not None


# ── Scenario F: status summary ─────────────────────────────────────────────────

def test_scenario_f_status_summary_shape(fresh_db):
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed
    from src.services.bankruptcy_alert.alerts import status_summary

    email = f"sum_{uuid.uuid4().hex[:6]}@example.com"
    _on_checkout_completed(_checkout_event(email, "cus_" + uuid.uuid4().hex[:8], "sub_" + uuid.uuid4().hex[:8]), fresh_db)
    fresh_db.flush()

    summary = status_summary(fresh_db)
    assert "subscribers_by_status" in summary
    assert "subscribers_total" in summary
    assert "alerts_sent" in summary
    assert "filings_total" in summary
    assert summary["subscribers_total"] >= 1


# ── Scenario G: post-signup invite (schedule → sweep) ─────────────────────────

def _make_property_subscriber(fresh_db, *, email, status="active"):
    """Insert a minimal property subscriber via the ORM (applies all the
    Python-side NOT-NULL defaults that a raw INSERT would miss). Returns id."""
    from src.core.models import Subscriber
    uid = uuid.uuid4().hex[:10]
    sub = Subscriber(
        stripe_customer_id=f"cus_inv_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status=status,
        email=email,
        event_feed_uuid=f"inv-{uid}",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub.id


def _scheduled_invite_count(fresh_db, sub_id):
    from sqlalchemy import text as sa_text
    return fresh_db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM message_outcomes
        WHERE subscriber_id = :sid AND template_id = 'bankruptcy_alert_invite'
          AND send_status = 'scheduled'
    """), {"sid": sub_id}).first().c


def test_scenario_g_schedule_invite_idempotent(fresh_db):
    from src.services.bankruptcy_alert.invite import schedule_invite

    sub_id = _make_property_subscriber(fresh_db, email=f"sch_{uuid.uuid4().hex[:6]}@example.com")

    assert schedule_invite(fresh_db, sub_id) is True
    fresh_db.flush()
    assert _scheduled_invite_count(fresh_db, sub_id) == 1

    # Second call is a no-op (NOT EXISTS guard) — still exactly one row.
    assert schedule_invite(fresh_db, sub_id) is False
    fresh_db.flush()
    assert _scheduled_invite_count(fresh_db, sub_id) == 1


def test_scenario_g_due_invite_sent_and_marked(fresh_db):
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.invite import schedule_invite, send_due_invites

    email = f"due_{uuid.uuid4().hex[:6]}@example.com"
    sub_id = _make_property_subscriber(fresh_db, email=email)
    schedule_invite(fresh_db, sub_id)
    # Force it due now (signup just happened → scheduled_send_at is in the future).
    fresh_db.execute(sa_text("""
        UPDATE message_outcomes SET scheduled_send_at = NOW() - INTERVAL '1 minute'
        WHERE subscriber_id = :sid AND template_id = 'bankruptcy_alert_invite'
    """), {"sid": sub_id})
    fresh_db.flush()

    with patch("src.services.bankruptcy_alert.subscription.create_checkout",
               return_value={"url": "https://stripe/cs_e2e", "session_id": "cs_e2e"}), \
         patch("src.services.email.send_email", return_value=True) as send:
        res = send_due_invites(fresh_db)
    fresh_db.flush()

    assert res.sent >= 1
    send.assert_called()
    # row flipped to 'sent'
    sent = fresh_db.execute(sa_text("""
        SELECT send_status FROM message_outcomes
        WHERE subscriber_id = :sid AND template_id = 'bankruptcy_alert_invite'
    """), {"sid": sub_id}).first()
    assert sent.send_status == "sent"

    # Re-running the sweep does nothing (no longer 'scheduled').
    with patch("src.services.email.send_email", return_value=True) as send2:
        res2 = send_due_invites(fresh_db)
    assert res2.sent == 0
    send2.assert_not_called()


def test_scenario_g_not_yet_due_is_skipped(fresh_db):
    from src.services.bankruptcy_alert.invite import schedule_invite, send_due_invites

    email = f"future_{uuid.uuid4().hex[:6]}@example.com"
    sub_id = _make_property_subscriber(fresh_db, email=email)
    schedule_invite(fresh_db, sub_id)  # due in the future (delay minutes)
    fresh_db.flush()

    with patch("src.services.email.send_email", return_value=True) as send:
        res = send_due_invites(fresh_db)
    # Nothing due yet → not sent, row stays scheduled.
    assert _scheduled_invite_count(fresh_db, sub_id) == 1
    # (other tests' rows might be due, but this subscriber's wasn't sent)
    send_calls_for_this = [c for c in send.call_args_list if c.args and c.args[0] == email]
    assert send_calls_for_this == []


# ── Scenario H: invite→paid conversion counts ONLY active (paid) subs ─────────

def test_scenario_h_conversion_counts_only_active_paid(fresh_db):
    """Locks the status='active' filter in _invite_conversion_stats: a
    same-email bankruptcy signup inside the window counts as a conversion
    ONLY once it is paid (active) — a trialing signup must NOT count.
    Delta assertions (not absolute), since the shared DB carries real
    invite/subscription rows this test can't see or control."""
    from sqlalchemy import text as sa_text
    from src.services.bankruptcy_alert.invite import schedule_invite
    from src.services.bankruptcy_alert.subscription import _on_checkout_completed
    from src.services.bankruptcy_alert.alerts import _invite_conversion_stats

    base = _invite_conversion_stats(fresh_db)

    # A property subscriber who received (was sent) a bankruptcy invite now.
    email = f"conv_{uuid.uuid4().hex[:6]}@example.com"
    sub_id = _make_property_subscriber(fresh_db, email=email)
    schedule_invite(fresh_db, sub_id)
    fresh_db.execute(sa_text("""
        UPDATE message_outcomes SET send_status = 'sent', sent_at = NOW()
        WHERE subscriber_id = :sid AND template_id = 'bankruptcy_alert_invite'
    """), {"sid": sub_id})
    fresh_db.flush()

    after_send = _invite_conversion_stats(fresh_db)
    assert after_send["invites_sent"] == base["invites_sent"] + 1
    assert after_send["converted"] == base["converted"]  # no bankruptcy signup yet

    # Same email signs up for bankruptcy — checkout creates a TRIALING row.
    _on_checkout_completed(
        _checkout_event(email, "cus_" + uuid.uuid4().hex[:8], "sub_" + uuid.uuid4().hex[:8]),
        fresh_db,
    )
    fresh_db.flush()
    trialing = _invite_conversion_stats(fresh_db)
    assert trialing["converted"] == base["converted"]  # trialing is NOT a paid conversion

    # Payment clears → active. Now it counts.
    fresh_db.execute(sa_text("""
        UPDATE bankruptcy_alert_subscriptions SET status = 'active'
        WHERE LOWER(email) = LOWER(:e)
    """), {"e": email})
    fresh_db.flush()
    active = _invite_conversion_stats(fresh_db)
    assert active["converted"] == base["converted"] + 1
