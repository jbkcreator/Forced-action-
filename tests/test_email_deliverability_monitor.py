from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select, text

from src.core.models import CampaignDailyAnalytics, EmailCampaign, MessageOutcome, ScraperAlertLog, Subscriber
from src.tasks import email_deliverability_monitor as monitor


def _make_campaign(db, *, county_id: str = "hillsborough") -> EmailCampaign:
    row = EmailCampaign(
        name="Warm Leads",
        county_id=county_id,
        status="active",
        geo_filter={},
        send_schedule={},
        instantly_settings={},
    )
    db.add(row)
    db.flush()
    return row


def _make_subscriber(db, *, email: str) -> Subscriber:
    sub = Subscriber(
        stripe_customer_id=f"cus_{email.split('@')[0]}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        email=email,
        event_feed_uuid=f"feed-{email.split('@')[0]}",
    )
    db.add(sub)
    db.flush()
    return sub


def _ensure_message_outcome_email_tracking_columns(db) -> None:
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS recipient_email VARCHAR(255)"))
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS provider_message_id VARCHAR(100)"))
    db.execute(text("ALTER TABLE message_outcomes ADD COLUMN IF NOT EXISTS failure_reason VARCHAR(255)"))
    db.flush()


def test_evaluate_trips_cold_bounce_rate(fresh_db):
    campaign = _make_campaign(fresh_db)
    today = date(2026, 7, 28)
    for offset, sent, bounced in ((0, 30, 2), (1, 40, 3), (2, 30, 1)):
        fresh_db.add(
            CampaignDailyAnalytics(
                campaign_id=campaign.id,
                snapshot_date=today - timedelta(days=offset),
                emails_sent=sent,
                bounces=bounced,
                total_contacts=sent,
                opens=0,
                open_rate=0,
                replies=0,
                reply_rate=0,
                clicks=0,
                unsubscribes=0,
                interested=0,
            )
        )
    fresh_db.flush()

    trips = monitor.evaluate(fresh_db, today=today, warmup_fetcher=lambda: [])

    assert any(t.rule == "deliverability_cold_bounce_rate_high" for t in trips)


def test_evaluate_trips_transactional_bounce_and_complaint(fresh_db):
    _ensure_message_outcome_email_tracking_columns(fresh_db)
    today = date(2026, 7, 28)
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    sub = _make_subscriber(fresh_db, email="txn@example.com")
    for i in range(100):
        failure_reason = None
        if i < 6:
            failure_reason = "hard_bounce"
        elif i == 6:
            failure_reason = "spam"
        fresh_db.add(
            MessageOutcome(
                subscriber_id=sub.id,
                message_type="email",
                template_id="welcome_email",
                channel="mailchimp",
                recipient_email=f"txn{i}@example.com",
                sent_at=now - timedelta(hours=i),
                send_status="sent",
                failure_reason=failure_reason,
            )
        )
    fresh_db.flush()

    trips = monitor.evaluate(fresh_db, today=today, warmup_fetcher=lambda: [])
    rules = {t.rule for t in trips}

    assert "deliverability_transactional_bounce_rate_high" in rules
    assert "deliverability_transactional_complaint_rate_high" in rules


def test_evaluate_trips_warmup_health(fresh_db):
    trips = monitor.evaluate(
        fresh_db,
        today=date(2026, 7, 28),
        warmup_fetcher=lambda: [
            {"email": "good@example.com", "health_score": 91},
            {"email": "bad@example.com", "health_score": 62},
        ],
    )

    trip = next(t for t in trips if t.rule == "deliverability_warmup_score_low")
    assert "bad@example.com=62" in trip.observed


def test_run_and_page_soft_launch_skips_send_and_dedup(fresh_db, monkeypatch):
    monkeypatch.setattr(monitor, "send_alert", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not send")))
    monkeypatch.delenv("SHIP_DELIVERABILITY_ALERTS", raising=False)
    county_id = "deliverability_soft_launch_test"

    campaign = _make_campaign(fresh_db, county_id=county_id)
    today = date(2026, 7, 28)
    fresh_db.add(
        CampaignDailyAnalytics(
            campaign_id=campaign.id,
            snapshot_date=today,
            emails_sent=100,
            bounces=6,
            total_contacts=100,
            opens=0,
            open_rate=0,
            replies=0,
            reply_rate=0,
            clicks=0,
            unsubscribes=0,
            interested=0,
        )
    )
    fresh_db.flush()
    before = fresh_db.execute(
        select(func.count()).select_from(ScraperAlertLog).where(
            ScraperAlertLog.alert_type == "deliverability_cold_bounce_rate_high"
        )
    ).scalar_one()

    monkeypatch.setattr(monitor, "get_db_context", lambda: _db_ctx(fresh_db))
    trips = monitor.run_and_page(today=today, county_id=county_id, warmup_fetcher=lambda: [])

    assert trips
    after = fresh_db.execute(
        select(func.count()).select_from(ScraperAlertLog).where(
            ScraperAlertLog.alert_type == "deliverability_cold_bounce_rate_high"
        )
    ).scalar_one()
    assert after == before


def test_run_and_page_live_sends_and_records_dedup(fresh_db, monkeypatch):
    sent = []
    monkeypatch.setattr(monitor, "send_alert", lambda subject, body: sent.append((subject, body)) or True)
    monkeypatch.setenv("SHIP_DELIVERABILITY_ALERTS", "1")
    county_id = "deliverability_live_test"

    # run_and_page commits its own ScraperAlertLog rows to the real shared DB
    # (no test-transaction rollback, matching repo convention) — a prior run
    # of this test within the last 12h would otherwise make the dedup guard
    # correctly-but-confusingly treat this as "already paged" and the first
    # call below would send nothing. Clear this rule+county_id's history so
    # the test is rerunnable within the dedup window.
    fresh_db.execute(
        text(
            "DELETE FROM scraper_alert_log WHERE alert_type = :t AND county_id = :c"
        ),
        {"t": "deliverability_cold_bounce_rate_high", "c": county_id},
    )
    fresh_db.commit()

    campaign = _make_campaign(fresh_db, county_id=county_id)
    today = date(2026, 7, 28)
    fresh_db.add(
        CampaignDailyAnalytics(
            campaign_id=campaign.id,
            snapshot_date=today,
            emails_sent=100,
            bounces=6,
            total_contacts=100,
            opens=0,
            open_rate=0,
            replies=0,
            reply_rate=0,
            clicks=0,
            unsubscribes=0,
            interested=0,
        )
    )
    fresh_db.flush()
    before = fresh_db.execute(
        select(func.count()).select_from(ScraperAlertLog).where(
            ScraperAlertLog.alert_type == "deliverability_cold_bounce_rate_high",
            ScraperAlertLog.county_id == county_id,
        )
    ).scalar_one()

    monkeypatch.setattr(monitor, "get_db_context", lambda: _db_ctx(fresh_db))
    monitor.run_and_page(today=today, county_id=county_id, warmup_fetcher=lambda: [])
    monitor.run_and_page(today=today, county_id=county_id, warmup_fetcher=lambda: [])

    assert len(sent) == 1
    after = fresh_db.execute(
        select(func.count()).select_from(ScraperAlertLog).where(
            ScraperAlertLog.alert_type == "deliverability_cold_bounce_rate_high",
            ScraperAlertLog.county_id == county_id,
        )
    ).scalar_one()
    assert after == before + 1


class _db_ctx:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc, tb):
        return False
