"""Tests for the Autonomous Inbound Content Loop (Task 5.1) tuning worker.

Decision-core unit tests (no DB) + integration tests (real Postgres via
fresh_db). See docs/adr/0021.

Includes regression coverage for PR #99 review findings:
  - campaign_slug parity between the posting flow and the tuning worker
  - cooldown_days re-clamp after deactivate/evict/insert
  - renewal invoices must not count as new keyword wins
  - evictions must size off truly-insertable (deduped) keywords
"""

from decimal import Decimal

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from src.tasks.autonomous_tuning_worker import (
    decide_action,
    performance_score,
    plan_evictions,
    tune_scraper_keywords,
)
from src.utils.quora_attribution import campaign_slug, clamp_cooldown

MIN_TRIAL = 5
MIN_SIGNUPS = 1


def test_performance_score_is_value_per_answer():
    # (signups * avg_ltv - spend) / (answers + 1)
    # (2 * 500 - 0.10) / (3 + 1) = 999.90 / 4 = 249.975
    score = performance_score(
        paid_signups=2,
        answers_posted=3,
        cumulative_spend=Decimal("0.10"),
        avg_ltv=Decimal("500"),
    )
    assert score == Decimal("249.975")


def test_deactivate_when_enough_trials_and_no_paid_signups():
    action = decide_action(
        paid_signups=0,
        answers_posted=5,
        min_trial_answers=MIN_TRIAL,
        min_signups_to_expand=MIN_SIGNUPS,
    )
    assert action == "deactivate"


def test_keep_when_too_few_trials_even_with_no_signups():
    # Fair-sample guard: a freshly-rotated keyword that has posted fewer than
    # MIN_TRIAL answers must NOT be deactivated (the spec's bare-threshold bug).
    action = decide_action(
        paid_signups=0,
        answers_posted=4,
        min_trial_answers=MIN_TRIAL,
        min_signups_to_expand=MIN_SIGNUPS,
    )
    assert action == "keep"


def test_expand_when_paid_signups_meet_minimum():
    action = decide_action(
        paid_signups=1,
        answers_posted=5,
        min_trial_answers=MIN_TRIAL,
        min_signups_to_expand=MIN_SIGNUPS,
    )
    assert action == "expand"


def test_replace_worst_evicts_lowest_scoring_to_fit_new_under_cap():
    # cap 3, 3 active, 2 incoming → 2 over cap → evict the 2 lowest scorers.
    active = [("a", Decimal("10")), ("b", Decimal("5")), ("c", Decimal("1"))]
    evicted = plan_evictions(active, num_new=2, cap=3)
    assert set(evicted) == {"c", "b"}


def test_no_eviction_when_pool_stays_under_cap():
    active = [("a", Decimal("10")), ("b", Decimal("5"))]
    assert plan_evictions(active, num_new=1, cap=30) == []


# ── Integration: real quora_topics round-trip (Postgres) ──────────────────────

def _seed_keyword(db, keyword, cluster, *, answers, spend_each, paid_signups, now):
    """Insert a quora_topics row plus its posted answers, spend, and paid signups."""
    from src.core.models import (
        AgentDecision,
        QuoraQuestion,
        QuoraTopic,
        Subscriber,
        SubscriptionInvoice,
    )

    topic = QuoraTopic(keyword=keyword, is_active=True, cluster=cluster)
    db.add(topic)
    db.flush()

    posted = now - timedelta(days=1)
    for i in range(answers):
        did = f"{i}-{keyword}"[:36]  # leading index keeps the 36-char id unique
        db.add(AgentDecision(
            decision_id=did, graph_name="quora_channel", cost_usd=spend_each,
        ))
        db.add(QuoraQuestion(
            url=f"https://quora.com/{keyword}/{i}", title=f"q{i}",
            matched_keyword=keyword, lifecycle_decision_id=did,
            answer_status="published", posted_at=posted,
        ))

    slug = "quora_" + "".join(c if c.isalnum() else "_" for c in keyword.lower()).strip("_")[:50]
    for j in range(paid_signups):
        sub = Subscriber(
            stripe_customer_id=f"cus_{keyword}_{j}"[:100], tier="pro",
            vertical="roofing", county_id="hillsborough", status="active",
            utm_campaign=slug,
        )
        db.add(sub)
        db.flush()
        db.add(SubscriptionInvoice(
            subscriber_id=sub.id, stripe_invoice_id=f"in_{keyword}_{j}"[:255],
            amount_collected_cents=29700, period_month=date(now.year, now.month, 1),
            paid_at=now - timedelta(days=2),
        ))
    db.flush()
    return topic


def test_round_trip_deactivates_loser_keeps_young_expands_winner(fresh_db):
    now = datetime.now(timezone.utc)
    db = fresh_db

    winner = _seed_keyword(db, "tdd51 fight insurance denial roof", "contractor_intent",
                           answers=5, spend_each=Decimal("0.01"), paid_signups=1, now=now)
    loser = _seed_keyword(db, "tdd51 cheap roof repairs", "contractor_intent",
                          answers=5, spend_each=Decimal("0.01"), paid_signups=0, now=now)
    young = _seed_keyword(db, "tdd51 new probate tampa", "distress",
                          answers=2, spend_each=Decimal("0.01"), paid_signups=0, now=now)

    captured = {}

    def fake_generate(keyword, cluster, count):
        captured["call"] = (keyword, cluster, count)
        return [f"{keyword} alt"]

    tune_scraper_keywords(db, generate_variations=fake_generate, now=now,
                          max_active_keywords=100)
    db.flush()
    db.expire_all()  # worker writes via raw SQL — drop stale ORM identity-map copies

    assert db.get(type(loser), loser.id).is_active is False     # deactivated
    assert db.get(type(young), young.id).is_active is True       # fair-sample guard
    assert db.get(type(winner), winner.id).is_active is True     # winner stays

    # winner expanded: a new active variation row inherits its cluster
    variation = db.execute(text(
        "SELECT cluster, is_active, last_run_at FROM quora_topics "
        "WHERE keyword = :k"
    ), {"k": "tdd51 fight insurance denial roof alt"}).first()
    assert variation is not None
    assert variation.cluster == "contractor_intent"
    assert variation.is_active is True
    assert variation.last_run_at is None
    assert captured["call"][0] == "tdd51 fight insurance denial roof"


# ── Regression: campaign_slug parity (PR #99 finding #1) ──────────────────────

def test_campaign_slug_matches_admin_router_footer_construction():
    """The posting flow (admin_router) and the tuning worker must derive the
    identical utm_campaign from the same matched_keyword — otherwise the
    tuning worker's signup lookup silently misses real conversions."""
    from src.api.admin_router import campaign_slug as admin_campaign_slug

    for keyword in [
        "Foreclosure Help - Tampa, FL!!",
        "  probate real estate florida  ",
        "a" * 80,  # long, punctuation-heavy, and over the 50-char slug cap
        "tax-lien/certificate (2026)",
    ]:
        assert admin_campaign_slug(keyword) == campaign_slug(keyword)


# ── Regression: cooldown clamp after pool mutation (PR #99 finding #2) ────────

def test_clamp_cooldown_reduces_to_fit_shrunk_pool(fresh_db):
    from src.core.models import QuoraTopic, QuoraSettings

    db = fresh_db
    # Set cooldown far above any plausible active pool size so the clamp is
    # guaranteed to bind regardless of pre-existing rows in the shared DB.
    db.merge(QuoraSettings(id=1, cooldown_days=999_999))
    db.add(QuoraTopic(keyword="tdd51-clamp-cooldown-test", is_active=True))
    db.flush()

    clamp_cooldown(db)
    db.flush()

    active_count = db.execute(text(
        "SELECT COUNT(*) FROM quora_topics WHERE is_active = true"
    )).scalar()
    row = db.execute(text("SELECT cooldown_days FROM quora_settings WHERE id = 1")).first()
    assert row.cooldown_days == max(0, active_count - 1)


def test_tune_scraper_keywords_reclamps_cooldown_after_deactivation(fresh_db):
    """A weekly run that deactivates keywords down to a small pool must never
    leave cooldown_days too high for quora_s6_orchestrator's picker to find
    an eligible row."""
    from src.core.models import QuoraSettings

    now = datetime.now(timezone.utc)
    db = fresh_db
    db.merge(QuoraSettings(id=1, cooldown_days=10))
    db.flush()

    loser = _seed_keyword(db, "tdd51-cooldown-loser", "distress",
                          answers=5, spend_each=Decimal("0.01"), paid_signups=0, now=now)

    tune_scraper_keywords(db, generate_variations=lambda *a: [], now=now,
                          max_active_keywords=100)
    db.flush()

    active_count = db.execute(text(
        "SELECT COUNT(*) FROM quora_topics WHERE is_active = true"
    )).scalar()
    row = db.execute(text("SELECT cooldown_days FROM quora_settings WHERE id = 1")).first()
    assert row.cooldown_days <= max(0, active_count - 1)


# ── Regression: renewals must not count as new wins (PR #99 finding #3) ───────

def test_renewal_from_old_subscriber_does_not_count_as_new_signup(fresh_db):
    from src.core.models import QuoraTopic, Subscriber, SubscriptionInvoice

    now = datetime.now(timezone.utc)
    db = fresh_db
    keyword = "tdd51-renewal-only"
    topic = QuoraTopic(keyword=keyword, is_active=True, cluster="distress")
    db.add(topic)
    db.flush()

    slug = campaign_slug(keyword)
    # Subscriber acquired 200 days ago (well before the 30-day window) whose
    # renewal invoice happens to land inside the window.
    old_sub = Subscriber(
        stripe_customer_id="cus_renewal_only", tier="pro", vertical="roofing",
        county_id="hillsborough", status="active", utm_campaign=slug,
        created_at=now - timedelta(days=200),
    )
    db.add(old_sub)
    db.flush()
    db.add(SubscriptionInvoice(
        subscriber_id=old_sub.id, stripe_invoice_id="in_renewal_only",
        amount_collected_cents=29700, period_month=date(now.year, now.month, 1),
        paid_at=now - timedelta(days=2),
    ))
    db.flush()

    result = tune_scraper_keywords(db, generate_variations=lambda *a: [], now=now,
                                   min_trial_answers=100, max_active_keywords=100)
    db.flush()
    db.expire_all()

    # No answers posted at all for this keyword — deactivate only fires at
    # min_trial_answers=100 (never reached), so "keep" is the only path that
    # could misfire into "expand" if the renewal were miscounted as a win.
    reloaded = db.get(type(topic), topic.id)
    assert reloaded.signup_count == 0, "renewal from a pre-window subscriber must not count as a new signup"
    assert result["expanded"] == 0


# ── Regression: evictions sized off deduped/insertable keywords (PR #99 #4) ───

def test_eviction_sized_off_deduped_insertable_keywords_not_raw_generated_count(fresh_db):
    """generate_variations returning duplicates/already-existing keywords must
    not evict more active keywords than can actually be (re)inserted."""
    from src.core.models import QuoraTopic

    now = datetime.now(timezone.utc)
    db = fresh_db

    winner = _seed_keyword(db, "tdd51-evict-winner", "distress",
                           answers=5, spend_each=Decimal("0.01"), paid_signups=1, now=now)

    # Pool of 3 additional low-scoring active keywords that would be eviction
    # candidates if the raw (non-deduped) generated count were used to size evictions.
    filler_ids = []
    for i in range(3):
        t = QuoraTopic(keyword=f"tdd51-evict-filler-{i}", is_active=True,
                       performance_score=Decimal("0"))
        db.add(t)
        db.flush()
        filler_ids.append(t.id)

    # Generator returns 3 raw variations, but 2 are duplicates of each other and
    # 1 already exists in quora_topics — only 1 keyword is truly insertable.
    db.add(QuoraTopic(keyword="tdd51-evict-already-exists", is_active=False))
    db.flush()

    def fake_generate(keyword, cluster, count):
        return ["tdd51-evict-dup", "tdd51-evict-dup", "tdd51-evict-already-exists"]

    tune_scraper_keywords(db, generate_variations=fake_generate, now=now,
                          max_active_keywords=4)  # cap tight enough to force eviction sizing to matter
    db.flush()
    db.expire_all()

    remaining_active_fillers = sum(
        1 for fid in filler_ids if db.get(QuoraTopic, fid).is_active
    )
    # Only 1 keyword ("tdd51-evict-dup") is truly insertable, so at most 1
    # filler should have been evicted — not 3 (the raw generated count).
    assert remaining_active_fillers >= 2, (
        "eviction must be sized off deduped/insertable keywords, not the raw "
        "generated variation count"
    )
