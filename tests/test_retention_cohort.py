"""T-B8-04 — compute_retention_cohorts (paid logo retention viewport).

Mirrors tests/test_operator_dashboard.py's pattern: real Postgres via
fresh_db, seeded ORM rows, far-past cohort months so the trailing-12-month
window in the query is deterministic regardless of when tests run.

See docs/adr/0038-retention-cohort-paid-logo-aged-off-mrr-ledger.md and
CONTEXT.md's "Retention Cohort" entry for the metric definition.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from src.core.models import CustomerAccount, MrrMovement, Subscriber, ZipTerritory
from src.services.retention_cohort import compute_retention_cohorts

# A cohort month firmly inside "now"'s trailing-12-month window, but with
# enough headroom for a few months of aging cells regardless of test run date.
# Truncate to day=1 AFTER subtracting, so this lands exactly on the same
# boundary date_trunc('month', ...) produces in the query — otherwise a
# fixed-day offset can land on day 2+ of an earlier month, one truncation off
# from the query's month bucket.
COHORT_MONTH = (datetime.now(timezone.utc) - timedelta(days=90)).replace(
    day=1, hour=0, minute=0, second=0, microsecond=0
)


def _sub(db, *, created_at=COHORT_MONTH, tier="pro", signup_source="direct", utm_source=None):
    tag = uuid.uuid4().hex[:12]
    s = Subscriber(
        stripe_customer_id=f"cus_rc_{tag}",
        tier=tier, vertical="roofing", county_id="hillsborough", status="active",
        event_feed_uuid=f"rc-{tag}", email=f"rc_{tag}@example.com",
        created_at=created_at, signup_source=signup_source, utm_source=utm_source,
    )
    db.add(s)
    db.flush()
    return s


def _acct(db, sub):
    a = CustomerAccount(subscriber_id=sub.id, status="active", mrr_cents=10000)
    db.add(a)
    db.flush()
    return a


def _movement(db, acct, *, movement_type, at, delta=10000):
    db.add(MrrMovement(
        account_id=acct.account_id, movement_type=movement_type,
        delta_cents=delta, mrr_after_cents=max(delta, 0), effective_at=at,
    ))
    db.flush()


def test_shape_has_filters_months_and_cohorts_keys(fresh_db):
    result = compute_retention_cohorts(fresh_db)
    assert set(result.keys()) == {"filters", "months", "cohorts"}
    assert result["filters"] == {"channel": None, "tier": None, "zip": None}
    assert result["months"] == 12


def test_subscriber_without_new_movement_is_excluded(fresh_db):
    sub = _sub(fresh_db)
    _acct(fresh_db, sub)  # no MrrMovement at all — never entered paid

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    assert key not in {c["cohort_month"] for c in result["cohorts"]}


def test_alive_at_m0_and_m1_for_a_never_churned_subscriber(fresh_db):
    subs = [_sub(fresh_db) for _ in range(5)]  # min cohort size, avoid suppression
    for sub in subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)

    assert cohort["size"] == 5
    assert cohort["suppressed"] is False
    m0 = next(c for c in cohort["cells"] if c["m"] == 0)
    m1 = next(c for c in cohort["cells"] if c["m"] == 1)
    assert m0 == {"m": 0, "alive": 5, "rate": 1.0}
    assert m1 == {"m": 1, "alive": 5, "rate": 1.0}


def test_churned_subscriber_drops_out_at_the_month_it_churns(fresh_db):
    subs = [_sub(fresh_db) for _ in range(5)]
    for i, sub in enumerate(subs):
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)
        if i == 0:
            # churns before month-1 boundary — dead from M1 onward
            _movement(fresh_db, acct, movement_type="churn", at=COHORT_MONTH + timedelta(days=20), delta=-10000)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)

    m0 = next(c for c in cohort["cells"] if c["m"] == 0)
    m1 = next(c for c in cohort["cells"] if c["m"] == 1)
    assert m0["alive"] == 5  # churn happens after M0 boundary
    assert m1["alive"] == 4  # dead by M1 boundary


def test_reactivation_recounts_subscriber_as_alive(fresh_db):
    subs = [_sub(fresh_db) for _ in range(5)]
    reactivated_acct = None
    for i, sub in enumerate(subs):
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)
        if i == 0:
            reactivated_acct = acct
            # both events must land inside the M1 cutoff (cohort_month + 1
            # month, as little as 28 days for February) to be re-counted alive
            # by month 1.
            _movement(fresh_db, acct, movement_type="churn", at=COHORT_MONTH + timedelta(days=10), delta=-10000)
            _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH + timedelta(days=20), delta=10000)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)
    m1 = next(c for c in cohort["cells"] if c["m"] == 1)
    assert m1["alive"] == 5  # reactivation within month 1 re-counts as alive


def test_small_cohort_is_suppressed(fresh_db):
    subs = [_sub(fresh_db) for _ in range(3)]  # below MIN_COHORT_SIZE
    for sub in subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)

    assert cohort["suppressed"] is True
    assert cohort["size"] == 3
    m0 = next(c for c in cohort["cells"] if c["m"] == 0)
    assert m0["rate"] is None
    assert m0["alive"] == 3  # raw count still returned, just no rate


def test_not_yet_aged_month_renders_null_not_zero(fresh_db):
    subs = [_sub(fresh_db) for _ in range(5)]
    for sub in subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    result = compute_retention_cohorts(fresh_db, tier="pro", months=24)
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)

    future_cell = cohort["cells"][-1]  # far beyond how much this cohort has aged
    assert future_cell["alive"] is None
    assert future_cell["rate"] is None


def test_tier_filter_isolates_matching_subscribers(fresh_db):
    pro_subs = [_sub(fresh_db, tier="pro") for _ in range(5)]
    starter_subs = [_sub(fresh_db, tier="starter") for _ in range(5)]
    for sub in pro_subs + starter_subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)
    assert cohort["size"] == 5  # only the pro-tier subscribers


def test_channel_filter_matches_channel_key_sql(fresh_db):
    meta_subs = [_sub(fresh_db, utm_source="facebook") for _ in range(5)]
    direct_subs = [_sub(fresh_db, signup_source="direct") for _ in range(5)]
    for sub in meta_subs + direct_subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    result = compute_retention_cohorts(fresh_db, channel="meta")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)
    assert cohort["size"] == 5  # facebook normalizes to 'meta' per _CHANNEL_KEY_SQL


def test_zip_filter_isolates_subscriber_holding_that_zip(fresh_db):
    zip_code = f"9{uuid.uuid4().int % 10000:04d}"  # unlikely to collide with shared-DB rows
    with_zip = _sub(fresh_db)
    without_zip = [_sub(fresh_db) for _ in range(4)]
    for sub in [with_zip] + without_zip:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=COHORT_MONTH)

    fresh_db.add(ZipTerritory(
        zip_code=zip_code, vertical="roofing", county_id="hillsborough",
        subscriber_id=with_zip.id, status="locked",
    ))
    fresh_db.flush()

    result = compute_retention_cohorts(fresh_db, zip_code=zip_code)
    key = COHORT_MONTH.strftime("%Y-%m")
    matching = [c for c in result["cohorts"] if c["cohort_month"] == key]
    assert len(matching) == 1
    assert matching[0]["size"] == 1


def test_late_conversion_is_alive_at_m0_not_m1(fresh_db):
    # Subscriber row created mid-month but doesn't convert to paid (first
    # 'new' movement) until weeks later — the free-trial-to-paid gap. Aging
    # must anchor to entered_at, not the calendar signup month, or this looks
    # dead at M0 and alive at M1 (an impossible 0% -> 100% curve).
    created_at = COHORT_MONTH + timedelta(days=10)
    entered_at = COHORT_MONTH + timedelta(days=25)
    subs = [_sub(fresh_db, created_at=created_at) for _ in range(5)]
    for sub in subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=entered_at)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")  # row still labeled by signup month
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)
    m0 = next(c for c in cohort["cells"] if c["m"] == 0)
    assert m0["alive"] == 5  # alive immediately upon conversion, not dead until M1


def test_free_subscriber_upgrading_in_a_later_month_is_not_churned_before_conversion(fresh_db):
    # Signup month and paid-conversion month can differ entirely (free tier
    # for a while, upgrade later). Still must not appear dead at M0.
    created_at = COHORT_MONTH
    entered_at = COHORT_MONTH + timedelta(days=45)  # converts ~1.5 months later
    subs = [_sub(fresh_db, created_at=created_at) for _ in range(5)]
    for sub in subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=entered_at)

    result = compute_retention_cohorts(fresh_db, tier="pro")
    key = COHORT_MONTH.strftime("%Y-%m")
    cohort = next(c for c in result["cohorts"] if c["cohort_month"] == key)
    m0 = next(c for c in cohort["cells"] if c["m"] == 0)
    assert m0["alive"] == 5


def test_cohorts_param_returns_exact_requested_count(fresh_db):
    # cohorts=1 must return only the current calendar month, not the current
    # month plus the prior one (the off-by-one this regression guards).
    now = datetime.now(timezone.utc)
    this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month = (this_month - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    this_month_subs = [_sub(fresh_db, created_at=this_month) for _ in range(5)]
    prev_month_subs = [_sub(fresh_db, created_at=prev_month) for _ in range(5)]
    for sub in this_month_subs + prev_month_subs:
        acct = _acct(fresh_db, sub)
        _movement(fresh_db, acct, movement_type="new", at=sub.created_at)

    result = compute_retention_cohorts(fresh_db, tier="pro", cohorts=1)
    keys = {c["cohort_month"] for c in result["cohorts"]}
    assert this_month.strftime("%Y-%m") in keys
    assert prev_month.strftime("%Y-%m") not in keys
