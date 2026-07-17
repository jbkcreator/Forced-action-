"""sunbiz_run_verdict — single source of truth for a Sunbiz run's health.

Regression for the drift bug: the cron task used a zero-tolerance
`failed == 0` rule and recorded scraper_error every day while enriching
~198/200 owners, whereas the engine used a tolerant threshold. Both now call
this one function.
"""
from src.scrappers.sunbiz.sunbiz_engine import sunbiz_run_verdict


def test_clean_run_is_healthy():
    assert sunbiz_run_verdict({"processed": 200, "failed": 0}) == (True, None, None)


def test_one_off_failure_is_still_healthy():
    # The exact case that false-alarmed daily under `failed == 0`.
    ok, etype, _ = sunbiz_run_verdict({"processed": 200, "failed": 1})
    assert ok is True and etype is None


def test_below_rate_threshold_is_healthy():
    # 3 failures but only 1.5% — under the 15% rate gate → healthy.
    ok, etype, _ = sunbiz_run_verdict({"processed": 200, "failed": 3})
    assert ok is True and etype is None


def test_frequent_failures_are_broken():
    ok, etype, msg = sunbiz_run_verdict({"processed": 200, "failed": 40})
    assert ok is False
    assert etype == "scraper_error"
    assert "20%" in msg


def test_rate_limited_is_informational_success():
    ok, etype, msg = sunbiz_run_verdict(
        {"processed": 50, "failed": 0, "rate_limited": True, "remaining_unprocessed": 150}
    )
    assert ok is True                      # throttle resumes next run, not a failure
    assert etype == "rate_limited"
    assert "150 owner(s) unprocessed" in msg


def test_rate_limited_wins_even_with_failures():
    # A throttle abort can also carry failures; rate_limited takes precedence.
    ok, etype, _ = sunbiz_run_verdict(
        {"processed": 10, "failed": 5, "rate_limited": True, "remaining_unprocessed": 190}
    )
    assert ok is True and etype == "rate_limited"


def test_empty_stats_does_not_divide_by_zero():
    assert sunbiz_run_verdict({}) == (True, None, None)
