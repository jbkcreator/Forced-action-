"""
Unit tests for Vera's live-state report (VERA-v2.2 sub-task V2).

Covers only the pure functions (no DB, no git, no network) — drift
classification, migration-target regex extraction, and crontab parsing.
check_deploy_drift() / check_cron_freshness() / check_silent_failures() /
run_live_state() need a live vera_readonly connection and are exercised via
`python -m src.agents.vera --live-state` in staging, not here.
"""
from datetime import date, datetime, timedelta, timezone

import src.agents.vera.checks.live_state as live_state
from src.agents.vera.checks.live_state import (
    CronBeat,
    _classify_drift,
    _extract_migration_targets,
    _off_day_pairs,
    _scheduled_source_types,
    check_freshness_regression,
    format_outcome_label,
    render_live_state_report,
)
from src.tasks.heartbeat_monitor import FreshnessRegression


def test_classify_drift_in_sync():
    assert _classify_drift("abc", "abc", "abc") == "in_sync"


def test_classify_drift_behind():
    assert _classify_drift("abc", "abc", "def") == "behind"


def test_classify_drift_head_mismatch_takes_priority():
    # last_good disagreeing with HEAD is a deploy-integrity finding in its
    # own right — reported even when HEAD happens to equal dev HEAD too.
    assert _classify_drift("abc", "xyz", "abc") == "head_mismatch"


def test_classify_drift_unknown_on_missing_data():
    assert _classify_drift(None, "abc", "def") == "unknown"
    assert _classify_drift("abc", "abc", None) == "unknown"


def test_classify_drift_no_last_good_file_falls_through_to_dev_compare():
    assert _classify_drift("abc", None, "abc") == "in_sync"
    assert _classify_drift("abc", None, "def") == "behind"


def test_extract_migration_targets_table_and_columns():
    sql_text = (
        "CREATE TABLE IF NOT EXISTS vera_facts (\n"
        "    id BIGSERIAL PRIMARY KEY\n"
        ")\n"
        "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS onboarding_completed BOOLEAN;\n"
        "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS preferred_property_type VARCHAR(50);\n"
    )
    tables, columns = _extract_migration_targets(sql_text)
    assert tables == ["vera_facts"]
    assert columns == [
        ("subscribers", "onboarding_completed"),
        ("subscribers", "preferred_property_type"),
    ]


def test_extract_migration_targets_unrecognized_shape_is_empty():
    # apply_vera_readonly_role.py's actual shape — a role-grant DO $$ block,
    # neither CREATE TABLE IF NOT EXISTS nor ADD COLUMN IF NOT EXISTS.
    role_grant_text = "DO $$ BEGIN CREATE ROLE vera_readonly LOGIN; END $$;"
    tables, columns = _extract_migration_targets(role_grant_text)
    assert tables == []
    assert columns == []


def test_scheduled_source_types_ignores_commented_lines():
    crontab_text = (
        "# comment line, ignored\n"
        "0 4 * * 1-6 $PROJECT/scripts/cron/run.sh "
        "src.scrappers.violation.violation_engine --load-to-db\n"
        "# 0 5 * * 1-6 $PROJECT/scripts/cron/run.sh "
        "src.scrappers.foreclosures.foreclosure_engine --disabled\n"
    )
    scheduled = _scheduled_source_types(crontab_text)
    assert scheduled == {"violations"}


def test_scheduled_source_types_lien_engine_feeds_all_subtypes():
    crontab_text = (
        "0 5 * * 1-6 $PROJECT/scripts/cron/run.sh src.scrappers.liens.lien_engine "
        "--load-to-db --county-id $COUNTY_ID\n"
    )
    scheduled = _scheduled_source_types(crontab_text)
    assert scheduled == {"lien_ml", "lien_tcl", "lien_ccl", "lien_hoa", "lien_tl", "judgments"}


def test_scheduled_source_types_unknown_module_is_ignored():
    crontab_text = "0 4 * * * $PROJECT/scripts/cron/run.sh src.tasks.some_unrelated_task\n"
    assert _scheduled_source_types(crontab_text) == set()


def test_render_live_state_report_includes_the_one_number_placeholder():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
        "drift": "in_sync", "pending_migrations": [],
        "migration_statuses": {"apply_x.py": "applied"},
    }
    beats = [CronBeat("violations", "hillsborough", 1500, datetime.now(timezone.utc), 10, False)]
    silent = {
        "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [], "unscheduled": [],
    }

    subject, body, html_body = render_live_state_report(
        deploy, beats, silent, report_date=date(2026, 7, 23),
    )

    assert "THE ONE NUMBER" in body
    assert "pending V3 revenue reconciliation" in body
    assert body.rstrip().endswith("— Vera.")
    assert "drift=in_sync" in subject
    assert "0 stale" in subject
    assert "<html" not in html_body.lower()  # a fragment, not a full document
    assert "THE ONE NUMBER" in html_body


def test_render_live_state_report_surfaces_stale_and_unrecognized():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "b" * 40,
        "drift": "behind", "pending_migrations": ["apply_new_thing.py"],
        "migration_statuses": {
            "apply_new_thing.py": "not_applied",
            "apply_vera_readonly_role.py": "unrecognized",
        },
    }
    stale_beat = CronBeat("violations", "pinellas", 1500, None, None, True)
    silent = {
        "zero_ingest": [{"source_type": "permits", "county_id": "hillsborough", "total_scraped": 0}],
        "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [{"source_type": "permits", "county_id": "hillsborough", "total_scraped": 0}],
        "unscheduled": ["insurance_claims"],
    }

    subject, body, html_body = render_live_state_report(
        deploy, [stale_beat], silent, report_date=date(2026, 7, 23),
    )

    assert "STALE" in body
    assert "violations/pinellas" in body
    assert "apply_new_thing.py: not_applied" in body
    assert "apply_vera_readonly_role.py" in body  # listed as unrecognized
    assert "permits/hillsborough" in body
    assert "insurance_claims" in body
    assert "1 stale" in subject
    assert "violations/pinellas" in html_body
    assert "permits/hillsborough" in html_body


def test_render_live_state_report_never_run_beat_shows_never_run_not_minutes():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
        "drift": "in_sync", "pending_migrations": [], "migration_statuses": {},
    }
    stale_beat = CronBeat("violations", "pinellas", 1500, None, None, True)
    silent = {
        "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [], "unscheduled": [],
    }

    body = render_live_state_report(deploy, [stale_beat], silent, report_date=date(2026, 7, 23))[1]
    assert "age=never run" in body


def test_render_live_state_report_formats_age_in_hours_not_raw_minutes():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
        "drift": "in_sync", "pending_migrations": [], "migration_statuses": {},
    }
    # 1834 minutes ago — should render as ~30.6h, not "1834 min".
    old = datetime.now(timezone.utc) - timedelta(minutes=1834)
    stale_beat = CronBeat("probate", "pinellas", 1500, old, 1834, True)
    silent = {
        "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [], "unscheduled": [],
    }

    body = render_live_state_report(deploy, [stale_beat], silent, report_date=date(2026, 7, 23))[1]
    assert "1834 min" not in body
    assert "30.6h" in body
    assert "SLA = max time allowed" in body  # the legend line


# ─────────────────────────────────────────────────────────────────────────────
# Regression: PR #173 review — off-day sources must not leave yesterday's
# stale verdict standing forever (live_state.py:336-384)
# ─────────────────────────────────────────────────────────────────────────────

def test_off_day_pairs_includes_multi_county_source_on_its_off_day():
    # "violations" is off on Sunday (weekday 6) and runs both counties.
    sunday = datetime(2026, 7, 26, tzinfo=timezone.utc)
    pairs = _off_day_pairs(sunday)
    assert ("violations", "hillsborough") in pairs
    assert ("violations", "pinellas") in pairs


def test_off_day_pairs_excludes_source_on_a_scheduled_day():
    monday = datetime(2026, 7, 27, tzinfo=timezone.utc)
    pairs = _off_day_pairs(monday)
    assert not any(source == "violations" for source, _county in pairs)


def test_write_off_day_facts_writes_not_scheduled_today_not_stale(monkeypatch):
    calls = []

    def _fake_write_fact(fact_key, fact_value, **kwargs):
        calls.append((fact_key, fact_value))

    monkeypatch.setattr(live_state, "write_fact", _fake_write_fact)
    live_state._write_off_day_facts([("violations", "hillsborough")])

    assert calls == [("cron.violations.freshness", "not_scheduled_today")]


def test_render_live_state_report_explains_missing_repo_dir_instead_of_bare_unknown():
    deploy = {
        "head_sha": None, "last_good_sha": None, "dev_head_sha": None,
        "drift": "unknown", "pending_migrations": [], "migration_statuses": {},
        "repo_dir": "/root/Forced-action-", "repo_dir_missing": True,
    }
    beats = []
    silent = {
        "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [], "unscheduled": [],
    }

    body, html_body = render_live_state_report(deploy, beats, silent, report_date=date(2026, 7, 23))[1:]

    assert "not found on this host" in body
    assert "/root/Forced-action-" in body
    # The old bare "unknown" wall must not appear alongside the new explanation.
    assert "Prod HEAD:" not in body
    assert "not found on this host" in html_body


# ─────────────────────────────────────────────────────────────────────────────
# format_outcome_label() — the shared "never confused about which taxonomy
# a label belongs to" tag, used in ZERO-INGEST SOURCES and CRON FRESHNESS's
# "last recorded outcome".
# ─────────────────────────────────────────────────────────────────────────────

def test_format_outcome_label_prefers_real_outcome_category():
    assert format_outcome_label("TIMEOUT", "scraper_error") == "[TIMEOUT]"


def test_format_outcome_label_falls_back_to_legacy_error_type_labeled_as_such():
    # Explicitly tagged "legacy:" so it can never be mistaken for one of the
    # 5 enforced outcome_category values.
    assert format_outcome_label(None, "scraper_error") == "[legacy: scraper_error]"


def test_format_outcome_label_unclassified_when_neither_present():
    assert format_outcome_label(None, None) == "[UNCLASSIFIED]"


# ─────────────────────────────────────────────────────────────────────────────
# CronBeat.last_attempt_label() — "why" a stale source is stale, not just
# "that" it is.
# ─────────────────────────────────────────────────────────────────────────────

def test_last_attempt_label_none_when_no_attempt_row_at_all():
    beat = CronBeat("violations", "pinellas", 1500, None, None, True)
    assert beat.last_attempt_label() is None


def test_last_attempt_label_distinguishes_long_dead_silence_from_a_recent_failure():
    # The exact scenario that motivated this feature: a source whose LAST
    # recorded row is a clean no_data day from days ago must not be
    # confused with "it's actively finding nothing every day."
    now = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    beat = CronBeat(
        "lien_ccl", "pinellas", 1500, None, 7200, True,
        last_attempt_run_date=date(2026, 8, 28),
        last_attempt_outcome_category="NO_DATA",
        last_attempt_error_type="no_data",
        last_attempt_error_message=None,
    )
    label = beat.last_attempt_label(now=now)
    assert "[NO_DATA]" in label
    assert "2026-08-28" in label
    assert "5d ago" in label
    assert "no runs recorded since" in label


def test_last_attempt_label_shows_todays_real_failure_with_message():
    now = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    beat = CronBeat(
        "bankruptcy", "hillsborough", 1500, None, 1519, True,
        last_attempt_run_date=date(2026, 9, 2),
        last_attempt_outcome_category=None,
        last_attempt_error_type="scraper_error",
        last_attempt_error_message="429 Client Error: Too Many Requests for url: https://www.courtlistener.com/...",
    )
    label = beat.last_attempt_label(now=now)
    assert "[legacy: scraper_error]" in label
    assert "today" in label
    assert "429 Client Error" in label


def test_render_live_state_report_shows_last_attempt_on_stale_lines():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
        "drift": "in_sync", "pending_migrations": [], "migration_statuses": {},
    }
    stale_beat = CronBeat(
        "bankruptcy", "hillsborough", 1500, None, 1519, True,
        last_attempt_run_date=date(2026, 9, 2),
        last_attempt_outcome_category="SOURCE_ERROR",
        last_attempt_error_type="scraper_error",
        last_attempt_error_message="429 Client Error: Too Many Requests",
    )
    silent = {
        "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
        "zero_ingest_unexplained": [], "unscheduled": [],
    }

    body = render_live_state_report(deploy, [stale_beat], silent, report_date=date(2026, 9, 2))[1]

    assert "last recorded outcome: [SOURCE_ERROR]" in body
    assert "429 Client Error" in body


def test_render_live_state_report_shows_category_on_silent_failure_entries():
    deploy = {
        "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
        "drift": "in_sync", "pending_migrations": [], "migration_statuses": {},
    }
    silent = {
        "zero_ingest": [{"source_type": "storm_damage", "county_id": "hillsborough",
                          "total_scraped": 0, "outcome_category": "NO_DATA", "error_type": "no_data"}],
        "zero_ingest_confirmed_no_data": [{"source_type": "storm_damage", "county_id": "hillsborough",
                                            "total_scraped": 0, "outcome_category": "NO_DATA", "error_type": "no_data"}],
        "zero_ingest_unexplained": [],
        "unscheduled": [],
    }

    body = render_live_state_report(deploy, [], silent, report_date=date(2026, 9, 2))[1]

    assert "storm_damage/hillsborough  [NO_DATA]" in body


_EMPTY_SILENT = {
    "zero_ingest": [], "zero_ingest_confirmed_no_data": [],
    "zero_ingest_unexplained": [], "unscheduled": [],
}
_IN_SYNC_DEPLOY = {
    "head_sha": "a" * 40, "last_good_sha": "a" * 40, "dev_head_sha": "a" * 40,
    "drift": "in_sync", "pending_migrations": [], "migration_statuses": {},
}


def test_render_live_state_report_omits_banner_when_no_regression_passed():
    """Backward-compat: `regression` defaults to None -> no banner, exactly
    like every pre-existing caller/test in this file that doesn't pass it."""
    body, html_body = render_live_state_report(
        _IN_SYNC_DEPLOY, [], _EMPTY_SILENT, report_date=date(2026, 9, 2),
    )[1:]
    assert "FRESHNESS REGRESSION" not in body
    assert "FRESHNESS REGRESSION" not in html_body


def test_render_live_state_report_omits_banner_below_threshold():
    """A FreshnessRegression with fewer than the minimum newly-stale sources
    must not render a banner — is_regression is False, not just "small"."""
    regression = FreshnessRegression(
        now_fresh_count=40, now_total_count=41, baseline_fresh_count=41,
        baseline_total_count=41, baseline_days=4, newly_stale=["permits"],
    )
    body = render_live_state_report(
        _IN_SYNC_DEPLOY, [], _EMPTY_SILENT, report_date=date(2026, 9, 2),
        regression=regression,
    )[1]
    assert "FRESHNESS REGRESSION" not in body


def test_render_live_state_report_shows_banner_and_subject_at_threshold():
    regression = FreshnessRegression(
        now_fresh_count=38, now_total_count=41, baseline_fresh_count=41,
        baseline_total_count=41, baseline_days=4,
        newly_stale=["permits", "foreclosures", "sunbiz"],
    )
    subject, body, html_body = render_live_state_report(
        _IN_SYNC_DEPLOY, [], _EMPTY_SILENT, report_date=date(2026, 9, 2),
        regression=regression,
    )
    assert "FRESHNESS REGRESSION" in body
    assert "38/41" in body
    assert "41/41" in body
    for label in regression.newly_stale:
        assert label in body
    assert "REGRESSION(3)" in subject
    assert "FRESHNESS REGRESSION" in html_body


def test_check_freshness_regression_diffs_against_four_day_baseline(monkeypatch):
    calls = []

    def fake_check_cron_freshness(now=None, include_off_days=False):
        calls.append((now, include_off_days))
        is_baseline = len(calls) == 2
        return [CronBeat("permits", "hillsborough", 1500, None, 0, is_stale=not is_baseline)]

    monkeypatch.setattr(live_state, "check_cron_freshness", fake_check_cron_freshness)
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)

    result = check_freshness_regression(now=now)

    assert result.baseline_days == 4
    assert calls[0] == (now, False)
    # include_off_days=True on the baseline call — regression for the
    # "Thursday baseline excludes most of the monitored fleet" finding:
    # without it, a baseline landing on a source's off-day silently omits
    # that source, and it can never be flagged newly stale.
    assert calls[1] == (now - timedelta(days=4), True)
    assert result.newly_stale == ["permits/hillsborough"]
