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
    render_live_state_report,
)


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
