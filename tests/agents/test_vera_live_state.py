"""
Unit tests for Vera's live-state report (VERA-v2.2 sub-task V2).

Covers only the pure functions (no DB, no git, no network) — drift
classification, migration-target regex extraction, and crontab parsing.
check_deploy_drift() / check_cron_freshness() / check_silent_failures() /
run_live_state() need a live vera_readonly connection and are exercised via
`python -m src.agents.vera --live-state` in staging, not here.
"""
from datetime import date, datetime, timezone

from src.agents.vera.checks.live_state import (
    CronBeat,
    _classify_drift,
    _extract_migration_targets,
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
    silent = {"zero_ingest": [], "unscheduled": []}

    subject, body = render_live_state_report(deploy, beats, silent, report_date=date(2026, 7, 23))

    assert "THE ONE NUMBER" in body
    assert "pending V3 revenue reconciliation" in body
    assert body.rstrip().endswith("— Vera.")
    assert "drift=in_sync" in subject
    assert "0 stale" in subject


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
        "unscheduled": ["insurance_claims"],
    }

    subject, body = render_live_state_report(deploy, [stale_beat], silent, report_date=date(2026, 7, 23))

    assert "STALE" in body
    assert "violations/pinellas" in body
    assert "apply_new_thing.py: not_applied" in body
    assert "apply_vera_readonly_role.py" in body  # listed as unrecognized
    assert "permits/hillsborough" in body
    assert "insurance_claims" in body
    assert "1 stale" in subject
