"""
Tests for Clone-Pack assembly (CLONE-v2.2 / CL4).

The Clone-Pack is what makes "this venture can run" a computed answer rather
than a human reading `--health` output, so these tests are mostly about the two
failure modes it exists to catch:

- a source URL still equal to the template county's, i.e. never overridden —
  the scraper resolves a real URL and quietly hits the wrong county's portal;
- Relay identity read from the resolved VentureConfig instead of the row, where
  CL3's env fallback substitutes venture #1's Instantly campaign and makes a
  fresh venture look ready.

Real Postgres: the coverage query uses JSONB and CTEs.
"""
from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text

from config.venture_template import DEFAULT_VENTURE_KEY, REQUIRED_SIGNAL_TYPES
from src.services import clone_pack


@pytest.fixture
def cl4_db(fresh_db):
    tables = fresh_db.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name IN ('venture_ladder_events', 'venture_ladder_evidence')
    """)).scalar_one()
    if tables < 2:
        pytest.skip(
            "CL4 tables absent — run "
            "`PYTHONPATH=. python migrations/apply_cl4_venture_ladder.py` first"
        )
    return fresh_db


@pytest.fixture
def venture(cl4_db):
    """A venture with a template county, one own county, and full Relay identity.

    Returns (venture_key, county_id, template_county_id).
    """
    key = f"test_pack_{uuid.uuid4().hex[:10]}"
    county_id = f"{key}_county"
    template_id = f"{key}_template"

    cl4_db.execute(
        text("""
            INSERT INTO ventures (
                venture_key, display_name, brand_name, template_county_id,
                relay_slack_channel, relay_approvers,
                relay_instantly_campaign_id, relay_instantly_sender_email,
                kill_switch_feature, ladder_stage, is_active
            ) VALUES (
                :key, 'Pack Test', 'Pack Test', :template_id,
                '#pack-test', CAST(:approvers AS jsonb),
                'camp_pack_test', 'pack@test.invalid',
                'relay_pack_test', 'spin_up', false
            )
        """),
        {"key": key, "template_id": template_id, "approvers": json.dumps(["U1"])},
    )
    for cid, name in ((template_id, "Template"), (county_id, "Own")):
        cl4_db.execute(
            text("""
                INSERT INTO counties (
                    county_id, display_name, venture_key, zip_prefixes, is_active
                ) VALUES (:cid, :name, :key, '[]'::jsonb, true)
            """),
            {"cid": cid, "name": name, "key": key},
        )
    # Template sources, then the venture county's own overridden sources.
    cl4_db.execute(
        text("""
            INSERT INTO county_sources (
                county_id, signal_type, url, is_active, date_range_available, scrape_mode
            ) VALUES (:cid, :sig, :url, true, true, 'ai_only')
        """),
        [
            {"cid": template_id, "sig": sig, "url": f"https://template.invalid/{sig}"}
            for sig in REQUIRED_SIGNAL_TYPES
        ] + [
            {"cid": county_id, "sig": sig, "url": f"https://own.invalid/{sig}"}
            for sig in REQUIRED_SIGNAL_TYPES
        ],
    )
    cl4_db.flush()
    return key, county_id, template_id


# ── coverage ─────────────────────────────────────────────────────────────────


def test_fully_overridden_venture_has_no_coverage_gaps(cl4_db, venture):
    key, county_id, template_id = venture
    coverage = clone_pack.source_coverage(cl4_db, key, template_county_id=template_id)
    assert coverage[county_id] == []


def test_inherited_url_counts_as_uncovered(cl4_db, venture):
    """The CL3 failure mode: a URL identical to the template's was never
    overridden, so the scraper would hit the wrong county's portal."""
    key, county_id, template_id = venture
    cl4_db.execute(
        text("""
            UPDATE county_sources SET url = :inherited
            WHERE county_id = :cid AND signal_type = 'foreclosures'
        """),
        {"cid": county_id, "inherited": "https://template.invalid/foreclosures"},
    )
    cl4_db.flush()

    coverage = clone_pack.source_coverage(cl4_db, key, template_county_id=template_id)
    assert coverage[county_id] == ["foreclosures"]


def test_missing_and_blank_urls_count_as_uncovered(cl4_db, venture):
    key, county_id, template_id = venture
    cl4_db.execute(
        text("UPDATE county_sources SET url = '' WHERE county_id = :cid AND signal_type = 'liens'"),
        {"cid": county_id},
    )
    cl4_db.execute(
        text("DELETE FROM county_sources WHERE county_id = :cid AND signal_type = 'permits'"),
        {"cid": county_id},
    )
    cl4_db.flush()

    coverage = clone_pack.source_coverage(cl4_db, key, template_county_id=template_id)
    assert sorted(coverage[county_id]) == ["liens", "permits"]


def test_inactive_source_counts_as_uncovered(cl4_db, venture):
    key, county_id, template_id = venture
    cl4_db.execute(
        text("""
            UPDATE county_sources SET is_active = false
            WHERE county_id = :cid AND signal_type = 'violations'
        """),
        {"cid": county_id},
    )
    cl4_db.flush()

    coverage = clone_pack.source_coverage(cl4_db, key, template_county_id=template_id)
    assert coverage[county_id] == ["violations"]


def test_template_county_is_exempt_from_the_inheritance_comparison(cl4_db, venture):
    """A county compared against itself is trivially 'inherited' — that must not
    read as a gap."""
    key, _county_id, template_id = venture
    coverage = clone_pack.source_coverage(cl4_db, key, template_county_id=template_id)
    assert coverage[template_id] == []


# ── assembly ─────────────────────────────────────────────────────────────────


def test_complete_pack_has_no_gaps(cl4_db, venture, tmp_path):
    key, _county_id, _template_id = venture
    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        f"*/30 * * * * run.sh src.services.relay --sweep --venture {key}\n",
        encoding="utf-8",
    )

    pack = clone_pack.assemble(cl4_db, key, crontab_path=crontab)
    assert pack.gaps == ()
    assert clone_pack.is_complete(pack)
    assert pack.relay_ready
    assert pack.cron_line_present
    assert pack.ladder_stage == "spin_up"
    assert pack.is_active is False


def test_missing_cron_line_is_a_gap(cl4_db, venture, tmp_path):
    key, _county_id, _template_id = venture
    crontab = tmp_path / "crontab.txt"
    crontab.write_text("# nothing here\n", encoding="utf-8")

    pack = clone_pack.assemble(cl4_db, key, crontab_path=crontab)
    assert not clone_pack.is_complete(pack)
    assert any("cron line" in gap for gap in pack.gaps)


def test_relay_gaps_come_from_the_row_not_the_resolver(cl4_db, venture, tmp_path):
    """CL3's env fallback resolves a NULL campaign to venture #1's. A pack built
    off the resolved config would report this venture as ready to send when it
    could only send into another business's sequence."""
    key, _county_id, _template_id = venture
    cl4_db.execute(
        text("""
            UPDATE ventures
            SET relay_instantly_campaign_id = NULL, relay_slack_channel = NULL
            WHERE venture_key = :key
        """),
        {"key": key},
    )
    cl4_db.flush()

    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        f"*/30 * * * * run.sh src.services.relay --sweep --venture {key}\n",
        encoding="utf-8",
    )
    pack = clone_pack.assemble(cl4_db, key, crontab_path=crontab)

    assert not pack.relay_ready
    assert any("relay_instantly_campaign_id" in gap for gap in pack.relay_gaps)
    assert any("relay_slack_channel" in gap for gap in pack.relay_gaps)
    # The resolver, by contrast, happily fills these in — which is exactly why
    # the gate must not read it.
    assert pack.venture.relay_instantly_campaign_id != "camp_pack_test"


def test_no_approvers_is_a_gap(cl4_db, venture, tmp_path):
    key, _county_id, _template_id = venture
    cl4_db.execute(
        text("UPDATE ventures SET relay_approvers = '[]'::jsonb WHERE venture_key = :key"),
        {"key": key},
    )
    cl4_db.flush()

    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        f"*/30 * * * * run.sh src.services.relay --sweep --venture {key}\n", encoding="utf-8"
    )
    pack = clone_pack.assemble(cl4_db, key, crontab_path=crontab)
    assert any("relay_approvers" in gap for gap in pack.relay_gaps)


def test_assemble_raises_for_an_unknown_venture(cl4_db):
    with pytest.raises(LookupError):
        clone_pack.assemble(cl4_db, "no_such_venture_anywhere")


# ── cron line detection ──────────────────────────────────────────────────────


def test_venture_one_line_carries_no_venture_flag(tmp_path):
    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        "*/30 * * * * run.sh src.services.relay --sweep\n", encoding="utf-8"
    )
    assert clone_pack.cron_line_present(DEFAULT_VENTURE_KEY, crontab_path=crontab)
    assert not clone_pack.cron_line_present("venture_two", crontab_path=crontab)


def test_commented_line_does_not_count(tmp_path):
    """docs/venture-onboarding.md ships a COMMENTED example line for venture
    two. Treating it as real would report every unprovisioned venture as
    scheduled."""
    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        "#   */30 * * * * run.sh src.services.relay --sweep --venture venture_two\n",
        encoding="utf-8",
    )
    assert not clone_pack.cron_line_present("venture_two", crontab_path=crontab)


def test_venture_one_is_not_matched_by_another_ventures_line(tmp_path):
    crontab = tmp_path / "crontab.txt"
    crontab.write_text(
        "*/30 * * * * run.sh src.services.relay --sweep --venture venture_two\n",
        encoding="utf-8",
    )
    assert not clone_pack.cron_line_present(DEFAULT_VENTURE_KEY, crontab_path=crontab)
    assert clone_pack.cron_line_present("venture_two", crontab_path=crontab)


def test_unreadable_crontab_reports_absent_rather_than_raising(tmp_path):
    """A missing crontab must not crash the evaluator mid-sweep."""
    assert not clone_pack.cron_line_present(
        "venture_two", crontab_path=tmp_path / "does_not_exist.txt"
    )


def test_the_repo_crontab_really_contains_venture_ones_line():
    """Guards against the check being vacuously false everywhere."""
    assert clone_pack.cron_line_present(DEFAULT_VENTURE_KEY)
