"""
Tests for the second-venture spin-up acceptance harness (CLONE-v2.2 / CL4).

The harness is the thing that answers "could a second venture be stood up right
now?" with an exit code, so these tests assert that the exit code actually
means something: 0 when the whole path works, 1 when any single check fails.

A harness that can only ever return PASS is worth nothing, so the negative test
breaks exactly one input and asserts the run fails.

Needs real Postgres and the CL4 migration — the harness drives the live
provisioning and ladder code paths, then rolls everything back.
"""
from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

_HARNESS_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "harness" / "venture_spinup_acceptance.py"
)


def _load_harness():
    """Load the harness by path.

    `scripts/` is not a package (no __init__.py), so importorskip on a dotted
    name would depend on namespace-package resolution and would SKIP rather than
    fail if it did not resolve — silently hiding every test in this file.
    """
    spec = importlib.util.spec_from_file_location(
        "venture_spinup_acceptance_harness", _HARNESS_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


harness = _load_harness()


@pytest.fixture
def cl4_ready(fresh_db):
    """Skip unless the CL4 migration has been applied to the shared DB."""
    tables = fresh_db.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name IN ('venture_ladder_events', 'venture_ladder_evidence')
    """)).scalar_one()
    if tables < 2:
        pytest.skip(
            "CL4 tables absent — run "
            "`PYTHONPATH=. python migrations/apply_cl4_venture_ladder.py` first"
        )
    columns = fresh_db.execute(text("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name = 'outbound_drafts'
          AND column_name IN ('venture_key', 'replied_at')
    """)).scalar_one()
    if columns < 2:
        pytest.skip("outbound_drafts CL4 columns absent — run the CL4 migration first")
    return True


@pytest.fixture
def stub_key():
    """A unique key per test run so two runs cannot collide mid-transaction."""
    return f"harness_test_{uuid.uuid4().hex[:8]}"


def test_harness_passes_and_exits_zero(cl4_ready, stub_key, capsys):
    exit_code = harness.main(["--venture-key", stub_key])
    output = capsys.readouterr().out

    assert exit_code == 0, output
    assert "RESULT: PASS" in output
    assert "FAIL" not in output.replace("0 failed", "")


def test_harness_reports_every_required_scenario(cl4_ready, stub_key, capsys):
    """The brief requires four scenarios be covered. They must be visible in the
    harness output, not just exercised somewhere internally."""
    harness.main(["--venture-key", stub_key])
    output = capsys.readouterr().out

    assert "presell gate blocks with no commitments" in output      # blocked advancement
    assert "presell gate satisfied by verified deposits" in output  # presell validation
    assert "auto-double fires on a high reply rate" in output       # auto-double triggering
    assert "reached portfolio" in output                            # successful advancement
    assert "Clone-Pack is complete" in output


def test_harness_rolls_everything_back(cl4_ready, stub_key, capsys):
    """Nothing the harness writes may survive it, on either path."""
    harness.main(["--venture-key", stub_key])
    capsys.readouterr()

    from src.core.database import get_db_context

    with get_db_context() as db:
        leftover_venture = db.execute(
            text("SELECT COUNT(*) FROM ventures WHERE venture_key = :k"),
            {"k": stub_key},
        ).scalar_one()
        leftover_events = db.execute(
            text("SELECT COUNT(*) FROM venture_ladder_events WHERE venture_key = :k"),
            {"k": stub_key},
        ).scalar_one()
        leftover_queue = db.execute(
            text("SELECT COUNT(*) FROM relay_approval_queue WHERE venture_key = :k"),
            {"k": stub_key},
        ).scalar_one()

    assert (leftover_venture, leftover_events, leftover_queue) == (0, 0, 0)


def test_harness_leaves_no_stub_in_the_config_cache(cl4_ready, stub_key, capsys):
    """The stub's config is cached during Clone-Pack assembly; the rollback
    removes the rows behind it, so the cache must not outlive the transaction."""
    harness.main(["--venture-key", stub_key])
    capsys.readouterr()

    from src.utils import venture_config

    resolved = venture_config.get_venture_config(stub_key)
    # No row -> the resolver falls back to env settings rather than serving the
    # rolled-back stub's Instantly campaign.
    assert resolved.relay_instantly_campaign_id != "camp_acceptance_stub"
    assert resolved.relay_slack_channel != "#relay-acceptance-stub"


def test_harness_fails_when_the_presell_gate_cannot_be_satisfied(
    cl4_ready, stub_key, capsys, monkeypatch
):
    """Break exactly one input — the presell seeding — and the run must fail."""
    monkeypatch.setattr(harness, "_seed_presell", lambda *a, **k: None)

    exit_code = harness.main(["--venture-key", stub_key])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "RESULT: FAIL" in output
    assert "presell gate satisfied by verified deposits" in output


def test_harness_fails_when_provisioning_leaves_inherited_urls(
    cl4_ready, stub_key, capsys, monkeypatch
):
    """A county with no source_url_overrides inherits the template county's
    portals — the CL3 failure mode. The harness must catch it."""
    original = harness._stub_county

    def _no_overrides(template_county_id):
        county = original(template_county_id)
        county["source_url_overrides"] = {}
        return county

    monkeypatch.setattr(harness, "_stub_county", _no_overrides)

    exit_code = harness.main(["--venture-key", stub_key])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "provisioning left no inherited URLs" in output


def test_harness_verbose_prints_gate_values(cl4_ready, stub_key, capsys):
    harness.main(["--venture-key", stub_key, "--verbose"])
    output = capsys.readouterr().out

    assert "market_score" in output
    assert "[green]" in output
