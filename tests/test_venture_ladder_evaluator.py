"""
Tests for the venture ladder evaluator task (CLONE-v2.2 / CL4).

This is the cron driver that makes the ladder autonomous, so what matters is
that it advances what it should, refuses what it must not touch unattended, and
survives one venture's bad data without abandoning the rest of the fleet.

Includes a regression test for the Slack token: settings.slack_bot_token is a
pydantic SecretStr, and passing it to WebClient unwrapped sends the literal
"**********" so every post 401s in silence.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from config.venture_ladder import AUTO_DOUBLE_MIN_SAMPLE, EVIDENCE_MARKET_SCORE
from src.services import venture_ladder
from src.tasks import venture_ladder_evaluator as evaluator


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
def radar_venture(cl4_db):
    """An inactive radar candidate with one county and a green market score."""
    key = f"test_eval_{uuid.uuid4().hex[:10]}"
    cl4_db.execute(
        text("""
            INSERT INTO ventures (
                venture_key, display_name, brand_name, ladder_stage, is_active
            ) VALUES (:key, 'Eval Test', 'Eval Test', 'radar', false)
        """),
        {"key": key},
    )
    cl4_db.execute(
        text("""
            INSERT INTO counties (
                county_id, display_name, venture_key, zip_prefixes, is_active
            ) VALUES (:cid, 'Eval County', :key, CAST(:zips AS jsonb), true)
        """),
        {"cid": f"{key}_county", "key": key, "zips": json.dumps(["00961"])},
    )
    venture_ladder.record_evidence(
        cl4_db, key,
        evidence_type=EVIDENCE_MARKET_SCORE, stage="radar",
        payload={"score": 85.0}, source_ref="eval-score",
        verified=True, recorded_by="test",
    )
    cl4_db.flush()
    return key


def _stage(db, key: str) -> str:
    return db.execute(
        text("SELECT ladder_stage FROM ventures WHERE venture_key = :k"), {"k": key}
    ).scalar_one()


def _activate_at_cell(db, key: str) -> None:
    db.execute(
        text("UPDATE ventures SET is_active = true, ladder_stage = 'cell' WHERE venture_key = :k"),
        {"k": key},
    )
    db.flush()


def _seed_traffic(db, venture_key: str, *, sends: int, replies: int, cell_id: str = "founder_tier_blitz") -> None:
    """Dispatched queue rows plus matching drafts — mirrors
    tests/test_venture_ladder.py's helper of the same name. Both are needed:
    cell_reply_rates() only counts a draft whose thread has a dispatched
    relay_approval_queue row."""
    now = datetime.now(timezone.utc)
    queue_rows = []
    draft_rows = []
    for n in range(sends):
        thread_id = f"OPP-EVAL-{venture_key}-{cell_id}-{n:05d}"
        dispatched_at = now - timedelta(days=1, minutes=n)
        queue_rows.append({
            "idempotency_key": f"eval-{venture_key}-{cell_id}-{n}",
            "venture_key": venture_key,
            "batch_id": f"eval-batch-{n % 3}",
            "thread_id": thread_id,
            "channel": "email",
            "recipient": f"t{n}@test.invalid",
            "payload": json.dumps({"subject": "t"}),
            "dispatched_at": dispatched_at,
        })
        draft_rows.append({
            "draft_id": str(uuid.uuid4()),
            "opportunity_thread_id": thread_id,
            "venture_key": venture_key,
            "cell_id": cell_id,
            "created_at": dispatched_at,
            "replied_at": dispatched_at + timedelta(hours=1) if n < replies else None,
        })

    db.execute(
        text("""
            INSERT INTO relay_approval_queue (
                idempotency_key, venture_key, batch_id, thread_id, channel,
                recipient, payload, status, dispatched_at
            ) VALUES (
                :idempotency_key, :venture_key, :batch_id, :thread_id, :channel,
                :recipient, CAST(:payload AS jsonb), 'sent', :dispatched_at
            )
        """),
        queue_rows,
    )
    db.execute(
        text("""
            INSERT INTO outbound_drafts (
                draft_id, opportunity_thread_id, buyer_entity_id, venture_key,
                cell_id, offer, avenue, angle, subject, body, facts_used,
                source_refs, recommended_channel, confidence_score, status,
                schema_version, published, is_followup, created_at, replied_at
            ) VALUES (
                :draft_id, :opportunity_thread_id, 1, :venture_key,
                :cell_id, 'founder_tier', 'flippers', 'scarcity_seat_number',
                's', 'b', '[]'::jsonb, '[]'::jsonb, 'email', 80,
                'approved_pending_send', 1, false, false, :created_at, :replied_at
            )
        """),
        draft_rows,
    )
    db.flush()


# ── advancement ──────────────────────────────────────────────────────────────


def test_advances_a_venture_whose_gates_are_green(cl4_db, radar_venture):
    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=False,
    )
    assert result["action"] == "advanced"
    assert result["to_stage"] == "probe"
    assert _stage(cl4_db, radar_venture) == "probe"


def test_dry_run_changes_nothing(cl4_db, radar_venture):
    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=True, advance_spin_up=False, auto_double=False,
    )
    assert result["action"] == "would_advance"
    assert _stage(cl4_db, radar_venture) == "radar"
    assert cl4_db.execute(
        text("SELECT COUNT(*) FROM venture_ladder_events WHERE venture_key = :k"),
        {"k": radar_venture},
    ).scalar_one() == 0


def test_blocked_venture_reports_reasons_and_is_audited(cl4_db, radar_venture):
    cl4_db.execute(
        text("DELETE FROM venture_ladder_evidence WHERE venture_key = :k"),
        {"k": radar_venture},
    )
    cl4_db.flush()

    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=False,
    )
    assert result["action"] == "blocked"
    assert result["blocked_reasons"]
    assert _stage(cl4_db, radar_venture) == "radar"
    assert cl4_db.execute(
        text("""
            SELECT decision FROM venture_ladder_events WHERE venture_key = :k
        """),
        {"k": radar_venture},
    ).scalar_one() == "blocked"


def test_terminal_venture_is_reported_not_advanced(cl4_db, radar_venture):
    cl4_db.execute(
        text("UPDATE ventures SET ladder_stage = 'portfolio' WHERE venture_key = :k"),
        {"k": radar_venture},
    )
    cl4_db.flush()

    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=False,
    )
    assert result["action"] == "terminal"
    assert result["gate_lines"] == []


# ── the transition a human has to release ────────────────────────────────────


def test_cell_to_spin_up_is_not_advanced_unattended(cl4_db, radar_venture, monkeypatch):
    """Spin-up buys a domain, mailbox warmup, an Instantly seat and proxies. The
    presell gate authorises that spend; a person releases it."""
    cl4_db.execute(
        text("UPDATE ventures SET ladder_stage = 'cell' WHERE venture_key = :k"),
        {"k": radar_venture},
    )
    cl4_db.flush()

    # All gates green, so the ONLY thing holding it back is the release rule.
    monkeypatch.setattr(
        venture_ladder, "evaluate",
        lambda db, key: venture_ladder.LadderEvaluation(
            venture_key=key, current_stage="cell", next_stage="spin_up",
            gates=(), blocked_reasons=(),
        ),
    )

    held = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=False,
    )
    assert held["action"] == "awaiting_release"
    assert _stage(cl4_db, radar_venture) == "cell"


def test_cell_to_spin_up_advances_when_explicitly_released(
    cl4_db, radar_venture, monkeypatch
):
    cl4_db.execute(
        text("UPDATE ventures SET ladder_stage = 'cell' WHERE venture_key = :k"),
        {"k": radar_venture},
    )
    cl4_db.flush()

    monkeypatch.setattr(
        venture_ladder, "evaluate",
        lambda db, key: venture_ladder.LadderEvaluation(
            venture_key=key, current_stage="cell", next_stage="spin_up",
            gates=(), blocked_reasons=(),
        ),
    )

    released = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=True, auto_double=False,
    )
    assert released["action"] == "advanced"
    assert _stage(cl4_db, radar_venture) == "spin_up"


def test_only_cell_to_spin_up_is_release_gated():
    assert evaluator.HUMAN_RELEASED_TRANSITIONS == frozenset({("cell", "spin_up")})


# ── auto-double wiring ───────────────────────────────────────────────────────


def test_auto_double_is_skipped_below_the_cell_stage(cl4_db, radar_venture):
    """The evaluator calls maybe_auto_double() for every venture on every
    pass — a radar-stage venture with a qualifying reply rate (seeded here
    directly, bypassing the ladder) must not have its ceiling touched."""
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(cl4_db, radar_venture, sends=sends, replies=int(sends * 0.15))

    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=True,
    )
    assert result["auto_double"] is None
    assert cl4_db.execute(
        text("SELECT COUNT(*) FROM venture_ladder_events WHERE venture_key = :k AND decision = 'auto_double'"),
        {"k": radar_venture},
    ).scalar_one() == 0


def test_evaluator_fires_venture_and_cell_auto_double_at_cell_stage(cl4_db, radar_venture):
    """Issue: maybe_auto_double_cell() was implemented but nothing in
    production ever called it, so per-cell scaling was a no-op. An active
    venture sitting at `cell` with a qualifying cell must get both the
    venture ceiling doubled AND that cell's production multiplier raised."""
    _activate_at_cell(cl4_db, radar_venture)
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(cl4_db, radar_venture, sends=sends, replies=int(sends * 0.15))

    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=True,
    )

    assert result["auto_double"] is not None
    assert "ceiling" in result["auto_double"]
    assert "cell founder_tier_blitz" in result["auto_double"]

    from config.venture_ladder import AUTO_DOUBLE_MULTIPLIER

    # 20 is ventures.relay_daily_ceiling's server_default — radar_venture never
    # sets it explicitly, so a doubling from there confirms the venture-level
    # rule actually fired (not just reported a note).
    stored_ceiling = cl4_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": radar_venture},
    ).scalar_one()
    assert stored_ceiling == 20 * AUTO_DOUBLE_MULTIPLIER

    multipliers = venture_ladder.cell_production_multipliers(cl4_db, radar_venture)
    assert multipliers.get("founder_tier_blitz") == AUTO_DOUBLE_MULTIPLIER


def test_evaluator_does_not_double_a_cell_with_no_traffic(cl4_db, radar_venture):
    """cell_reply_rates() only returns cells with sends in the window, so a
    cell with nothing to measure must never be passed to
    maybe_auto_double_cell() at all — not merely declined by it."""
    _activate_at_cell(cl4_db, radar_venture)

    result = evaluator.evaluate_venture(
        cl4_db, radar_venture,
        dry_run=False, advance_spin_up=False, auto_double=True,
    )
    assert result["auto_double"] is None
    assert cl4_db.execute(
        text(
            "SELECT COUNT(*) FROM venture_ladder_events "
            "WHERE venture_key = :k AND decision = 'auto_double'"
        ),
        {"k": radar_venture},
    ).scalar_one() == 0


# ── fleet resilience ─────────────────────────────────────────────────────────


def test_one_bad_venture_does_not_stop_the_fleet(cl4_db, radar_venture, monkeypatch):
    calls: list[str] = []
    real_evaluate = evaluator.evaluate_venture

    def _flaky(db, venture_key, **kwargs):
        calls.append(venture_key)
        if venture_key == "poison":
            raise RuntimeError("bad data")
        return real_evaluate(db, venture_key, **kwargs)

    monkeypatch.setattr(evaluator, "evaluate_venture", _flaky)
    monkeypatch.setattr(
        evaluator, "_all_venture_keys", lambda db: ["poison", radar_venture]
    )
    monkeypatch.setattr(evaluator, "_post_to_slack", lambda blocks: None)

    from contextlib import contextmanager

    @contextmanager
    def _session():
        yield cl4_db

    monkeypatch.setattr(evaluator, "get_db_context", _session)
    monkeypatch.setattr(cl4_db, "commit", cl4_db.flush)

    assert evaluator.main(["--no-auto-double"]) == 0
    # The poison venture was attempted and the healthy one still evaluated.
    assert calls == ["poison", radar_venture]
    assert _stage(cl4_db, radar_venture) == "probe"


def test_evaluator_includes_inactive_radar_candidates(cl4_db, radar_venture):
    """A radar candidate is an inactive row. Excluding it would mean the
    evaluator never looks at the ventures it exists to assess."""
    keys = evaluator._all_venture_keys(cl4_db)
    assert radar_venture in keys


# ── Slack ────────────────────────────────────────────────────────────────────


def test_slack_token_is_unwrapped_before_use(monkeypatch):
    """Regression: a SecretStr passed straight to WebClient sends
    "**********" and every post 401s silently."""
    from pydantic import SecretStr

    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, token):
            captured["token"] = token

        def chat_postMessage(self, **kwargs):
            captured["payload"] = kwargs

    monkeypatch.setattr(evaluator, "WebClient", _FakeClient)
    monkeypatch.setattr(
        evaluator, "get_settings",
        lambda: SimpleNamespace(
            slack_bot_token=SecretStr("xoxb-real-token"),
            relay_slack_channel="#ladder",
        ),
    )

    evaluator._post_to_slack([{"type": "section"}])

    assert captured["token"] == "xoxb-real-token"
    assert captured["payload"]["channel"] == "#ladder"


def test_slack_is_skipped_when_unconfigured(monkeypatch):
    monkeypatch.setattr(
        evaluator, "get_settings",
        lambda: SimpleNamespace(slack_bot_token=None, relay_slack_channel=None),
    )

    def _explode(token):
        raise AssertionError("WebClient must not be constructed without config")

    monkeypatch.setattr(evaluator, "WebClient", _explode)
    evaluator._post_to_slack([{"type": "section"}])  # must not raise


def test_slack_failure_does_not_break_the_run(monkeypatch):
    from pydantic import SecretStr

    class _Boom:
        def __init__(self, token):
            pass

        def chat_postMessage(self, **kwargs):
            raise RuntimeError("slack down")

    monkeypatch.setattr(evaluator, "WebClient", _Boom)
    monkeypatch.setattr(
        evaluator, "get_settings",
        lambda: SimpleNamespace(
            slack_bot_token=SecretStr("x"), relay_slack_channel="#c"
        ),
    )
    evaluator._post_to_slack([{"type": "section"}])  # swallowed, not raised


def test_digest_marks_imputed_gates_and_shows_na():
    """"Passing" and "not measured, treated as passing" must stay visually
    distinct in the digest — the distinction ADR 0006 lost."""
    evaluation = venture_ladder.LadderEvaluation(
        venture_key="v", current_stage="cell", next_stage="spin_up",
        gates=(
            venture_ladder.GateResult(
                name="measured_gate", value=12.0, threshold=10.0, color="green",
                description="a real measurement", imputed=False,
            ),
            venture_ladder.GateResult(
                name="unmeasured_gate", value=None, threshold=2.0, color="green",
                description="no metric available", imputed=True,
            ),
        ),
        blocked_reasons=(),
    )
    lines = evaluator._format_gate_lines(evaluation)

    measured = next(line for line in lines if "measured_gate" in line and "unmeasured" not in line)
    unmeasured = next(line for line in lines if "unmeasured_gate" in line)

    assert "12" in measured and "imputed" not in measured
    assert "N/A" in unmeasured and "imputed" in unmeasured
    # Both are green, so colour alone would not have told them apart.
    assert ":white_check_mark:" in measured and ":white_check_mark:" in unmeasured


def test_radar_gates_are_really_measured_not_imputed(cl4_db, radar_venture):
    """county_overlap and geography_resolvable are computed from live counts, so
    a fresh radar venture should have no imputed gates at all — its greens are
    earned, not defaulted."""
    evaluation = venture_ladder.evaluate(cl4_db, radar_venture)
    assert [g.name for g in evaluation.gates if g.imputed] == []
