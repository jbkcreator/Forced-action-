"""T-B13-02 — async outcome dispatch via the transactional outbox.

The one-tap deal-capture emits a single outcome.recorded event instead of
calling the recalculation consumers inline. The outcome dispatch sweep then
fans it out to the idempotent poll consumers, routing on reason_fault_class:
only lead-fault dead (and closed) outcomes feed scoring; buyer-neutral dead is
score-protected.
"""
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.core.models import LifecycleSuppression, DealOutcome, Property, Subscriber
from src.consumers import outcome_consumers
from src.tasks.outcome_dispatch_sweep import run_sweep


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk(fresh_db):
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_disp_{uid}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"disp-{uid}", email=f"disp_{uid}@example.com",
        name=f"Disp {uid}", status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    prop = Property(
        parcel_id=f"P-DISP-{uid}", address=f"2 Dispatch #{uid}",
        city="Tampa", state="FL", zip="33601", county_id="hillsborough",
    )
    fresh_db.add(prop)
    fresh_db.flush()
    # Delivered lead → a prospect row exists for the property; the prospect-scoped
    # outbox resolves prospect_id from it (exercises the async emit path).
    fresh_db.execute(
        text("INSERT INTO prospects (property_id) VALUES (:pid)"), {"pid": prop.id}
    )
    fresh_db.commit()
    return sub, prop


def _cleanup(fresh_db, sub, prop):
    ev_subq = ("SELECT event_id FROM events WHERE source_component = 'deal_capture' "
               "AND (payload->>'subscriber_id')::int = :s")
    fresh_db.execute(text(f"DELETE FROM processed_events WHERE event_id IN ({ev_subq})"), {"s": sub.id})
    fresh_db.execute(text(f"DELETE FROM event_failures WHERE event_id IN ({ev_subq})"), {"s": sub.id})
    fresh_db.execute(text("DELETE FROM events WHERE source_component = 'deal_capture' AND (payload->>'subscriber_id')::int = :s"), {"s": sub.id})
    fresh_db.execute(DealOutcome.__table__.delete().where(DealOutcome.subscriber_id == sub.id))
    fresh_db.execute(LifecycleSuppression.__table__.delete().where(LifecycleSuppression.subscriber_id == sub.id))
    fresh_db.execute(text("DELETE FROM referral_prompt_funnel WHERE subscriber_id = :sid"), {"sid": sub.id})
    fresh_db.execute(text("DELETE FROM prospects WHERE property_id = :p"), {"p": prop.id})
    fresh_db.delete(sub)
    fresh_db.delete(prop)
    fresh_db.commit()


def _post(client, sub, prop, **fields):
    body = {"feed_uuid": sub.event_feed_uuid, "property_id": prop.id, **fields}
    with patch("src.services.win_graphic.generate", return_value=None):
        return client.post("/api/deal-capture", json=body)


def test_tap_emits_event_not_inline_recalc(client, fresh_db):
    """A terminal tap writes one outcome.recorded event; recalc is not inline."""
    sub, prop = _mk(fresh_db)
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy:
        resp = _post(client, sub, prop, outcome_state="closed", deal_amount=8000)
        assert resp.status_code == 201
        # No inline recalculation on the request thread.
        snap.assert_not_called()
        autopsy.assert_not_called()
    n = fresh_db.execute(
        text("SELECT count(*) FROM events WHERE event_type='outcome.recorded' "
             "AND (payload->>'subscriber_id')::int = :s"), {"s": sub.id},
    ).scalar()
    assert n == 1
    _cleanup(fresh_db, sub, prop)


def test_pending_emits_nothing(client, fresh_db):
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="pending")
    n = fresh_db.execute(
        text("SELECT count(*) FROM events WHERE event_type='outcome.recorded' "
             "AND (payload->>'subscriber_id')::int = :s"), {"s": sub.id},
    ).scalar()
    assert n == 0
    _cleanup(fresh_db, sub, prop)


def test_sweep_dispatches_closed_to_snapshot(client, fresh_db):
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="closed", deal_amount=8000)
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy, \
         patch("src.services.deal_win_social_proof.maybe_send_deal_win_social_proof_prompt") as social:
        run_sweep(fresh_db)
    assert snap.call_count == 1
    assert snap.call_args.kwargs["outcome_status"] == "funded"
    autopsy.assert_not_called()  # closed is not a loss
    social.assert_not_called()
    _cleanup(fresh_db, sub, prop)


def test_sweep_dispatches_big_win_to_social_proof_prompt(client, fresh_db):
    sub, prop = _mk(fresh_db)
    with patch("src.services.lifecycle_suppression.create_suppression"), \
         patch("src.tasks.annual_push._push_annual_offer", return_value=False):
        _post(client, sub, prop, outcome_state="closed", deal_amount=15000)
    with patch("src.services.snapshot_service.capture_snapshot"), \
         patch("src.services.loss_autopsy.run_loss_autopsy"), \
         patch("src.services.deal_win_social_proof.maybe_send_deal_win_social_proof_prompt") as social:
        run_sweep(fresh_db)
    assert social.call_count >= 1
    assert any(
        call.args[0].id == sub.id and call.args[1].subscriber_id == sub.id
        for call in social.call_args_list
    )
    _cleanup(fresh_db, sub, prop)


def test_sweep_lead_fault_dead_feeds_snapshot_and_autopsy(client, fresh_db):
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="dead", dead_reason="wrong_owner")
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy:
        run_sweep(fresh_db)
    assert snap.call_count == 1
    assert snap.call_args.kwargs["outcome_status"] == "lost"
    assert autopsy.call_count == 1
    _cleanup(fresh_db, sub, prop)


def test_sweep_buyer_neutral_dead_is_score_protected(client, fresh_db):
    """buyer-neutral dead must NOT feed the snapshot or the loss autopsy."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="dead", dead_reason="too_busy")
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy:
        run_sweep(fresh_db)
    snap.assert_not_called()
    autopsy.assert_not_called()
    _cleanup(fresh_db, sub, prop)


def test_feeds_scoring_routing():
    """Pure routing unit — no DB."""
    assert outcome_consumers._feeds_scoring({"outcome_state": "closed"}) is True
    assert outcome_consumers._feeds_scoring(
        {"outcome_state": "dead", "reason_fault_class": "lead_fault"}) is True
    assert outcome_consumers._feeds_scoring(
        {"outcome_state": "dead", "reason_fault_class": "buyer_neutral"}) is False
    assert outcome_consumers._feeds_scoring({"outcome_state": "pending"}) is False


# ---------------------------------------------------------------------------
# Review fix regression tests
# ---------------------------------------------------------------------------

def test_legacy_skip_still_feeds_snapshot_and_autopsy(client, fresh_db):
    """Legacy deal_size_bucket='skip' carries no reason taxonomy, but must still
    feed the learning loop — pre-Block-13 behavior, preserved via an implicit
    lead_fault classification so legacy clients aren't silently dropped from
    scoring now that dead outcomes are fault-gated."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, deal_size_bucket="skip")
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy:
        run_sweep(fresh_db)
    assert snap.call_count == 1
    assert snap.call_args.kwargs["outcome_status"] == "lost"
    assert autopsy.call_count == 1
    _cleanup(fresh_db, sub, prop)


def test_legacy_nonskip_bucket_feeds_snapshot_funded(client, fresh_db):
    """Legacy non-skip bucket (closed) still feeds the snapshot as a win."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, deal_size_bucket="5_10k", deal_amount=6000)
    with patch("src.services.snapshot_service.capture_snapshot") as snap, \
         patch("src.services.loss_autopsy.run_loss_autopsy") as autopsy:
        run_sweep(fresh_db)
    assert snap.call_count == 1
    assert snap.call_args.kwargs["outcome_status"] == "funded"
    autopsy.assert_not_called()
    _cleanup(fresh_db, sub, prop)


def test_rapid_closed_then_dead_before_sweep_captures_final_state_only(client, fresh_db):
    """Reporting closed then dead before the sweep runs must not leave a stale
    'funded' snapshot behind — only the final (dead) state should be captured,
    via the consumer's own current-state check (a superseded event is skipped)."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="closed", deal_amount=9000)
    _post(client, sub, prop, outcome_state="dead", dead_reason="wrong_owner")
    n = fresh_db.execute(
        text("SELECT count(*) FROM events WHERE event_type='outcome.recorded' "
             "AND (payload->>'subscriber_id')::int = :s"), {"s": sub.id},
    ).scalar()
    assert n == 2, "both taps emit their own event"

    # capture_snapshot runs for real to prove the staleness logic; run_loss_autopsy
    # is mocked purely to avoid a real LLM call — the final (dead) event is
    # lead_fault and current, so it does reach the autopsy consumer.
    with patch("src.services.loss_autopsy.run_loss_autopsy"):
        run_sweep(fresh_db)

    row = fresh_db.execute(
        text("SELECT id, pipeline_stage FROM deal_outcomes WHERE subscriber_id=:s"),
        {"s": sub.id},
    ).mappings().one()
    assert row["pipeline_stage"] == "closed_lost"

    snapshots = fresh_db.execute(
        text("SELECT outcome_status FROM pre_decision_snapshots WHERE deal_outcome_id=:d"),
        {"d": row["id"]},
    ).fetchall()
    assert [s[0] for s in snapshots] == ["lost"], (
        "the stale 'closed' event must be skipped as superseded, not captured as funded"
    )
    _cleanup(fresh_db, sub, prop)


def test_rapid_dead_then_closed_before_sweep_captures_final_state_only(client, fresh_db):
    """Same ordering hazard in the opposite direction: dead then closed before
    the sweep runs must end with a single 'funded' snapshot, not 'lost'."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="dead", dead_reason="wrong_owner")
    _post(client, sub, prop, outcome_state="closed", deal_amount=9000)

    # The dead event is now stale (superseded by closed) so it never reaches
    # the autopsy consumer, and closed never calls it either — but mock it
    # anyway so this test can't accidentally make a real LLM call if that
    # invariant ever shifts.
    with patch("src.services.loss_autopsy.run_loss_autopsy"):
        run_sweep(fresh_db)

    row = fresh_db.execute(
        text("SELECT id, pipeline_stage FROM deal_outcomes WHERE subscriber_id=:s"),
        {"s": sub.id},
    ).mappings().one()
    assert row["pipeline_stage"] == "closed_won"

    snapshots = fresh_db.execute(
        text("SELECT outcome_status FROM pre_decision_snapshots WHERE deal_outcome_id=:d"),
        {"d": row["id"]},
    ).fetchall()
    assert [s[0] for s in snapshots] == ["funded"]
    _cleanup(fresh_db, sub, prop)


def test_snapshot_failure_is_retried_not_silently_marked_processed(client, fresh_db):
    """A genuine capture_snapshot failure (returns None, no existing row) must
    make poll_and_dispatch record a failure and retry — not mark the event
    processed as if it had succeeded."""
    sub, prop = _mk(fresh_db)
    _post(client, sub, prop, outcome_state="closed", deal_amount=9000)

    with patch("src.services.snapshot_service.capture_snapshot", return_value=None):
        result = run_sweep(fresh_db)
    assert result["outcome_snapshot"]["failed"] == 1
    assert result["outcome_snapshot"]["processed"] == 0

    row = fresh_db.execute(
        text("SELECT id FROM deal_outcomes WHERE subscriber_id=:s"), {"s": sub.id}
    ).scalar_one()
    n_snapshots = fresh_db.execute(
        text("SELECT count(*) FROM pre_decision_snapshots WHERE deal_outcome_id=:d"),
        {"d": row},
    ).scalar()
    assert n_snapshots == 0, "a failed capture must not be mistaken for a written snapshot"

    # A retry (with the real, working capture_snapshot) now succeeds.
    with patch("src.services.loss_autopsy.run_loss_autopsy"):
        result2 = run_sweep(fresh_db)
    assert result2["outcome_snapshot"]["processed"] == 1
    _cleanup(fresh_db, sub, prop)
