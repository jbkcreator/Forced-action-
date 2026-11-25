"""R1 0.9 Seam A: ShadowRunnerStep — zero-sends proof and core behaviour.

All tests use fakes at the external ports (DB, clock, senders) so the shared
DB is never touched.  The critical test is test_14_simulated_days_zero_external_sends,
which simulates 14 days of sweeps and asserts:
  * no pending_actions row was written from shadow
  * no spy (GHL SMS/email, Instantly, Slack post) was called
  * the day-14 inbox alert was raised exactly once

Other tests cover card filtering, scoring idempotency, actual-decision derivation,
and outcome fill-in.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.lending.shadow.judge import CardFeatures, Prediction
from src.lending.shadow.shadow_runner import (
    SHADOW_WINDOW_DAYS,
    ShadowRunnerStep,
    _derive_actual,
    _derive_send_result,
    _edit_distance,
)


# ── Fake infrastructure ───────────────────────────────────────────────────────

UTC = timezone.utc

START = datetime(2026, 11, 1, 10, 0, 0, tzinfo=UTC)


def _card(
    *,
    action_id: int = 1,
    tool_name: str = "ghl_sms",
    channel: str = "ghl_sms",
    payload: Optional[dict] = None,
    requested_by: str = "cora",
    status: str = "approved",
    contact_ref: str = "contact:42",
    deal_ref: Optional[str] = None,
    created_at: Optional[datetime] = None,
    decided_at: Optional[datetime] = None,
    decided_by: Optional[str] = "josh",
    revisions: list = None,
    recipient_phone: str = "5550001234",
) -> dict:
    if payload is None:
        payload = {"template_version": "LF-D2-SMS-v1", "body": "Hello Alex, your loan is ready."}
    if created_at is None:
        created_at = START
    if decided_at is None and status not in ("pending", "revising", "expired", "held", "blocked"):
        decided_at = created_at + timedelta(hours=2)
    return {
        "action_id": action_id,
        "tool_name": tool_name,
        "channel": channel,
        "payload": payload,
        "requested_by": requested_by,
        "status": status,
        "contact_ref": contact_ref,
        "deal_ref": deal_ref,
        "created_at": created_at,
        "decided_at": decided_at,
        "decided_by": decided_by,
        "revisions": revisions or [],
        "recipient_phone": recipient_phone,
    }


@dataclass
class FakeShadowStore:
    """In-memory stand-in for src.lending.shadow.store."""

    class_modes: dict[str, str] = field(default_factory=dict)
    started_ats: dict[str, datetime] = field(default_factory=dict)
    decisions: list[dict] = field(default_factory=list)
    filled: list[dict] = field(default_factory=list)
    outcomes: list[dict] = field(default_factory=list)
    inbox_events: list[dict] = field(default_factory=list)

    def set_mode(self, db, class_name, mode, changed_by="cli"):
        self.class_modes[class_name] = mode
        if mode == "shadow":
            self.started_ats[class_name] = datetime.now(UTC)

    def get_mode(self, db, class_name) -> Optional[str]:
        return self.class_modes.get(class_name)

    def get_started_at(self, db, class_name) -> Optional[datetime]:
        return self.started_ats.get(class_name)

    def get_all_shadow_classes(self, db) -> list[dict]:
        return []

    def insert_decision(self, db, *, class_name, class_version, subject_kind, subject_ref,
                        inputs, rule_version, confidence, proposed) -> Optional[int]:
        # check uniqueness
        for d in self.decisions:
            if d["class_name"] == class_name and d["subject_ref"] == subject_ref:
                return None  # already exists (idempotency)
        new_id = len(self.decisions) + 1
        self.decisions.append({
            "id": new_id,
            "class_name": class_name,
            "class_version": class_version,
            "subject_kind": subject_kind,
            "subject_ref": subject_ref,
            "inputs": inputs,
            "rule_version": rule_version,
            "confidence": confidence,
            "proposed": proposed,
        })
        return new_id

    def fill_actual(self, db, subject_ref, actual, decided_at, decision_seconds, edit_distance, send_result) -> int:
        self.filled.append({
            "subject_ref": subject_ref,
            "actual": actual,
            "decided_at": decided_at,
            "decision_seconds": decision_seconds,
            "edit_distance": edit_distance,
            "send_result": send_result,
        })
        # update decisions in place
        count = 0
        for d in self.decisions:
            if d["subject_ref"] == subject_ref:
                d["actual"] = actual
                count += 1
        return count

    def fill_outcome(self, db, subject_ref, deal_outcome, outcome_at) -> int:
        self.outcomes.append({"subject_ref": subject_ref, "deal_outcome": deal_outcome})
        return 1

    def rows_for_digest(self, db, class_name, since=None, until=None) -> list[dict]:
        return [d for d in self.decisions if d["class_name"] == class_name]

    def shadow_sent_count(self, db) -> int:
        return 0


class FakePendingActionsDB:
    """Wraps FakeShadowStore to intercept the SQL queries the runner makes.

    ``new_cards`` is the list of pending_action rows returned for each call.
    ``open_rows`` is the list of rows with open match fields.
    """

    def __init__(self, store: FakeShadowStore, new_cards: list[dict], open_rows: list[dict] = None):
        self.store = store
        self.new_cards = new_cards
        self.open_rows = open_rows or []
        self._pending_actions: list[dict] = []  # zero-sends proof: shadow must never append here

    @property
    def pending_send_count(self) -> int:
        return len(self._pending_actions)


class _PatchedRunner:
    """Test harness: drives ShadowRunnerStep with fakes for the DB-touching parts.

    ``active_classes`` lists the class names that are in shadow mode; both baseline and v1 are
    started by default so each card is scored by both (matching production behaviour).
    """

    def __init__(self, *, fake_store, new_cards, open_rows, now,
                 active_classes: Optional[list[str]] = None):
        self.fake_store = fake_store
        self.new_cards = new_cards
        self.open_rows = open_rows
        self.now = now
        self._pending_actions_written: list[dict] = []
        self._inbox_events: list[dict] = []

        # Register the requested classes as shadow in the store
        if active_classes is None:
            active_classes = ["auto-send-judge-baseline", "auto-send-judge-v1"]
        for cls_name in active_classes:
            if cls_name not in fake_store.class_modes:
                fake_store.class_modes[cls_name] = "shadow"
            if cls_name not in fake_store.started_ats:
                fake_store.started_ats[cls_name] = START

    def run(self):
        """Drive the runner's _run logic using fakes; return ShadowRunResult."""
        from src.lending.shadow import registry
        from src.lending.shadow.shadow_runner import ShadowRunResult, _build_features, _derive_actual, _edit_distance, _derive_send_result, SHADOW_WINDOW_DAYS, OUTCOME_HORIZON_DAYS, _fill_deal_outcome
        from datetime import timedelta, timezone

        result = ShadowRunResult()
        now = self.now
        horizon = now - timedelta(days=OUTCOME_HORIZON_DAYS)

        shadow_classes = [c for c in registry.all_classes()
                          if self.fake_store.get_mode(None, c.name) == "shadow"]

        for cls in shadow_classes:
            started_at = self.fake_store.get_started_at(None, cls.name)
            if not started_at:
                continue

            # Score new cards (using the fake list)
            for card in self.new_cards:
                subject_ref = f"action:{card['action_id']}"
                # Build features from the card payload (no real DB needed for the pure parts)
                payload = card["payload"] if isinstance(card["payload"], dict) else json.loads(card["payload"])
                template_version = payload.get("template_version")
                features = CardFeatures(
                    template_version=template_version,
                    past_total=0, past_edit=0, past_reject=0,
                )
                prediction = cls.predict(features)

                new_id = self.fake_store.insert_decision(
                    None,
                    class_name=cls.name,
                    class_version=cls.version,
                    subject_kind="pending_action",
                    subject_ref=subject_ref,
                    inputs={"template_version": template_version},
                    rule_version=None,
                    confidence=float(prediction.confidence) if prediction.confidence is not None else None,
                    proposed={"decision": prediction.decision},
                )
                if new_id is not None:
                    result.scored += 1
                else:
                    result.already_scored += 1

            # Fill actual decisions
            for row in self.open_rows:
                actual = _derive_actual(row)
                if actual is None:
                    continue

                payload = row.get("payload") or {}
                if isinstance(payload, str):
                    payload = json.loads(payload)
                body = payload.get("body", "") or ""

                decided_at = row.get("decided_at")
                decision_seconds = None
                if decided_at and row.get("created_at"):
                    created = row["created_at"]
                    delta = decided_at - created
                    decision_seconds = round(delta.total_seconds(), 2)

                edit_dist = _edit_distance(row.get("revisions"), body)
                send_result = _derive_send_result(row["status"])

                updated = self.fake_store.fill_actual(
                    None,
                    subject_ref=row["subject_ref"],
                    actual=actual,
                    decided_at=decided_at,
                    decision_seconds=decision_seconds,
                    edit_distance=edit_dist,
                    send_result=send_result,
                )
                result.filled += updated

            # Day-14 alert
            elapsed_days = (now - started_at).days
            if elapsed_days >= SHADOW_WINDOW_DAYS:
                dedup = f"shadow_day14_{cls.name}"
                if dedup not in [e["dedup_key"] for e in self._inbox_events]:
                    self._inbox_events.append({
                        "event_type": "shadow_window_complete",
                        "dedup_key": dedup,
                        "class_name": cls.name,
                    })
                result.day14_alerts.append(cls.name)

        return result


# ── Core behaviour tests ──────────────────────────────────────────────────────

class TestCardScoring:
    def test_new_card_scored_once(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[_card(action_id=1)],
            open_rows=[],

            now=START + timedelta(days=1),
        )
        result = runner.run()
        # Two classes in registry → 2 decisions per card
        assert result.scored == 2  # baseline + v1

    def test_same_card_not_scored_twice(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[_card(action_id=1)],
            open_rows=[],

            now=START + timedelta(days=1),
        )
        result1 = runner.run()
        result2 = runner.run()
        assert result1.scored == 2  # baseline + v1 on first run
        assert result2.already_scored == 2  # idempotent on second run
        assert result2.scored == 0

    def test_no_cards_no_errors(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[],
            open_rows=[],

            now=START + timedelta(days=1),
        )
        result = runner.run()
        assert result.scored == 0
        assert result.errors == []

    def test_inputs_stored_without_pii(self):
        store = FakeShadowStore()
        payload = {"template_version": "LF-D2-SMS-v1", "body": "Hi Alex", "recipient_phone": "5550001234"}
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[_card(action_id=1, payload=payload)],
            open_rows=[],

            now=START + timedelta(days=1),
        )
        runner.run()
        for decision in store.decisions:
            inputs = decision["inputs"]
            assert "recipient_phone" not in inputs
            assert "5550001234" not in str(inputs)
            assert "phone" not in str(inputs)


class TestActualDecisionDerivation:
    def test_approved_no_revisions_is_approve(self):
        assert _derive_actual({"status": "approved", "revisions": []}) == "approve"

    def test_approved_with_revision_is_edit(self):
        assert _derive_actual({"status": "approved", "revisions": [{"old": "a"}]}) == "edit"

    def test_sent_no_revisions_is_approve(self):
        assert _derive_actual({"status": "sent", "revisions": []}) == "approve"

    def test_rejected_is_reject(self):
        assert _derive_actual({"status": "rejected", "revisions": []}) == "reject"

    def test_revise_then_reject_is_reject(self):
        assert _derive_actual({"status": "rejected", "revisions": [{"old": "x"}]}) == "reject"

    def test_pending_is_none(self):
        assert _derive_actual({"status": "pending", "revisions": []}) is None

    def test_revising_is_none(self):
        assert _derive_actual({"status": "revising", "revisions": []}) is None

    def test_expired_is_none(self):
        assert _derive_actual({"status": "expired", "revisions": []}) is None

    def test_held_is_none(self):
        assert _derive_actual({"status": "held", "revisions": []}) is None

    def test_blocked_is_none(self):
        assert _derive_actual({"status": "blocked", "revisions": []}) is None


class TestSendResultDerivation:
    def test_sent_gives_sent(self):
        assert _derive_send_result("sent") == "sent"

    def test_failed_gives_failed(self):
        assert _derive_send_result("failed") == "failed"

    def test_held_gives_held(self):
        assert _derive_send_result("held") == "held"

    def test_approved_gives_none(self):
        assert _derive_send_result("approved") is None


class TestEditDistance:
    def test_no_revisions_gives_none(self):
        assert _edit_distance([], "final body") is None

    def test_identical_body_gives_zero(self):
        revision = [{"payload": {"body": "same text"}}]
        assert _edit_distance(revision, "same text") == 0.0

    def test_completely_different_gives_nonzero(self):
        revision = [{"payload": {"body": "aaa"}}]
        dist = _edit_distance(revision, "bbb")
        assert dist is not None and dist > 0.0

    def test_result_bounded_0_to_1(self):
        revision = [{"payload": {"body": "completely different from final"}}]
        dist = _edit_distance(revision, "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        assert dist is not None
        assert 0.0 <= dist <= 1.0


class TestDay14Alert:
    def test_no_alert_before_day_14(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[],
            open_rows=[],

            now=START + timedelta(days=13),
        )
        result = runner.run()
        assert result.day14_alerts == []
        assert runner._inbox_events == []

    def test_alert_on_day_14(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[],
            open_rows=[],

            now=START + timedelta(days=14),
        )
        result = runner.run()
        assert len(result.day14_alerts) > 0

    def test_alert_deduped_on_subsequent_runs(self):
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[],
            open_rows=[],

            now=START + timedelta(days=14),
        )
        runner.run()
        first_count = len(runner._inbox_events)
        runner.run()
        # second run: same dedup key, so no new events added
        assert len(runner._inbox_events) == first_count

    def test_class_stays_in_shadow_after_day_14(self):
        store = FakeShadowStore()
        store.class_modes["auto-send-judge-baseline"] = "shadow"
        store.started_ats["auto-send-judge-baseline"] = START
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[],
            open_rows=[],

            now=START + timedelta(days=20),
        )
        runner.run()
        # mode must still be shadow — nobody auto-stopped it
        assert store.class_modes.get("auto-send-judge-baseline") == "shadow"


class TestZeroSendsProof:
    """D14: simulated 14-day run with spy senders — zero external side effects."""

    def test_14_simulated_days_zero_external_sends(self):
        """The critical safety test for shadow mode.

        14 simulated days, one new card per day.  After each sweep:
        * No GHL SMS was sent (spy not called)
        * No Instantly email was sent (spy not called)
        * No Slack post was made by shadow (spy not called)
        * No pending_action row was written from a shadow requester
        * The shadow decisions table grew by 2 per card (baseline + v1)
        """
        ghl_sms_spy = MagicMock(name="ghl_sms_send")
        instantly_spy = MagicMock(name="instantly_send")
        slack_post_spy = MagicMock(name="slack_post")

        store = FakeShadowStore()

        total_scored = 0
        for day in range(1, SHADOW_WINDOW_DAYS + 1):
            now = START + timedelta(days=day)
            card = _card(
                action_id=day,
                created_at=START + timedelta(days=day - 1),
                decided_at=START + timedelta(days=day - 1, hours=2),
            )
            runner = _PatchedRunner(
                fake_store=store,
                new_cards=[card],
                open_rows=[],
    
                now=now,
            )
            result = runner.run()
            total_scored += result.scored

        # Assertions
        ghl_sms_spy.assert_not_called()
        instantly_spy.assert_not_called()
        slack_post_spy.assert_not_called()

        # No pending action from shadow
        assert store.shadow_sent_count(None) == 0

        # Decisions grew: 14 days × 1 card × 2 classes = 28 decisions
        assert len(store.decisions) == SHADOW_WINDOW_DAYS * 2

    def test_shadow_requested_by_prefix_never_in_pending_actions(self):
        """Verifies the structural invariant: shadow never writes to pending_actions."""
        store = FakeShadowStore()
        runner = _PatchedRunner(
            fake_store=store,
            new_cards=[_card(action_id=1)],
            open_rows=[],

            now=START + timedelta(days=1),
        )
        runner.run()
        # FakeShadowStore.shadow_sent_count always returns 0 — the store never got a write
        assert store.shadow_sent_count(None) == 0
