"""R1 0.9 Seam A: ``ShadowRunnerStep`` driven through its call, with fakes only at the ports (card reader, store,
inbox, clock) and spies on every real send path. Nothing here touches a database."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock

import pytest
import slack_sdk

from packages.agent_core.pending_actions import PendingActionQueue
from packages.agent_core.relay import Relay
from packages.agent_core.send_gate import SendGate
from src.lending import ops_alerts
from src.lending.ghl_email import GhlEmailSender
from src.lending.ghl_sms import GhlSmsSender
from src.lending.instantly_email import NdlInstantlySender
from src.lending.shadow.judge import CardFeatures
from src.lending.shadow.shadow_runner import ShadowRunnerStep, edit_distance

START = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)
CLASSES = ("auto-send-judge-baseline", "auto-send-judge-v1")


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FakeStore:
    """In-memory shadow store with the same rules as the SQL one (insert-if-absent, settled rows not reopened)."""

    def __init__(self, clock: Clock, started_at: datetime = START, classes=CLASSES) -> None:
        self.clock = clock
        self.classes = {name: {"mode": "shadow", "started_at": started_at} for name in classes}
        self.rows: dict[tuple[str, str], dict] = {}

    def get_mode(self, db, class_name):
        return self.classes.get(class_name, {}).get("mode")

    def get_started_at(self, db, class_name):
        return self.classes.get(class_name, {}).get("started_at")

    def insert_decision(self, db, *, class_name, class_version, subject_kind, subject_ref, inputs, rule_version,
                        confidence, proposed) -> Optional[int]:
        key = (class_name, subject_ref)
        if key in self.rows:
            return None
        self.rows[key] = {"class_name": class_name, "subject_ref": subject_ref, "subject_kind": subject_kind,
                          "inputs": json.loads(json.dumps(inputs)), "rule_version": rule_version,
                          "proposed": proposed, "created_at": self.clock.now, "actual": None,
                          "decision_match": None, "decided_at": None, "send_result": None, "deal_outcome": None,
                          "outcome_at": None, "edit_distance": None}
        return len(self.rows)

    def open_rows(self, db, class_name, since):
        def settled(r):
            return (r["deal_outcome"] is not None and r["actual"] in ("approve", "edit", "reject")
                    and (r["actual"] == "reject" or r["send_result"] in ("sent", "failed")))
        return [dict(r) for (cls, _), r in self.rows.items()
                if cls == class_name and r["created_at"] >= since and not settled(r)]

    def update_match(self, db, class_name, subject_ref, **fields) -> int:
        row = self.rows[(class_name, subject_ref)]
        if all(row[k] == v for k, v in fields.items() if k != "decision_seconds"):
            return 0
        row.update(fields)
        return 1

    def fill_outcome(self, db, class_name, subject_ref, deal_outcome, outcome_at) -> int:
        row = self.rows[(class_name, subject_ref)]
        if row["deal_outcome"] is not None:
            return 0
        row.update(deal_outcome=deal_outcome, outcome_at=outcome_at)
        return 1

    def discard(self, db, class_name, subject_ref) -> int:
        return 1 if self.rows.pop((class_name, subject_ref), None) else 0

    def row(self, class_name: str, action_id: int) -> dict:
        return self.rows[(class_name, f"action:{action_id}")]


class FakeReader:
    def __init__(self) -> None:
        self.by_id: dict[int, dict] = {}
        self.features_by_id: dict[int, CardFeatures] = {}
        self.outcomes: dict[str, tuple[str, datetime]] = {}
        self.since_seen: list[datetime] = []
        self.rule_version_asked: list[datetime] = []

    def add(self, card: dict, features: Optional[CardFeatures] = None) -> dict:
        self.by_id[card["action_id"]] = card
        if features is not None:
            self.features_by_id[card["action_id"]] = features
        return card

    def new_cards(self, db, since):
        self.since_seen.append(since)
        return [dict(c) for c in self.by_id.values() if c["created_at"] >= since]

    def features(self, db, card):
        return self.features_by_id.get(card["action_id"],
                                       CardFeatures(template_version="T-1", past_total=0, past_edit=0, past_reject=0,
                                                    body_length=80))

    def cards(self, db, action_ids):
        return {i: dict(self.by_id[i]) for i in action_ids if i in self.by_id}

    def deal_outcome(self, db, deal_ref):
        return self.outcomes.get(deal_ref)

    def rule_version_at(self, db, at):
        self.rule_version_asked.append(at)
        return 7


class FakeInbox:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._keys: set[str] = set()

    def __call__(self, db, **event) -> Optional[int]:
        self.calls.append(event)
        if event["dedup_key"] in self._keys:
            return None
        self._keys.add(event["dedup_key"])
        return len(self._keys)


def _card(action_id: int, *, status: str = "pending", created_at: datetime = START + timedelta(minutes=1),
          decided_at: Optional[datetime] = None, decided_by: Optional[str] = None, revisions: Optional[list] = None,
          deal_ref: Optional[str] = None, body: str = "Hi Alex, your term sheet is ready.") -> dict:
    return {"action_id": action_id, "tool_name": "send_sms", "channel": "ghl_sms", "status": status,
            "payload": {"template_version": "T-1", "body": body, "to_phone": "+15550001234"},
            "contact_ref": "ghl:abc", "deal_ref": deal_ref, "created_at": created_at, "decided_at": decided_at,
            "decided_by": decided_by, "revisions": revisions or [], "executed_at": None,
            "recipient_phone": "+15550001234", "recipient_email": "alex@example.com"}


@pytest.fixture
def clock():
    return Clock(START + timedelta(minutes=2))


@pytest.fixture
def reader():
    return FakeReader()


@pytest.fixture
def store(clock):
    return FakeStore(clock)


@pytest.fixture
def inbox():
    return FakeInbox()


@pytest.fixture
def step(reader, store, inbox, clock):
    return ShadowRunnerStep(None, reader=reader, store=store, inbox=inbox, clock=clock)


class TestScoring:
    def test_each_new_card_scored_once_per_class(self, step, reader, store):
        reader.add(_card(1))
        assert step().scored == 2
        assert step().scored == 0
        assert {k[0] for k in store.rows} == set(CLASSES)

    def test_excluded_cards_are_skipped(self, step, reader, store):
        reader.add(_card(1, created_at=START - timedelta(minutes=1)))  # before start
        reader.add(_card(2, status="approved", decided_at=START, decided_by="auto:graduated"))
        reader.add(_card(3, status="blocked"))  # lane-blocked at submit: never decided
        reader.add(_card(4, status="blocked", decided_at=START + timedelta(minutes=1), decided_by="josh"))
        step()
        assert {ref for _, ref in store.rows} == {"action:4"}

    def test_class_not_in_shadow_mode_scores_nothing(self, step, reader, store):
        store.classes = {"auto-send-judge-v1": {"mode": "off", "started_at": START}}
        reader.add(_card(1))
        assert step().scored == 0

    def test_later_sweeps_read_since_the_last_run_less_the_overlap(self, step, reader, clock):
        clock.now = START + timedelta(hours=1)
        step()
        clock.now += timedelta(minutes=1)
        step()
        assert reader.since_seen == [START] * 2 + [START + timedelta(hours=1) - timedelta(minutes=5)] * 2  # per class

    def test_overlap_never_reaches_back_before_the_start(self, step, reader, clock):
        step()
        clock.now += timedelta(minutes=1)
        step()
        assert reader.since_seen == [START] * 4

    def test_rule_version_asked_at_card_creation(self, step, reader, store):
        card = reader.add(_card(1, created_at=START + timedelta(minutes=1)))
        step()
        assert reader.rule_version_asked == [card["created_at"]]
        assert store.row("auto-send-judge-v1", 1)["rule_version"] == 7

    def test_inputs_hold_no_personal_details(self, step, reader, store):
        reader.add(_card(1))
        step()
        stored = json.dumps(store.row("auto-send-judge-v1", 1)["inputs"])
        for secret in ("5550001234", "alex@example.com", "Alex", "term sheet"):
            assert secret not in stored

    def test_inputs_frozen_when_the_card_changes_later(self, step, reader, store, clock):
        reader.add(_card(1), CardFeatures(template_version="T-1", past_total=3, past_edit=0, past_reject=0,
                                          body_length=80))
        step()
        reader.features_by_id[1] = CardFeatures(template_version="T-1", past_total=99, past_edit=0, past_reject=0)
        clock.now += timedelta(hours=1)
        step()
        assert store.row("auto-send-judge-v1", 1)["inputs"]["past_total"] == 3


class TestActualDecision:
    @pytest.mark.parametrize("card, actual, send_result", [
        (dict(status="approved"), "approve", None),
        (dict(status="sent"), "approve", "sent"),
        (dict(status="failed"), "approve", "failed"),
        (dict(status="sent", revisions=[{"payload": {"body": "old"}}]), "edit", "sent"),
        (dict(status="rejected", revisions=[{"payload": {"body": "old"}}]), "reject", None),
        (dict(status="expired"), "no_decision", None),
        (dict(status="held"), "no_decision", "held"),
        (dict(status="pending"), "no_decision", None),
    ])
    def test_derivation(self, step, reader, store, card, actual, send_result):
        decided = START + timedelta(minutes=30) if card["status"] not in ("expired", "pending") else None
        reader.add(_card(1, decided_at=decided, decided_by="josh" if decided else None, **card))
        step()
        row = store.row("auto-send-judge-baseline", 1)
        assert (row["actual"], row["send_result"]) == (actual, send_result)
        assert row["decision_match"] is (None if actual == "no_decision" else actual == "approve")

    def test_pending_card_decided_later_is_updated(self, step, reader, store, clock):
        card = reader.add(_card(1))
        step()
        assert store.row("auto-send-judge-v1", 1)["actual"] == "no_decision"
        card.update(status="approved", decided_at=START + timedelta(days=20), decided_by="josh")
        clock.now = START + timedelta(days=20, minutes=1)
        step()
        row = store.row("auto-send-judge-v1", 1)
        assert row["actual"] == "approve"
        assert row["decided_at"] == START + timedelta(days=20)

    def test_card_auto_approved_after_scoring_is_discarded(self, step, reader, store, clock):
        card = reader.add(_card(1))
        step()
        card.update(status="approved", decided_at=START + timedelta(minutes=3), decided_by="auto:graduated")
        clock.now += timedelta(minutes=5)
        assert step().discarded == 2
        assert not store.rows

    def test_unchanged_rows_are_not_rewritten(self, step, reader, clock):
        reader.add(_card(1, status="sent", decided_at=START + timedelta(minutes=1), decided_by="josh"))
        assert step().updated == 2
        clock.now += timedelta(minutes=1)
        assert step().updated == 0


def test_edit_distance_compares_first_draft_with_final_body():
    assert edit_distance([{"payload": {"body": "abcd"}}], "abcd") == 0.0
    assert edit_distance([{"payload": {"body": "abcd"}}], "wxyz") == 1.0
    assert edit_distance([], "abcd") is None


class TestDealOutcome:
    def test_outcome_filled_when_the_deal_settles_after_the_decision(self, step, reader, store, clock):
        reader.add(_card(1, status="sent", decided_at=START + timedelta(minutes=1), decided_by="josh",
                         deal_ref="loan_file:5"))
        step()
        funded_at = START + timedelta(days=30)
        reader.outcomes["loan_file:5"] = ("funded", funded_at)
        clock.now = funded_at + timedelta(minutes=1)
        assert step().outcomes == 2
        assert store.row("auto-send-judge-v1", 1)["deal_outcome"] == "funded"

    def test_no_outcome_checked_after_90_days(self, step, reader, store, clock):
        reader.add(_card(1, status="sent", decided_at=START + timedelta(minutes=1), decided_by="josh",
                         deal_ref="loan_file:5"))
        step()
        reader.outcomes["loan_file:5"] = ("funded", START + timedelta(days=95))
        clock.now = START + timedelta(days=91)
        assert step().outcomes == 0
        assert store.row("auto-send-judge-v1", 1)["deal_outcome"] is None

    def test_card_without_deal_ref_gets_no_outcome(self, step, reader, store, clock):
        reader.add(_card(1, status="sent", decided_at=START + timedelta(minutes=1), decided_by="josh"))
        reader.outcomes["loan_file:5"] = ("funded", START)
        assert step().outcomes == 0


class TestDay14:
    def test_one_inbox_item_and_the_class_stays_in_shadow(self, step, store, inbox, clock):
        alerts = []
        for minute in range(0, 3 * 24 * 60, 30):
            clock.now = START + timedelta(days=13, minutes=minute)
            alerts += step().day14_alerts
        assert sorted(alerts) == sorted(CLASSES)
        assert len(inbox.calls) == 2
        assert all(c["mode"] == "shadow" for c in store.classes.values())

    def test_no_item_before_day_14(self, step, inbox, clock):
        clock.now = START + timedelta(days=13, hours=23)
        step()
        assert inbox.calls == []

    def test_restart_raises_a_new_item(self, step, store, inbox, clock):
        clock.now = START + timedelta(days=14)
        step()
        restarted = START + timedelta(days=20)
        for c in store.classes.values():
            c["started_at"] = restarted
        clock.now = restarted + timedelta(days=14)
        assert sorted(step().day14_alerts) == sorted(CLASSES)


class TestZeroSends:
    def test_14_simulated_days_make_no_send_and_no_pending_action(self, monkeypatch, step, reader, store, clock):
        spies = {}
        for owner, name in ((SendGate, "submit"), (PendingActionQueue, "enqueue"), (PendingActionQueue, "approve"),
                            (Relay, "run"), (GhlSmsSender, "__call__"), (GhlEmailSender, "__call__"),
                            (NdlInstantlySender, "send"), (slack_sdk.WebClient, "chat_postMessage"),
                            (ops_alerts, "post_ops_alert"), (ops_alerts, "post_ops_notice")):
            spies[f"{getattr(owner, '__name__', owner)}.{name}"] = spy = MagicMock()
            monkeypatch.setattr(owner, name, spy)

        action_id = 0
        for day in range(14):
            for hour in (9, 15):
                action_id += 1
                created = START + timedelta(days=day, hours=hour)
                reader.add(_card(action_id, created_at=created))
                clock.now = created + timedelta(minutes=1)
                step()
                reader.by_id[action_id].update(status="approved", decided_at=created + timedelta(hours=2),
                                               decided_by="josh")
            clock.now = START + timedelta(days=day, hours=23)
            step()

        assert len(store.rows) == 28 * len(CLASSES)
        assert all(r["actual"] == "approve" for r in store.rows.values())
        called = {name: spy.call_count for name, spy in spies.items() if spy.called}
        assert called == {}
