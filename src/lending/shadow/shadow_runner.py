"""R1 0.9 shadow runner: the ``ShadowRunnerStep`` minute-sweep step and its CLI.

    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --start auto-send-judge-v1 [--by NAME]
    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --stop auto-send-judge-v1 [--by NAME]
    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --list
"""
from __future__ import annotations

import argparse
import difflib
import getpass
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from config.settings import get_settings
from src.lending.cora_inbox import add_to_inbox
from src.lending.shadow import registry
from src.lending.shadow import store as shadow_store
from src.lending.shadow.cards import (
    APPROVED_STATUSES,
    CardReader,
    SqlCardReader,
    card_features_as_inputs,
    payload_of,
    revisions_of,
)
from src.lending.shadow.registry import NO_DECISION, ShadowClassDef

logger = logging.getLogger(__name__)

SHADOW_WINDOW_DAYS = 14
OUTCOME_HORIZON = timedelta(days=90)
NEW_CARD_OVERLAP = timedelta(minutes=5)  # a card committed late is still caught; insert-if-absent drops repeats
_SEND_RESULTS = ("sent", "failed", "held")


def derive_actual(status: str, revisions: list, decided_at: Optional[datetime]) -> str:
    """approve / edit (approved after a Revise) / reject; expired, held, pending, revising and a card withdrawn before
    anyone decided it (blocked with no decision time) are no_decision."""
    if status == "rejected":
        return "reject"
    if status in APPROVED_STATUSES and decided_at is not None:
        return "edit" if revisions else "approve"
    return NO_DECISION


def derive_send_result(status: str) -> Optional[str]:
    return status if status in _SEND_RESULTS else None


def edit_distance(revisions: list, final_body: str) -> Optional[float]:
    """1 − similarity of the first draft's body and the final body; None without a revision."""
    if not revisions or not isinstance(revisions[0], dict):
        return None
    first_body = (revisions[0].get("payload") or {}).get("body") or ""
    if not first_body and not final_body:
        return None
    return round(1.0 - difflib.SequenceMatcher(None, first_body, final_body).ratio(), 4)


def is_excluded(card: dict, started_at: datetime) -> bool:
    """Not scored: cards from before the start, auto-approved cards, and lane-blocked cards (blocked at submit, so
    never decided; a send-time block after an approval keeps its decided_at and is scored)."""
    return (card["created_at"] < started_at or card.get("decided_by") == AUTO_GRADUATED_ACTOR
            or (card["status"] == "blocked" and card.get("decided_at") is None))


def _action_id(subject_ref: str) -> int:
    return int(subject_ref.split(":", 1)[1])


@dataclass
class ShadowRunResult:
    scored: int = 0
    updated: int = 0
    outcomes: int = 0
    discarded: int = 0
    day14_alerts: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.scored or self.updated or self.outcomes or self.discarded or self.day14_alerts)


class ShadowRunnerStep:
    """Cora's minute-sweep step. It is never given a send gate, queue or sender, so it can only log."""

    def __init__(self, engine: Optional[Engine], *, reader: Optional[CardReader] = None,
                 store: ModuleType | Any = shadow_store, inbox: Callable[..., Optional[int]] = add_to_inbox,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self._engine = engine
        self._reader = reader or SqlCardReader()
        self._store = store
        self._inbox = inbox
        self._clock = clock
        self._last_run: dict[tuple[str, datetime], datetime] = {}
        self._alerted: set[tuple[str, datetime]] = set()

    def __call__(self) -> ShadowRunResult:
        now = self._clock()
        result = ShadowRunResult()
        scanned: list[tuple[str, datetime]] = []
        alerted: list[tuple[str, datetime]] = []
        with Session(self._engine) as db, db.begin():
            features_cache: dict[int, Any] = {}
            for cls in registry.all_classes():
                if self._store.get_mode(db, cls.name) != "shadow":
                    continue
                started_at = self._store.get_started_at(db, cls.name)
                if started_at is None:
                    continue
                self._score_new(db, cls, started_at, features_cache, result)
                self._fill(db, cls, now, result)
                if self._day14(db, cls, started_at, now, result):
                    alerted.append((cls.name, started_at))
                scanned.append((cls.name, started_at))
        for key in scanned:  # only after the commit, so a failed sweep rescans the same cards and retries the alert
            self._last_run[key] = now
        self._alerted.update(alerted)
        return result

    def _score_new(self, db: Session, cls: ShadowClassDef, started_at: datetime, features_cache: dict,
                   result: ShadowRunResult) -> None:
        last_run = self._last_run.get((cls.name, started_at))
        since = max(started_at, last_run - NEW_CARD_OVERLAP) if last_run else started_at
        for card in self._reader.new_cards(db, since):
            if is_excluded(card, started_at):
                continue
            if card["action_id"] not in features_cache:
                features_cache[card["action_id"]] = (self._reader.features(db, card),
                                                     self._reader.rule_version_at(db, card["created_at"]))
            features, rule_version = features_cache[card["action_id"]]
            prediction = cls.predict(features)
            new_id = self._store.insert_decision(
                db, class_name=cls.name, class_version=cls.version, subject_kind=cls.subject_kind,
                subject_ref=f"action:{card['action_id']}", inputs=card_features_as_inputs(features, card),
                rule_version=rule_version,
                confidence=prediction.confidence, proposed={"decision": prediction.decision})
            if new_id is not None:
                result.scored += 1

    def _fill(self, db: Session, cls: ShadowClassDef, now: datetime, result: ShadowRunResult) -> None:
        rows = self._store.open_rows(db, cls.name, now - OUTCOME_HORIZON)
        cards = self._reader.cards(db, [_action_id(r["subject_ref"]) for r in rows])
        for row in rows:
            ref = row["subject_ref"]
            card = cards.get(_action_id(ref))
            if card is None:
                continue
            if card.get("decided_by") == AUTO_GRADUATED_ACTOR:
                result.discarded += self._store.discard(db, cls.name, ref)
                continue
            revisions = revisions_of(card)
            actual = derive_actual(card["status"], revisions, card.get("decided_at"))
            decided_at = card["decided_at"] if actual != NO_DECISION else None
            result.updated += self._store.update_match(
                db, cls.name, ref, actual=actual, decision_match=cls.compare(row["proposed"], actual),
                decided_at=decided_at,
                decision_seconds=round((decided_at - card["created_at"]).total_seconds(), 2) if decided_at else None,
                edit_distance=edit_distance(revisions, payload_of(card).get("body") or ""),
                send_result=derive_send_result(card["status"]))
            if row["deal_outcome"] != "funded" and card.get("deal_ref"):
                outcome, outcome_at = self._reader.deal_outcome(db, card["deal_ref"]) or (None, None)
                result.outcomes += self._store.set_outcome(db, cls.name, ref, outcome, outcome_at)

    def _day14(self, db: Session, cls: ShadowClassDef, started_at: datetime, now: datetime,
               result: ShadowRunResult) -> bool:
        """True once the day-14 item is queued or found already queued; False on an error, so the next sweep retries."""
        if (cls.name, started_at) in self._alerted or now - started_at < timedelta(days=SHADOW_WINDOW_DAYS):
            return False
        try:
            with db.begin_nested():
                new_id = self._inbox(
                    db, event_type="shadow_window_complete",
                    dedup_key=f"shadow_day14:{cls.name}:{started_at.isoformat()}",
                    summary=f"Shadow class {cls.name} has run 14 days. It stays in shadow until someone runs --stop.",
                    payload={"class_name": cls.name, "started_at": started_at.isoformat()})
        except Exception as exc:  # class only: SQL errors embed bound parameters; retried next sweep
            logger.error("[shadow] day-14 alert for %s not queued (%s)", cls.name, type(exc).__name__)
            return False
        if new_id is not None:
            result.day14_alerts.append(cls.name)
        return True


def _cli_main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="R1 0.9 shadow classes")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", metavar="CLASS", help="put a class in shadow mode (restarts its 14-day clock)")
    group.add_argument("--stop", metavar="CLASS", help="turn a class off")
    group.add_argument("--list", action="store_true", help="list every class and its mode")
    parser.add_argument("--by", default=getpass.getuser(), help="who is changing the mode (default: OS user)")
    args = parser.parse_args(argv)

    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with Session(engine) as db, db.begin():
        if args.list:
            modes = {r["class_name"]: r for r in shadow_store.get_all_shadow_classes(db)}
            for cls in registry.all_classes():
                row = modes.get(cls.name, {})
                print(f"{cls.name:30s} mode={row.get('mode', 'off'):7s} started={row.get('started_at')} "
                      f"by={row.get('changed_by')}")
            return
        name = args.start or args.stop
        if registry.get(name) is None:
            raise SystemExit(f"unknown class {name!r}; known: {', '.join(c.name for c in registry.all_classes())}")
        shadow_store.set_mode(db, name, "shadow" if args.start else "off", changed_by=args.by)
        print(f"{name}: {'shadow' if args.start else 'off'} (by {args.by})")


if __name__ == "__main__":
    _cli_main()
