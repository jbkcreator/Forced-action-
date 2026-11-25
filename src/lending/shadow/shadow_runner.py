"""R1 0.9 Shadow runner: ``ShadowRunnerStep`` sweep step and CLI.

One call per minute sweep:

1. For each class in shadow mode, read cards created since ``started_at`` (or the last run)
   that have not been scored yet, build frozen features and log the prediction.
2. For each stored row under 90 days old with open match fields, derive the actual decision,
   decision time, edit distance, send result and deal outcome, and fill them in.
3. On day 14 exactly, raise one Cora inbox item (using add_to_inbox_safely; dedup-keyed so only
   one is ever posted).  The class stays in shadow until a human runs ``--stop``.

Cards excluded from scoring (D10):
  * created before the class was started
  * approved by ``auto:graduated`` (auto-approval actor)
  * lane-blocked (status = 'blocked')

CLI:
    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --start auto-send-judge-baseline
    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --stop  auto-send-judge-baseline
    PYTHONPATH=. python -m src.lending.shadow.shadow_runner --list

No SendGate, PendingActionQueue or write session other than the shadow store is ever used here.
"""
from __future__ import annotations

import argparse
import difflib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from src.lending.audit import deal_ledger
from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.contracts import LoanStage
from src.lending.shadow import registry, store
from src.lending.shadow.judge import CardFeatures

logger = logging.getLogger(__name__)

SHADOW_WINDOW_DAYS = 14          # the spec's 14-day window
OUTCOME_HORIZON_DAYS = 90        # keep checking outcomes for this long (D13)
SHADOW_REQUESTER_PREFIX = "shadow:"  # D14c: "Sent by shadow: 0" check

# ── SQL helpers ──────────────────────────────────────────────────────────────

_NEW_CARDS = text("""
    SELECT
        pa.action_id,
        pa.tool_name,
        pa.channel,
        pa.payload,
        pa.requested_by,
        pa.status,
        pa.contact_ref,
        pa.deal_ref,
        pa.created_at,
        pa.decided_at,
        pa.decided_by,
        pa.revisions,
        pa.recipient_phone
    FROM lending.pending_actions pa
    WHERE pa.created_at >= :since
      AND pa.status NOT IN ('blocked')
      AND (pa.decided_by IS NULL OR pa.decided_by NOT LIKE :auto_prefix)
    ORDER BY pa.created_at
""")

_OPEN_ROWS = text("""
    SELECT
        sd.id,
        sd.subject_ref,
        sd.proposed,
        sd.created_at,
        pa.status,
        pa.decided_at,
        pa.decided_by,
        pa.revisions,
        pa.payload,
        pa.deal_ref,
        pa.contact_ref
    FROM lending.shadow_decisions sd
    JOIN lending.pending_actions pa ON pa.action_id = CAST(split_part(sd.subject_ref, ':', 2) AS bigint)
    WHERE sd.class_name = :class_name
      AND sd.actual IS NULL
      AND sd.created_at >= :horizon
      AND sd.subject_kind = 'pending_action'
    ORDER BY sd.created_at
""")

_TEMPLATE_HISTORY = text("""
    SELECT
        COUNT(*) FILTER (WHERE status IN ('approved', 'sent', 'sending', 'failed')) AS total,
        COUNT(*) FILTER (WHERE status = 'rejected')                                 AS rejected,
        COUNT(*) FILTER (WHERE status IN ('approved', 'sent', 'sending', 'failed')
                            AND jsonb_array_length(revisions) > 0)                 AS edited
    FROM lending.pending_actions
    WHERE payload->>'template_version' = :template_version
      AND status NOT IN ('pending', 'revising', 'expired', 'held', 'blocked')
      AND created_at < :before
""")

_CONTACT_EDIT_HISTORY = text("""
    SELECT COUNT(*) AS prev_edited
    FROM lending.pending_actions
    WHERE contact_ref = :contact_ref
      AND status IN ('approved', 'sent', 'sending', 'failed')
      AND jsonb_array_length(revisions) > 0
      AND created_at < :before
""")

_LAST_MESSAGE_TIME = text("""
    SELECT MAX(created_at) AS last_at
    FROM lending.pending_actions
    WHERE contact_ref = :contact_ref
      AND status IN ('sent', 'sending', 'failed')
      AND created_at < :before
""")

# ── Feature extraction ───────────────────────────────────────────────────────

def _build_features(card: Any, db: Session, now: datetime) -> CardFeatures:
    """Build frozen features from a card row (pending_actions).  No PII copied (D16)."""
    payload: dict = card["payload"] if isinstance(card["payload"], dict) else json.loads(card["payload"])
    template_version: Optional[str] = payload.get("template_version")
    card_created: datetime = card["created_at"]

    # Template history counts at card creation time
    history = db.execute(
        _TEMPLATE_HISTORY,
        {"template_version": template_version or "", "before": card_created},
    ).mappings().first() if template_version else None

    past_total = int(history["total"]) if history else 0
    past_edit = int(history["edited"]) if history else 0
    past_reject = int(history["rejected"]) if history else 0

    # Previous draft edited for this contact?
    prev_edited_count = 0
    if card.get("contact_ref"):
        row = db.execute(_CONTACT_EDIT_HISTORY,
                         {"contact_ref": card["contact_ref"], "before": card_created}).scalar_one()
        prev_edited_count = int(row or 0)

    # Time since last message to this contact
    hours_since: Optional[float] = None
    is_first_touch = True
    if card.get("contact_ref"):
        last = db.execute(_LAST_MESSAGE_TIME,
                          {"contact_ref": card["contact_ref"], "before": card_created}).scalar_one()
        if last:
            is_first_touch = False
            delta = card_created - last if last.tzinfo else card_created - last.replace(tzinfo=timezone.utc)
            hours_since = round(delta.total_seconds() / 3600, 2)

    # Body signals
    body: str = payload.get("body", "") or ""
    first_name_hint: str = payload.get("first_name", "") or ""
    missing_first_name = bool(first_name_hint and not body.lstrip().startswith(first_name_hint))

    # Agent job verdict (from payload if the agent job wrote it)
    agent_job_verdict = payload.get("agent_job_verdict", "unknown") or "unknown"

    return CardFeatures(
        template_version=template_version,
        past_total=past_total,
        past_edit=past_edit,
        past_reject=past_reject,
        borrower_replied_since_draft=bool(payload.get("borrower_replied_since_draft", False)),
        agent_job_verdict=agent_job_verdict,
        hold_history_count=int(payload.get("hold_history_count", 0) or 0),
        prev_draft_edited=prev_edited_count > 0,
        is_first_touch=is_first_touch,
        hours_since_last_message=hours_since,
        body_length=len(body),
        has_link=("http://" in body or "https://" in body),
        missing_first_name=missing_first_name,
    )


def _safe_inputs(features: CardFeatures) -> dict:
    """Strip anything that could be PII; keep only refs and features."""
    return {
        "template_version": features.template_version,
        "past_total": features.past_total,
        "past_edit": features.past_edit,
        "past_reject": features.past_reject,
        "borrower_replied_since_draft": features.borrower_replied_since_draft,
        "agent_job_verdict": features.agent_job_verdict,
        "hold_history_count": features.hold_history_count,
        "prev_draft_edited": features.prev_draft_edited,
        "is_first_touch": features.is_first_touch,
        "hours_since_last_message": features.hours_since_last_message,
        "body_length": features.body_length,
        "has_link": features.has_link,
        "missing_first_name": features.missing_first_name,
    }


# ── Actual decision derivation ───────────────────────────────────────────────

def _derive_actual(card_row: Any) -> Optional[str]:
    """Derive the human decision from the card's current status.

    * approved with no revisions → 'approve'
    * approved with ≥1 revision → 'edit'
    * rejected → 'reject'
    * expired / held / still pending / revising → None (not decided yet)
    """
    status: str = card_row["status"]
    if status in ("pending", "revising", "expired", "held", "blocked"):
        return None  # not decided yet

    if status == "rejected":
        return "reject"

    if status in ("approved", "sent", "sending", "failed"):
        revisions = card_row.get("revisions") or []
        if isinstance(revisions, str):
            try:
                revisions = json.loads(revisions)
            except (ValueError, TypeError):
                revisions = []
        return "edit" if revisions else "approve"

    return None  # unknown status → not decided


def _derive_send_result(status: str) -> Optional[str]:
    if status == "sent":
        return "sent"
    if status == "failed":
        return "failed"
    if status == "held":
        return "held"
    return None


def _edit_distance(revisions: Any, final_body: str) -> Optional[float]:
    """Normalised Levenshtein between first revision and final body.  Null when no revisions."""
    if not revisions:
        return None
    if isinstance(revisions, str):
        try:
            revisions = json.loads(revisions)
        except (ValueError, TypeError):
            return None
    if not revisions:
        return None
    first_body: str = ""
    if isinstance(revisions[0], dict):
        first_body = revisions[0].get("payload", {}).get("body", "") or ""
    if not first_body and not final_body:
        return None
    ratio = difflib.SequenceMatcher(None, first_body, final_body).ratio()
    return round(1.0 - ratio, 4)


# ── ShadowRunnerStep ──────────────────────────────────────────────────────────

@dataclass
class ShadowRunResult:
    scored: int = 0
    already_scored: int = 0
    filled: int = 0
    day14_alerts: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class ShadowRunnerStep:
    """Cora's minute sweep step for shadow mode (R1 0.9).

    Constructor dependencies:
      engine        – SQLAlchemy Engine (read-only DB access + shadow store writes)
      clock         – injectable for tests; defaults to utcnow()

    Suppression by construction: this class never receives a SendGate, PendingActionQueue
    or write session for anything other than the shadow tables.
    """

    def __init__(
        self,
        engine: Engine,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._engine = engine
        self._clock = clock

    def __call__(self) -> ShadowRunResult:
        result = ShadowRunResult()
        with Session(self._engine) as db:
            with db.begin():
                self._run(db, result)
        return result

    def _run(self, db: Session, result: ShadowRunResult) -> None:
        now = self._clock()
        horizon = now - timedelta(days=OUTCOME_HORIZON_DAYS)

        shadow_classes = [c for c in registry.all_classes()
                          if store.get_mode(db, c.name) == "shadow"]

        for cls in shadow_classes:
            started_at = store.get_started_at(db, cls.name)
            if not started_at:
                continue

            # Score new cards
            cards = db.execute(_NEW_CARDS, {
                "since": started_at,
                "auto_prefix": f"{AUTO_GRADUATED_ACTOR}%",
            }).mappings().all()

            for card in cards:
                subject_ref = f"action:{card['action_id']}"
                features = _build_features(card, db, now)
                prediction = cls.predict(features)
                rule_ver = deal_ledger.rule_version_at(db)
                inputs = _safe_inputs(features)

                new_id = store.insert_decision(
                    db,
                    class_name=cls.name,
                    class_version=cls.version,
                    subject_kind="pending_action",
                    subject_ref=subject_ref,
                    inputs=inputs,
                    rule_version=rule_ver,
                    confidence=float(prediction.confidence) if prediction.confidence is not None else None,
                    proposed={"decision": prediction.decision},
                )
                if new_id is not None:
                    result.scored += 1
                else:
                    result.already_scored += 1

            # Fill actual decisions on existing open rows
            open_rows = db.execute(_OPEN_ROWS, {"class_name": cls.name, "horizon": horizon}).mappings().all()
            for row in open_rows:
                actual = _derive_actual(row)
                if actual is None:
                    # Cards permanently stuck or past the 14-day window never get a real decision.
                    # Write 'no_decision' so miss-rate stays accurate instead of leaving actual=NULL.
                    card_age_days = (now - row["created_at"]).days if row.get("created_at") else None
                    permanently_terminal = row["status"] in ("expired", "held", "blocked")
                    past_window = card_age_days is not None and card_age_days > SHADOW_WINDOW_DAYS
                    if permanently_terminal or past_window:
                        store.fill_actual(
                            db,
                            subject_ref=row["subject_ref"],
                            actual="no_decision",
                            decided_at=None,
                            decision_seconds=None,
                            edit_distance=None,
                            send_result=None,
                        )
                        result.filled += 1
                    continue  # not decided yet (or just wrote no_decision)

                payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
                body = payload.get("body", "") or ""

                decided_at = row["decided_at"]
                decision_seconds: Optional[float] = None
                if decided_at and row["created_at"]:
                    created = row["created_at"]
                    if isinstance(created, str):
                        from dateutil.parser import parse
                        created = parse(created)
                    delta = decided_at - created if created.tzinfo else decided_at - created.replace(tzinfo=timezone.utc)
                    decision_seconds = round(delta.total_seconds(), 2)

                edit_dist = _edit_distance(row.get("revisions"), body)
                send_result = _derive_send_result(row["status"])

                updated = store.fill_actual(
                    db,
                    subject_ref=row["subject_ref"],
                    actual=actual,
                    decided_at=decided_at,
                    decision_seconds=decision_seconds,
                    edit_distance=edit_dist,
                    send_result=send_result,
                )
                result.filled += updated

                # Deal outcome (only cards with deal_ref; loan_file: prefix only)
                deal_ref = row.get("deal_ref") or ""
                if deal_ref.startswith("loan_file:") and row.get("deal_outcome") is None:
                    _fill_deal_outcome(db, row["subject_ref"], deal_ref)

            # Day-14 alert
            elapsed_days = (now - started_at).days
            if elapsed_days >= SHADOW_WINDOW_DAYS:
                dedup = f"shadow_day14_{cls.name}"
                add_to_inbox_safely(
                    db,
                    event_type="shadow_window_complete",
                    dedup_key=dedup,
                    summary=f"Shadow class {cls.name!r} has been running for {elapsed_days} days. Review and run --stop when ready.",
                    payload={"class_name": cls.name, "elapsed_days": elapsed_days},
                )
                result.day14_alerts.append(cls.name)


def _fill_deal_outcome(db: Session, subject_ref: str, deal_ref: str) -> None:
    """Attempt to derive and store the deal outcome for a card tied to a loan file."""
    try:
        file_id_str = deal_ref.split(":", 1)[1]
        row = db.execute(
            text("SELECT stage, closed_at, closed_reason FROM lending.loan_files WHERE id = :id"),
            {"id": int(file_id_str)},
        ).mappings().first()
        if not row:
            return
        stage = row["stage"]
        if stage in (LoanStage.FUNDED.value, LoanStage.PAID.value):
            store.fill_outcome(db, subject_ref, deal_outcome=stage.lower(), outcome_at=row["closed_at"] or datetime.now(timezone.utc))
        elif row.get("closed_reason") == "declined":
            store.fill_outcome(db, subject_ref, deal_outcome="declined", outcome_at=row["closed_at"] or datetime.now(timezone.utc))
    except Exception as exc:  # class only: missing table, bad id
        logger.warning("[shadow] deal outcome fill failed for %s (%s)", subject_ref, type(exc).__name__)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli_main() -> None:
    from config.settings import get_settings
    from sqlalchemy import create_engine as _create_engine

    parser = argparse.ArgumentParser(description="Shadow mode CLI (R1 0.9)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", metavar="CLASS", help="Start a shadow class (mode → shadow)")
    group.add_argument("--stop", metavar="CLASS", help="Stop a shadow class (mode → off)")
    group.add_argument("--list", action="store_true", help="List all shadow classes")
    args = parser.parse_args()

    engine = _create_engine(get_settings().database_url, pool_pre_ping=True)
    with Session(engine) as db:
        with db.begin():
            if args.list:
                rows = store.get_all_shadow_classes(db)
                if not rows:
                    print("No shadow classes registered.")
                for r in rows:
                    print(f"{r['class_name']:40s}  mode={r['mode']:8s}  started={r['started_at']}")
            elif args.start:
                known = registry.get(args.start)
                if not known:
                    raise SystemExit(f"Unknown class {args.start!r}. Known: {[c.name for c in registry.all_classes()]}")
                store.set_mode(db, args.start, "shadow", changed_by="cli")
                print(f"Started shadow class {args.start!r}")
            else:
                store.set_mode(db, args.stop, "off", changed_by="cli")
                print(f"Stopped shadow class {args.stop!r}")


if __name__ == "__main__":
    _cli_main()
