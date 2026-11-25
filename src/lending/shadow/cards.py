"""R1 0.9 card reader: read-only access to approval cards, their features and settled outcomes."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, Optional, Protocol, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from src.lending.audit import deal_ledger
from src.lending.contracts import LoanStage
from src.lending.shadow.judge import CardFeatures
from src.lending.slack_names import names_by_ghl_contact, names_by_phone

APPROVED_STATUSES = ("approved", "sending", "sent", "failed", "blocked")
_CARD_COLUMNS = ("action_id, tool_name, channel, payload, status, contact_ref, deal_ref, created_at, decided_at, "
                 "decided_by, revisions, executed_at, recipient_phone, recipient_email")

_NEW_CARDS = text(f"SELECT {_CARD_COLUMNS} FROM lending.pending_actions WHERE created_at >= :since ORDER BY created_at")

_CARDS_BY_ID = text(f"SELECT {_CARD_COLUMNS} FROM lending.pending_actions WHERE action_id = ANY(:ids)")

_TEMPLATE_HISTORY = text("""
    SELECT count(*) AS total,
           count(*) FILTER (WHERE status = 'rejected') AS rejected,
           count(*) FILTER (WHERE status <> 'rejected' AND jsonb_array_length(revisions) > 0) AS edited
      FROM lending.pending_actions
     WHERE payload->>'template_version' = :template_version
       AND decided_at < :before
       AND decided_by IS DISTINCT FROM :auto_actor
       AND status IN ('rejected', 'approved', 'sending', 'sent', 'failed', 'blocked')
""")

_PREVIOUS_DRAFT_EDITED = text("""
    SELECT jsonb_array_length(revisions) > 0 FROM lending.pending_actions
     WHERE contact_ref = :contact_ref AND action_id <> :action_id AND decided_at < :before
       AND status IN ('approved', 'sending', 'sent', 'failed', 'blocked')
     ORDER BY decided_at DESC LIMIT 1
""")

_LAST_SENT = text("""
    SELECT max(executed_at) FROM lending.pending_actions
     WHERE contact_ref = :contact_ref AND status = 'sent' AND executed_at < :before
""")

_REPLIED_SINCE = text("""
    SELECT EXISTS (SELECT 1 FROM lending.instantly_replies
                    WHERE lead_email = :email AND received_at < :before
                      AND received_at > COALESCE(CAST(:since AS timestamptz), '-infinity'::timestamptz)
                      AND action_id IS DISTINCT FROM :action_id)
""")

_AGENT_JOB_HELD = text("SELECT held_reason IS NOT NULL FROM lending.agent_jobs WHERE action_id = :action_id")

# Same deal-ref resolution as the deal ledger (loan_file / ghl_opportunity / doc_chase).
_DEAL_FILE = text("""
    SELECT CASE
        WHEN starts_with(:ref, 'ghl_opportunity:') THEN
            (SELECT min(f.id) FROM lending.loan_files f WHERE f.ghl_opportunity_id = substr(:ref, 17) HAVING count(*) = 1)
        WHEN :ref ~ '^loan_file:[0-9]+$' THEN substr(:ref, 11)::bigint
        WHEN :ref ~ '^doc_chase:[0-9]+$' THEN
            (SELECT d.loan_file_id FROM lending.doc_chases d WHERE d.id = substr(:ref, 11)::bigint)
    END
""")

_FILE_OUTCOME = text("""
    SELECT f.closed_at,
           (SELECT min(e.created_at) FROM lending.loan_stage_events e
             WHERE e.loan_file_id = f.id AND e.outcome = 'applied' AND e.to_stage IN (:funded, :paid)) AS funded_at
      FROM lending.loan_files f WHERE f.id = :file_id
""")


class CardReader(Protocol):
    def new_cards(self, db: Session, since: datetime) -> list[dict]: ...
    def features(self, db: Session, card: dict) -> CardFeatures: ...
    def cards(self, db: Session, action_ids: Sequence[int]) -> dict[int, dict]: ...
    def deal_outcome(self, db: Session, deal_ref: str) -> Optional[tuple[str, datetime]]: ...
    def rule_version_at(self, db: Session, at: datetime) -> Optional[int]: ...


def payload_of(card: dict) -> dict:
    payload = card.get("payload") or {}
    return json.loads(payload) if isinstance(payload, str) else payload


def revisions_of(card: dict) -> list:
    revisions = card.get("revisions") or []
    return json.loads(revisions) if isinstance(revisions, str) else revisions


def _first_name(db: Session, card: dict) -> Optional[str]:
    contact_ref = card.get("contact_ref") or ""
    name = None
    if contact_ref.startswith("ghl:"):
        name = names_by_ghl_contact(db, [contact_ref[4:]]).get(contact_ref[4:])
    if not name and card.get("recipient_phone"):
        name = names_by_phone(db, [card["recipient_phone"]]).get(card["recipient_phone"])
    return name.split()[0] if name and " or " not in name else None


class SqlCardReader:
    def new_cards(self, db: Session, since: datetime) -> list[dict]:
        return [dict(r) for r in db.execute(_NEW_CARDS, {"since": since}).mappings()]

    def features(self, db: Session, card: dict) -> CardFeatures:
        """Every input as it stood at card creation: no peeking at anything after it."""
        payload = payload_of(card)
        created = card["created_at"]
        template_version = payload.get("template_version")
        history = db.execute(_TEMPLATE_HISTORY, {"template_version": template_version, "before": created,
                                                 "auto_actor": AUTO_GRADUATED_ACTOR}).mappings().one() \
            if template_version else {"total": 0, "edited": 0, "rejected": 0}

        contact_ref = card.get("contact_ref")
        prev_edited = last_sent = None
        if contact_ref:
            prev_edited = db.execute(_PREVIOUS_DRAFT_EDITED, {"contact_ref": contact_ref, "before": created,
                                                              "action_id": card["action_id"]}).scalar()
            last_sent = db.execute(_LAST_SENT, {"contact_ref": contact_ref, "before": created}).scalar()

        replied = bool(card.get("recipient_email") and db.execute(_REPLIED_SINCE, {
            "email": card["recipient_email"], "before": created, "since": last_sent,
            "action_id": card["action_id"]}).scalar())
        from_job = (card.get("tool_name") or "").startswith("agent_job:")
        job_held = bool(from_job and db.execute(_AGENT_JOB_HELD, {"action_id": card["action_id"]}).scalar())

        body = payload.get("body") or ""
        first_name = _first_name(db, card) if body else None
        return CardFeatures(
            template_version=template_version,
            past_total=int(history["total"]),
            past_edit=int(history["edited"]),
            past_reject=int(history["rejected"]),
            borrower_replied_since_last_send=replied,
            from_agent_job=from_job,
            agent_job_held=job_held,
            prev_draft_edited=bool(prev_edited),
            is_first_touch=last_sent is None,
            hours_since_last_message=round((created - last_sent).total_seconds() / 3600, 2) if last_sent else None,
            body_length=len(body),
            has_link="http://" in body or "https://" in body,
            missing_first_name=bool(first_name and first_name.lower() not in body.lower()),
        )

    def cards(self, db: Session, action_ids: Sequence[int]) -> dict[int, dict]:
        if not action_ids:
            return {}
        rows = db.execute(_CARDS_BY_ID, {"ids": list(action_ids)}).mappings()
        return {r["action_id"]: dict(r) for r in rows}

    def deal_outcome(self, db: Session, deal_ref: str) -> Optional[tuple[str, datetime]]:
        file_id = db.execute(_DEAL_FILE, {"ref": deal_ref}).scalar()
        if file_id is None:
            return None
        row = db.execute(_FILE_OUTCOME, {"file_id": file_id, "funded": LoanStage.FUNDED.value,
                                         "paid": LoanStage.PAID.value}).mappings().first()
        if row is None:
            return None
        if row["funded_at"]:
            return "funded", row["funded_at"]
        if row["closed_at"]:
            return "declined", row["closed_at"]
        return None

    def rule_version_at(self, db: Session, at: datetime) -> Optional[int]:
        return deal_ledger.rule_version_at(db, at)


def card_features_as_inputs(features: CardFeatures, card: dict[str, Any]) -> dict:
    """The frozen inputs stored on the row: references and features only, never a name, phone, email or body."""
    return {"action_id": card["action_id"], "channel": card.get("channel"), "deal_ref": card.get("deal_ref"),
            **asdict(features)}
