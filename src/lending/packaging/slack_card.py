"""R1 1.7 — Slack card for a secondary lender package (D21).

One card per package in the ops channel (not #approvals).  Renders a pure dict
so tests can verify it without hitting Slack (Seam 4).

Card fields:
  - Lender name + "box not confirmed — do not submit" stamp when unconfirmed
  - Fit reasons (from lender_fit evaluator)
  - Checklist (all items; green checkmark for received, red X for missing)
  - Blocked or quarantined documents with reasons
  - Buttons:
      Mark submitted  — disabled when box_confirmed=false or any doc is blocked
      Dismiss
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from config.lender_matrix import LENDER_MATRIX
from src.lending.packaging.packet_validator import Outcome


def _lender_display_name(lender_key: str) -> str:
    for r in LENDER_MATRIX:
        if r.key == lender_key:
            return r.name
    return lender_key


def _checklist_text(checklist: list[dict]) -> str:
    lines = []
    for item in checklist:
        mark = ":white_check_mark:" if item.get("received") else ":x:"
        lines.append(f"{mark} {item['label']}")
    return "\n".join(lines) if lines else "_No items_"


def _problem_docs_text(verdict_json: Optional[str]) -> str:
    if not verdict_json:
        return ""
    try:
        verdict = json.loads(verdict_json) if isinstance(verdict_json, str) else verdict_json
    except (json.JSONDecodeError, TypeError):
        return ""
    lines = []
    for d in verdict.get("per_doc", []):
        if d.get("outcome") in ("block", "quarantine"):
            reasons = "; ".join(d.get("reasons", []))
            tag = "🔴 QUARANTINED" if d.get("outcome") == "quarantine" else "⚠️ BLOCKED"
            lines.append(f"{tag} doc #{d.get('upload_id')} ({d.get('item_code') or 'unclassified'}): {reasons}")
    return "\n".join(lines)


@dataclass(frozen=True)
class PackageCard:
    """Pure in-memory representation of the Slack card — no Slack SDK dependency."""
    blocks: list[dict]
    # True when the Mark submitted button should be enabled
    can_submit: bool
    package_id: int
    lender_key: str


def build_card(
    package_id: int,
    lender_key: str,
    box_confirmed: bool,
    checklist: list[dict],
    verdict_json: Optional[str],
    fit_reasons: Optional[str] = None,
) -> PackageCard:
    """Build the Slack block-kit card dict. Pure function; no I/O."""
    display_name = _lender_display_name(lender_key)

    # decide if submit is enabled
    has_blocked = False
    if verdict_json:
        try:
            v = json.loads(verdict_json) if isinstance(verdict_json, str) else verdict_json
            has_blocked = any(d.get("outcome") in ("block", "quarantine")
                               for d in v.get("per_doc", []))
        except (json.JSONDecodeError, TypeError):
            has_blocked = True  # fail safe

    can_submit = box_confirmed and not has_blocked

    # header
    header_text = f":package: *{display_name}* — Secondary Lender Package #{package_id}"
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Package #{package_id} — {display_name}", "emoji": True},
        },
        {"type": "divider"},
    ]

    # unconfirmed stamp
    if not box_confirmed:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":warning: *Box not confirmed — do not submit*\nThis lender's rules are placeholder values. Package is for review only.",
            },
        })

    # fit reasons
    if fit_reasons:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Fit reasons*\n{fit_reasons}"},
        })

    # checklist
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*Checklist*\n{_checklist_text(checklist)}"},
    })

    # problem docs
    problems = _problem_docs_text(verdict_json)
    if problems:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Document issues*\n{problems}"},
        })

    blocks.append({"type": "divider"})

    # action buttons
    submit_btn: dict = {
        "type": "button",
        "text": {"type": "plain_text", "text": "Mark submitted", "emoji": False},
        "value": json.dumps({"action": "mark_submitted", "package_id": package_id}),
        "action_id": f"pkg_submit_{package_id}",
    }
    if not can_submit:
        submit_btn["style"] = "danger"  # visual disabled indicator
        submit_btn["confirm"] = {
            "title": {"type": "plain_text", "text": "Cannot submit"},
            "text": {"type": "plain_text",
                     "text": "Box is unconfirmed or documents are blocked. Submission not allowed."},
            "confirm": {"type": "plain_text", "text": "OK"},
            "deny": {"type": "plain_text", "text": "Cancel"},
        }

    blocks.append({
        "type": "actions",
        "elements": [
            submit_btn,
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Dismiss", "emoji": False},
                "value": json.dumps({"action": "dismiss", "package_id": package_id}),
                "action_id": f"pkg_dismiss_{package_id}",
                "style": "danger",
            },
        ],
    })

    return PackageCard(
        blocks=blocks,
        can_submit=can_submit,
        package_id=package_id,
        lender_key=lender_key,
    )


def post_card(card: PackageCard, *, channel: str, slack_client) -> Optional[str]:
    """Post the card to Slack. Returns the message ts or None on error."""
    try:
        resp = slack_client.chat_postMessage(channel=channel, blocks=card.blocks)
        return resp.get("ts")
    except Exception:
        return None
