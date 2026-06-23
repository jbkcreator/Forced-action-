"""
Win-Story Auto-Publisher (Sprint S5).

Writes sanitised proof statements to win_story_assets whenever a lead pack is
delivered or (future: Part 2) a loan is funded. No PII — county + deal type only.

Stories are staged (is_public=False) and require one-tap Slack approval before
going live. See POST /api/admin/win-stories/{id}/approve and
POST /api/admin/slack/win-story/interact.

Callers wrap this in try/except: a publish failure must never break the delivery
or payment path that triggered it.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.core.models import WinStoryAsset

logger = logging.getLogger(__name__)

_LEAD_PACK_TEMPLATES = {
    "roofing":          "5 fresh roofing leads just claimed in {county} County!",
    "restoration":      "5 fresh restoration leads just claimed in {county} County!",
    "public_adjusters": "5 fresh public adjuster leads just claimed in {county} County!",
    "wholesalers":      "5 fresh wholesaler leads just claimed in {county} County!",
    "fix_flip":         "5 fresh fix & flip leads just claimed in {county} County!",
    "attorneys":        "5 fresh attorney leads just claimed in {county} County!",
}
_LEAD_PACK_DEFAULT = "5 fresh leads just claimed in {county} County!"

# Part 2 (loan_funded) template — wired when S1 LoanReferral.funded is built.
_LOAN_FUNDED_TEMPLATE = "Another {amount_range} loan funded in {county} County!"


def publish_win_story(
    event_type: str,
    county_id: str,
    db: Session,
    *,
    detail: Optional[str] = None,
    amount_range: Optional[str] = None,
) -> None:
    """
    Format, persist (staged), and notify Slack for review.

    Args:
        event_type:   'lead_pack' or 'loan_funded'.
        county_id:    county slug (e.g. 'hillsborough').
        db:           SQLAlchemy session — caller owns the transaction.
        detail:       For 'lead_pack': the vertical slug (e.g. 'roofing').
        amount_range: For 'loan_funded': human-readable range (e.g. '$250k').
    """
    try:
        county_display = county_id.replace("_", " ").title()

        if event_type == "lead_pack":
            template = _LEAD_PACK_TEMPLATES.get(detail or "", _LEAD_PACK_DEFAULT)
            proof_text = template.format(county=county_display)
        elif event_type == "loan_funded":
            ar = amount_range or "a"
            proof_text = _LOAN_FUNDED_TEMPLATE.format(
                amount_range=ar, county=county_display
            )
        else:
            logger.warning("[WinStory] unknown event_type=%s — skipping", event_type)
            return

        asset = WinStoryAsset(
            event_type=event_type,
            county_id=county_id,
            proof_text=proof_text,
            amount_range=amount_range,
            is_public=False,
            created_at=datetime.now(timezone.utc),
        )
        db.add(asset)
        db.flush()

        slack_ts = _notify_slack_draft(asset)
        if slack_ts:
            asset.slack_message_ts = slack_ts

        logger.info(
            "[WinStory] staged event=%s county=%s id=%d slack_ts=%s",
            event_type, county_id, asset.id, slack_ts,
        )
    except Exception:
        logger.warning(
            "[WinStory] failed to stage event=%s county=%s",
            event_type, county_id, exc_info=True,
        )


def _notify_slack_draft(asset: WinStoryAsset) -> Optional[str]:
    """Post a draft win-story to Slack with an Approve button. Returns message ts."""
    from config.settings import get_settings
    settings = get_settings()

    token = settings.slack_bot_token
    channel = settings.cora_incident_slack_channel
    if not token or not channel:
        logger.info("[WinStory] Slack not configured — skipping draft notification")
        return None

    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        resp = client.chat_postMessage(
            channel=channel,
            text=f":memo: Win-story draft pending approval — {asset.proof_text}",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f":memo: *Win-story draft — approval required*\n"
                            f"*County:* {asset.county_id}\n"
                            f"*Type:* {asset.event_type}\n"
                            f"*Copy:* {asset.proof_text}"
                        ),
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Approve"},
                            "style": "primary",
                            "action_id": "approve_win_story",
                            "value": json.dumps({"asset_id": asset.id, "action": "approve"}),
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Dismiss"},
                            "action_id": "dismiss_win_story",
                            "value": json.dumps({"asset_id": asset.id, "action": "dismiss"}),
                        },
                    ],
                },
            ],
        )
        return resp.get("ts")
    except Exception:
        logger.warning("[WinStory] Slack draft notification failed", exc_info=True)
        return None
