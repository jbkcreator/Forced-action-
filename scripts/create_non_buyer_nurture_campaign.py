"""Create (and optionally test) the shared Non-Buyer Nurture Instantly campaign.

This creates a STANDALONE Instantly campaign via the API key in the env — it
does NOT write an EmailCampaign DB row, so the DBPR contractor top-up cron
(run_all_topups) never injects contractor leads into it. That separation is the
whole point: the nurture audience must never mix with DBPR cold prospecting.

PREREQUISITES (external, cannot be scripted):
  1. Instantly workspace on an active PAID plan.
  2. A dedicated sending domain (separate from the DBPR cold domain) with
     SPF/DKIM/DMARC, and >= 1 mailbox connected to the workspace + warmed
     (2-4 weeks). Put those mailbox addresses in SENDING_INBOXES below.

USAGE:
  # 1. create the campaign, print its id
  PYTHONPATH=. python scripts/create_non_buyer_nurture_campaign.py

  # 2. put the printed id in env:  NON_BUYER_NURTURE_CAMPAIGN_ID=<id>

  # 3. enroll one test address to confirm a send end-to-end
  PYTHONPATH=. python scripts/create_non_buyer_nurture_campaign.py \
      --test-email lesly.vj@heu.ai --campaign-id <id>
"""
from __future__ import annotations

import argparse
import sys

from src.services import instantly_service as instantly
from src.services import email_templates as templates

# --- EDIT THESE before running -------------------------------------------------

CAMPAIGN_NAME = "Non-Buyer Nurture"

# Warmed sending mailboxes on the dedicated lifecycle domain. REQUIRED — an empty
# list creates a campaign that can never send.
SENDING_INBOXES: list[str] = [
    # "hello@try-forcedaction.com",
    # "team@try-forcedaction.com",
]

# CAN-SPAM footer appended to every step. {{unsubscribe}} is Instantly's native
# one-click unsubscribe merge tag. Physical mailing address is legally required.
FOOTER = (
    "\n\n—\nForced Action · <PHYSICAL MAILING ADDRESS HERE>\n"
    "Don't want these? Unsubscribe: {{unsubscribe}}"
)

# 3-touch sequence. delay_days is the gap BEFORE that step fires (step 1 = 0 =
# fires when the lead is added by the daily sweep). Replace the copy.
STEPS = [
    {"step_number": 1, "delay_days": 0,
     "subject": "Still deciding on Forced Action?",
     "body": "Hi,\n\n<touch 1 copy>." + FOOTER},
    {"step_number": 2, "delay_days": 3,
     "subject": "The leads you'd have gotten this week",
     "body": "Hi,\n\n<touch 2 copy>." + FOOTER},
    {"step_number": 3, "delay_days": 4,
     "subject": "Last note from us",
     "body": "Hi,\n\n<touch 3 copy>." + FOOTER},
]

# Business-hours schedule, Mon-Fri. Instantly v2 enum rejects America/New_York —
# America/Detroit is the working ET value (see instantly-api gotchas).
SCHEDULE = {
    "schedules": [{
        "name": "Default",
        "timing": {"from": "09:00", "to": "17:00"},
        "days": {"1": True, "2": True, "3": True, "4": True, "5": True},
        "timezone": "America/Detroit",
    }],
}

# Keep at/under the domain warm-up schedule. Ramp up over weeks.
DAILY_LIMIT = 25

# ------------------------------------------------------------------------------


def create() -> None:
    if not instantly._is_configured():
        print("ERROR: Instantly not configured (INSTANTLY_API_KEY / INSTANTLY_ENABLED).")
        sys.exit(1)
    if not SENDING_INBOXES:
        print("ERROR: SENDING_INBOXES is empty — edit the script with your warmed mailbox(es) first.")
        sys.exit(1)

    steps = templates.build_instantly_sequence(STEPS)
    result = instantly.create_campaign(
        name=CAMPAIGN_NAME,
        schedule=SCHEDULE,
        sequence_steps=steps,
        email_list=SENDING_INBOXES,
    )
    if not result or not result.get("id"):
        print("ERROR: create_campaign returned no id — check logs above (paid plan? valid inboxes?).")
        sys.exit(1)

    campaign_id = result["id"]
    instantly.update_campaign(campaign_id, {"daily_limit": DAILY_LIMIT})
    instantly.activate_campaign(campaign_id)

    print("Created + activated Non-Buyer Nurture campaign.")
    print(f"campaign_id: {campaign_id}")
    print(f"\nSet this in env:\n  NON_BUYER_NURTURE_CAMPAIGN_ID={campaign_id}")


def test_email(address: str, campaign_id: str) -> None:
    """Enroll one address so Instantly sends step 1 (on the campaign schedule)."""
    if not instantly._is_configured():
        print("ERROR: Instantly not configured.")
        sys.exit(1)
    res = instantly.add_leads(campaign_id, [{"email": address}])
    if res is None:
        print(f"ERROR: add_leads failed for {address} (see logs).")
        sys.exit(1)
    created = res.get("leads_created", 0)
    skipped = res.get("leads_skipped", 0)
    print(f"Enrolled {address} into {campaign_id}: created={created} skipped={skipped}")
    print("Instantly will send step 1 on the campaign's next scheduled window.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-email", help="Enroll this address as a test lead")
    parser.add_argument("--campaign-id", help="Campaign id (required with --test-email)")
    args = parser.parse_args()

    if args.test_email:
        if not args.campaign_id:
            print("ERROR: --test-email requires --campaign-id")
            sys.exit(1)
        test_email(args.test_email, args.campaign_id)
    else:
        create()
