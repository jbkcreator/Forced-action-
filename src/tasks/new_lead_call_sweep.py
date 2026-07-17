"""
New-lead call SLA-timeout sweep — safety net for the new_lead_voice_call
graph (src/agents/graphs/new_lead_voice_call.py), triggered at signup by
src/services/signup_engine.py::create_free_account_by_email.

Nothing in the router/supervisor self-detects "this event's outcome never
happened" (Redis down, agents process down, event dropped) — this sweep
fills that gap, modeled directly on the same 5-minute-SLA pattern already
proven by src/tasks/owner_alert_sweep.py.

Run every 2 minutes via cron:

    */2 * * * * python -m src.tasks.new_lead_call_sweep

Finds free-signup subscribers (excludes the separate phone-inbound signup
path, which never fires this event and would otherwise always look
"missed") whose signup is 4+ minutes old with no new_lead_voice_call
agent_decisions row yet, and pages Josh directly (owner_alert.notify_owner)
plus posts to the ops Slack channel (cora_slack.post_incident_alert) — but
only once per subscriber, checked via owner_alert_dispatch before firing
either, since post_incident_alert has no idempotency of its own.
"""

import logging
import sys
from types import SimpleNamespace

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.cora_slack import post_incident_alert
from src.services.owner_alert import notify_owner

logger = logging.getLogger(__name__)

BUFFER_MINUTES = 4      # fires 1 minute before the 5-minute SLA breaches
LOOKBACK_MINUTES = 60    # caps the scan window; older signups already resolved or alerted once

# Phone-inbound signup sources never fire new_lead_signup at all (a different
# function, create_free_account/onboard_inbound_caller) — excluding them
# here, not including free-signup sources, so direct/affiliate/admin/unknown
# free-signup rows are never wrongly skipped.
_PHONE_INBOUND_SOURCES = ("missed_call", "cora_sms", "dbpr_email")


def sweep_stalled_new_lead_calls() -> int:
    """Fallback-alert any new-lead signup whose call never fired within SLA. Returns count alerted."""
    alerted = 0
    with get_db_context() as db:
        stale = db.execute(
            text(
                "SELECT s.id, s.phone, s.created_at FROM subscribers s "
                "WHERE s.phone IS NOT NULL "
                "  AND s.signup_source NOT IN :phone_inbound_sources "
                "  AND s.created_at < now() - make_interval(mins => :buffer_minutes) "
                "  AND s.created_at > now() - make_interval(mins => :lookback_minutes) "
                "  AND NOT EXISTS ("
                "      SELECT 1 FROM agent_decisions d "
                "      WHERE d.subscriber_id = s.id "
                "        AND d.graph_name = 'new_lead_voice_call' "
                # Only a SUCCESSFUL dispatch suppresses the fallback. The graph
                # writes a decision row for compliance aborts, hierarchy blocks,
                # Synthflow failures and exceptions too — those must still page
                # the founder, so we require terminal_status='completed' AND the
                # summary's sent flag to be true.
                "        AND d.terminal_status = 'completed' "
                "        AND d.summary->>'sent' = 'true'"
                "  )"
            ),
            {
                "phone_inbound_sources": _PHONE_INBOUND_SOURCES,
                "buffer_minutes": BUFFER_MINUTES,
                "lookback_minutes": LOOKBACK_MINUTES,
            },
        ).all()

        for row in stale:
            idempotency_key = f"new_lead_call:{row.id}"
            try:
                already_claimed = db.execute(
                    text("SELECT 1 FROM owner_alert_dispatch WHERE alert_key = :key"),
                    {"key": idempotency_key},
                ).first()
            except Exception:
                logger.warning("new_lead_call_sweep: idempotency check failed for subscriber=%s", row.id, exc_info=True)
                continue
            if already_claimed:
                continue

            subject = f"New lead #{row.id} — voice call did not fire"
            body = f"Signup at {row.created_at} had no Synthflow call within SLA. Call now: {row.phone}"

            try:
                notify_owner(subject=subject, body=body, idempotency_key=idempotency_key)
            except Exception:
                logger.warning("new_lead_call_sweep: notify_owner failed for subscriber=%s", row.id, exc_info=True)

            try:
                post_incident_alert(
                    SimpleNamespace(
                        metric_name="new_lead_call_sla_breach", severity="high",
                        observed_value=None, threshold_value=None, baseline_value=None,
                        county_id=None, feature_name="speed_to_lead_call",
                        action_taken="fallback_alert_fired", duration_hours=None,
                        breach_started=row.created_at,
                    ),
                    kind="human_required",
                    action_summary=f"Subscriber {row.id} signed up, no voice call dispatched within SLA.",
                )
            except Exception:
                logger.warning("new_lead_call_sweep: post_incident_alert failed for subscriber=%s", row.id, exc_info=True)

            alerted += 1

    if alerted:
        logger.info("new_lead_call_sweep: alerted on %d stalled new-lead call(s)", alerted)
    return alerted


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    sweep_stalled_new_lead_calls()
