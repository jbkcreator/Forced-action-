"""
Relay constants — kill-switch key, queue-item status vocabulary, Slack tap
surface. Kept separate from config/settings.py (env-backed connection
strings/secrets live there) because these are fixed vocabulary, not
deployment configuration.
"""
from __future__ import annotations

# Kill-switch feature key Relay checks before every batch and every action.
# Mirrors src.services.kill_switch_service.get_kill_switch_status(feature) —
# the same mechanism Vera uses with "vera_global" (Agent Lane v2.2 dev-split
# §6b: no bespoke per-agent kill mechanism). "STOP ALL" sets the fleet-wide
# "global" override key, which always takes precedence over this one.
KILL_SWITCH_FEATURE = "relay_global"

# relay_approval_queue.status values.
STATUS_PENDING = "pending"    # awaiting Josh's Slack decision
STATUS_APPROVED = "approved"  # decided, not yet executed
STATUS_REJECTED = "rejected"  # decided, will never execute
STATUS_SENT = "sent"          # dispatcher succeeded
STATUS_FAILED = "failed"      # dispatcher raised
STATUS_SKIPPED = "skipped"    # idempotency: same key already processed, or picked up twice

# Spec §1.1.13 ("Tap surface") — Decision Packets render as interactive
# approve/reject buttons in this Slack channel.
SLACK_RELAY_CHANNEL = "#agent-daily"

# Default TTL for a kill-switch override set via /slack/kill. Auto-expires
# so a forgotten STOP doesn't permanently wedge sending — Josh (or anyone
# with Slack access) can also clear it early by setting the override to
# "green" before the TTL elapses.
KILL_OVERRIDE_TTL_SECONDS = 3600
