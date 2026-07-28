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

# Guard verdict outcomes (RELAY-v2.2 sub-task R3). DEFER leaves the row
# 'approved' and unclaimed for a later sweep; BLOCK marks it STATUS_SKIPPED
# and it is never retried. See src.services.relay.guards.evaluate().
GUARD_ALLOW = "allow"
GUARD_DEFER = "defer"
GUARD_BLOCK = "block"

# Guard reason strings, written to the queue row's `error` column on BLOCK
# (via mark_skipped) or logged only on DEFER (see engine.execute_batch()).
# The daily-ceiling reason is logged directly in engine.py rather than kept
# here -- reserve_daily_slot() returns a plain bool, not a Verdict, since PR
# #179's fix moved the ceiling out of evaluate() into its own atomic
# reserve/release step (see guards.py).
REASON_OUTSIDE_SEND_WINDOW = "outside_send_window"
REASON_SUPPRESSED = "suppressed"  # suffixed with ":{cause}", e.g. "suppressed:email_opt_out"
