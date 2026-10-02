"""WP-GL-9 text-back: client-approved wording and the rules around it (values only, no logic)."""
from __future__ import annotations

# Statuses that occupy a contact's one text slot for the Eastern day. A skipped, dry-run or
# failed event does NOT hold it, so a later call the same day can still be texted once consent
# exists. send_unknown (crash after the claim) holds it: we cannot prove nothing went out.
SLOT_HOLDING_STATUSES = ("pending", "sending", "sent", "send_unknown")
SLOT_HOLDING_SQL = "status IN (" + ", ".join(f"'{s}'" for s in SLOT_HOLDING_STATUSES) + ")"
