"""FA Max Fundability Agent configuration (WP-T3-8).

WP-T3-8 owns the enrichment-exhaustion policy referenced in T3-7's
qualification worker: after ENRICHMENT_EXHAUSTION_DAYS of consecutive
pending_enrichment decisions for the ARV gap, the agent escalates once via
the EXCEPTIONS lane rather than staying silent indefinitely.

Exhaustion threshold note: SOT.md Part 6 specifies "retried on the next
batch, never surfaced with a guessed contact" but gives no cap.
ENRICHMENT_EXHAUSTION_DAYS = 5 is a conservative default (absorbs 4-5 nightly
arv_sweep cycles including weekends) while surfacing genuine ARV deserts
before they silently stall an opportunity for weeks. Confirm with Josh before
treating as final.
"""
from __future__ import annotations

# Days of consecutive pending_enrichment before one EXCEPTIONS escalation.
# After this threshold the worker raises once, pointing Josh at WP-8B's
# "Override ARV" Slack button. Deduplication via exceptions_alert_queue's
# DEDUP_WINDOW_HOURS prevents repeat paging for the same opportunity.
ENRICHMENT_EXHAUSTION_DAYS: int = 5

# arv_source label written into fa_max_opportunity_facts by this agent.
# Pattern-constrained to ^[a-z0-9_.:-]+$ by the facts API validator.
# Matches WP-8B's arv_confidence vocabulary so the Scenario Builder can
# distinguish a comp-based ARV (published_arv_engine) from a manual override.
FUNDABILITY_ARV_SOURCE: str = "published_arv_engine"

# venture_key for all FA Max EXCEPTIONS alerts.
FA_MAX_VENTURE_KEY: str = "fa_max_lending"

# Opportunity stages where ARV enrichment is meaningful.
# Opportunities past scoping (ready_to_submit, submitted, funded, dead) either
# already have a sufficient ARV or are terminal — skip them.
FUNDABILITY_ELIGIBLE_STAGES: frozenset[str] = frozenset({
    "qualifying",
    "scoping",
    "warm_hold",
})

# Opportunity outcomes that should be enriched.
# Only open opportunities are enriched; funded/dead/recycled are terminal.
FUNDABILITY_ELIGIBLE_OUTCOMES: frozenset[str] = frozenset({"open"})
