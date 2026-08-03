"""
THROUGH-v2.2 — Founder Throughput Layer.

The founder-facing batch-approval bridge between Cora's cold-outreach drafts
(outbound_drafts, status='draft') and Relay's execution queue
(relay_approval_queue). Cora's own drafting code never calls Relay directly
(src.agents.cora.contracts: "Cora never calls Relay, only defines the
shape") — this package is that missing bridge, plus the volume-management
(cap + digest + timers) and standing-order layers built on top of it.

    builder.py    — periodic batch construction + Slack posting (T1/T2)
    batch_slack.py — Slack block-kit posting for one batch (T1)
    decisions.py   — batch-approve / exception-reject / ratify-standing-order (T1/T4)
    power_block.py — Daily Revenue Power Block digest (T2)
    standing_order_compiler.py — proposes auto-approve rules from approval history (T4)
"""
