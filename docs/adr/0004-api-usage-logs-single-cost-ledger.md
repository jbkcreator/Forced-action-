# 4. `api_usage_logs` is the single cost ledger for Claude

Date: 2026-05-29
Status: Accepted

## Context

Claude API cost is currently tracked in three places:

- `api_usage_logs` — per-call rows (model, tokens, cost, task_type, graph_name), but only
  written when a `db=` session is passed to `call_claude*`. The high-volume paths
  (`compose_and_send` for the whole Cora fleet, concierge chat, recovery/save email tasks,
  forward-pack renderer) omitted `db=`, so they were never logged.
- `agent_decisions.cost_usd` / `tokens_used` — a per-decision rollup written by the agent
  graphs from running state totals.
- `chat_messages.claude_model` / `tokens_in` / `tokens_out` — per-message usage stored on the
  chat row.

We need a trustworthy baseline for cost-aware routing. The baseline is **counterfactual**:
for each call, compare actual cost to a naive "all-Sonnet" cost computed from token counts.
That computation requires one complete table with per-call tokens. Reconciling three tables
risks double-counting (agent calls hit both `api_usage_logs` and `agent_decisions.cost_usd`).

## Decision

Make **`api_usage_logs` the single source of truth** for Claude cost and routing analysis.
Every `call_claude*` path passes `db=` so it writes exactly one row per call there —
including concierge chat. `agent_decisions.cost_usd` and `chat_messages.tokens_*` are retained
as **derived/display values** (per-decision budget cap, per-message UI) and are **never** used
as cost-report inputs. The counterfactual savings report and Haiku-share metric read only
`api_usage_logs`.

## Consequences

- Cost reporting reads one complete table; the counterfactual baseline is well-defined.
- Concierge chat now double-writes (its own `chat_messages` columns **and** `api_usage_logs`).
  Accepted: the chat columns serve per-message UI; the ledger serves cost. They must not be
  summed together.
- Agent compose opens a short independent session per call to write the ledger row
  (commits regardless of decision outcome — true API spend is recorded even on abort).
- Anyone adding a new Claude call site MUST pass `db=`, or that spend becomes invisible.
  Enforce in review.
- Reversing this (going back to a multi-source union) would require rewriting the cost report
  and re-deriving the baseline — hence recorded here.
