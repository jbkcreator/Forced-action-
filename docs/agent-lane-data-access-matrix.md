# Agent Lane — Data-Access Matrix

Per Agent Lane v2.2 §1.1.12 ("Data-access matrix, structurally enforced ... a
one-page table in the repo"). One row per agent-facing DB role. Updated by
whoever ships that role's provisioning migration — append, don't restructure.

| Agent | DB role | Read | Write | Provisioned by |
|---|---|---|---|---|
| Vera | `vera_readonly` | SELECT on all tables/sequences, schema `public` (incl. future tables via default privileges) | **None, anywhere** — no INSERT/UPDATE/DELETE/DDL grants; session-level `default_transaction_read_only=on` as a second, independent guard | `migrations/apply_vera_readonly_role.py` |
| Vera (facts writer) | app role (`DATABASE_URL`) | n/a (write-only path) | `vera_facts` only — the one table Vera's code ever writes to, via `src.agents.vera.facts.write_fact()` | `migrations/apply_vera_facts.py` |
| Hunter | *(not yet provisioned)* | — | — | HUNTER-01, when built |
| Relay | Normal app DB role (`src.core.database.get_db_context`) — no dedicated read-only role, unlike Vera | `relay_approval_queue` (its own table); suppression tables at R3 (`EmailOptOut`, `SmsOptOut`, `CoraSuppression`, `DncPhoneCheck`) | `relay_approval_queue` only | `migrations/apply_relay_approval_queue.py` |
| Agent Lane experiments (Cora/REVINT/Hunter/LEARN) | Normal app DB role | `agent_lane_experiments`, `agent_lane_experiment_assignments` (own tables); `price_assignments` | Same tables — never Lifecycle's `ab_tests`/`ab_assignments` (see below) | `migrations/apply_agent_lane_experiments.py` |

**Enforcement note:** Vera's two rows are deliberately separate roles, not
one role with mixed grants — a bug in Vera's read-path code cannot reach a
write grant that doesn't exist on that connection, and the fact-write path
is scoped to exactly one table regardless of what the read connection can
see.

## Relay's table

- **`relay_approval_queue`** — one row per proposed outreach action, from
  `pending` (awaiting Josh's Slack decision) through `approved`/`rejected`
  to `sent`/`failed`/`skipped`. See `migrations/apply_relay_approval_queue.py`
  and `src.core.models.RelayApprovalQueueItem`.
- Granted `SELECT` to `vera_readonly` (conditionally, if that role exists)
  so Vera can audit approved-vs-sent state without any write access —
  see the `GRANT` statement in the migration above.

## Agent Lane's experiment tables

- **`agent_lane_experiments`** / **`agent_lane_experiment_assignments`** —
  Agent Lane's own experiment registry and per-opportunity arm assignments
  (`src.core.models.AgentLaneExperiment` / `AgentLaneExperimentAssignment`),
  read/written via `src/services/agent_lane_experiment_engine.py`.
  Deliberately separate from Lifecycle's `ab_tests`/`ab_assignments` —
  Agent Lane (pre-customer) and Lifecycle (post-customer) are different
  engines; sharing one experiment table would couple their schemas and
  blast radius (Lifecycle's `ab_rollback_check` walks every active
  `AbTest` with no name filter, so a row inserted there is already
  subject to Lifecycle's own rollback math). `agent_lane_experiment_
  assignments` keys only on `opportunity_thread_id` — never
  `subscriber_id` — since Agent Lane is pre-customer by definition.
  REVINT-v2.2 originally extended `AbTest`/`AbAssignment` directly for
  this; `migrations/apply_agent_lane_experiments.py` created these tables
  and repointed `price_assignments` at them, and `migrations/apply_agent_
  lane_experiment_separation_cleanup.py` removes the now-unused columns
  from `ab_tests`/`ab_assignments` once that redirect is confirmed live.

## Adding a row

Whichever Agent Lane component's DB-role subtask lands next (Hunter) adds
its own row to the table above, following the same format. Do not remove
or restructure existing rows without confirming with whoever owns that
component's branch.
