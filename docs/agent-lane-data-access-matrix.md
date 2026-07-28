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

## Adding a row

Whichever Agent Lane component's DB-role subtask lands next (Hunter) adds
its own row to the table above, following the same format. Do not remove
or restructure existing rows without confirming with whoever owns that
component's branch.
