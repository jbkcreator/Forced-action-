# Agent Lane — Data-Access Matrix

Per Agent Lane v2.2 §1.1.12 ("Data-access matrix, structurally enforced ... a
one-page table in the repo"). One row per agent-facing DB role. Updated by
whoever ships that role's provisioning migration — append, don't restructure.

| Agent | DB role | Read | Write | Provisioned by |
|---|---|---|---|---|
| Vera | `vera_readonly` | SELECT on all tables/sequences, schema `public` (incl. future tables via default privileges) | **None, anywhere** — no INSERT/UPDATE/DELETE/DDL grants; session-level `default_transaction_read_only=on` as a second, independent guard | `migrations/apply_vera_readonly_role.py` |
| Vera (facts writer) | app role (`DATABASE_URL`) | n/a (write-only path) | `vera_facts` only — the one table Vera's code ever writes to, via `src.agents.vera.facts.write_fact()` | `migrations/apply_vera_facts.py` |
| Hunter | *(not yet provisioned)* | — | — | HUNTER-01, when built |
| Relay | *(not yet provisioned)* | — | — | RELAY-v2.2, when built |

**Enforcement note:** Vera's two rows are deliberately separate roles, not
one role with mixed grants — a bug in Vera's read-path code cannot reach a
write grant that doesn't exist on that connection, and the fact-write path
is scoped to exactly one table regardless of what the read connection can
see.
