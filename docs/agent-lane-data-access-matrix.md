# Agent Lane — Data Access Matrix

One-page reference: which DB role each Agent Lane component reads/writes,
and which tables. Per build spec §1.1.12 ("Data-access matrix, structurally
enforced ... a one-page table in the repo").

**Note on this file's history:** this file was created fresh on the RELAY
task branch (`feature/relay-v2.2-execution-service`) because VERA's own
version of this file (added on `feature/vera-v2.2-truth-verification-agent`)
is not yet merged into `dev` at the time RELAY started. Per the dev split's
own guidance (§6b: "whoever finishes their DB-role subtask first ... should
start the doc; whoever picks up the other adds their row"), RELAY started
it here. When Vera's branch merges, this file will need a small manual
merge to combine both components' rows — expected, not an error.

| Component | DB role | Reads | Writes |
|---|---|---|---|
| **Relay** (RELAY-v2.2 R1) | Normal app DB role (`src.core.database.get_db_context`) — no dedicated read-only role, unlike Vera | `relay_approval_queue` (its own table); suppression tables at R3 (`EmailOptOut`, `SmsOptOut`, `CoraSuppression`, `DncPhoneCheck`) | `relay_approval_queue` only |
| **Vera** (VERA-v2.2, separate branch — not yet merged) | `vera_readonly` (SELECT-only across all tables) for checks; normal app role for the one designated write target | Everything (read-only) | `vera_facts`, `vera_promises` only |

## Relay's table

- **`relay_approval_queue`** — one row per proposed outreach action, from
  `pending` (awaiting Josh's Slack decision) through `approved`/`rejected`
  to `sent`/`failed`/`skipped`. See `migrations/apply_relay_approval_queue.py`
  and `src.core.models.RelayApprovalQueueItem`.
- Granted `SELECT` to `vera_readonly` (conditionally, if that role exists)
  so Vera can audit approved-vs-sent state without any write access —
  see the `GRANT` statement in the migration above.

## Adding a row

Whichever Agent Lane component's DB-role subtask lands next (Hunter, or
Vera once merged) adds its own row to the table above. Do not remove or
restructure existing rows without confirming with whoever owns that
component's branch.
