# Deal audit ledger (Release 1, item 0.8)

`lending.deal_audit_events` is an append-only record of everything that happens to a loan file. Code:
`src/lending/deal_ledger.py`; migration: `migrations/apply_lending_deal_audit_ledger.py`; endpoint:
`src/api/lending_deal_ledger_router.py`.

## Read a file's whole life

```
python -m src.lending.deal_ledger --loan-file 12            # one line per event, oldest first
python -m src.lending.deal_ledger --loan-file 12 --json
GET /api/lending/loan-files/12/lifecycle                      # X-Admin-Token
```

```
2026-11-24 10:02  file_created      system:ingest            rules v1
2026-11-24 10:05  stage_transition   human:U0JOSH             rules v2    stage: INTAKE -> FIT; status: received -> held
2026-11-24 10:07  cora_draft         cora                     rules v2
2026-11-24 10:08  human_edit         human:U0JOSH             rules v2    message: Hi Jane -> Hello Jane
2026-11-24 10:09  card_approved      human:U0JOSH             rules v2
```

## What a row holds

| Column | Meaning |
|---|---|
| `event_type` | `file_created`, `stage_transition`, `stage_rejected`, `closed`, `status_change`, `lender_submission`, `quote_generated`, `field_update`, `payment_recorded`, `cora_draft`, `human_edit`, `card_approved`, `card_rejected` |
| `actor` | `cora`, `human:<slack id or name>`, or `system:<source>` (webhook, router, partner_door, standing_send, backfill ...). Never empty; the table refuses anything else. |
| `rule_version` | `lender_rule_snapshots.version_no` in force when it happened (NULL before snapshots existed). A rollback shows up as a new version, so each later event names the rules it ran under. |
| `diff` | `{field: {before, after}}` for the fields that changed; a card edit shows the text before and after. |
| `detail`, `ref_table`, `ref_id` | what and where: source, reason, the `loan_stage_events` / `card_edits` / `lender_submissions` row, the quote id. |

The table refuses UPDATE, DELETE and TRUNCATE (triggers), and a loan file that has events cannot be deleted (foreign key).

## How events get in

* **Same transaction as the change** (if the ledger write fails, the change fails): every `state_machine` path: file
  created, stage moved, move refused (`stage_rejected`), file closed, lender submission, payment, and field edits made
  through `state_machine.update_fields`. Use `update_fields` to change a loan file's fields; it records what changed.
* **Partner flow** (live before the ledger, so a ledger problem must not block a partner notice): the partner door
  creating a file, a status change by the router, and the lender-fit evaluation (`quote_generated`, `kind: lender_fit`)
  record through `record_safely`: a failure is logged and skipped.
* **Quotes**: an instant quote is recorded when it is tied to a file (when a document chase starts with a `quote_id`),
  stamped with the time the quote was made and the rule version then.
* **Cora's cards**: drafts, edits and approvals live in the agent tables, so Cora's minute sweep catches them up
  (`sync_card_events`, last 3 days, safe to repeat). Expect up to a minute of delay. A card is tied to a file by its
  `deal_ref` (`loan_file:<id>`, `doc_chase:<id>`) or else by the borrower's phone (the newest file for that number); a
  card about nobody in the pipeline is not recorded. A card approved by a graduated template shows as
  `system:standing_send`.

## Adding a new way to change a loan file

`tests/lending/test_deal_ledger_rules.py::test_every_path_that_writes_a_loan_file_is_classified` counts the SQL
statements that write `lending.loan_files` in `src/`. A new one fails the test until it records an event (or is listed
there as exempt with a reason). The partner-notice stamps (`notice_*`) are exempt: delivery bookkeeping, not the deal.

## Backfill

The migration adds, once and idempotently, a `file_created` row per existing file and a `status_change` row per existing
`loan_file_status_events` row, actor `system:backfill`, rule version NULL. Older card edits are not imported (they were
never tied to a file).

## Deploy

1. Run `apply_lending_loan_state_machine.py` and `apply_lending_lender_rule_snapshots.py` first, then
   `PYTHONPATH=. python migrations/apply_lending_deal_audit_ledger.py` (idempotent). Run it before the code deploys:
   every loan-file write now records an event.
2. Restart `fa-lending-cora` so the sweep picks up the card catch-up, and `lending-api` for the lifecycle endpoint.
