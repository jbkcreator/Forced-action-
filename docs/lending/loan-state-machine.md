# Loan state machine (Release 1, item 0.1)

`lending.loan_files.stage` is the source of truth for where a deal is. Code: `src/lending/state_machine.py`;
contracts: `src/lending/contracts.py` (R1 0.1 section); endpoint: `src/api/lending_loan_stage_router.py`;
migration: `migrations/apply_lending_loan_state_machine.py`.

## Stages

`INTAKE -> FIT -> SUBMITTED -> ACCEPTED -> TERM_SHEET_ISSUED -> CLOSING -> FUNDED -> PAID`

A file moves one step forward at a time. Any other move (a skip such as INTAKE -> FUNDED, a step back, any move on a
closed file) raises `IllegalStageTransitionError`; the API answers HTTP 400.

`loan_files.status` (received / held / routed / declined / approved / funded) is derived from the stage
(`STATUS_FOR_STAGE`), so partner notices and the ledger export read it as before.

| Stage | status | | Stage | status |
|---|---|---|---|---|
| INTAKE | received | | CLOSING | approved |
| FIT | held | | FUNDED | funded |
| SUBMITTED | routed | | PAID | funded |
| ACCEPTED | approved | | closed (any stage) | declined |
| TERM_SHEET_ISSUED | approved | | | |

## Dead deals

A dead deal is not a stage. `close_file()` sets `closed_reason` (a `DeclineReason` code) and `closed_at`; the stage stays
where the deal died and every later move is refused (`file_closed`). Reopening is not supported.

## Replays and duplicates

Every event has a key `(source, source_event_id)`, stored once in `lending.loan_stage_events` (unique index).
A replay finds its row: nothing changes, no status event, no balance recalculation. A move to the stage the file is
already at is a no-op. An event with no id (the manual API call without `source_event_id`) relies on the no-op rule.

`outcome` on each event row: `applied`, `noop`, or `rejected`.

## Rejected events

A rejected event is stored (`outcome = rejected`, with the reason), the stage does not move, and a card is queued in
Cora's inbox (`loan_stage_rejected`) so Josh can step the file through the missing stages. Callers must commit in their
`except IllegalStageTransitionError` branch to keep the rejected row. A webhook answers 200 to its sender so it does not
retry forever; the API answers 400.

List them: `SELECT * FROM lending.loan_stage_events WHERE outcome = 'rejected' ORDER BY created_at DESC;`

## Unknown inputs

Rehab budget, purchase price, loan amount, payoff balance and ARV are NULL when unknown, never 0
(`state_machine.amount_or_none`; the contracts reject a blank or text amount). `loan_amount` and `property_state`
on `loan_files` are now nullable.

## Payments and balance

`lending.loan_payments`, unique on `(source, payment_id)`. A duplicate inserts nothing and recalculates nothing.
`outstanding_balance = payoff_balance - sum(payments)`, NULL while the payoff balance is unknown. A FUNDED file moves to
PAID when the balance reaches 0, or when the payment is flagged `mark_paid` (v1: Josh moves the deal to "Paid" in GHL;
that event has no amount).

## Borrowers, properties, files

`ingest_loan_file()`: one `borrower_profiles` row per normalized phone (then email), one `properties` row per
normalized address, one `loan_files` row per (borrower, property). Re-ingesting the same pair returns the existing
file. `record_lender_submission()` always adds a row (attempt 1, 2, 3 ... per lender).
The existing `BorrowerProfile` contract (lender-fit call facts) is unchanged; the persisted identity is `BorrowerRecord`.

`ConsentRecord` maps to `lending.text_consents` and `DocumentStipulation` to `lending.doc_chase_items`
(`PartnerSubmission` to `lending.partner_submissions`); there is one consent store.

## Move a file by hand

```
curl -X POST https://<lending-host>/api/lending/loan-files/<id>/stage \
  -H "X-Admin-Token: $LENDING_ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"to_stage": "FIT", "actor": "slack:U123", "reason": "fit confirmed on call"}'
```

Answers: 200 `{loan_file_id, from_stage, to_stage, outcome}`; 400 not allowed; 401 bad token; 404 no such file;
422 unknown stage. `LENDING_ADMIN_TOKEN` unset refuses every caller.

## Deploy

1. `PYTHONPATH=. python migrations/apply_lending_loan_state_machine.py` (idempotent, 5 s lock timeout). Run it before
   the code. It locks `loan_files` briefly; run it off-hours. Existing files get their stage from their status once.
2. Set `LENDING_ADMIN_TOKEN` in the lending-api environment and restart `lending-api`.

## Check

```
SELECT stage, count(*) FROM lending.loan_files GROUP BY 1;                 -- backfill
SELECT count(*) FROM lending.loan_files WHERE borrower_id IS NULL;         -- files from before this item
```
