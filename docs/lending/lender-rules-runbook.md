# Lender rules as data (T-05)

Josh's A1 answer: a lender field edit or a "confirmed" mark is a data edit, not a deploy.

## How it works
- `config/lender_matrix.py` still defines the lenders, the field names and the defaults.
- `lending.lender_rule_values` holds one row per lender key and field: `value` (JSON), `status`
  (`confirmed` / `placeholder` / `confirm_at_submission`), `note`, `updated_by`, `updated_at`, and
  `effective_from` (L1: the ET date the row takes effect; a row dated later is not applied until that day,
  and the field keeps its config value meanwhile). Seed rows take the date in `RULES_EFFECTIVE_FROM`.
- With `LENDING_RULES_FROM_DB=true`, `evaluate_lender_fit`, the lead card and the soft approval read the
  table; each row overrides that field for that lender. Anything not in the table keeps its config value.
  With the flag off (default), or if the table is missing or unreadable, the config alone is used.
- A lender counts as confirmed only when all its fields are confirmed. The fit score stays hidden until
  all six lenders are confirmed.
- **L1: nothing routes to an unverified lender.** R-1 never routes to a lender while any of its fields is
  `placeholder`, even when it is signed (`confirm_at_submission` does not block). Backflip and Central Lending
  are the real lenders; RCN, Easy Street, Kiavi and ABL are kept as unverified placeholders (dated 2026-11-03).
  Central Lending (`central_lending`) is a shell until the client's credit box arrives.

## Set up (once)
1. Check the target DB (print host and database first).
2. `PYTHONPATH=. python migrations/apply_lending_lender_rule_values.py` (creates the table and seeds one row
   per lender and field from the config; re-running never overwrites an edit). Re-run it for L1: it adds
   `effective_from` (existing rows take their `updated_at` date) and seeds `central_lending`. Then re-run
   `migrations/apply_lending_lender_routing_seed.py` to add Central Lending to routing as `pending`.
3. Set `LENDING_RULES_FROM_DB=true` and restart `lending-api`.

## Edit a value or mark it confirmed
```sql
UPDATE lending.lender_rule_values
SET value = '0.80'::jsonb, status = 'confirmed', updated_by = 'josh', updated_at = now(),
    effective_from = (now() AT TIME ZONE 'America/New_York')::date
WHERE lender_key = 'rcn_capital' AND field = 'max_ltc';
```
Value formats: numbers as `700` or `0.85` (percentages 0 to 1); lists as `["FL","GA"]`; loan types as
`["FIX_AND_FLIP","BRIDGE"]`; booleans `true`/`false`; no limit as `null` (with status `confirmed`).
Fields: `loan_types, min_loan_amount, max_loan_amount, max_ltc, max_ltv, min_purchase_price, credit_floor,
min_completed_projects, requires_ground_up_build, approved_states, allowed_property_types,
origination_points, rate_spread, hold_months`. Lender keys: `generic_backflip_flip,
generic_backflip_construction, rcn_capital, easy_street, kiavi_affiliate, abl, central_lending`.

Until Josh sends the real rules (client list Nov 2, Q1), every lender, Backflip included, runs on the safe
placeholder set: credit 680, LTC 85%, ARV 70%, FL/GA/TX, $75K-$2M. A database seeded before that change
still holds Backflip's Oct 1 values; `PYTHONPATH=. python migrations/apply_lending_lender_rules_safe_set.py`
resets the Backflip rows still owned by the seed and leaves any hand edit alone (logged).

A row that fails validation is skipped and logged at ERROR (`[lender-rules] row skipped`), and that field
keeps its config value. Watch the log after an edit.

## Not covered
- A new lender or a new field needs a code change; the table only edits what the config defines.
- The purchase and rehab shares used by the soft approval PDF are still in `config/lending_soft_approval.py`.
- **R-1 routing uses these same values** (no second copy of the numbers). Each row of
  `lending.lender_routing_rules` can name `rule_keys` (the lender keys above); the router then judges a file
  against those keys' values, read the same way as the fit card (the table when `LENDING_RULES_FROM_DB=true`,
  else the config). So a rule edit or a "confirmed" mark changes the fit score and where a file is routed.
  `lending.lender_routing_rules` holds only what is about routing: waterfall order, agreement status, seat cap,
  primary flag. A lender T-05 does not define (a new one Josh adds) has no `rule_keys` and uses its own
  `lender_product_boxes`. A lender with neither is never routed to (the file goes to the hold queue).
  The fit card still lists only the T-05 lenders; a new routing-only lender does not change its score.

## Versions and 1-click rollback (R1 0.7)

Every change to `lending.lender_rule_values` writes the whole rule set as a new row in
`lending.lender_rule_snapshots` (`version_no`, `rules`, `created_by`, `reason`, `created_at`). A trigger does it, so
the SQL edit above is versioned too, and another trigger refuses any UPDATE or DELETE on the snapshots: history is never
rewritten. An edit that changes nothing writes nothing. The newest `version_no` is the rule version other records name
(the deal audit ledger).

Say who and why in the same transaction as a hand edit; without it the snapshot records the database user and
"table edit":

```sql
BEGIN;
SELECT set_config('lending.rule_actor', 'josh', true), set_config('lending.rule_reason', 'Central LTV confirmed on call', true);
UPDATE lending.lender_rule_values SET value = '0.80'::jsonb, status = 'confirmed' WHERE lender_key = 'central_lending' AND field = 'max_ltv';
COMMIT;
```

Code that edits a rule uses `lender_rules_store.set_rule_value(...)`, which refuses a hard-invalid value before anything
is written: wrong type, unknown lender or field, `max_ltv` / `max_ltc` outside 0 to 1, a negative amount, a minimum above
the maximum. A value that passes but is wrong for the business (Central max LTV 0.90 typed as 0.99) is accepted and goes
live; that is the case rollback is for. A hand-written SQL value is not checked on the way in; the loader skips a bad row
and logs it, as before.

### Roll back

```
python scripts/rollback_lender_rules.py --list              # versions, newest first
python scripts/rollback_lender_rules.py --show 14           # what rolling back to v14 would change
python scripts/rollback_lender_rules.py --to 14 --yes       # restore v14
```

A rollback copies the old rules back and is itself a new version ("rollback to v14"), so rolling forward again is one more
rollback. It posts to #cora (who, from which version, to which) and does nothing when the rules already match.
The same operations over HTTP, with `X-Admin-Token`: `GET /api/lending/lender-rules/versions`,
`POST /api/lending/lender-rules/rollback` with `{"version": 14, "actor": "josh"}`.

## Database backup (R1 0.7)

`scripts/pg_backup.py` (cron 03:20 UTC) writes `/var/backups/postgres/<database>_<UTC timestamp>.dump` (pg_dump custom
format, seven days kept, the newest never deleted) and a `.counts.json` beside it with the row counts of the audit tables.
Before this item the server had no database backup of its own; the provider's disk backups are the off-server copy.

`scripts/verify_backup_restore.py --scratch-url postgresql://USER:PASSWORD@HOST/restore_check_test` restores the newest
dump into a scratch database (its name must contain `test` and it must not be the app database) and checks that the dump
is under 26 hours old, that each audit table holds at least the rows counted at dump time and no more than production
holds now, and that the restored `lender_rule_snapshots` still refuses UPDATE and DELETE. It exits non-zero on any
problem. The weekly cron line for it is commented out in `scripts/cron/crontab.txt` until a scratch database exists.

## Staging mirror (R1 0.7)

`config/staging.env.example` lists the settings that make a staging host a mirror of production that cannot reach a
real borrower, lender or the production database. To copy production's rules to staging:

```
# on production
python scripts/rollback_lender_rules.py --export 14 > rules.json
# on staging
python scripts/rollback_lender_rules.py --import rules.json --yes
```

The import replaces the staging table and is recorded as one version ("imported from rules.json"). Staging has no
host yet (client approval pending, issue "Staging environment").

## Deploy (R1 0.7)

1. `PYTHONPATH=. python migrations/apply_lending_lender_rule_snapshots.py` (idempotent; adds a table and two triggers,
   writes version 1 from the table as it is). Run it before deploying the code. Needs `LENDING_ADMIN_TOKEN` for the
   endpoints (see the loan state machine runbook).
2. `crontab` picks up the nightly backup from `scripts/cron/crontab.txt` (`bash scripts/cron/install_cron.sh`).
