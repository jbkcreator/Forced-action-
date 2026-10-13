# Lender rules as data (T-05)

Josh's A1 answer: a lender field edit or a "confirmed" mark is a data edit, not a deploy.

## How it works
- `config/lender_matrix.py` still defines the lenders, the field names and the defaults.
- `lending.lender_rule_values` holds one row per lender key and field: `value` (JSON), `status`
  (`confirmed` / `placeholder` / `confirm_at_submission`), `note`, `updated_by`, `updated_at`.
- With `LENDING_RULES_FROM_DB=true`, `evaluate_lender_fit`, the lead card and the soft approval read the
  table; each row overrides that field for that lender. Anything not in the table keeps its config value.
  With the flag off (default), or if the table is missing or unreadable, the config alone is used.
- A lender counts as confirmed only when all its fields are confirmed. The fit score stays hidden until
  all five lenders are confirmed.

## Set up (once)
1. Check the target DB (print host and database first).
2. `PYTHONPATH=. python migrations/apply_lending_lender_rule_values.py` (creates the table and seeds one row
   per lender and field from the config; re-running never overwrites an edit).
3. Set `LENDING_RULES_FROM_DB=true` and restart `lending-api`.

## Edit a value or mark it confirmed
```sql
UPDATE lending.lender_rule_values
SET value = '0.80'::jsonb, status = 'confirmed', updated_by = 'josh', updated_at = now()
WHERE lender_key = 'rcn_capital' AND field = 'max_ltc';
```
Value formats: numbers as `700` or `0.85` (percentages 0 to 1); lists as `["FL","GA"]`; loan types as
`["FIX_AND_FLIP","BRIDGE"]`; booleans `true`/`false`; no limit as `null` (with status `confirmed`).
Fields: `loan_types, min_loan_amount, max_loan_amount, max_ltc, max_ltv, min_purchase_price, credit_floor,
min_completed_projects, requires_ground_up_build, approved_states, allowed_property_types,
origination_points, rate_spread, hold_months`. Lender keys: `generic_backflip_flip,
generic_backflip_construction, rcn_capital, easy_street, kiavi_affiliate, abl`.

A row that fails validation is skipped and logged at ERROR (`[lender-rules] row skipped`), and that field
keeps its config value. Watch the log after an edit.

## Not covered
- A new lender or a new field needs a code change; the table only edits what the config defines.
- The purchase and rehab shares used by the soft approval PDF are still in `config/lending_soft_approval.py`.
- Overlap with R-1's `lender_routing_rules` / `lender_product_boxes` (branch `feat/ndl-r1-overflow-router`,
  not merged): this table is separate. Reconcile when R-1 merges.
