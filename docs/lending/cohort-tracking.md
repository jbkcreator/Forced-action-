# 90-day cohort and batch tracking (Runbook § 4.6, Item 6)

Each LendingFlow purchase is a batch. For every batch the report shows the leads bought and delivered, the loans
funded within 30, 60 and 90 days of delivery, the fees collected, Revenue Per Lead, and replacement requests
submitted, credited and denied.

Code: `src/lending/cohort_report.py`; settings: `config/lending_cohorts.py`; tables: `lending.lendingflow_batches`,
`lending.lendingflow_replacements` (`migrations/apply_lending_cohorts.py`).

## 1. Setup

1. `PYTHONPATH=. python migrations/apply_lending_cohorts.py` (idempotent; new tables only).
2. **Funded stage.** The "Booked Calls" pipeline has a stage named **Funded** (checked through the GHL API on
   2026-10-19), matched by `FUNDED_STAGE_KEYS` (`config/lending_cohorts.py`). Josh confirms it is the stage he uses.
3. **GHL webhook.** The stage webhook that feeds the scoreboard ("GHL showed feed",
   `docs/lending/dialer-disposition-runbook.md`) must also fire for the funded stage: add that stage to the
   workflow's **Pipeline Stage Changed** trigger, or build a second workflow with the same Webhook action. Add one
   field to the webhook body: `monetary_value` = the opportunity's value (`{{opportunity.monetary_value}}`, check
   the exact name in the GHL merge-field picker).
4. **Fee.** Josh enters the origination fee as the opportunity's value before moving it to the funded stage. Plain
   amounts only (`1500`, `$1,500.00`); anything else is ignored and the loan counts as funded without a fee.

## 2. Recording purchases and replacements

```
python -m src.lending.cohort_report add-batch --name "LF 2026-10 #1" --purchased 2026-10-20 --leads 100
python -m src.lending.cohort_report request-replacement --lead <lendingflow_leads.id>
python -m src.lending.cohort_report decide-replacement --lead <id> --status credited   # or denied
python -m src.lending.cohort_report                                                  # the report
```

- Record each purchase **before** its leads arrive, or at least before the next purchase. Leads fill the batches in
  arrival order, oldest batch first. Leads that arrived before the first purchase date (Eastern) are not counted.
  The report warns when more leads have arrived than the recorded batches hold.
- A replacement can be requested only for a lead that meets the replacement rule (5+ outbound attempts over 5+
  Eastern days, `python -m src.lending.replacement_eligibility`), once per lead. `decide-replacement` records
  LendingFlow's answer.

## 3. How the figures are worked out

- **Funded:** the first time an opportunity on the lead's phone enters a funded stage after the lead arrived. If two
  leads share a phone, the loan goes to the lead delivered most recently before it funded.
- **30 / 60 / 90 days:** counted from the lead's delivery (`lendingflow_leads.received_at`). A window shows
  **open** (None) until the batch is full and its last lead is that old, so a young batch never shows a low number.
- **Fees collected:** the sum of the fees of the batch's funded loans. **Revenue Per Lead** = fees / leads delivered.
  Both are empty (not 0) while no funded loan in the batch has a fee.
- **Replacements:** requests for the batch's leads, and how many were credited or denied.

## 4. Decisions and limits

- The Runbook names `src/services/campaign_pilot_rollup.py`. That file is the FA Max campaign rollup, not lending,
  so the cohort report is a lending module instead.
- The Runbook asks for PostgreSQL views. The report is a query: a view on lending tables blocks the migrations that
  re-apply column types (it happened with `call_dispositions`).
- No funded or fee data exists until steps 1.2 to 1.4 are done. Funded loans are counted from GHL until the funded
  loan ledger (Chunk 2) exists.
- Replacement leads LendingFlow sends are delivered like any other lead and fill the batch in arrival order; the
  data does not mark which leads are replacements.

## 5. Open: the GHL webhook is rejected (422)

The workflow "NDL – Stage Funded → cohort report" (GHL Custom Webhook, key/value body) reaches
`/webhooks/lending/ghl-stage` and passes the secret, but gets **422** (lending-api log, 2026-10-19 13:16–13:24 UTC):
`opportunity_id` / `stage_name` are not found. The body is valid JSON (an earlier wrong secret gave 401, which is
checked after the body). Most likely GHL nests the key/value fields under `customData`, while
`src/lending/ghl_consent_events.field()` reads only the top level and `contact`. **Not confirmed and not fixed.**
The fix is to also read `customData` in `field()`, which the opt-out and consent webhooks share. It needs a deploy
of lending-api, then the funded test is re-run. Until then no funded loan reaches the report.

## 6. Waiting on Josh

- [ ] The name of the funded stage in his pipeline.
- [ ] Whether he records the origination fee as the opportunity value.
- [ ] His purchases so far: name, date, number of leads.
