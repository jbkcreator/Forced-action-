# Nightly ledger export (P1.2)

`python -m src.tasks.lending_ledger_export` writes the agents' outcome feed to Drive `NDL Ledger Exports/{ET day}/`
(cron 05:00 UTC, retry 05:55 UTC; code in `src/lending/ledger_export.py`). Off until `LENDING_LEDGER_EXPORT_ENABLED=true`
and `LENDING_LEDGER_EXPORT_FOLDER_ID` is set. Read-only on the database.

| File | Rows |
| --- | --- |
| `contacts.csv` | Snapshot of `lending.contacts` plus first-contact rank and time (`lending.first_contact_snapshots`) |
| `consents.csv` | Text consents, suppressions, **website-form boxes ticked** (`web_form_consent`) and **email-only opt-outs** (`email_opt_out`; matched to a website lead's contact by email, else no contact columns) |
| `touches.csv` | The exported ET day: calls, missed-call texts, Cora sends (SMS, email, GHL actions), **booking reminders** (`booking_text` / `booking_email`) and the **LendingFlow intro text** (`intro_text`) |
| `dispositions.csv` | The exported day's calls |
| `outcomes.csv` | The exported day's GHL stage entries, LendingFlow routing events, **every partner-file status change** (`partner_file`, one row per change from `lending.loan_file_status_events`, so an approval and a funding on the same day are both exported) and **partner routing events** (`partner_routing`) |
| `scores.csv` | Borrower score per LendingFlow / website lead, with missing inputs |
| `cohorts.csv` | One row per LendingFlow batch (`cohort_report.cohorts`): bought, delivered, funded at 30/60/90 days (blank while a window is open), fees, replacements. No contact columns |
| `audiences.csv` | The retargeting audiences as counts only (team decision Q33): one row per platform (google, meta) with the rows in that night's file and the contacts per source. The hashed audience files never go to Drive; they stay on the server |

Never written: a full phone number or email, or a phone hash. Rows are keyed by `ghl_contact_id`, `contact_id` and the
last 4 digits. Test rows are left out: an `is_test` row, a row whose phone belongs to a test contact or lead, and a
phoneless row (an email-only send or opt-out) whose GHL id belongs to a test lead.

Not in the export: the retargeting audience files (`lending_audience_export`) hold hashed phones and emails and stay off
Drive until Josh picks an option (P1.2 question 1). GHL-workflow texts that no code sends (for example workflow reminders)
are not logged anywhere in our database, so they cannot be exported.

Setup: set the two settings, share the folder with the service account and with Josh and the agents' account, then check
`manifest.json` in the day's folder (row counts per file). Test: `tests/lending/test_ledger_export_db.py` runs every query
on Postgres.

Partner-file history: `partners.record_status_change` (the one writer of a file's status) and `save_submission` add a row to
`lending.loan_file_status_events` in the same transaction as the change; a failed write is logged and never undoes the change.
Run `migrations/apply_lending_loan_file_status_events.py` before deploying (it also gives each existing partner file one starting
row). Files that changed before the table existed show one `partner_file` row (their status then), not their earlier history.
