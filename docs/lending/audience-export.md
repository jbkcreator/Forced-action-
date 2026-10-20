# Retargeting audience export (Runbook § 4.7, Item 7)

A nightly job builds hashed contact files for **Google Customer Match** and **Meta Custom Audiences** from the
lending leads, and stages them on the server for later ad activation. **Nothing is uploaded** to Google or Meta.

Code: `src/tasks/lending_audience_export.py`; settings: `config/lending_audience_export.py`, and
`LENDING_AUDIENCE_EXPORT_ENABLED` / `LENDING_AUDIENCE_EXPORT_DIR` in `config/settings.py`; cron 08:15 UTC
(`scripts/cron/crontab.txt`).

## 1. Who is in the files

- LendingFlow leads (`lending.lendingflow_leads`) and website form submissions (`lending.web_leads`) that are **not
  suppressed** (decision 2026-10-19: non-suppressed only, as the Runbook says).
- Left out: a lead marked `suppressed`, and anyone whose phone **or** email is on `lending.suppression_list` or on a
  `lending.contacts` row with `do_not_contact`. A match on either one removes the whole contact.
- **Past borrowers are not included**: no lending table holds them yet.

## 2. What the files hold

- `google_customer_match_<YYYY-MM-DD>.csv`: columns `Email`, `Phone`.
- `meta_custom_audience_<YYYY-MM-DD>.csv`: columns `email`, `phone`.
- **SHA-256 hashes only, never raw values.** Normalisation before hashing:
  - email: trimmed, lower-cased; Google file only: periods before `@gmail.com` / `@googlemail.com` removed
  - phone: E.164 with `+` for Google (`+18135550100`), digits without `+` for Meta (`18135550100`)
- Each email and each phone appears once per file. Files are written owner-only (0600); the last 7 nights are kept.
- Logs carry counts only.

## 3. Switching it on

1. Josh (or his compliance adviser) confirms ad audiences may use these leads, and the Privacy Policy covers it.
2. Pick the folder on the server; set `LENDING_AUDIENCE_EXPORT_DIR` and `LENDING_AUDIENCE_EXPORT_ENABLED=true`.
3. Check once by hand: `python -m src.tasks.lending_audience_export --dry-run` (counts, no files), then without
   `--dry-run`.
4. Before the first upload, check the headers against each platform's current upload template.

## 4. Evidence (2026-10-19)

- Tests: `tests/lending/test_lending_audience_export.py`, 12 passed (hashing, de-duplication, suppressed leads left
  out, hashes-only files, dry run writes nothing, last nights kept, off by default).
- Dry run on the live data: 14 eligible contacts (5 LendingFlow, 9 website), 12 rows per file after de-duplication.
- Real run on the live data (read-only connection, files to a local test folder, deleted after the check): both files
  written with the template headers, 12 rows each (10 emails, 9 phones), every cell a 64-character SHA-256 hex value,
  no "@" or "+" anywhere in either file, no repeated email.

## 5. Open

- Consent for ad audiences and the Privacy Policy wording (Josh / compliance).
- The server folder.
- A source for past borrowers.
