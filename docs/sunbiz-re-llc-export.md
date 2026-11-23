# Florida real estate LLC daily export

Every morning a CSV of the **active Florida LLCs filed in the last 30 days** (taken from the state's daily filings
file) whose name contains **REI, Homes, Investments or Capital** lands in Google Drive, for the agents' outreach.

## How it works

1. **Source.** The Florida Division of Corporations publishes the filings entered each day as a fixed-width file on its
   public SFTP server: host `sftp.floridados.gov`, folder `doc/cor`, file `yyyymmddc.txt` (the date the filings were
   entered). The login is public and shown on the state's
   [Daily Data](https://dos.fl.gov/sunbiz/other-services/data-downloads/daily-data/) page. Layout:
   [Corporate File Definitions](https://dos.sunbiz.org/data-definitions/cor.html) (1440 characters per record). The
   file appears at about 11:30 PM ET the same night; no file is produced on a day with no filings. Every connection
   checks the server's SSH host key, pinned in `config/sunbiz_re_llc.py` (`SFTP_HOST_KEY`, read Nov 16, 2026); if the
   state changes it, the export fails and alerts, and `SUNBIZ_SFTP_HOST_KEY=<type> <base64>` sets the new key.
2. **Filter** (`src/scrappers/sunbiz/daily_filings.py`, keywords in `config/sunbiz_re_llc.py`): filing type `FLAL`
   (Florida LLC), status `A`, and a keyword as a whole word in the name, case-insensitive.
3. **CSV** (`src/tasks/sunbiz_re_llc_export.py`): one row per LLC: document number, name, matched keyword, filing date,
   principal and mailing address, registered agent, and up to 6 managers / members (title, name, address). The FEI
   (tax) number is never exported. Names are as the state stores them (`LAST FIRST`).
4. **Drive.** `LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID/{YYYY-MM-DD}/sunbiz_re_llcs_{YYYY-MM-DD}.csv`, uploaded through
   the lending Drive service account (`LENDING_DRIVE_SERVICE_ACCOUNT_KEY_PATH`, `src/lending/drive_upload.py`). A
   re-run replaces the file.

Data notes from the real file (Nov 15, 2026: 1357 filings, 22 matched): dates are `MMDDYYYY`; the principal-address
state is blank in every record (the mailing, agent and officer states are filled); most filing dates in one day's file
span the past ~10 days plus some future-effective dates, with a few older entities; "new" means filed within 30 days
of the file's date (below).

## Schedule

`scripts/cron/crontab.txt` (times in UTC): `0 11 * * *` (6 AM EST / 7 AM EDT) exports yesterday's ET date. A failure
exits 1 and `run.sh` retries. It also exports any of the 3 days before that whose CSV is not in Drive yet, so a file
the state posted late goes out the next morning by itself (`CATCH_UP_DAYS`). The `30 13 * * *` `--retry` run shares
the `flock`, so it waits for the main run and its retries; it does nothing if the day's file is already in Drive, else
runs again and, if it still fails, posts one notice to `LENDING_OPS_NOTICE_CHANNEL` in plain words (server key
changed, login rejected, state folder not found, or the error type). A catch-up day that fails posts its own notice
naming the day and is tried again the next morning; it does not fail the run. If the state's file for the day is still
missing at the `--retry` run, it posts a notice too. A missing state folder is reported as an error, never as a day
without filings. Without the SFTP login the main run exits 0 (a retry cannot fix it) and `--retry` alerts. Warnings
and errors reach `logs/cron/sunbiz_re_llc_export.log`.

"New": only filings dated within 30 days of the file's date (`MAX_FILING_AGE_DAYS`; future-effective dates count).
Over 8 real days (Nov 9-16, 2026) the file carried 6 matched LLCs filed earlier than that, and no LLC appeared in two
daily files (0 of 286), so there is no cross-day dedup. Cells starting with `=`, `+`, `-` or `@` are written as text
(prefixed `'`), so nothing runs as a formula when an agent opens the CSV.

## Switching on

1. Drive folder: **`NDL Real Estate LLCs`** at the top level of the NDL Cora shared drive (its own folder, separate
   from FA Exports), shared with the lending Drive service account as Content manager; its id goes in
   `LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID` (the real value is in the server `.env`, not here). To move the export,
   share the new folder with the service account the same way and change only that setting.
2. `.env`:
   ```
   SUNBIZ_RE_LLC_EXPORT_ENABLED=true
   LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID=<Drive folder id>
   SUNBIZ_SFTP_USERNAME=<from the state's Daily Data page>
   SUNBIZ_SFTP_PASSWORD=<from the state's Daily Data page>
   LENDING_DRIVE_SERVICE_ACCOUNT_KEY_PATH=<path to the service account JSON>
   ```
3. `pip install -r requirements.txt` (adds `paramiko`).
4. Check without uploading: `PYTHONPATH=. python -m src.tasks.sunbiz_re_llc_export --day <YYYY-MM-DD> --dry-run`
   (prints filings, matched count).

## Commands

```bash
python -m src.tasks.sunbiz_re_llc_export                       # yesterday (ET)
python -m src.tasks.sunbiz_re_llc_export --day 2026-11-15      # one day (backfill)
python -m src.tasks.sunbiz_re_llc_export --day 2026-11-15 --dry-run
```

## Contact lookup (T3)

The state file has no phone or email, so `src/tasks/sunbiz_llc_trace.py` (cron 12:00 UTC, after the export) asks
Tracerfy for one person per LLC: the first person-type officer, else a person registered agent
(`src/lending/sunbiz_llc_trace.py`). The state writes names `LAST FIRST`, so the name is reordered for the lookup. An
address shared by more than 2 of the day's LLCs is skipped (formation services and agents). Hits are stored in
`lending.sunbiz_llc_contacts` (migration `migrations/apply_lending_sunbiz_llc_contacts.py`, run it before enabling);
emails and phones are never written to the Drive CSV. Each run covers the day and the 3 days before it (newest first),
so a state file posted late is still looked up and a lead the cost cap left over gets another try. Every lookup goes
through the shared Tracerfy ledger under address + ZIP + person, so it never blocks a PropertyRadar lookup at the same
address and a person is never paid for twice (a hit costs 2 cents, a miss nothing; the same person already traced through
PropertyRadar is looked up again here). `SUNBIZ_LLC_TRACE_MAX_COST_CENTS` limits a run, counted as if every lookup hit:
batches are sized to it, so a day over the cap is looked up up to the cap, and `0` means no lookups. Off until
`SUNBIZ_LLC_TRACE_ENABLED=true`.

```bash
python -m src.tasks.sunbiz_llc_trace --dry-run --day 2026-11-15   # counts the lookups and the worst-case cost; spends nothing
python -m src.tasks.sunbiz_llc_trace                              # yesterday (ET), when enabled
```

