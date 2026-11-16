# Florida real estate LLC daily export (P3.2)

Every morning a CSV of the **Florida LLCs registered the day before** whose name contains **REI, Homes, Investments or
Capital** lands in Google Drive, for the agents' outreach.

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
state is blank in every record (the mailing, agent and officer states are filled); filing dates in one day's file span
the past ~10 days plus some future-effective dates, so "new" means "in that day's file".

## Schedule

`scripts/cron/crontab.txt`: `0 11 * * *` exports yesterday (ET). A failure exits 1 and `run.sh` retries. The
`30 13 * * *` `--retry` run does nothing if the day's file is already in Drive, else runs again and, if it still
fails, posts one notice to `LENDING_OPS_NOTICE_CHANNEL`. If the state's file for the day is still missing at the
`--retry` run (no filings that day, or posted late), it posts a notice too; once the file appears, export it with
`--day`. Warnings and errors reach `logs/cron/sunbiz_re_llc_export.log`.

## Switching on

1. Drive folder: **`NDL Real Estate LLCs`** at the top level of the NDL Cora shared drive (its own folder, separate
   from FA Exports), id `1hQroR2KdfqzEsjVOgkLtaNvDlpc8GIJB`, shared with the lending service account
   (`fa-lending-engine@forcedaction.iam.gserviceaccount.com`) as Content manager. To move the export, share the new
   folder with the service account the same way and change only `LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID`.
2. `.env`:
   ```
   SUNBIZ_RE_LLC_EXPORT_ENABLED=true
   LENDING_SUNBIZ_LLC_EXPORT_FOLDER_ID=1hQroR2KdfqzEsjVOgkLtaNvDlpc8GIJB
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
