# Nightly PropertyRadar pipeline (Florida + Georgia)

Every night: pull the newly updated PropertyRadar records for three signals, skip trace them, check them against
do-not-call lists, load the clean numbers into BatchDialer in the morning and push them to GHL, link them to
Forced Action properties, and write a daily export to Drive "FA Exports".

| Signal | List key | What it searches (per state) | BatchDialer campaign | GHL tags |
|---|---|---|---|---|
| Loans coming due | `verified_maturity` | Investor-owned, not listed, first loan private or from a named hard-money lender, estimated maturity about 165 days out (each assumed 1/2/3-year term searches its own recording dates) | Verified maturity | `ndl`, `ndl-sig-maturity` |
| Cash purchases | `cash_buyer` | All-cash purchase by an investor owning 2–99 properties | Transaction ready | `ndl`, `ndl-sig-cash-purchase` |
| Investor-owned portfolios | `investor_purchase` | Any purchase by an investor owning 2–99 properties (cash purchases are left to the cash list) | Transaction ready | `ndl`, `ndl-sig-portfolio` |

"Investor" is not owner-occupied in Florida and company-owned in Georgia (Georgia's owner-occupied flag is
unreliable, and only companies may be cold-called there). Each night searches the last 30 days (PropertyRadar shows
a recording days or weeks after it happens; listing is free and only records not already stored are bought).

GHL contacts also get the existing tags: the call-list tag, `ndl-cold` and `ndl-state-<xx>`.

## Schedule (UTC, `scripts/cron/crontab.txt`)

| Time | Job | What it does |
|---|---|---|
| 06:30 | `lending_pr_nightly --stage pull --apply` | Free listing, buys only new records within the night's export cap, then links new records to FA properties |
| 08:30 | `--stage trace --apply` | Resumes and replays any paid Tracerfy queue first (free), then traces new people within the night's trace credits |
| 10:30 | `--stage dnc --apply` | Do-not-call checks traced phones never checked, within the night's DNC credits |
| 13:05 and 14:05 | `--stage load --apply` | Acts only during 09:xx ET: loads every DNC-clean person not already in BatchDialer |
| every 5 min | `src.lending.ghl_load_sync` | Pushes each new BatchDialer load to GHL (needs `LENDING_GHL_LOAD_SYNC_ENABLED=true`) |
| 14:45, 15:45 | `lending_pr_daily_export` (+ `--retry`) | Drive "FA Exports/{ET day}/": `maturing_loans.csv`, `cash_purchases.csv`, `investor_purchases.csv`, then `manifest.json` |

The old `property_radar_maturity_pull`, `property_radar_runner` and `lending_pr_maturity_bridge` cron lines stay
disabled: they cover the same named-lender maturities through `property_radar_records` and would buy and dial them
a second time.

## Spend safety

- **Every cap is per ET night** (`LENDING_PR_NIGHTLY_*`; 0 means that stage spends nothing) and counts what the night
  already spent: exports from `lending.pr_list_pulls` (`run_kind = 'nightly'`), Tracerfy rows from the night's
  landing files (`data/property_radar_lists/<list>/tracerfy/nightly-<date>/`, written before each queue is polled;
  every row sent counts as a paid hit). A run that fails and is retried by run.sh never spends a cap twice.
- **Paid PropertyRadar exports**: none unless `LENDING_PR_NIGHTLY_MAX_PAID_EXPORTS` allows them; the free balance is
  read before every page.
- **Nothing is bought twice**: free RadarID listing, records already stored or landed on disk are skipped, every
  bought page is on disk before the database load.
- **One run at a time**: the calling-list pull lock (shared with the manual commands; a lock left by a crashed run is
  taken over), plus `flock` in cron.
- **The live pull refuses** unless `PROPERTY_RADAR_MODE=live`.
- **Compliance**: the BatchDialer load runs the dialer load's own gates (suppression, Georgia company-only, homestead,
  attempt limits, DNC/litigator, calling hours, emergency halt). It loads only phones whose fresh scrub is clean, so it
  buys no scrub of its own.

## Turning it on

1. Run `PYTHONPATH=. python migrations/apply_lending_pr_list_nightly.py` (after `apply_lending_pr_calling_lists.py`
   and `apply_lending_pr_list_trace_subjects.py`).
2. Dry run (free): `python -m src.tasks.lending_pr_nightly --stage pull`. It prints, per list and state, how many
   records match and how many would be bought tonight.
3. Set the caps Josh approved, `PROPERTY_RADAR_ENABLED=true`, `PROPERTY_RADAR_MODE=live`, `PROPERTY_RADAR_API_KEY`,
   `TRACERFY_API_KEY`, `BATCHDIALER_API_KEY`, then `LENDING_PR_NIGHTLY_ENABLED=true`.
4. `LENDING_GHL_LOAD_SYNC_ENABLED=true` for the GHL push; `LENDING_PR_EXPORT_ENABLED=true` and
   `LENDING_PR_EXPORT_FOLDER_ID` for the Drive export.
5. The first live night: start with small caps (for example 25 exports, 25 trace and 25 DNC credits) and check the
   summary posted to the ops channel after each stage.

The one-time backlog (maturities 90–180 days out, and older purchases) stays a manual run with the operator's
approval: `python -m src.tasks.lending_pr_calling_list_pull --list verified_maturity --state GA` (dry run), then
`--live --confirm-exports N`.

## Linking to Forced Action properties

Each pull links new records in every county whose `counties` row has a FIPS code and parcels in `properties`
(`src/lending/pr_list_linking.py`): parcel, then address at the county's auto-match threshold; never owner name. The
pull summary lists the linkable FIPS codes. Records in other counties stay unlinked and are linked the night their
county's parcels are first loaded. Georgia has no Forced Action parcels yet, so no Georgia record links.
