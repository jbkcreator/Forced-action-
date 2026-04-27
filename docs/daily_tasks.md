# Stabilize Data Flow — Task Split (Today / Tomorrow)

Branch: `stabilize/data-pipeline` (off `dev`)

---

## TODAY — Complete ✅

1. **Wire ScraperRunStats into 6 blind-spot scrapers** — bankruptcy, evictions, probate, liens, permits, tax_delinquencies. Each now records `run_success=True/False` and `error_type='no_data'` on every code path, independent of `--load-to-db`.

2. **Required-scraper check in load_validator** — Added `REQUIRED_DAILY_SCRAPERS` (10 source_types, weekdays) and `REQUIRED_WEEKLY_SCRAPERS` (tax_delinquencies on Monday). New `_get_missing_scrapers()` diffs against what ran today; fires an immediate alert under `alert_type='missing_scraper'` with cooldown suppression.

3. **Tax delinquency download hardening** — `_locate_download()` now logs all searched directories and seconds elapsed before returning None. Tax engine `__main__` records failure stats on exception.

4. **Fix hardcoded county_id in subscriber_email.py** — `_check_scraper_health()` now takes `county_id` param (default `"hillsborough"`); both the alert log query and insert use it.

5. **Gold+ false-positive monitor** — Added `LeadQualitySnapshot` model + migration (`p7q8r9s0t1u2`). New `src/tasks/lead_quality_monitor.py` snapshots leads sent ~30 days ago, classifies outcome (`active` / `decayed` / `sold` / `resolved`), computes rolling false-positive rate, alerts >20%, sends Monday digest. Wired into crontab at 07:35 UTC.

6. **Tax delinquency staleness fix via admin upload layer** — Cron scraper disabled (county portal blocked). `TaxDelinquencyLoader` UPDATE branch now bumps `date_added = today` on every row touched, so freshness reporting reflects every admin upload (insert OR refresh). Removed `tax_delinquencies` from `REQUIRED_WEEKLY_SCRAPERS` to stop missing-scraper alerts.

7. **Tax delinquent engine log-level fixes** — Elevated AI agent launch / completion / "all records exist" / SNIPER progress messages to WARNING so cron's grep filter captures them. Eliminates the silent-long-run problem.

8. **CSV deduplicator unique-key fix** — `'tax': ['Account Number']` → `['Account Number', 'Tax Yr']` to prevent cross-year dedup collisions.

9. **Flood scraper — NFIP integration** — Added FEMA `FimaNfipClaims` v2 fetcher as Source 3 alongside Disasters and NWS. Captures historical paid flood claims that don't trigger active alerts.

10. **Fire scraper — browser-use refactor** — Added `_download_calls_csv_ai()` using browser-use Agent + new YAML prompt at `config/prompts/fire_prompts.yaml`. AI is now primary download path (resilient to Dojo widget ID changes); Playwright remains as fallback.

11. **Weekly executive one-pager** — New `src/tasks/weekly_one_pager.py` task. HTML email with 6 KPI sections (total Gold+, new Gold+ per vertical, scraper freshness, match rate, county coverage, A/B variant results), week-over-week deltas, and 2-week downtrend detection. Auto-writes `reports/remediation/YYYY-MM-DD.md` with metric-specific suggested actions when anything trends down 2 weeks beyond a 10% floor. Wired into crontab Monday 09:30 UTC. A/B section gracefully degrades on dev until phase-2-b/one merges. `send_alert()` extended with optional `html_body` and `to` overrides.

12. **Alembic chain repair** — Backported 4 missing migrations from `phase-2-b/one` (`p6q7r8s9t0u1` bundle_purchases, `q1r2s3t4u5v6` sms_opt_ins, `r2s3t4u5v6w7` agent_decisions, `s3t4u5v6w7x8` sandbox_outbox) so alembic can traverse the chain on dev. The new `lead_quality_snapshots` migration was applied successfully.

---

13. **SunBiz LLC enrichment — wired into crontab** — Added `record_scraper_stats()` call to `run_sunbiz_pipeline()` (matched=enriched, unmatched=skipped+failed). Added cron line at 07:40 UTC Mon–Fri (after skip tracing, before GHL sync).

14. **Divorce / dissolution-of-marriage scraper** — New `src/scrappers/divorce/divorce_engine.py` downloads the same civil dailyfilings CSV as the eviction scraper, filters for DR case types (`DIVORCE_CASE_PATTERNS`). New `DivorceLoader` in `legal_proceedings.py` matches by petitioner → respondent address → petitioner name with LLM fallback. Stores in `LegalProceeding` with `record_type='Divorce'`. Migration `t4u5v6w7x8y9` extends CHECK constraint. `divorce_filings` added to `VERTICAL_WEIGHTS` (wholesalers=60, fix_flip=55, attorneys=65, restoration=25, roofing=20, public_adjusters=20). Wired into `LOADER_MAP`, `DEDUP_CONFIG`, `DATA_TYPE_TO_SOURCE`, `REQUIRED_DAILY_SCRAPERS`, and crontab at 04:47 UTC.

15. **HOA lien scoring boost** — Increased `hoa_liens` weights for wholesalers (55→68), fix_flip (50→65), attorneys (65→70) in `config/scoring.py` to reflect that unpaid HOA dues = financial distress = motivated seller.

---

## TOMORROW — Pending

5. **Consolidate two county config systems** — `config/constants.py` has a hardcoded `COUNTY_CONFIG` dict (uses `urls`/`court` keys). `config/counties.json` uses `portals`/`court` keys. Fix: extend `counties.json` to add a `urls` section matching what `constants.py` scrapers expect, then replace the hardcoded dict with a JSON loader. No scraper changes needed.

6. **Add Pinellas / Pasco / Polk / Manatee dormant configs** — Add 4 entries to `config/counties.json` with `"status": "dormant"`. All use `flmb` bankruptcy court + `8:` division prefix (FL Middle District).
   - Pinellas FIPS 12103 | Pasco FIPS 12101 | Polk FIPS 12105 | Manatee FIPS 12081

7. **Multi-county cron support** — Add `COUNTY_ID` variable to `scripts/cron/run.sh` (default `hillsborough`); pass `--county-id $COUNTY_ID` to all scraper invocations. Add dormant-county guard at script entry.

8. **Proxy session rotation (stretch)** — Add `OXYLABS_ROTATE=true` env flag to `src/utils/http_helpers.py`. When set, append random session token to username so each Playwright launch gets a fresh IP.
