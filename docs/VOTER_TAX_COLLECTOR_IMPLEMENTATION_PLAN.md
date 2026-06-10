# Voter Registry + Tax Collector Loaders — Implementation Plan

**Task 4: Supervisor of Elections & Tax Collector bulk loaders.**
Design authority: `FA/BULK_REGISTRY_LOADERS_GRILL.md` (decisions Q1–Q11),
ADR 0013 (contact isolation), ADR 0014 (absentee middle path).

Goal: lift the contactable-profile baseline via two free county sources —
voter registry (alt names/addresses/phones/emails per household) and tax-collector
billing addresses (absentee flags + direct-mail fallback addresses).

---

## Phase 0 — Preflight (verify before touching anything)

| Check | How |
|---|---|
| Alembic heads snapshot | `alembic heads` — record IDs; new migrations target by revision ID |
| `check_enriched_source` constraint exact name/def | query `pg_constraint` on `enriched_contacts` |
| Existing upload endpoint behavior | read `src/api/admin_router.py:133` (tax-delinquency) end-to-end |
| Tax loader column synonyms | read `src/loaders/tax.py` header maps |
| `counties` rows + `county_sources` for Hillsborough/Pinellas | SELECT; note county ids |
| Sample files on hand | Hills voters xlsx/txt, Hills "Unpaid R/E Accounts", Pinellas "Delinq Taxes-Certs Unpaid" |

**Exit:** facts confirmed, no code changed.

---

## Phase 1 — Database schema (Alembic migrations, applied via script)

1. **`voters` table** (new 1:Many spoke off `properties`):
   - `id` PK, `property_id` FK → properties (NOT NULL; unmatched rows go to quarantine, never inserted),
     `county_id` FK → counties,
   - `source_voter_id` (county VoterID), `voter_name`, `first_name`, `middle_name`, `last_name`,
   - `residential_address`, `residential_city`, `residential_zip`,
   - `mailing_address` (full normalized single string; NULL when same as residence),
   - `registration_status` (`ACT`/`INA`), `registration_date`,
   - `phones` JSONB (list, history accumulates across monthly loads), `phone_1` (normalized current),
   - `email`,
   - `meta_data` JSONB, `created_at`, `updated_at`.
   - Indexes: `property_id`, `county_id`, UNIQUE (`county_id`, `source_voter_id`).
2. **Alter `check_enriched_source`** on `enriched_contacts`: allow `'tax_collector'`
   (existing set: batch_skip_tracing, idi, pdl).
3. **`owners.direct_mail_eligible`** BOOLEAN NOT NULL DEFAULT false.
4. ORM models added to `src/core/models.py` (`Voter`; `Owner`/`EnrichedContact` updated).
5. Migration files written under `alembic/versions/`, **applied with a python script**
   (multi-head flow — never `upgrade head`).

**Exit:** tables/constraints live in DB; `python -c "import src.core.models"` clean.

---

## Phase 2 — VoterRegistryLoader

1. `SIGNAL_SCHEMAS["voter_registry"]` added to `src/loaders/column_mapper.py`
   (canonical fields: source_voter_id, voter_name, first/middle/last, residential address
   parts, mailing address parts, registration_status, registration_date, phone, email).
2. `src/loaders/voter_registry.py` — `VoterRegistryLoader(BaseLoader)`:
   - Input: DataFrame (post-ColumnMapper). Parser helpers accept:
     **(a)** Hillsborough SOE quoted-CSV `.txt`/`.csv` (header present),
     **(b)** FL DOS statewide extract (tab-delimited, **no header** → inject the 38-field
     header from the official layout).
   - Phones → `src/services/phone_utils.normalize`; multi-line mailing → single normalized string;
     skip rows with exemption flag where fields are blanked.
   - Property match: address cascade via `BaseLoader.find_property_by_address`
     (house# prefix → pg_trgm → rapidfuzz). Matched → insert/upsert; unmatched →
     `quarantine_unmatched` (`UnmatchedRecord`).
   - **Upsert key (`county_id`, `source_voter_id`)**: update status/addresses; **append**
     newly-seen phone to `phones` JSONB (history), set `phone_1` to current.
   - Batch inserts (`insert().values([...])`, chunks of ~1k); stream the DataFrame.
   - **NO rescore trigger** (contact-only — skip `get_affected_property_ids`).
3. Unit tests: `tests/test_voter_registry_loader.py` — fixtures from real-file rows
   (both formats), match/quarantine/upsert/phone-history cases.

**Exit:** loading a 100-row fixture produces voters + quarantined rows, idempotent re-run.

---

## Phase 3 — Voter upload endpoint + admin UI card

1. `POST /api/admin/upload/voter-registry` in `src/api/admin_router.py`
   (mirror tax-delinquency endpoint): admin JWT, multipart file (`.csv`/`.txt`/`.zip` —
   zip → extract inner `.txt`), `county_id` form field; ColumnMapper → approve-flow if new
   headers; runs loader; returns counts (loaded/updated/quarantined).
2. **Separate card** (Decision Q6=B): `Forced-action-ui/src/components/admin/VoterUploadCard.jsx`
   + API client fn; existing tax card untouched; Col Mappings dashboard reused unchanged.
3. Error shape `{"detail": ...}`; 400 bad file, 422 unmapped columns, 409 duplicate batch.

**Exit:** end-to-end manual upload of Hillsborough sample through the portal works.

---

## Phase 4 — Tax loader learns Hillsborough "Unpaid R/E Accounts" format

1. Extend `src/loaders/tax.py` synonym map: `Billing Address` → `owner_address`,
   `Billing Address Name` → meta, `Balance Amount`/`Total Tax` → amounts, `Use Code` → meta.
2. **Roll-year split (ADR 0014):** rows with `Tax Yr >= current roll year` are **excluded**
   from `tax_delinquencies` inserts (installment noise) but **retained** for Phase-5 enrichment.
3. Dedup remains on `source_account_number` + tax_year + cert.
4. Fixture tests from the real file (incl. a 2026 installment row asserting exclusion).

**Exit:** uploading "Unpaid R/E Accounts" creates ≤2025 delinquency rows only; counts logged.

---

## Phase 5 — Tax-collector enrichment step (absentee + alt mailing)

1. `src/services/tax_collector_enrichment.py`:
   - Input: the upload batch (all rows incl. current-roll) joined to matched properties.
   - Normalize billing vs `owners.mailing_address` (shared address normalizer from BaseLoader).
   - **Differs or owner mailing NULL →** upsert `enriched_contacts`
     (`source='tax_collector'`, `mailing_address`, confidence 0.9, meta: billing name, tax yr).
   - **Absentee middle path:** classify billing addr — state ≠ FL → `Out-of-State`;
     FL but outside county (zip-set of county's properties) → `Out-of-County`; else In-County.
     Write `owners.absentee_status` only if NULL **or** normalized mailing differs from
     appraiser value. Never downgrade to NULL.
   - Collect changed `property_id`s → **trigger CDS rescore** (reuse loader rescore path).
   - Single batched UPDATE/INSERT statements; no per-row commits.
2. Wire: called at end of tax-delinquency upload flow for **both** counties
   (Pinellas `Owner Address` already flows to the same canonical field).
3. Tests: NULL-fill, override-on-differ, no-op-on-match, OOS/OOC classification, rescore set.

**Exit:** running Hillsborough file yields ~4.5k absentee flags + ~16k `tax_collector`
enriched contacts (per measured analysis); rescore fires only for changed properties.

---

## Phase 6 — Direct-mail fallback (flag + resolver)

1. `resolve_best_mailing_address(property_id)` in a small service
   (`src/services/direct_mail.py`): priority — `enriched_contacts(tax_collector)` →
   `voters.mailing_address` (any voter at property w/ separate mailing) →
   `owners.mailing_address`. Returns address + source.
2. Skip-trace waterfall MISS hook (`src/services/skip_trace_waterfall.py`): on final miss,
   if resolver returns an address → `owners.direct_mail_eligible = true` (log decision).
3. Expose `direct_mail_eligible` in lead detail/admin API response (read-only field).
4. **No mail vendor integration** (explicitly out of scope — Decision 3).

**Exit:** seeded miss-scenario test flips the flag; resolver unit tests pass.

---

## Phase 7 — Hillsborough voter auto-refresh (cron task)

1. `src/tasks/voter_registry_refresh.py`:
   - MediaFire public API `folder/get_content` on root key `91e7q622dhkgk` →
     newest month subfolder (by `created_utc`) → files listing → "All Eligible Voters.zip"
     page link → regex `href="https://download...` → **streamed** download (~420 MB).
   - Extract inner `.txt` → ColumnMapper (approved Hillsborough profile) →
     `VoterRegistryLoader`.
   - State: remember last processed `folderkey` (county_sources meta / small table) — skip if no
     new month. All HTTP via `requests_get_with_retry`; failure → ERROR log, no partial load.
2. Cron: monthly window — run daily 19th–25th at 04:45 UTC, exits fast when no new folder
   (publication date drifts; folder appears ~1 month after the data month).
3. Pinellas stays manual-upload until SOE reply (then either fold into this task or keep manual).

**Exit:** dry-run against current "April 2026" folder loads (or skips if already processed).

---

## Phase 8 — Tests, docs, hygiene

1. Full pytest run; new tests marked plain unit (no `scenario` unless sandbox-dependent).
2. Update `CLAUDE.md`: new `voters` spoke, new loader, new service, new task, ColumnMapper
   signal types, `direct_mail_eligible`.
3. Update `BULK_REGISTRY_LOADERS_GRILL.md` → mark IMPLEMENTED w/ file map.
4. Commit per phase on `dev` (no push without ask).

---

## Sequencing & dependencies

```
P0 → P1 → P2 → P3 ──┐
        └→ P4 → P5 ─┴→ P6 → P8
                P7 (after P2/P3, parallel to P5/P6)
```

Rollback safety: each phase is additive (new table/columns/endpoints); the only mutation of
existing behavior is P4's roll-year split + P5's absentee writes — both behind the upload flow,
no scraper/cron touched until P7.
