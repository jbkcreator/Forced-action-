# Block 4 — Attribution: Full Analysis & Implementation Plan

**Source of truth for scope:** `tasks/13-07-26/FA-Next-Build-Developer-Brief-FINAL-v3.md` (Block 4, lines 261–284; Section 4 gating; Section 6 anti-overbid).
**Codebase verified:** dev branch, direct read of services/models/routers (this session).
**Decisions locked:**
- **Channel key = `COALESCE(utm_source, signup_source)`** (NOT coarse `signup_source` alone) — paid channels (Meta/Quora/Google) all land as `signup_source='landing_page'` and are only distinguishable by `utm_source`. See §2.8. `signup_source` itself is unchanged (locked 9-value CHECK allow-list); the report's channel *dimension* uses the COALESCE.
- **Spend sourcing = hybrid**: admin manually enters Meta/Google/email spend into a `marketing_spend` table; the compiler **auto-pulls** Quora spend (`quora_topics.cumulative_spend`) and affiliate cost (`affiliate_payout_ledger`) from existing tables — no double-entry.
- **Admin entry UI** lives in the `Forced-action-ui` admin section (new sub-section next to the existing `RoasDashboard.jsx`).
- **Report surface = frontend admin UI only (`CacDashboard.jsx` + `GET /api/admin/cac-payback`)** — REVISED. A `daily_dashboard.py` PDF section was considered but dropped: the brief's DoD only requires "the weekly CAC/payback report compiles per channel," it does not mandate a PDF; the admin endpoint + dashboard already satisfy that, and editing the 2,884-line task file + 1,159-line Jinja template was a materially bigger, riskier change for no added requirement. Feeds Block 8 the same way.
- **Payback margin = per-customer MRR (simple)** now; MRR-minus-lead-COGS is a documented later upgrade.
- **Frontend attribution capture is confirmed already wired** (see §2.7) — #22 is backend bug-fix only.

---

## 1. What the task is (plain English)

Tag every signup and purchase with **where it came from**, and make that tag survive the whole journey from anonymous visitor → checkout → created account. Then produce a **weekly CAC / payback report broken down by channel**, so the founder can answer:

1. Which channel produced each paying customer? (source / campaign / affiliate)
2. What does each channel cost vs. earn? (CAC and payback period, per channel)

The brief splits this into two bid units:

- **#22** — Route bindings that preserve tracking across anon → checkout → signup.
- **#23** — CAC/payback report compiler (revenue + churn by channel).

**Definition of Done (brief lines 281–284):**
- A purchase traces back to its originating source/campaign/affiliate across the anon→signup boundary.
- The weekly CAC/payback report compiles per channel without manual joins.

---

## 2. What already exists in the code

The capture-and-persist pipeline for **#22 is already built end-to-end.** Nothing about capturing/forwarding/storing attribution is greenfield. The brief's own "exists on dev" list is accurate.

### 2.1 Capture at signup — `src/services/signup_engine.py`
- `create_free_account(...)` and `create_free_account_by_email(...)` both accept and persist `utm_source, utm_medium, utm_campaign, campaign_id, attribution_token, affiliate_ref`.
- `_apply_signup_source` (line 59): **first-touch wins** — an existing real source is never overwritten; `signup_source` only upgrades from `""/direct/unknown`; utm fields are backfill-only.
- Emits business events `SIGNUP_COMPLETED`, `SIGNUP_SOURCE_ATTRIBUTED`; hands off to `affiliate_engine.attribute_signup` and `referral_engine.process_signup`.

### 2.2 Storage — `src/core/models.py`
- **`Subscriber`** (lines 1259–1272): `signup_source` String(30) NOT NULL, indexed, CHECK allow-list `{direct, landing_page, dbpr_email, cora_sms, missed_call, referral, admin, unknown, affiliate}`; `utm_source/medium/campaign`, `campaign_id`, `attribution_token`, `affiliate_ref` (indexed). **No `acquisition_source` column** on Subscriber (only `CustomerAccount` has one).
- **`conversion_attribution_events`** (lines 2837–2901): revenue per conversion + 8 internal product dimensions, `revenue_amount`, `occurred_at`, unique on `(source_table, source_event_id)`.
- **`free_to_paid_attribution`** (lines 7127–7148): first-touch free→paid per account.
- **`CustomerAccount.acquisition_source`** Text (line 6989) — free text, **no writer** in traced paths.

### 2.3 Forward through checkout — `src/api/main.py`
- `POST /api/checkout` (line 781) builds `checkout_metadata` and merges `_attribution_stripe_metadata(request, payload.attribution)` (line 954).
- `_attribution_stripe_metadata` (lines 701–721) forwards `utm_source, utm_medium, utm_campaign, utm_content, utm_term, campaign_id, attribution_token, fbclid, ref` from the frontend **plus captures buyer IP + User-Agent from the request**. Same bridge reused by lead-pack (line 3590) and bundle (line 6615) checkouts.
- Affiliate `?aff=` capture: `affiliate_ref_cookie` middleware (line 92) persists to a cookie, read by `/api/free-signup` (line 5970).

### 2.4 Stamp back at payment — `src/services/stripe_webhooks.py`
- `_stamp_campaign_fields(subscriber, meta, db)` (lines 83–102) backfills `utm_source, utm_medium, utm_campaign, campaign_id, attribution_token` from Stripe metadata onto the subscriber — **NULL-only, never clobbers** free-signup attribution. Called at line 958.
- Also fires Meta CAPI Purchase (fbclid/utm), records DBPR email-campaign conversion (`campaign_attribution.py`), writes revenue attribution (`attribution_service.record_conversion_attribution`).

### 2.5 The one existing channel rollup — `src/api/admin_router.py`
- `GET /api/admin/roas` (line 951): groups `conversion_attribution_events.revenue_amount` by `COALESCE(campaign_id, utm_campaign, 'unattributed')` → `{campaign_key, total_revenue, ad_spend, roas, purchase_count}`.
- **Limitations:** spend is a manual per-call query param (not stored); computes ROAS (revenue/spend) not CAC (spend/customer) or payback; does not touch MRR or churn.

### 2.7 Frontend capture — CONFIRMED wired (`Forced-action-ui` repo)
- `src/utils/attribution.js`: `captureAttribution()` reads `utm_source, utm_medium, utm_campaign, utm_content, utm_term, campaign_id, attribution_token, fbclid, ref` from the landing URL on load and persists them first-touch to `localStorage` (`fa_attribution`), only overwriting when the URL actually carries params (so in-app navigation never wipes the first touch).
- `src/hooks/useStripeCheckout.js`: sends attribution into **both** `createFreeSignup(...)` (line 42) and `createCheckout({ ..., attribution: getAttribution() })` (line 70).
- Admin UI already present: `RoasDashboard.jsx`, `AttributionEventsTable.jsx`, `AttributionTrace.jsx`, `AttributionSubscriberPanel.jsx`.
- **Conclusion:** the anon→checkout→signup capture is complete on both ends. #22 is purely the two backend bugs below.

### 2.8 Signup sources — the complete map (verified in code)

`signup_source` allow-list ([signup_engine.py:31-41](../../src/services/signup_engine.py#L31), DB CHECK-enforced): `direct, landing_page, dbpr_email, cora_sms, missed_call, referral, admin, unknown, affiliate`.

**Critical:** paid ad channels do NOT get their own `signup_source` — they all land as `landing_page` and are only distinguishable via `utm_source`/`utm_campaign`. Hence the channel key is `COALESCE(utm_source, signup_source)`.

| Channel (report row) | Underlying capture | Cost source for CAC |
|---|---|---|
| **facebook / meta** | `signup_source=landing_page` + `utm_source=facebook` + `fbclid` (`meta_capi_service.py`) | **Manual** (`marketing_spend`) |
| **google / other paid** | `landing_page` + `utm_source=google` | **Manual** (`marketing_spend`) |
| **quora** | `landing_page` + `utm_campaign=quora_<slug>` (`quora_attribution.py`) | **Auto** — `quora_topics.cumulative_spend` (models:4102) |
| **dbpr_email (Instantly)** | `signup_source=dbpr_email`, HMAC token (`campaign_attribution.py`) | **Manual** — Instantly is flat monthly, no per-campaign cost via API; analytics endpoint returns engagement only (`instantly_service.py:127`), no spend field |
| **affiliate** | `signup_source=affiliate`, `?aff=` (`affiliate_engine.py:116`) | **Auto** — `affiliate_payout_ledger` (real per-signup commissions) |
| **referral** | `signup_source=referral`, `?ref=` (`referral_engine.py`) | None (wallet-credit reward, not cash) |
| **cora_sms** | `signup_source=cora_sms` link (`cora_sms_sender.py`) | Telnyx cost in `api_usage_logs` (opex, not marketing) |
| **missed_call** | `signup_source=missed_call`, inbound voice (`onboard_inbound_caller`) | Synthflow/voice opex |
| **landing_page / direct / unknown** | organic / typed-in / no params | None |

### 2.6 Revenue / churn data available for #23
- `revenue_metrics.compute_revenue_metrics(db, frm, to)` (line 30): MRR, new MRR, churn $ (voluntary/involuntary), churned count, free_to_paid_rate, avg_time_to_convert. **Groups by grade and account only — no channel dimension.**
- `MrrMovement` ledger (models line 7018): `movement_type IN (new, expansion, contraction, churn)`, `delta_cents`, `is_involuntary`, `account_id`, unique `stripe_event_id`. **No channel column** — channel must be joined via subscriber/account.
- Churn risk (`churn_risk.py`, `churn_signals.py`): predictive per-subscriber, no channel breakdown. Realized churn $ lives in `MrrMovement` type='churn'.

---

## 3. What is missing (the actual work)

### 3.1 Sub-task #22 — two real bugs, otherwise complete

**Bug A — dbpr_email origin silently lost.**
`stripe_webhooks.py:998` executes `subscriber.acquisition_source = "dbpr_email"`, but `Subscriber` has no `acquisition_source` column. SQLAlchemy sets a transient Python attribute that never persists; wrapped in try/except so it fails invisibly. Intended field is `signup_source`.

**Bug B — paid-first buyers recorded as `direct`.**
The new-subscriber constructor at `stripe_webhooks.py:491–506` omits `signup_source`, so it falls back to `server_default="direct"`. `_stamp_campaign_fields` deliberately excludes `signup_source` from its backfill set. Result: a buyer who arrives from a campaign and checks out **without** first hitting `/api/free-signup` gets `signup_source="direct"` even though utm_* were captured. Attribution is only complete when the free-signup step ran first.

### 3.2 Sub-task #23 — net-new, and was blocked on missing ad-spend data

- No CAC/payback compiler exists anywhere (`grep` for cac/payback/by_channel → no functional hits).
- No report breaks revenue/churn by channel.
- **No marketing ad-spend is stored** — only vendor/API opex (`api_usage_logs`, `enrichment_usage_logs`), which is cost-to-produce-a-lead, not acquisition spend. `kill_switch_metric_ingest.py:237` hardcodes `cac_paid_channels = None` with the comment "requires ad-spend tracking… Add when marketing budgets land."
- **Resolution (locked):** introduce a **manual `marketing_spend` table + admin entry route**. This is the minimum that makes CAC computable and matches the code's own deferral note.

---

## 4. Implementation plan

### Phase 1 — Fix & verify #22 (route bindings)

**1.1 Fix Bug A** — `src/services/stripe_webhooks.py:998`
- Replace `subscriber.acquisition_source = "dbpr_email"` with `subscriber.signup_source = "dbpr_email"`.
- Guard the write with the allow-list / first-touch rule so it does not overwrite an existing real source (reuse the `_apply_signup_source` intent from `signup_engine.py` — do not re-implement; either import a shared helper or replicate the same "upgrade only from direct/unknown/empty" check).

**1.2 Fix Bug B** — `src/services/stripe_webhooks.py:491` + `:74-80`
- In the new-paid-subscriber constructor, set `signup_source` derived from checkout metadata: if `utm_source`/`campaign_id` present → `landing_page`; else `direct`.
- Add `signup_source` to `_stamp_campaign_fields`' `_CAMPAIGN_META_FIELDS` as a **NULL-only** backfill (so it never clobbers a free-signup value, consistent with the existing utm backfill behavior).

**1.3 Verify (DoD #1)**
- One `test_*.py`: simulate a `checkout.session.completed` carrying `utm_source=facebook, campaign_id=tampa_investors` for (a) a subscriber who did free-signup first and (b) a brand-new paid subscriber. Assert both end with correct `signup_source` + utm_* persisted. Assert the dbpr_email path writes `signup_source='dbpr_email'`.

*Scope note:* no new tables, no new routes. This is a ~3-edit fix + one test. Everything else in #22 already works.

### Phase 2 — Marketing spend ledger (unblocks #23)

Manual entry is only for channels with NO stored cost: **Meta, Google, email/Instantly.** Quora and affiliate are auto-pulled in Phase 3.

**2.1 New model** — `src/core/models.py`: `MarketingSpend`
- Columns: `id` BigInteger PK; `channel` String (must match a `utm_source`/`signup_source` value used by the channel key — e.g. `facebook`, `google`, `dbpr_email`); `campaign_key` String nullable (optional `utm_campaign` for finer entry); `period_start` Date, `period_end` Date; `amount_cents` Integer; `currency` String(3) default `usd`; `notes` Text; `created_at`/`updated_at`.
- Unique on `(channel, campaign_key, period_start, period_end)` — idempotent entry.
- `channel` values validated against an allow-list so manual spend joins the compiler's channel key (see §7 — spend-vocabulary risk).

**2.2 Migration** — `migrations/apply_marketing_spend.py`
- Idempotent `CREATE TABLE IF NOT EXISTS` + the unique index. Run once against the shared DB (ADR 0024, scripts-only).

**2.3 Admin route** — extend `src/api/admin_router.py`
- `POST /api/admin/marketing-spend` (JWT, raw SQL `text()`): upsert a spend row; reject `channel` not in the allow-list.
- `GET /api/admin/marketing-spend`: list spend rows for a window.

**2.4 Admin UI** — `Forced-action-ui`
- New admin sub-section (next to `RoasDashboard.jsx`) to enter/list Meta/Google/email spend per period. Quora + affiliate are shown read-only (auto-sourced), not entered.

### Phase 3 — CAC/payback compiler (#23)

**3.1 Channel rollup function** — extend `src/services/revenue_metrics.py`
- Add `compute_channel_metrics(db, frm, to) -> list[dict]` (keep `compute_revenue_metrics` untouched; new function, single responsibility).
- **Channel dimension key = `COALESCE(subscribers.utm_source, subscribers.signup_source, 'unattributed')`** — separates Meta/Quora/Google (all `signup_source='landing_page'`) via `utm_source`. One canonical expression, reused by the report and the admin endpoint so they can never disagree.
- Per channel, one CTE-based query (no per-row loops, no Python joins):
  - **new customers**: count of accounts whose first `mrr_movements` row (type='new') falls in window, joined to subscriber channel key.
  - **revenue**: sum of new + expansion MRR in window by channel.
  - **churn**: sum of `mrr_movements` type='churn' delta by channel; churned count.
  - **spend (hybrid)**: `LEFT JOIN`/`UNION` three cost sources into one per-channel spend figure —
    - Meta/Google/email → `marketing_spend.amount_cents` (manual)
    - `quora` → `quora_topics.cumulative_spend` (auto)
    - `affiliate` → `SUM(affiliate_payout_ledger)` for the window (auto)
    Normalize all to cents; map each source to the same channel-key vocabulary.
  - **CAC** = spend ÷ new customers (guard divide-by-zero → null).
  - **payback (months)** = CAC ÷ per-customer monthly MRR (simple margin; MRR-minus-lead-COGS refinement documented in §7).

**3.2 Surface it — admin UI only (no PDF)**
- Expose `GET /api/admin/cac-payback` (thin wrapper over `compute_channel_metrics`) — the frontend `CacDashboard.jsx` (Phase 4) reads it on demand. This satisfies the brief's DoD ("compiles per channel without manual joins") without touching `daily_dashboard.py`.

**3.3 Verify (DoD #2)**
- Test: seed 2 channels with spend + subscribers + mrr_movements, assert `compute_channel_metrics` returns correct CAC and payback per channel and a clean `null` CAC when spend is absent — "compiles per channel without manual joins."

### Phase 4 — Frontend (`Forced-action-ui`)

Verified frontend conventions (see §8 for the full map). All admin calls go through `adminFetch(token, path, opts)` in `src/api/admin.js` (Bearer token from `localStorage['admin_token']`, JSON-encodes object bodies, throws `Error` with `.status/.detail`). Sections register in **two** parallel places: `src/App.jsx` (lazy import + route) and `src/components/admin/AdminSidebar.jsx` (`SECTIONS` array). Style = Tailwind + inline dark-glass `style` objects; accent `#facc15`; reuse `fmtDollars` + green/yellow/red threshold coloring from `RoasDashboard.jsx`.

**4.1 API client helpers** — `src/api/admin.js` (mirror existing `adminFetch` pattern)
- `createMarketingSpend(token, body)` → `POST /api/admin/marketing-spend` (body: `{channel, campaign_key?, period_start, period_end, amount, notes?}`)
- `fetchMarketingSpend(token, {from, to})` → `GET /api/admin/marketing-spend?from=&to=`
- `fetchCacPayback(token, {from, to})` → `GET /api/admin/cac-payback?from=&to=`

**4.2 Section shell** — `src/components/admin/sections/MarketingSpendSection.jsx` (Pattern A — `SectionLayout` with two tabs, mirrors `RevenueSection.jsx`)
- Tabs: `{id:'add', label:'Add Spend', Component: MarketingSpendForm}`, `{id:'cac', label:'CAC / Payback', Component: CacDashboard}`. Each Component receives `token` as a prop.

**4.3 Spend-entry form** — `src/components/admin/MarketingSpendForm.jsx` (mirror `CreateEmailCampaignModal.jsx` form logic, drop the Modal)
- Single `form` state object + `set(key,val)`; native inputs: `<select>` channel (allow-list: facebook, google, dbpr_email — **only the manual channels**; Quora/affiliate excluded, they're auto), `type="date"` period_start/period_end, `type="number" step="0.01"` amount ($), `type="text"` optional campaign_key + notes.
- Inline validation (channel required, amount > 0, period_start ≤ period_end); `handleSubmit` → `createMarketingSpend`; success/error banners (emerald/red) + disabled "Saving…" button, per the existing pattern.
- Below the form: a table of recent spend rows via `fetchMarketingSpend` (reuse the RoasDashboard table styling), with the auto-sourced Quora/affiliate spend shown read-only + labeled "(auto)".

**4.4 CAC dashboard** — `src/components/admin/CacDashboard.jsx` (mirror `RoasDashboard.jsx`)
- Date-range filter (from/to) + "Load" button → `fetchCacPayback`.
- Table columns: Channel · New Customers · Revenue · Churn · Spend · **CAC** · **Payback (mo)**. Reuse `fmtDollars`; color CAC/payback cells (green/yellow/red thresholds); show `—` for null CAC (no spend) and a "(auto)" tag on Quora/affiliate rows.

**4.5 Register the section** (two files, three edits)
- `src/App.jsx`: add `const MarketingSpendSection = lazy(() => import('./components/admin/sections/MarketingSpendSection'));` and `<Route path="marketing-spend" element={withBoundary(<MarketingSpendSection />)} />` under `/admin`.
- `src/components/admin/AdminSidebar.jsx`: add `{ id:'marketing-spend', label:'Marketing Spend', path:'/admin/marketing-spend', icon:(…) }` to `SECTIONS`.

**4.6 Verify** — with the backend running, enter a Meta spend row, confirm it persists (reload lists it) and the CAC tab shows a computed CAC/payback for a channel that has both spend and customers, and `—` where spend is absent.

*Note:* The existing `RoasDashboard.jsx` "Ad Spend Override" input is **ephemeral** (a GET query param, never stored). Leave it as-is; the new persisted flow is separate. Optionally, once `marketing_spend` exists, a later tweak could have ROAS read stored spend — deferred, not in scope.

---

## 5. Anti-overbid note (what NOT to rebuild)

Per brief Section 6 and verified in code, bill these as **wiring/extension only**:
- Attribution capture (`signup_engine.py`), forwarding (`_attribution_stripe_metadata`), stamp-back (`_stamp_campaign_fields`) — **already built**; #22 is bug-fix + verify, not a build.
- `conversion_attribution_events`, `attribution_service.py`, `campaign_attribution.py`, Meta CAPI, affiliate/referral loops — **already built**.
- `MrrMovement` ledger and `revenue_metrics.py` — **read/extend**, do not recreate.

**Genuinely net-new:** the `marketing_spend` table + admin entry (Phase 2) and the channel rollup / CAC-payback compiler (Phase 3). Everything in Phase 1 is a fix.

---

## 6. Dependencies & sequencing

- **Phase 1** has no dependencies — Blocks 0/1/3 (done) already provide the storefront + signup paths it corrects. Ship independently.
- **Phase 2** is standalone (new table + admin route).
- **Phase 3** depends on Phase 2 (needs the spend table) and reads existing revenue/churn data.
- Feeds **Block 8 (Operator Dashboard)** — its "revenue + churn by channel" KPI reads `compute_channel_metrics`.

## 7. Risks / documented later-upgrades

- **Spend vocabulary must match the channel key**: manual spend `channel` values AND the auto-pulled Quora/affiliate labels MUST map to the same `COALESCE(utm_source, signup_source)` vocabulary, or the spend↔revenue join misses and CAC breaks. Enforce with an allow-list on the admin entry route + a fixed mapping for the auto sources (RESOLVED by design — flagged so it isn't dropped in build).
- **Campaign drill-down (later upgrade)**: channel key uses `utm_source`; grouping additionally by `utm_campaign` is an additive change (data already captured), not a rewrite.
- **True gross-margin payback (later upgrade)**: payback uses per-customer MRR now; subtracting per-lead COGS from `enrichment_usage_logs` gives true margin when wanted.
- **Auto ad-spend sync (later upgrade)**: manual entry is period-level; CAC accuracy tracks entry discipline. Meta/Google Ads API sync is deferred.
- **Spend granularity**: manual entry is monthly/period-level; CAC is only as accurate as entry discipline.

---

## 8. Frontend architecture reference (`Forced-action-ui`, verified)

- **Admin shell**: `src/pages/AdminPage.jsx` — JWT in `localStorage['admin_token']`, provides `AdminContext ({token, onLogout})`, renders `<AdminSidebar>` + React Router `<Outlet/>`. Sections get `token` via `useAdminContext()` (Pattern B) or as a prop from `SectionLayout` (Pattern A).
- **API client**: `src/api/admin.js` → `adminFetch(token, path, opts)` sets `Authorization: Bearer`, JSON-encodes object bodies, leaves FormData alone, throws `Error` with `.status/.detail/.body`. Base URL `import.meta.env.VITE_API_BASE_URL`. **Do not use `src/api/client.js`** (that's the tokenless subscriber client).
- **Closest analog**: `src/components/admin/RoasDashboard.jsx` — filter card + "Load" button + results table; `fetchRoas` → `GET /api/admin/roas`. Its "Ad Spend Override" is ephemeral (query param), NOT stored. Reuse its `CARD`/`INPUT_STYLE` constants, `fmtDollars`, and `RoasCell` threshold coloring.
- **Form template**: `src/components/admin/email-campaigns/CreateEmailCampaignModal.jsx` — single `form` state, `set(key,val)`, `inputProps(key)` factory, inline validation, `async handleSubmit` with `submitting` flag, emerald/red banners. Mirror minus the Modal.
- **Section registration is two-file**: `src/App.jsx` (lazy import + `<Route>`) AND `src/components/admin/AdminSidebar.jsx` (`SECTIONS` array). No central registry — keep both in sync.
- **Styling**: Tailwind utilities + inline dark-glass `style` objects; accent `#facc15`; semantic emerald/yellow/red. Tables: `overflow-x-auto` → `<table className="w-full text-sm">`, uppercase slate-400 headers, colSpan empty/loading rows.

---

## 9. Combined file-by-file build order & checklist

**Phase 1 — #22 bug fixes (backend only, ship first, no deps) — ✅ DONE**
- [x] `src/services/stripe_webhooks.py` — `acquisition_source` → `signup_source` (allow-list guarded, first-touch safe)
- [x] `src/services/stripe_webhooks.py` — set `signup_source` on new paid subscriber from checkout metadata
- [x] `src/services/stripe_webhooks.py` — `_stamp_campaign_fields` upgrades `signup_source` (not NULL-only backfill), first-touch preserved
- [x] `tests/test_signup_source_attribution.py` — 7 tests, all passing: paid-first with/without utm, first-touch not clobbered, dbpr_email fix (DoD #1)

**Phase 2 — marketing spend store (backend done; FE form pending)**
- [x] `src/core/models.py` — `MarketingSpend` model
- [x] `migrations/apply_marketing_spend.py` — idempotent create + unique index; **run against dev DB**
- [x] `src/api/admin_router.py` — `POST/GET /api/admin/marketing-spend` (allow-list validated: facebook/google/dbpr_email only)
- [ ] `Forced-action-ui/src/api/admin.js` — `createMarketingSpend`, `fetchMarketingSpend`
- [ ] `Forced-action-ui/.../MarketingSpendForm.jsx` — entry form + recent-rows table

**Phase 3 — CAC compiler (backend done; FE view pending; no PDF)**
- [x] `src/services/revenue_metrics.py` — `compute_channel_metrics(db, frm, to)` — channel key `COALESCE(utm_source, quora-via-utm_campaign-prefix, signup_source)`; hybrid spend join: manual + Quora (`quora_topics.cumulative_spend`) + affiliate (`affiliate_payout_ledger`)
- [x] `src/api/admin_router.py` — `GET /api/admin/cac-payback` (wraps the function)
- [x] `tests/test_channel_metrics.py` — 4 tests, all passing: CAC/payback correctness, null-spend case, Quora regression guard (DoD #2)
- [ ] ~~`src/tasks/daily_dashboard.py` PDF section~~ — DROPPED; admin UI only satisfies the DoD (see decision log)
- [ ] `Forced-action-ui/src/api/admin.js` — `fetchCacPayback`
- [ ] `Forced-action-ui/.../CacDashboard.jsx` — CAC/payback table

**Phase 4 — wire up FE section (shared by 2 & 3) — pending**
- [ ] `Forced-action-ui/.../sections/MarketingSpendSection.jsx` — two-tab shell
- [ ] `Forced-action-ui/src/App.jsx` — lazy import + `<Route path="marketing-spend">`
- [ ] `Forced-action-ui/src/components/admin/AdminSidebar.jsx` — `SECTIONS` entry
- [ ] End-to-end verify: enter Meta spend → CAC tab shows computed CAC/payback per channel

**Sequencing**: Phase 1 is independent (ship anytime). Phase 2 before Phase 3 (compiler needs the spend table). Phase 4's section shell can land with Phase 2 (Add-Spend tab) and gain the CAC tab in Phase 3.
