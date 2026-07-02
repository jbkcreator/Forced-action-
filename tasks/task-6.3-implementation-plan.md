# Task 6.3 — Predictive Engagement & Churn Defense Logic
## Detailed Implementation Plan (Backend + Frontend, phased)

> **Source of truth for requirements:** `tasks/Phase5_Phase6_Developer_Specs.pdf`, page 8 (Task 6.3).
> **Source of truth for current state:** live source code + the live DB at `5.78.184.159:5432/distress_db` (verified read-only, 2026-07-02).
> This plan implements **exactly** what the spec asks for Task 6.3 — nothing more.

---

## 1. What we are building (plain English)

The portal serves paying subscribers who log in, view a lead feed at `/dashboard/{feedUuid}`, and export leads to CSV. (The ~65 rows currently in the `subscribers` table are **test accounts used by the dev team**, not real live subscribers — the feature must still be built for real subscribers, but expect the live table to be effectively empty of production data during development/testing.) **None of that engagement is measured today.** Task 6.3 adds:

1. **Behavioral listeners** in the portal UI that emit three events: login, dashboard view, lead download.
2. A **weekly background worker** that, for each subscriber, computes an *engagement decay score* — recent 7-day activity relative to their own 30-day baseline.
3. An **automated retention trigger**: when a subscriber's activity has collapsed (decay `< 0.35`), write a `churn_defense_leads` row, generate a personalized pitch from their purchased-ZIP data (Pitch Generator), and fire a GoHighLevel check-in sequence.

**Formula (spec p.8):**
```
engagement_decay = ( (dashboard_views_7day × 0.4) + (lead_downloads_7day × 0.6) ) / (baseline_30day_mean + 1)
```
- Downloads weigh more than views (0.6 vs 0.4).
- `baseline_30day_mean` = **weekly-equivalent mean** of that same weighted activity over the prior 30 days = `(weighted activity over 30 days) ÷ (30/7 ≈ 4.29)`. This makes a steady user's recent-7-day activity ≈ baseline → decay ≈ 1.0, matching the spec's "near 1.0 = normal" note.
- `+1` guards divide-by-zero.
- `< 0.35` (a >65% drop) → trigger.

---

## 2. Spec requirement → coverage traceability

Every literal Task 6.3 requirement and where it is satisfied. Nothing outside this list is built.

| # | Spec requirement (p.8) | Covered by |
|---|---|---|
| R1 | Table `subscriber_session_metrics` (exact columns) | Backend Step B1 |
| R2 | Table `churn_defense_leads` (exact columns) | Backend Step B1 |
| R3 | Behavioral listeners in the portal UI | Frontend Steps F1–F3 |
| R4 | `dashboard_views_7_day` signal | Frontend F2 (`DASHBOARD_VIEW`) → Backend B5 |
| R5 | `lead_downloads_7_day` signal | Frontend F3 (`LEAD_DOWNLOAD`) → Backend B5 |
| R6 | `last_login_at` / `auth_intervals_seconds` | Frontend F1 (`SUBSCRIBER_LOGIN`) → Backend B5 |
| R7 | Engagement-decay computation, every 7 days | Backend B5 (worker) + B6 (weekly cron) |
| R8 | Trigger when decay `< 0.35` | Backend B5 |
| R9 | Create a `churn_defense_leads` row | Backend B5 |
| R10 | Generate personalized outreach via Pitch Generator, from purchased-ZIP metrics | Backend B5 (`pitch_builder.py` + `zip_territories`) |
| R11 | Fire automated check-in sequence via GoHighLevel webhook | Backend B5 (`push_subscriber_to_ghl`) |

> **Out of scope (intentionally, to avoid over-engineering):** no admin UI for churn leads; no new column for pitch text (spec defines none — pitch is handed to GHL); no changes to the existing wallet-based churn system (`churn_scoring.py`, fa051); the `ENGAGED`/`CONVERTED` `outreach_status` transitions are downstream GHL callbacks the spec does not define, so the column supports them but we do not implement those transitions.

---

## 3. Recommended build order

**Backend first, then Frontend.** Rationale:

- The event ingestion endpoint (`/api/business-event`) **silently drops** any `event_type` not in its allow-list. If the frontend ships first, all three new events are discarded until the backend allow-list is deployed. Shipping backend first (or together) prevents silent data loss.
- Backend is fully self-contained and testable with seeded data before any frontend exists.
- Once backend is live, the frontend simply starts feeding real events into an already-working pipeline.

**Phasing:**
- **Phase 1 — Backend (Steps B1–B6):** tables, models, allow-list, config, worker, cron. Fully deployable and unit-testable on its own.
- **Phase 2 — Frontend (Steps F1–F3):** fire the three events. After this, real data begins accumulating.

> ⚠️ **Operational reality (not a bug):** because no `DASHBOARD_VIEW`/`LEAD_DOWNLOAD` rows exist yet, the worker produces **no meaningful churn signal until ~30 days of event history accumulate** after Phase 2 ships. The **history-based** cold-start guard (Step B5) makes this safe — every subscriber has a zero baseline / no tracked history at launch, so **zero accounts are flagged** in the first ~30 days regardless of account age. (An account-age-based guard would NOT achieve this — see the critical note in Step B5.) Communicate this expectation to stakeholders.

---

## 4. Current-state facts (verified — do not re-derive)

- `subscriber_session_metrics`, `churn_defense_leads` — **do not exist** in the live DB.
- `subscribers` — 65 rows, but these are **dev-team test accounts** (the table has an `is_test` boolean), **not real live subscribers**; `id` is `integer` (sequence-backed); **no `last_login_at`**; has `event_feed_uuid`, `churned_at`, `status`, `created_at`, `is_test`. **43** tables already FK to `subscribers.id`.
- `webhook_events` — 1,800 rows; `subscriber_id integer NULL` (indexed), `event_type varchar` (indexed), `source varchar` (indexed), `processed_at timestamptz NOT NULL` (indexed), composite `idx_webhook_events_source_processed(source, processed_at)`. **60% of rows have `subscriber_id = NULL`** (pre-login events) — so new events MUST carry `feed_uuid`.
- `zip_territories` — 35 rows; `subscriber_id integer`, `zip_code`, `vertical`, `county_id`. **This is the "purchased ZIP metrics" source** (`lead_pack_purchases` and `bundle_purchases` are both empty).
- Alembic: live DB carries **3 heads** — `fa093_m5_score_feedback`, `fa096_macro_signals`, `fa110_platform_revenue_ledger`. The Alembic CLI is documented as *unusable* on this multi-head tree; `fa_` migrations ship a standalone `scripts/apply_*.py`.

---

# PHASE 1 — BACKEND IMPLEMENTATION PLAN

Repo: `Forced-action-`. Follow `CLAUDE.md` rules: SQLAlchemy 2.0, `text()` for all reads (ORM only for `add`/`delete`), Pydantic settings, no `print()` in `src/`, every external call wrapped in try/except with contextual logging, batch DB I/O.

### Step B1 — Create the two tables (idempotent DDL + apply script)

**What:** create `subscriber_session_metrics` and `churn_defense_leads`.
**Why:** every downstream step reads/writes these.
**Where:**
- New `alembic/versions/fa_6_3_churn_defense.py`
- New `scripts/apply_fa_6_3_churn_defense.py`

**How — mirror `alembic/versions/fa_s1_commission_ledger.py` exactly:**
- Version file: `revision = "fa_6_3_churn_defense"`, `down_revision = "fa110_platform_revenue_ledger"` (any current head), `branch_labels = None`, `depends_on = None`.
- `upgrade()` uses `op.execute(sa.text("CREATE TABLE IF NOT EXISTS ..."))` + `CREATE INDEX IF NOT EXISTS ...`.
- `downgrade()` drops both tables.
- `scripts/apply_fa_6_3_churn_defense.py` opens a session (`from src.core.database import get_db_context`) and executes the **same idempotent DDL** — this is how it is actually applied to the live DB.

**Exact DDL (subscriber_id upgraded to INTEGER FK per locked decision):**
```sql
CREATE TABLE IF NOT EXISTS subscriber_session_metrics (
    id                      SERIAL PRIMARY KEY,
    subscriber_id           INTEGER NOT NULL UNIQUE REFERENCES subscribers(id) ON DELETE CASCADE,
    last_login_at           TIMESTAMP WITH TIME ZONE,
    dashboard_views_7_day   INTEGER NOT NULL DEFAULT 0,
    lead_downloads_7_day    INTEGER NOT NULL DEFAULT 0,
    auth_intervals_seconds  INTEGER NOT NULL DEFAULT 0,
    engagement_decay_scalar NUMERIC(3,2) NOT NULL DEFAULT 1.00,
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_subscriber_session_metrics_subscriber
    ON subscriber_session_metrics (subscriber_id);

CREATE TABLE IF NOT EXISTS churn_defense_leads (
    id              SERIAL PRIMARY KEY,
    subscriber_id   INTEGER NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    risk_score      NUMERIC(4,3) NOT NULL,
    outreach_status VARCHAR(50) NOT NULL DEFAULT 'STAGED',
    triggered_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_subscriber
    ON churn_defense_leads (subscriber_id);
CREATE INDEX IF NOT EXISTS ix_churn_defense_leads_triggered_at
    ON churn_defense_leads (triggered_at);
```

**Column notes (all spec columns kept):**
- `subscriber_id`: spec literal is `VARCHAR(100)`; upgraded to INTEGER FK to match the live integer-keyed schema (43 existing FK tables). This is the one deliberate deviation, locked with the product owner.
- `auth_intervals_seconds`: kept because the spec's `CREATE TABLE` includes it; the decay formula does **not** use it (populate best-effort from login gaps, else leave 0).
- `outreach_status`: allowed values `STAGED, SEQUENCE_TRIGGERED, ENGAGED, CONVERTED` (spec comment). We write only the first two.

**Verify:** run the apply script against dev DB; `\d subscriber_session_metrics` / `\d churn_defense_leads` show the columns + indexes; re-run → no-op (idempotent).

---

### Step B2 — ORM models

**What:** add `SubscriberSessionMetrics` and `ChurnDefenseLead`.
**Why:** the worker uses ORM instances for `session.add()` (per project rule: ORM for add/delete, `text()` for reads).
**Where:** `src/core/models.py`, near the Subscriber-related models (~lines 1166–1332, before `ZipTerritory`).
**How:** mirror existing `Mapped[...] = mapped_column(...)` style; match the DDL above exactly (types, nullability, defaults). A `relationship` back to `Subscriber` is optional and not required.

---

### Step B3 — Extend the business-event allow-list

**What:** register the three new event types.
**Why:** `log_business_event` validates `event_type` against the `BUSINESS_EVENT_TYPES` frozenset and **silently drops** anything not listed (`src/services/business_events.py:79`). The endpoint `POST /api/business-event` and `log_webhook_event` themselves need **no change**.
**Where:** `src/services/business_events.py:36-61` (the `BUSINESS_EVENT_TYPES` frozenset).
**How:** add three strings:
```python
"SUBSCRIBER_LOGIN",
"DASHBOARD_VIEW",
"LEAD_DOWNLOAD",
```
**This is the only edit to the ingestion pipeline.** No new endpoint, no request-model change, no schema change.

---

### Step B4 — Config constants

**What:** centralize the formula constants (mirrors `config/churn.py`).
**Why:** no magic numbers in the worker.
**Where:** new `config/churn_defense.py`.
```python
VIEW_WEIGHT = 0.4
DOWNLOAD_WEIGHT = 0.6
DECAY_THRESHOLD = 0.35              # below this → flag
RECENT_WINDOW_DAYS = 7
BASELINE_WINDOW_DAYS = 30
BASELINE_WEEKS = BASELINE_WINDOW_DAYS / RECENT_WINDOW_DAYS   # 30/7 ≈ 4.2857
MIN_BASELINE_HISTORY_DAYS = 30     # cold-start guard: require at least this many days of TRACKED engagement history before an account can be flagged (measured from first tracked event, not account creation)
OUTREACH_COOLDOWN_DAYS = 30        # don't re-fire outreach within this window
DECAY_SCALAR_MAX = 9.99            # numeric(3,2) storage ceiling (clamp before insert)
GHL_CHURN_DEFENSE_TAG = "churn_defense"
```

---

### Step B5 — Weekly worker (core of Task 6.3)

**What:** compute the decay score per subscriber, snapshot it, and fire the retention trigger for at-risk accounts.
**Why:** this is the deliverable's heart (R7–R11).
**Where:** new `src/tasks/churn_defense_engagement_decay.py`.
**Structure — mirror `src/tasks/churn_scoring.py`:** an entry function **`run(dry_run: bool = False)`** (NOT `main()`), using `with get_db_context() as db:`, and:
```python
if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
```

**Logic (single batched pass — no per-subscriber query loops, per CLAUDE.md):**

1. **One aggregation query** over `webhook_events` (uses the existing `idx_webhook_events_source_processed` index):
```sql
SELECT
    w.subscriber_id,
    COUNT(*) FILTER (WHERE w.event_type='DASHBOARD_VIEW'  AND w.processed_at >= now() - interval '7 days')  AS views_7,
    COUNT(*) FILTER (WHERE w.event_type='LEAD_DOWNLOAD'   AND w.processed_at >= now() - interval '7 days')  AS downloads_7,
    COUNT(*) FILTER (WHERE w.event_type='DASHBOARD_VIEW')                                                    AS views_30,
    COUNT(*) FILTER (WHERE w.event_type='LEAD_DOWNLOAD')                                                     AS downloads_30,
    MAX(w.processed_at) FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                       AS last_login_at,
    MIN(w.processed_at) FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                       AS first_login_at,
    COUNT(*)            FILTER (WHERE w.event_type='SUBSCRIBER_LOGIN')                                        AS login_count,
    MIN(w.processed_at)                                                                                      AS first_event_at
FROM webhook_events w
WHERE w.source = 'frontend'
  AND w.subscriber_id IS NOT NULL
  AND w.event_type IN ('DASHBOARD_VIEW','LEAD_DOWNLOAD','SUBSCRIBER_LOGIN')
  AND w.processed_at >= now() - interval '30 days'
GROUP BY w.subscriber_id;
```

2. **Fetch the scoring population** in one query — only non-churned subscribers:
```sql
SELECT id, event_feed_uuid, churned_at FROM subscribers WHERE churned_at IS NULL;
```
Build a `dict[subscriber_id → row]` from step 1 for O(1) lookup (subscribers with no events default to zero counts). The cold-start guard uses `first_event_at`/`baseline` from step 1 (tracked history), **not** `subscribers.created_at`. (Subscribers whose `event_feed_uuid` is NULL cannot generate resolvable events and will simply have zero counts.)

> **Decision — score everyone (no `is_test` filter):** the worker intentionally scores all non-churned subscribers, including the current dev test accounts. There are no live subscribers yet, so including test accounts lets us exercise the full loop end-to-end. GHL is configured but the outreach workflow is **not yet activated**, so no real emails send until it is switched on at first live subscriber; the test accounts will be deleted before go-live. Once live, the same `churned_at IS NULL` query naturally scopes to real subscribers.

3. **Per subscriber (in Python, from the two result sets — no DB calls in the loop):**
   - `recent = views_7 * VIEW_WEIGHT + downloads_7 * DOWNLOAD_WEIGHT`
   - `baseline_30day_mean = (views_30 * VIEW_WEIGHT + downloads_30 * DOWNLOAD_WEIGHT) / BASELINE_WEEKS`
   - `engagement_decay = recent / (baseline_30day_mean + 1)`
   - **Cold-start guard — based on *tracked engagement history*, NOT account age.** Skip flagging (still write the snapshot, set `engagement_decay_scalar` to the computed/clamped value or `1.00`) when **either**:
     - `baseline_30day_mean == 0` — the subscriber has no tracked prior engagement, so there is nothing to have "dropped off" from (a zero baseline is not churn); **or**
     - `first_event_at` is `NULL` or newer than `MIN_BASELINE_HISTORY_DAYS` ago — the subscriber's tracked history is too short to establish a baseline.

     > **Why this matters (critical):** the subscriber accounts were created long before event tracking existed, and events only start accruing when the frontend (Phase 2) ships. An *age-based* guard would NOT skip these established accounts, so the first worker run would see `recent=0, baseline=0 → decay=0 < 0.35` for everyone and flag the entire subscriber base (creating `churn_defense_leads` rows + paid Claude pitch calls for all). Guarding on tracked-history/baseline presence makes launch genuinely safe: with no `first_event_at`/zero baseline, nobody is flagged until ~30 days of real engagement data exist. A genuinely churning subscriber (had a baseline, now silent) still has `baseline > 0` and is correctly flagged.
   - **Storage clamp:** `scalar = min(round(engagement_decay, 2), DECAY_SCALAR_MAX)` — `engagement_decay_scalar` is `numeric(3,2)` (max 9.99); a very active user with a near-zero baseline can exceed that and would otherwise raise a numeric-overflow error on insert.

4. **Upsert the snapshot** into `subscriber_session_metrics` — one batched statement:
```sql
INSERT INTO subscriber_session_metrics
    (subscriber_id, last_login_at, dashboard_views_7_day, lead_downloads_7_day,
     auth_intervals_seconds, engagement_decay_scalar, updated_at)
VALUES (:sid, :last_login, :v7, :d7, :auth_secs, :scalar, now())
ON CONFLICT (subscriber_id) DO UPDATE SET
    last_login_at           = EXCLUDED.last_login_at,
    dashboard_views_7_day   = EXCLUDED.dashboard_views_7_day,
    lead_downloads_7_day    = EXCLUDED.lead_downloads_7_day,
    auth_intervals_seconds  = EXCLUDED.auth_intervals_seconds,
    engagement_decay_scalar = EXCLUDED.engagement_decay_scalar,
    updated_at              = now();
```
(`auth_intervals_seconds`: not used by the decay formula. Populate best-effort as the average login interval `= (last_login_at − first_login_at) / (login_count − 1)` in seconds when `login_count ≥ 2`, else `0`. Leaving it `0` is acceptable — the spec includes the column but never uses it in scoring.)

5. **Trigger** for subscribers where `engagement_decay < DECAY_THRESHOLD` **and** not cold-start **and** no non-terminal `churn_defense_leads` row within `OUTREACH_COOLDOWN_DAYS` (dedupe check — one batched query up front: `SELECT subscriber_id FROM churn_defense_leads WHERE triggered_at >= now() - interval '30 days' AND outreach_status NOT IN ('CONVERTED')`):
   - **Insert** `churn_defense_leads(subscriber_id, risk_score=round(1 - engagement_decay, 3), outreach_status='STAGED')`. (Since decay < 0.35, `risk_score` ∈ (0.65, 1.0] — always positive, fits `numeric(4,3)`.)
   - **Pitch Generator (R10):** `SELECT zip_code FROM zip_territories WHERE subscriber_id = :id` → pick one ZIP → find a recent scored property in that ZIP → `build_property_pitch_context(session, property_id)` then `generate_pitch_with_claude(context, request_options={}, subscriber_id=sid, db=db)`. If the subscriber has no ZIP or no property, log a WARNING and skip pitch generation **but still fire GHL**. The pitch text is passed into the GHL payload (note / custom field) — **no new DB column** (spec defines none).
   - **GHL sequence (R11):** `push_subscriber_to_ghl(subscriber, stage=None, tags=[GHL_CHURN_DEFENSE_TAG], db=db)` (`src/services/ghl_webhook.py:491`). Tag presence fires the GHL-side check-in workflow. On success, update that lead's `outreach_status = 'SEQUENCE_TRIGGERED'`.
   - Wrap the Claude and GHL calls in try/except with contextual logging; **one subscriber's failure must not abort the batch.**

6. **Commit once** at the end (not per subscriber). Log an INFO summary: `scored=N, flagged=M, skipped_coldstart=K, ghl_ok=X, ghl_failed=Y`.

**Support `--dry-run`:** compute and log everything, write nothing.

---

### Step B6 — Cron registration

**What:** run the worker every 7 days.
**Why:** spec says "every 7 days."
**Where:** `scripts/cron/crontab.txt` (respect the documented stagger; place after `churn_scoring` at 13:00 UTC).
**How:**
```
# Task 6.3 — Churn Defense engagement-decay scorer — weekly Monday 14:00 UTC
0 14 * * 1 $PROJECT/scripts/cron/run.sh src.tasks.churn_defense_engagement_decay
```

### Backend done-when
- Apply script creates both tables idempotently.
- `python -m src.tasks.churn_defense_engagement_decay --dry-run` reads `webhook_events`, logs the summary, writes nothing.
- Unit tests pass for the decay math (see §5 verification).

---

# PHASE 2 — FRONTEND IMPLEMENTATION PLAN

Repo: `Forced-action-ui`, branch `dev`. Reuse the existing helper `logBusinessEvent(eventType, { feedUuid, payload })` (`src/api/phase2b.js:57`) — fire-and-forget, never blocks UX. **Every call MUST pass `feedUuid`**, or the event lands with `subscriber_id = NULL` and the worker cannot count it.

### Step F1 — Fire `SUBSCRIBER_LOGIN` on login success

**What:** emit a login event so the worker can populate `last_login_at`.
**Where:** `src/hooks/useSubscriberAuth.js`, inside `login()` (~line 17), after `setSession(...)`, before `return data`.
**How:**
```js
logBusinessEvent('SUBSCRIBER_LOGIN', { feedUuid: data?.feed_uuid });
```
`data` from the login response contains `feed_uuid` (verified). Import `logBusinessEvent` from `../api/phase2b` if not already imported.

### Step F2 — Fire `DASHBOARD_VIEW` once per mount

**What:** emit a dashboard-view event (R4). Must fire **once per visit**, not on every filter/sort/refetch (those re-render the component and would inflate the count).
**Where:** `src/pages/DashboardPage.jsx` (`logBusinessEvent` is already imported and used here). `feedUuid` comes from `useParams()` (line 99); `isAuthReady` useMemo exists (lines 103–106).
**How — ref-guarded effect:**
```js
const viewLogged = useRef(false);
useEffect(() => {
  if (isAuthReady && !viewLogged.current) {
    viewLogged.current = true;
    logBusinessEvent('DASHBOARD_VIEW', { feedUuid });
  }
}, [isAuthReady, feedUuid]);
```
Gating on `isAuthReady` ensures we only count authenticated views (so `subscriber_id` resolves).

### Step F3 — Fire `LEAD_DOWNLOAD` on export

**What:** emit a download event (R5) when a subscriber exports their leads.
**Where:** `src/components/dashboard/ExportButton.jsx` (currently a 100% client-side CSV blob, props `{ leads, page }`, no network call/event). Rendered at `DashboardPage.jsx:730`.
**How:**
1. Add `feedUuid` to `ExportButton`'s props and pass it from `DashboardPage.jsx:730`:
   `<ExportButton leads={leads} page={filters.page} feedUuid={feedUuid} />`
2. Import `logBusinessEvent` from `../../api/phase2b`.
3. At the top of `handleExport()`, after the existing `if (!leads?.length) return;` guard:
   ```js
   logBusinessEvent('LEAD_DOWNLOAD', { feedUuid, payload: { count: leads.length, page } });
   ```

### Frontend done-when
- In devtools: logging in → loading the dashboard → clicking Export produces three `POST /api/business-event` calls, each with the correct `event_type` and a non-null `feed_uuid`.
- Those rows appear in `webhook_events` with `source='frontend'` and a resolved `subscriber_id`.

---

## 5. Verification (end-to-end)

**Backend unit tests** (`tests/`, pytest):
- `views_7=0, downloads_7=0, baseline>0` → decay≈0 → flagged.
- Steady activity (recent ≈ weekly baseline) → decay≈1 → not flagged.
- `baseline=0` → `+1` guard prevents divide-by-zero.
- Zero baseline (no tracked events) → **not flagged**, `engagement_decay_scalar=1.00`.
- Tracked history shorter than `MIN_BASELINE_HISTORY_DAYS` (recent `first_event_at`) → **not flagged**.
- **Launch-flood test:** an old account (`created_at` months ago) with **zero** `webhook_events` → **not flagged** (history-based guard, not age-based). This is the key regression test.
- Very active user with near-zero baseline → stored scalar clamped to 9.99 (no overflow).
- Duplicate-run guard: running twice in the same week creates **one** `churn_defense_leads` row per subscriber (cooldown).

**Backend integration:**
- Apply migration to dev DB → confirm tables + indexes.
- Seed `webhook_events` for a test subscriber: high 30-day `DASHBOARD_VIEW`/`LEAD_DOWNLOAD` volume, near-zero in the last 7 days → run worker (no `--dry-run`, GHL/Claude mocked) → assert exactly one snapshot row and one `churn_defense_leads` row advancing to `SEQUENCE_TRIGGERED`, `risk_score` ∈ (0,1).
- Seed a normal-activity subscriber and an insufficient-history subscriber → assert neither is flagged.

**Frontend manual:** the devtools + `webhook_events` checks in the "done-when" sections above.

**Full loop:** ship backend → ship frontend → let ≥30 days of events accumulate → confirm real snapshots populate and only genuinely-collapsed accounts flag.

---

## 6. Logical-holes / edge-case audit (all handled above)

| Risk | Handling |
|---|---|
| New event types silently dropped by allow-list | Step B3 adds all three to `BUSINESS_EVENT_TYPES` |
| Events land with `subscriber_id = NULL` (uncountable) | Every frontend call passes `feedUuid`; worker filters `subscriber_id IS NOT NULL` |
| `DASHBOARD_VIEW` inflated by re-renders | `useRef` guard fires once per mount (F2) |
| Divide-by-zero in decay | `+1` denominator guard |
| `numeric(3,2)` overflow on high decay | Clamp `engagement_decay_scalar` to 9.99 (B4/B5) |
| False positives for brand-new / never-engaged accounts | History-based cold-start guard: skip flagging when `baseline_30day_mean == 0` or tracked history < 30 days (NOT account age) |
| **Launch flood** — every established account has zero tracked events on first run → all flagged | Same history-based guard: zero baseline ⇒ not flagged, so the first ~30 days produce zero flags |
| Duplicate outreach on consecutive weekly runs | `OUTREACH_COOLDOWN_DAYS` dedupe check before firing |
| Outreach to already-churned users | Worker scores only `churned_at IS NULL` subscribers |
| Subscriber has no purchased ZIP / no property | Log WARNING, skip pitch, still fire GHL |
| Claude/GHL call failure aborting the whole batch | Per-subscriber try/except; batch continues |
| No signal for ~30 days after launch | Documented operational reality; history-based cold-start guard makes it safe |
| Alembic CLI unusable on multi-head tree | Idempotent DDL + standalone apply script (fa_s1 precedent) |

---

## 7. File change summary

**Backend (`Forced-action-`)**
- `alembic/versions/fa_6_3_churn_defense.py` — NEW
- `scripts/apply_fa_6_3_churn_defense.py` — NEW
- `src/core/models.py` — add 2 models
- `src/services/business_events.py` — add 3 strings to `BUSINESS_EVENT_TYPES`
- `config/churn_defense.py` — NEW
- `src/tasks/churn_defense_engagement_decay.py` — NEW
- `scripts/cron/crontab.txt` — add 1 line

**Frontend (`Forced-action-ui`, branch `dev`)**
- `src/hooks/useSubscriberAuth.js` — fire `SUBSCRIBER_LOGIN`
- `src/pages/DashboardPage.jsx` — fire `DASHBOARD_VIEW` (ref-guarded) + pass `feedUuid` to `ExportButton`
- `src/components/dashboard/ExportButton.jsx` — accept `feedUuid` prop, fire `LEAD_DOWNLOAD`
