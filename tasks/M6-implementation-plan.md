# M6 — Lead Quality Truth Engine — Implementation Plan (Dev C / C1)

> Final deliverable on approval: this content is saved to `tasks/M6-implementation-plan.md`.
> Primary spec reference: `tasks/FA-v4-Phase2-StreamA-S1-Spec-FINAL.docx` (§3.1, §3.1a, §4.5, §5, §8A, §8B, §9, §11, §12.1, §12.4, §12.5) + task card C1 in `tasks/FA_414_Task_Cards-md.md`.
> Source of truth for current state: live DB (`distress_db`) + source code on branch `feat/414-s1-m1-backbone` (M1 backbone) and the working branch.

---

## 1. Context — why this is being built

The platform's "best" leads are barely reachable (~1–3% phone contactability on Gold+). A high distress score alone is therefore a misleading signal. **M6 is the arbiter that reconciles claimed quality (CDS score) against realized reachability (contactability) and turns an enriched prospect into a graded, routed, revenue-ready lead.** It produces an *explainable* verdict (grade + reasons + routed channel), surfaces the quality-vs-reachability gap instead of hiding it, and is the hard prerequisite for Dev B's M10 lead delivery (delivery cannot route a lead it has not been graded).

M6 does **not** compute the CDS score — it reads it. It does **not** create, merge, or mutate prospects — that is exclusive to `prospect_service`/the cascade. It reads two inputs, applies a config-driven threshold table, writes a verdict, and emits one event.

---

## 2. Dependencies & current state (verified against DB + code)

### 2.1 Hard dependency: M1 backbone (branch `feat/414-s1-m1-backbone`)
The working branch does **not** yet contain the Prospect/Event models or services. They exist on `feat/414-s1-m1-backbone` and are applied to the live DB (alembic head = `fa090_events_prospect_id_not_null`). M6 imports these contracts.

**Development workflow (chosen): branch M6 *off* Dev A's branch — do NOT merge Dev A's unfinished branch into anything.** Dev A is still committing to `feat/414-s1-m1-backbone`.
```
git fetch origin
git checkout feat/414-s1-m1-backbone
git checkout -b feat/414-s1-m6-truth-engine   # all M6 work here
# periodically pull Dev A's updates:
git merge origin/feat/414-s1-m1-backbone        # (or rebase)
```
M6 is almost entirely additive (new files + an append to `models.py` + a one-line mount in `main.py` + new migrations), so conflicts with Dev A's ongoing work stay small. Merge order is preserved: because the M6 branch is based on Dev A's, when Dev A merges to main, Dev C rebases onto main and M6 drops in on top. (Branch merge/push handled by the user.)

**Coordinate these with Dev A as a frozen interface (ping if changed):**
1. `get_prospect()` return dict keys: `property_id`, `contactability_state`, `contactability_rate`, `enriched_contact{source,confidence,mobile}`, `channel_consent`.
2. `emit_event()` signature + the `events.event_type` CHECK must keep allowing `truth.verdict`.
3. Columns M6/cohort job read: `prospects.property_id`, `distress_scores.final_cds_score`, `distress_scores.lead_tier`, `distress_scores.score_date`, `distress_scores.county_id`, `enriched_contacts.source`.
4. **Migration numbering** — agree who owns which `faNNN`; set the M6 migration's `down_revision` to Dev A's tip at rebase time and reconcile on each sync (do not hardcode a number that may collide with Dev A's new migrations).

### 2.2 CDS score source (verified)
`prospects` has **no** score column — only `property_id` (int, FK → `properties.id`, unique). CDS lives in `distress_scores` (live: 80,014 rows; one row per property per day). M6 reads the **latest** row by `score_date`. Columns: `final_cds_score` (Numeric, **0–100**), `lead_tier`, `county_id`, `vertical_scores` (jsonb), `factor_scores` (jsonb). Existing read patterns to mirror: `cds_engine.get_latest_score_for_property()` (`src/services/cds_engine.py:2378`), `src/tasks/contact_freshness_sweep.py:93`. **New code uses `session.execute(text(...))` per the repo SQL rule.** Because `distress_scores` is well-populated, nearly every enriched prospect has a score — the "no score" branch is a rare edge case (§12).

### 2.3 Contactability (verified — critical)
`prospects.contactability_rate` is a **Postgres GENERATED column**: `CASE WHEN contact_attempts >= 5 THEN successful_contacts::numeric/contact_attempts ELSE NULL END` (persisted). Pre-launch there is zero outreach, so it is **NULL for every prospect**; it only populates in a future outbound-calling phase. `contactability_state` (`unknown|enriching|contactable|invalid|exhausted`) is the signal available now; the cascade (Dev A) sets it (`contactable` on `enrichment.completed`, `exhausted` on `enrichment.failed`). `contact_attempts`, `successful_contacts`, `cohort_key` exist on `prospects` — `cohort_key` is currently NULL and **left untouched** (the cohort job computes its key in-query).

### 2.4 What already exists vs. new
- **Already built:** Prospect/Event/ProcessedEvent backbone, event bus helpers, `get_prospect`, CDS engine + `distress_scores`, `LEAD_TIER_THRESHOLDS` (`config/scoring.py:344`, 0–100), system-cron task framework (`scripts/cron/crontab.txt`, `run.sh`, `get_db_context`), API/auth (`get_current_admin`), test fixtures (`tests/conftest.py`: `mock_db`, `in_memory_db`, `fresh_db`, `pg_engine`).
- **Completely new (this task):** `verdicts` table+model; `grade_thresholds` table+model+seed; `cohort_rates` table+model; `config/grading.py`; `src/services/truth_engine.py`; `src/tasks/truth_engine_batch.py` + crontab; `src/tasks/cohort_rate_recompute.py` + crontab; `src/api/verdict_router.py` (+mount); tests. Confirmed absent today: no truth engine / verdict / grade_threshold / cohort_rates / routed_channel / `config/grading.py`.

---

## 3. Confirmed design decisions (locked with product owner)

1. **CDS source** → read `distress_scores` directly by `property_id`, latest `score_date` (0–100). No reliance on a `cds.scored` event (none is emitted).
2. **CDS scale** → store thresholds on **0–100** to match `final_cds_score`; record the spec's original 0–1 values in a `notes` column.
3. **Contactability resolution (3-tier)** → (a) per-prospect `contactability_rate` when ≥5 attempts; (b) else the **cohort rate** (`cohort_rates`, built now); (c) else the `contactability_state` gate. The numeric `contactability_min` floor applies whenever a rate (per-prospect or cohort) exists. In S1 there are zero attempts, so tiers (a)/(b) are empty and the state gate is the effective path — the full rate machinery is wired and activates automatically when attempt data arrives.
4. **Cohort key** → `grade_band = the CDS lead_tier` from `distress_scores` (independent of the verdict, to break the §12.1 circularity): `cohort_key = lead_tier | county_id | enrichment_source`, computed **in-query** (never stamped onto `prospects`).
5. **Trigger** → **daily batch cron** consuming `enrichment.completed` + `enrichment.failed` events idempotently (consumer `truth_engine`). Not real-time.
6. **Zero prospect writes** → M6 and the cohort job never mutate `prospects` (not `contactability_state`, not `cohort_key`). No-score leads are held via the unprocessed event (retried next run).

---

## 4. Deviations from the spec (explicit, with justification)

| Spec wording | S1 behavior | Justification |
|---|---|---|
| §3.1a/§12.1: grade gated on a contactability **rate (%)** | 3-tier resolution (per-prospect rate → cohort rate → state); numeric floor applies whenever a rate exists | The rate machinery (per-prospect + cohort) is **built** (§5.3/§8.2), but S1 has zero attempts so both rate tiers are empty and `contactability_state` is the effective gate. Floor activates automatically when attempt data arrives — no further code change. The state tier is an S1 bootstrap fallback (spec assumes a cohort rate always exists; in S1 it doesn't). |
| §12.5 / TABLE 21: on missing CDS score "hold lead in **enriching**" | M6 leaves `contactability_state` untouched; holds via the unprocessed event (retried next run) | The cascade already set the state correctly (usually `contactable`) before M6 runs; overwriting to `enriching` would regress a correct value and make the next-run retry mis-grade the lead `sub_grade`. The pending event is the durable hold. Also respects prospect-mutation ownership (§12.6). |
| §189: "enrich-to-graded within **minutes**" | Up to ~24h (daily batch) | Pre-launch volume is low; daily batch fits the existing cron architecture. Mitigation if needed: run the verdict batch 2–3×/day. |

**Two future-phase integration caveats (recorded in code comments + here):**
- **Caveat A — state backfill must be visible to the event-driven batch.** If `contactability_state` is later backfilled for already-enriched prospects via a bulk `UPDATE` (no event), M6 won't see them. The backfill must emit one `enrichment.completed` per prospect, **or** M6 needs a one-time fallback scan.
- **Caveat B — rate/cohort changes need a re-grade trigger.** A prospect graded today is not re-graded when its rate/cohort later changes (no new enrichment event fires). The grading *function* uses the rate correctly when called; the future rate phase must add a re-grade trigger (a `contactability.updated` event M6 also consumes, or a periodic re-grade pass hooked into the nightly cohort job). Also covers spec §133 ("downgrades on next pass").

---

## 5. DB schema changes

All schema via a single Alembic migration. **Run `alembic heads` after the M1 rebase**; chain `down_revision` to Dev A's tip (currently `fa090_events_prospect_id_not_null`); add a merge migration first if a parallel head exists. UUID PKs use `server_default=text("generate_uuidv7()")` (confirmed present in DB) to match the M1 backbone.

### 5.1 New table: `verdicts` (spec TABLE 10 / §4.5)
| Column | Type | Null | Default | Constraints / Notes |
|---|---|---|---|---|
| `verdict_id` | `UUID` | NO | `generate_uuidv7()` | PK |
| `prospect_id` | `UUID` | NO | — | FK → `prospects.prospect_id`; index |
| `grade` | `varchar` | NO | — | CHECK in (`Ultra`,`Platinum`,`Gold`,`Silver`,`Bronze`,`sub_grade`) |
| `contributing_factors` | `jsonb` | NO | `'{}'::jsonb` | explainable evidence (§7.3) |
| `contactability_flag` | `bool` | NO | `false` | TRUE when a rate pulls a Gold+ CDS-band lead down (§7 step 6) |
| `routed_channel` | `varchar` | NO | — | CHECK in (`loan_lane`,`contractor_subscription`,`storm_retainer`,`data_pack_bulk`,`free_hand_delivered`,`recycle_suppress`) |
| `created_at` | `timestamptz` | NO | `NOW()` | audit (§187) |

Indexes: `idx_verdicts_prospect_id`, `idx_verdicts_created_at`. Append-only (latest by `created_at` is current). Relates to `prospects` (many verdicts → 1 prospect).

### 5.2 New table: `grade_thresholds` (spec §3.1a — config, tunable without deploy per §190)
| Column | Type | Null | Default | Constraints / Notes |
|---|---|---|---|---|
| `id` | `int` | NO | identity | PK |
| `grade` | `varchar` | NO | — | UNIQUE; CHECK in (`Ultra`,`Platinum`,`Gold`,`Silver`,`Bronze`,`sub_grade`) |
| `cds_min` | `int` | YES | — | inclusive lower bound (0–100); NULL = no lower bound (sub_grade) |
| `cds_max` | `int` | YES | — | inclusive upper bound (0–100); NULL = no upper bound (Ultra) |
| `contactability_min` | `numeric(5,4)` | YES | — | 0–1 floor; NULL = no floor (Bronze/sub_grade). Dormant until a rate exists. |
| `requires_mobile_consent` | `bool` | NO | `false` | TRUE for Ultra (spec TABLE 4) |
| `notes` | `text` | YES | — | records spec's original 0–1 values |
| `is_active` | `bool` | NO | `true` | grading reads only active rows |
| `updated_at` | `timestamptz` | NO | `NOW()` | |

**Seed rows** (0–100 CDS / 0–1 contactability):

| grade | cds_min | cds_max | contactability_min | requires_mobile_consent |
|---|---|---|---|---|
| Ultra | 85 | NULL | 0.40 | true |
| Platinum | 70 | 84 | 0.25 | false |
| Gold | 50 | 69 | 0.12 | false |
| Silver | 30 | 49 | 0.05 | false |
| Bronze | 15 | 29 | NULL | false |
| sub_grade | NULL | 14 | NULL | false |

> **Note:** M6's `grade` is distinct from the CDS engine's `lead_tier` (CDS Gold≥55 vs M6 Gold 50–69; CDS "Ultra Platinum" vs spec "Ultra"). M6 grades off the raw `final_cds_score` number — `lead_tier` is used only as the cohort `grade_band` (§5.3).

### 5.3 New table: `cohort_rates` (spec §12.1 cohort fallback)
| Column | Type | Null | Default | Constraints / Notes |
|---|---|---|---|---|
| `cohort_key` | `varchar` | NO | — | PK. Format `{lead_tier}|{county_id}|{source}` |
| `contact_attempts` | `int` | NO | `0` | summed rolling-30d attempts across the cohort |
| `successful_contacts` | `int` | NO | `0` | summed rolling-30d successes across the cohort |
| `contactability_rate` | `numeric(5,4)` | YES | — | `successful/attempts`; NULL when attempts = 0 |
| `sample_size` | `int` | NO | `0` | # prospects in the cohort |
| `computed_at` | `timestamptz` | NO | `NOW()` | last recompute |

No FK (composite derived key). Written/refreshed by the nightly job (§8.2). Consumed read-only by M6 (§7 step 3b).

### 5.4 Zero schema ALTERs and zero prospect writes
M6/the cohort job read `prospects`, `events`, `processed_events`, `distress_scores`, `properties`, `enriched_contacts`. **Nothing we build writes to `prospects`** — the cohort job computes the cohort key **in-query** (join `distress_scores` + `enriched_contacts`) rather than stamping `prospects.cohort_key`. `prospect_id` FK assumes the M1 rebase is in place.

---

## 6. New config: `config/grading.py`
Mirrors the constant style of `ROUTING_THRESHOLDS` in `config/scoring.py`.
```python
GRADE_CHANNEL_ROUTING: dict[str, list[str]] = {
    "Ultra":     ["loan_lane", "contractor_subscription"],
    "Platinum":  ["loan_lane", "contractor_subscription"],
    "Gold":      ["contractor_subscription", "storm_retainer"],
    "Silver":    ["data_pack_bulk"],
    "Bronze":    ["free_hand_delivered"],
    "sub_grade": ["recycle_suppress"],
}
GRADE_ORDER = ["sub_grade", "Bronze", "Silver", "Gold", "Platinum", "Ultra"]  # ascending

def primary_channel(grade: str) -> str: ...   # first entry; default "recycle_suppress"

def compute_cohort_key(cds_lead_tier: str, county_id: str, source: str) -> str:
    """grade_band = CDS lead_tier (independent of the verdict — breaks §12.1 circularity)."""
    return f"{cds_lead_tier or 'unknown'}|{county_id or 'unknown'}|{source or 'unknown'}"
```
`verdicts.routed_channel` stores the **primary** channel; the full ordered list goes in `contributing_factors.routed_channels` (preserves §8A's multi-route intent).

---

## 7. Core service: `src/services/truth_engine.py`

### 7.1 `grade_prospect(session, prospect_id, *, actor="truth_engine") -> dict | None`
1. `p = get_prospect(session, prospect_id)`; if `None` → return `None`.
2. Read latest CDS for `p["property_id"]`: `text("SELECT final_cds_score, lead_tier, county_id, vertical_scores, factor_scores FROM distress_scores WHERE property_id=:pid ORDER BY score_date DESC LIMIT 1")`.
   - If no row or `final_cds_score IS NULL` → return `{"held": True, "reason": "no_cds_score"}`. Caller does **not** mark the event processed; **no** prospect mutation. (Rare — see §2.2.)
3. **Resolve realized contactability (3-tier):**
   - (a) `p["contactability_rate"]` not None (≥5 attempts) → `(rate, "per_prospect")`;
   - (b) else `cohort_key = compute_cohort_key(lead_tier, county_id, source)`; look up `cohort_rates`; row with `contact_attempts > 0` → `(cohort_rate, "cohort")`;
   - (c) else → `(None, "state_only")` (S1 path).
4. Load active `grade_thresholds`.
5. **Assign grade** on the ordered scale `GRADE_ORDER` (`sub_grade < Bronze < Silver < Gold < Platinum < Ultra`):
   - `cds_band_grade` = grade whose `[cds_min, cds_max]` contains the score.
   - `contactability_grade`:
     - rate present (3a/3b): highest grade whose `contactability_min` the rate meets (Bronze floor is NULL = always met, so a scored lead never drops below Bronze on contactability alone).
     - no rate (3c): `= cds_band_grade` when `contactability_state == 'contactable'`; else `sub_grade` (covers exhausted/failed, §5/§130).
   - **`final_grade = min(cds_band_grade, contactability_grade)`** — contactability can only pull the grade **down**, never promote above the CDS band (enforces §3.1a "both must hold").
   - **Ultra cap** (`requires_mobile_consent`): if `final_grade == Ultra` but no validated mobile (`enriched_contact.mobile`) + `channel_consent` (sms or call), cap at Platinum.
6. **`contactability_flag = (rate is not None) and (cds_band_grade in {Gold,Platinum,Ultra}) and (final_grade < cds_band_grade)`** — a numeric rate pulled a Gold+-quality lead down (§4.5/§376 "claims quality but contactability low"). The lead is **not** suppressed — it routes to its `final_grade` channel (§5 "do not suppress, flag it"). In pure S1 (no rates) this never fires; exhausted leads are handled by the sub_grade path, not this flag.
7. `routed_channel = primary_channel(final_grade)`.
8. Build `contributing_factors` (§7.3); insert `Verdict` via ORM `session.add(...)`.
9. `emit_event(session, event_type="truth.verdict", actor=actor, source_component="truth_engine", prospect_id=prospect_id, payload={"verdict_id":..., "grade":final_grade, "routed_channel":..., "contactability_flag":...})`.
10. Return verdict dict. **Caller commits** → verdict row + event are one atomic transaction (outbox).

### 7.2 Pure helper `assign_grade(score, rate, state, has_mobile_consent, thresholds) -> (grade, flag)`
DB-free, unit-testable in isolation (implements steps 5–6).

### 7.3 `contributing_factors` JSON (§4.5 "store the factors, never just a number")
`{ cds_score, cds_scale:"0-100", cds_lead_tier, cds_band_grade, vertical_scores, factor_scores, contactability_state, contactability_rate, contactability_source ("per_prospect"|"cohort"|"state_only"), cohort_key, enriched_contact:{source,confidence}, routed_channels:[...], thresholds_grade_id, graded_at }`.

---

## 8. Scheduled tasks (system cron)

### 8.1 Daily verdict batch: `src/tasks/truth_engine_batch.py` (+ crontab)
Mirrors existing task modules (`run_*()` + `__main__`, `get_db_context`, `src.utils.logger`).
`run_truth_engine_batch(dry_run=False) -> dict`:
1. Select unprocessed trigger events:
   ```sql
   SELECT e.event_id, e.prospect_id, e.event_type
   FROM events e
   WHERE e.event_type IN ('enrichment.completed','enrichment.failed')
     AND NOT EXISTS (SELECT 1 FROM processed_events pe
                     WHERE pe.event_id=e.event_id AND pe.consumer='truth_engine')
   ORDER BY e.occurred_at
   ```
   (Efficient: `processed_events` PK `(event_id,consumer)`; `events` has `idx_events_type`/`idx_events_occurred_at`.)
2. Per event (try/except, count errors, continue):
   - `enrichment.failed` (exhausted) → write a `sub_grade`/`recycle_suppress` verdict **directly, without a CDS-score lookup** (an unreachable lead is suppressed regardless of distress score, §5/§130) + emit `truth.verdict`; `mark_processed`.
   - `enrichment.completed` → `result = grade_prospect(session, prospect_id)`.
     - If `result` is "held" (no CDS score) → **do not** `mark_processed` (retry next run); continue.
     - Else `mark_processed(session, event_id, "truth_engine")`.
   - Commit per event (or small batches) so one bad row doesn't roll back the run.
3. Log `{scanned, graded, held, suppressed, errors}`; return dict.

Cron (after CDS 07:00, skip-trace 07:30, cohort recompute 07:45) — add to `scripts/cron/crontab.txt`:
```
# Truth Engine — daily verdict batch (08:15 UTC, Mon-Sat)
15 8 * * 1-6 $PROJECT/scripts/cron/run.sh src.tasks.truth_engine_batch
```

### 8.2 Nightly cohort recompute: `src/tasks/cohort_rate_recompute.py` (+ crontab)
`run_cohort_rate_recompute(dry_run=False) -> dict`:
1. **No prospect writes.** Aggregate into `cohort_rates` (`INSERT … ON CONFLICT (cohort_key) DO UPDATE`) by computing the cohort key **in-query**: join each prospect to its latest `distress_scores` (`lead_tier`, `county_id`) and best `enriched_contacts.source`, `GROUP BY` the composite key; write `SUM(contact_attempts)`, `SUM(successful_contacts)`, derived `contactability_rate` (NULL when attempts=0), `sample_size`, `computed_at=NOW()`.
2. Log `{cohorts_written}`. **S1 reality:** zero attempts ⇒ every `contactability_rate` is NULL — the machinery is live, numbers fill in when outreach starts.

Cron:
```
# Cohort rate recompute — daily 07:45 UTC (after CDS+skip-trace, before verdict batch)
45 7 * * * $PROJECT/scripts/cron/run.sh src.tasks.cohort_rate_recompute
```

---

## 9. Read API: `src/api/verdict_router.py` (spec §8B `getVerdict`)
New router, admin-gated (RBAC §186 — verdicts are internal; account-scoped access is M10/delivery's concern), mounted in `src/api/main.py` via `app.include_router(verdict_router)`.
```
GET /api/admin/verdicts/{prospect_id}
  -> { grade, contributing_factors, routed_channel, contactability_flag }  (latest verdict)
  404 {"detail":"verdict not found"} if none
```
- `db: Session = Depends(get_db)`, `_admin: dict = Depends(get_current_admin)` (pattern from `src/api/admin_router.py`).
- `text("SELECT ... FROM verdicts WHERE prospect_id=:pid ORDER BY created_at DESC LIMIT 1")`.
- Optional `POST /api/admin/verdicts/{prospect_id}/grade` (admin) to force on-demand grade+commit (for §8A acceptance / ops); returns the verdict or `409 {"detail":"held: no CDS score"}`. actor = `f"admin:{_admin['sub']}"`.
- Error shape `{"detail": "..."}`, correct status codes (404/409/422).

---

## 10. Tests: `tests/test_truth_engine.py` (+ `tests/test_cohort_rates.py`)
Pure `assign_grade` unit tests + service/endpoint via `fresh_db`/`pg_engine`; endpoint test uses `app.dependency_overrides` for `get_db` + `get_current_admin` (pattern from `tests/test_affiliate_ledger.py`).
- **§8A worked example (headline):** `assign_grade(score=72, rate=0.28, state="contactable", has_mobile_consent=True)` → `("Platinum", False)`; `routed_channel="loan_lane"`.
- **S1 state path:** `score=72, rate=None, state="contactable"` → `Platinum` (floor dormant, `min` = CDS band).
- **Cohort fallback (tier b):** `rate=None`, `cohort_rates` row attempts>0 rate=0.30 → grading uses 0.30.
- **Contactability pulls down + flag:** `score=72 (Platinum band), rate=0.05` → `Silver` (0.05 meets Silver floor), `flag=True`, not suppressed.
- **Contactability never promotes:** `score=40 (Silver band), rate=0.90` → `Silver` (min), not Ultra.
- **Enrichment failed/exhausted:** event `enrichment.failed` → `sub_grade`, `recycle_suppress`, no CDS lookup required.
- **Ultra cap:** `score=90, rate=0.45`, no mobile/consent → `Platinum`.
- **Sub-grade:** `score=10` → `sub_grade`.
- **No CDS score:** `grade_prospect` returns `held`, writes no verdict, emits no event, prospect untouched, event NOT marked processed.
- **Idempotency:** same `enrichment.completed` processed twice → exactly one verdict.
- **`truth.verdict` emitted** with correct payload.
- **Seed round-trip:** `grade_thresholds` has 6 rows (Ultra `cds_min=85, contactability_min=0.40, requires_mobile_consent=true`; sub_grade `cds_max=14`).
- **`config/grading.py`:** all 6 grades; `primary_channel("Gold")=="contractor_subscription"`; `compute_cohort_key("Gold","hillsborough","tracerfy")=="Gold|hillsborough|tracerfy"`; `GRADE_ORDER` ascending.
- **Cohort recompute:** seed prospects with attempts across 2 cohorts → run job → `cohort_rates` sums + rate correct; zero-attempt cohort → rate NULL; **no `prospects` rows modified**.

---

## 11. Spec requirement coverage matrix (M6 scope — nothing dropped)

| Spec point | Where addressed |
|---|---|
| §4.5 / TABLE 10 Verdict schema | §5.1 |
| §3.1a / TABLE 4 grade thresholds in a config table | §5.2 (seed), §6, §7 |
| §3.1 / TABLE 3 channel routing | §6, §7 step 7 |
| §4.5 verdicts explainable (store factors) | §7.3 |
| §4.5 / §376 surface Gold+ contactability gap (flag, don't suppress) | §7 step 6 |
| Reads CDS, does not recompute | §2.2, §7 step 2 |
| Emits `truth.verdict` to bus | §7 step 9 |
| §8B `getVerdict(prospect_id)` | §9 |
| §12.1 contactability metric — per-prospect rate + cohort fallback (grade_band×county×source) + inputs | §5.3, §8.2, §7 step 3, decision 4 |
| §12.4 idempotency (dedupe on event_id, processed_events) | §8.1 step 2 |
| §12.5 / TABLE 21 CDS scorer fail → don't grade on missing score | §7 step 2 (deviation §4) |
| §5 / §130 enrichment fully fails → sub_grade, suppressed, logged | §8.1 step 2 |
| §5 / §133 high CDS but realized dead → downgrade on next pass | Caveat B |
| §5 duplicate prospect → re-point verdicts | upstream `prospect_service` merge chain (`get_prospect` resolves survivor) |
| §9/§187 audit logging (actor+timestamp, immutable) | `verdicts.created_at` + `events.actor`; append-only |
| §9/§186 RBAC | §9 admin-gated read |
| §9/§188 data retention (sub_grade retained, not deleted) | verdicts append-only; sub_grade rows kept |
| §9/§190 config over code (thresholds are data) | §5.2 table |
| §11 DoD: explainable verdict, Gold+ gap flagged, thresholds from config | §7, §10 |

---

## 12. Edge cases & risks
- **No CDS score yet** → held, retried next run; no mutation (§7 step 2). Rare — `distress_scores` has 80k rows and the batch runs after the 07:00 CDS run; only a never-scored property is held.
- **`enrichment.failed`/exhausted** → `sub_grade`/`recycle_suppress`, no CDS lookup, verdict written (audit/learning).
- **Multiple `enrichment.completed` for one prospect** (re-enrichment) → each distinct event_id yields its own append-only verdict; `getVerdict` returns the latest. Acceptable for S1.
- **Merged/duplicate prospect** → `get_prospect` returns the surviving record; verdict keyed to survivor.
- **Events table growth** → `NOT EXISTS` on `processed_events` PK is efficient at S1 volume.
- **Migration multiple heads** → `alembic heads` post-rebase; merge migration if a parallel head exists.
- **Scale mismatch (0–100 vs 0–1)** → contained: CDS on 0–100, contactability on 0–1; documented in `grade_thresholds.notes` and `config/grading.py`.
- **Cohort job NULL rates in S1** (zero attempts) → expected; machinery dormant on data, not logic.
- **Caveat A / Caveat B** — future-phase, documented in §4 + code comments.

---

## 13. Self-review & corrections (Step 4)
1. **Grade enum mismatch.** Spec verdict grade is `Ultra…sub_grade`, not CDS "Ultra Platinum". Fixed: verdict/threshold tables use the spec enum; M6 grades off the numeric score so CDS cutoffs never conflict.
2. **Missed Ultra "validated mobile + consent" gate** (TABLE 4). Fixed: `requires_mobile_consent` column + Ultra cap (§7 step 5).
3. **"Hold in enriching" vs retry correctness.** Fixed: leave-untouched; overwriting would regress state and mis-grade on retry. Hold = unprocessed event (§4, §7 step 2).
4. **Cohort circularity (§12.1).** Resolved: grade_band = CDS `lead_tier` (independent of verdict); `cohort_key = lead_tier|county|source`.
5. **Cohort fallback built now** (owner decision) via `cohort_rates` + nightly recompute + 3-tier resolution — dormant on data until attempts exist.
6. **No prospect writes (owner directive).** Removed `cohort_key` stamping; cohort job computes the key in-query. M6 + cohort job perform **zero** `prospects` mutations (incl. `contactability_state`).
7. **Grade algorithm made unambiguous.** Replaced vague "downgrade" with `final_grade = min(cds_band_grade, contactability_grade)` on an explicit `GRADE_ORDER` — contactability only pulls down, never promotes (matches §3.1a).
8. **`contactability_flag` tightened to the real gap.** Fires only when a numeric rate pulls a Gold+ CDS-band lead down, and the lead is **not** suppressed (routes to its final grade, §5). Exhausted leads use the sub_grade path.
9. **`enrichment.failed` needs no CDS score.** Fixed: failed/exhausted leads are graded `sub_grade` directly.
10. **"No CDS score" reframed as a rare edge case** (was implied common) — `distress_scores` is well-populated.
11. **Routing as constant vs table.** §190 enumerates *thresholds* as data, not routing → routing is a `config/grading.py` constant; thresholds remain a DB table.
12. **SQL style.** All new queries use `session.execute(text(...))` per repo rule.

---

## 14. Out of scope (M6 / S1)
M5 retrain wiring; M10 delivery consumption of `routed_channel`; the future re-grade trigger (Caveat B); attempt-data *production* (outbound calling that fills `contact_attempts`/`successful_contacts`); prospect creation/merge/mutation (owned by `prospect_service`/cascade); RESPA fee-config flags; multi-touch attribution.

---

## 15. Verification (end-to-end, after implementation)
1. **Migration:** after M1 rebase, `alembic heads` → single head; `alembic upgrade <m6 revision>`; verify `verdicts`, `grade_thresholds` (6 seeded rows), `cohort_rates` exist.
2. **Unit:** `pytest tests/test_truth_engine.py tests/test_cohort_rates.py` — all `assign_grade` cases incl. §8A, cohort fallback, pull-down+flag, no-promotion, idempotency, no-score-held, Ultra cap.
3. **Cohort job dry-run:** seed two cohorts with attempts → `python -m src.tasks.cohort_rate_recompute` → `cohort_rates` rows + correct rates; assert **no `prospects` rows changed**.
4. **Verdict batch dry-run:** seed a prospect with a `distress_score` + an `enrichment.completed` event → `python -m src.tasks.truth_engine_batch` → one `verdicts` row + one `truth.verdict` event + one `processed_events` row; re-run → no new rows (idempotent). Seed an `enrichment.failed` event → `sub_grade` verdict.
5. **API:** `GET /api/admin/verdicts/{prospect_id}` (admin token) returns the verdict; unknown id → 404.
6. **§8A acceptance:** prospect CDS 72 + contactable → `Platinum`, `routed_channel=loan_lane`, `contributing_factors` populated, `contactability_flag=False`.
