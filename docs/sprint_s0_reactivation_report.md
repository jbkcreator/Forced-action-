# Sprint S0 — Reactivation Scheduler: Task Completion Report

---

## Objective

Automated reactivation of waitlist subscribers via geo-triggered outreach — notifying waiting entries when a county launches (county-live) or when a sold-out ZIP becomes available again (sold-out).

---

## Deliverables

| Artifact | Path | Status |
|---|---|---|
| Pre-scheduler foundation migration | `alembic/versions/fa_s0_reactivation_foundation.py` | ✅ Done (source missing — .pyc only) |
| County-live reactivation task | `src/tasks/county_live_reactivation.py` | ✅ Done |
| Sold-out reactivation task | `src/tasks/sold_out_reactivation.py` | ✅ Done |
| E2E seed + cleanup script | `tests/seeds/seed_reactivation_s0.py` | ✅ Done |
| Foundation test suite | `tests/test_reactivation_foundation.py` | ✅ Done (.pyc only — source missing) |
| Scheduler test suite | `tests/test_reactivation_scheduler.py` | ✅ Done (.pyc only — source missing) |

> **Note:** `src/services/geo_interest.py`, `src/services/reactivation_eligibility.py`, and `src/tasks/reactivation_scheduler.py` have compiled `.pyc` artifacts but no source files. The production implementation uses `county_live_reactivation.py` and `sold_out_reactivation.py` directly.

---

## Architecture

The two cohorts are implemented as independent modules, not a unified scheduler:

```
[CRON: every 5 min]
│
▼
county_live_reactivation.run_county_live_reactivation()
│
└─ ExpansionCandidate (status='launched')
       └─ WaitlistEntry (waitlist_type='coming_soon', status='waiting')
            ├─ SMS: can_send() → send_sms() → entry.status='notified', entry.notified_sms_at
            └─ Email: send_email() → entry.status='notified', entry.notified_email_at


[EVENT-DRIVEN — called from ZipTerritory unlock path]
│
▼
sold_out_reactivation.reactivate_for_zip(zip_code, vertical, county_id)
│
└─ WaitlistEntry (waitlist_type='sold_out', status='waiting')
       └─ SMS only: can_send() → send_sms() → entry.status='notified'
                                              entry.reactivation_decision_id = UUID
            └─ mark_sold_out_losers() — called when ZIP re-locks
```

---

## Business Requirement Mapping

### Requirement 1 — Segment leads into two target cohorts

#### County-Live Reactivations

> Notify waiting subscribers when a county they were waiting on launches.

**Implementation:**

`run_county_live_reactivation()` queries all `ExpansionCandidate` rows with `status='launched'` (no recency window — processes all launched counties each run), then for each county fetches all `WaitlistEntry` rows with `waitlist_type='coming_soon'` and `status='waiting'`:

```python
entries = db.execute(
    select(WaitlistEntry).where(
        WaitlistEntry.county_id == county_id,
        WaitlistEntry.status == "waiting",
        WaitlistEntry.waitlist_type == "coming_soon",
    )
).scalars().all()
```

Phone dedup is applied within each county: entries are grouped by `phone_e164` via a `defaultdict` so one phone number receives at most one SMS per run, regardless of how many vertical interests it has.

**Eligibility gates applied per entry:**

| Gate | Implementation |
|---|---|
| Has phone | `entry.phone_e164` is not None |
| SMS opted in | `entry.sms_opt_in` is True |
| Send compliance | `can_send(phone, db)` — TCPA quiet hours + opt-out check |

Email fallback applies to entries with no phone or no SMS opt-in: calls `src.services.email.send_email()` directly.

**Scarcity label:** computed live from `zip_territories` at send time via `compute_slots_remaining()`:

| `slots_remaining` | Label in message |
|---|---|
| > 20 | `"limited spots"` |
| 1–20 | `"{n} left"` |
| 0 | `"just opened"` |

**Message template (county-live SMS):**
> `{name}, {county_id} is LIVE! You waited {wait_label}. {scarcity} for {vertical}. Lock yours: https://forcedactionleads.com`

---

#### Sold-Out Reactivations

> Notify all waiting sold-out entries when a ZIP becomes available.

**Implementation:**

`reactivate_for_zip(zip_code, vertical, county_id)` is called event-driven from the `ZipTerritory` unlock path — not from cron. It is not a periodic scan. It queries `WaitlistEntry` with `waitlist_type='sold_out'` and `status='waiting'` for the specific `(zip_code, vertical, county_id)` tuple:

```python
entries = db.execute(
    select(WaitlistEntry).where(
        WaitlistEntry.zip_code == zip_code,
        WaitlistEntry.vertical == vertical,
        WaitlistEntry.county_id == county_id,
        WaitlistEntry.waitlist_type == "sold_out",
        WaitlistEntry.status == "waiting",
    )
).scalars().all()
```

All matching entries are notified simultaneously — first to claim wins. A `decision_id` (UUID) is stamped on each notified entry via `entry.reactivation_decision_id` to scope the wave for downstream `mark_sold_out_losers()`.

**Eligibility gates applied per entry:**

| Gate | Implementation |
|---|---|
| Has phone | `entry.phone_e164` is not None |
| SMS opted in | `entry.sms_opt_in` is True |
| Send compliance | `can_send(phone, db)` |

No email fallback for sold-out — SMS only.

**Message template (sold-out SMS):**
> `{name}, 1 slot just opened for {zip_code} {vertical} in {county_id}. {n} were waiting. Lock it: https://forcedactionleads.com?zip={zip_code}  Reply STOP to opt out.`

**Loser marking:** once a ZIP re-locks, `mark_sold_out_losers(zip_code, vertical, county_id, decision_id)` transitions all `status='notified'` entries from that wave to `status='lost'`:

```python
result = db.execute(
    update(WaitlistEntry).where(*conditions).values(status="lost")
)
```

---

### Requirement 2 — Outreach channel routing

**County-live:**

| Priority | Channel | Condition |
|---|---|---|
| 1 | SMS via Telnyx | `entry.sms_opt_in AND entry.phone_e164 IS NOT NULL` |
| 2 | Email | `phone IS NULL OR sms_opt_in IS False` |

**Sold-out:**

| Priority | Channel | Condition |
|---|---|---|
| 1 | SMS via Telnyx | `entry.sms_opt_in AND entry.phone_e164 IS NOT NULL` |

All SMS sends route through `sms_compliance.can_send()` before transmission. Campaign tag: `"county_live_reactivation"` (both cohorts use this tag).

---

### Requirement 3 — Dispatch tracking

Tracking is done at the `WaitlistEntry` level, not via a `MessageOutcome` table:

| Event | Column written |
|---|---|
| SMS sent (county-live) | `entry.notified_sms_at = now`, `entry.status = "notified"` |
| Email sent (county-live) | `entry.notified_email_at = now`, `entry.status = "notified"` |
| SMS sent (sold-out) | `entry.notified_sms_at = now`, `entry.reactivation_decision_id = UUID`, `entry.status = "notified"` |
| ZIP re-locks after sold-out wave | `entry.status = "lost"` (via `mark_sold_out_losers`) |

There is no `last_reactivation_attempt_at` cooldown on the `subscribers` table. Idempotency for county-live is provided by the `status='waiting'` filter — entries already notified have `status='notified'` and are excluded from subsequent runs.

---

## Schema Changes

**`waitlist_entries` table — columns used by this sprint:**

| Column | Type | Purpose |
|---|---|---|
| `waitlist_type` | `VARCHAR` | `'coming_soon'` (county-live) or `'sold_out'` |
| `status` | `VARCHAR` | `waiting` → `notified` → `lost` |
| `notified_sms_at` | `TIMESTAMPTZ NULL` | Stamped on SMS dispatch |
| `notified_email_at` | `TIMESTAMPTZ NULL` | Stamped on email dispatch |
| `reactivation_decision_id` | `UUID NULL` | Wave scoping for sold-out loser marking |
| `sms_opt_in` | `BOOLEAN` | SMS gate |
| `phone_e164` | `VARCHAR NULL` | E.164 normalised phone |

**`gold_plus_zip_snapshots` table** — created by the foundation migration and seeded by `seed_reactivation_s0.py`, but not consumed by either reactivation task in the current implementation.

---

## Performance Design

| Decision | Rationale |
|---|---|
| `by_phone` dict dedup in county-live | O(1) membership test prevents one phone number receiving multiple county-live SMS in a single run when the same subscriber has multiple vertical interests |
| `status='waiting'` filter | DB-level exclusion of already-notified entries — no application-layer cooldown needed |
| Event-driven sold-out | Avoids scanning all waitlist entries on a schedule; fires only when a territory actually unlocks |
| `mark_sold_out_losers` batch UPDATE | Single `UPDATE ... WHERE status='notified'` per decision_id instead of per-row updates |

---

## CLI

```bash
# County-live — process all currently-launched counties
python -m src.tasks.county_live_reactivation

# Sold-out — notify waitlist for a specific ZIP/vertical
python -m src.tasks.sold_out_reactivation --zip-code 33701 --vertical roofing --county-id pinellas
```

**E2E seed harness:**
```bash
python tests/seeds/seed_reactivation_s0.py --seed
python tests/seeds/seed_reactivation_s0.py --run-test          # dry-run
python tests/seeds/seed_reactivation_s0.py --run-test --live   # real send
python tests/seeds/seed_reactivation_s0.py --cleanup
```

---

## Cron Placement

```
*/5 * * * *   county_live_reactivation    ← fires within 5 min of any county launch
*/15 * * * *  county_waitlist_notifier    ← companion: T+0 bulk notify on launch
*/15 * * * *  county_launch_runner        ← upstream: executes approved launches

sold_out_reactivation                     ← event-driven, not in cron
                                            called from ZipTerritory unlock path
```
