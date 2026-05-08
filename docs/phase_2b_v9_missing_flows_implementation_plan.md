# Phase 2B v9 Missing & Partial Flow Implementation Plan

**Document status:** v2 — critically evaluated, expanded with code paths, schema deltas, unit tests
**Primary source of truth:** Code audit (2026-05-06) + direct file reads
**Goal:** Complete Phase 2B v9 missing/partial revenue flows. No regression to implemented flows.

---

## 0. Critical Evaluation of v1 Plan

v1 plan correct in scope but thin. Gaps fixed in v2:

| Gap in v1 | Fix in v2 |
|---|---|
| No DB schema deltas | Section 2 lists every column/index/migration needed |
| Function signatures abstract (`...`) | Real signatures with types + return values |
| No idempotency strategy per flow | Section 3 defines idempotency keys per event |
| No Stripe API specifics for Pause | Section 7 specifies `pause_collection` with `behavior=void` |
| No event registration steps | Section 4 lists `EVENT_TO_GRAPH` updates needed |
| No regression check for "Implemented 7" | Section 14 verifies + adds smoke tests |
| Unit tests vague ("add tests for X") | Section 15 lists exact test cases per flow |
| Open questions left dangling | Section 16 gives recommended answer for each |
| Concurrency/race conditions ignored | Locks specified where needed (wallet uses `with_for_update()` already) |
| AP Lite "manual action" undefined | Section 6 defines: any event in `agent_decisions` where `action_blocked_reason='manual_user_action'` + lead unlock count |

---

## 1. Audit Status (Verified)

| Status | Count | Items |
|---|---:|---|
| Implemented | 7 | Territory Lock, Charter Annual Push, Data-Only Save, Cora Lock Close, Stripe Recovery, What You Missed, Gate Monitoring |
| Partial | 6 | AP Lite, ZIP Counter/Map/Waitlist (map only), Dynamic Flash Scarcity (no prod trigger), Save/Pause (pause missing), Synthflow Voice Drop (no trigger), Earlier Retention (cadence unverified) |
| Not Implemented | 3 | Wallet-to-Lock, Partner Tier, Thin Human Backup Close |

---

## 2. Cross-Cutting DB Schema Changes

Single Alembic revision recommended: `phase2bv9_missing_flows.py`. All deltas idempotent (use `op.add_column` with `IF NOT EXISTS` via `server_default` where applicable).

### 2.1 `subscribers` table

```sql
-- Wallet-to-Lock + AP Lite candidate flags
ALTER TABLE subscribers ADD COLUMN lock_candidate_zip VARCHAR(10) NULL;
ALTER TABLE subscribers ADD COLUMN lock_candidate_at TIMESTAMP NULL;
ALTER TABLE subscribers ADD COLUMN ap_lite_candidate_at TIMESTAMP NULL;

-- Pause flow
ALTER TABLE subscribers ADD COLUMN paused_at TIMESTAMP NULL;
ALTER TABLE subscribers ADD COLUMN pause_resume_at TIMESTAMP NULL;

-- Human close routing
ALTER TABLE subscribers ADD COLUMN escalation_routed_at TIMESTAMP NULL;
ALTER TABLE subscribers ADD COLUMN escalation_channel VARCHAR(20) NULL;  -- 'slack' | 'ghl' | 'sms' | 'email'

CREATE INDEX idx_sub_lock_candidate ON subscribers(lock_candidate_at) WHERE lock_candidate_at IS NOT NULL;
CREATE INDEX idx_sub_paused ON subscribers(paused_at) WHERE paused_at IS NOT NULL;
```

CHECK constraint already supports `'paused'` status (`src/core/models.py:768`). No DDL change for status enum.

### 2.2 `wallet_transactions` — add ZIP attribution

Currently `WalletTransaction` has no `zip_code` column. Required for Wallet-to-Lock detection.

```sql
ALTER TABLE wallet_transactions ADD COLUMN zip_code VARCHAR(10) NULL;
CREATE INDEX idx_wallet_txn_sub_zip_created ON wallet_transactions(subscriber_id, zip_code, created_at);
```

Backfill: optional. Future txns populate via `wallet_engine.debit(..., zip_code=)` (signature change, see 5.1).

### 2.3 New table: `partner_zips`

Multi-ZIP per partner subscriber. Existing `ZipTerritory` is one-row-per-ZIP and `subscriber_id` is single FK — fine, no change needed. Partner tier just creates N rows in `zip_territories`.

But add audit trail:

```sql
CREATE TABLE partner_subscriptions (
    id SERIAL PRIMARY KEY,
    subscriber_id INT NOT NULL REFERENCES subscribers(id),
    max_zips INT NOT NULL DEFAULT 5,
    activated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deactivated_at TIMESTAMP NULL,
    UNIQUE(subscriber_id)
);
```

### 2.4 New table: `human_close_escalations`

```sql
CREATE TABLE human_close_escalations (
    id SERIAL PRIMARY KEY,
    subscriber_id INT NOT NULL REFERENCES subscribers(id),
    decision_id VARCHAR(40) NOT NULL,
    revenue_signal_score INT NOT NULL,
    interactions_count INT NOT NULL,
    target_tier VARCHAR(20) NOT NULL,
    channel VARCHAR(20) NOT NULL,
    routed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    closer_assigned VARCHAR(80) NULL,
    outcome VARCHAR(20) NULL,  -- won | lost | no_response | rescheduled
    outcome_at TIMESTAMP NULL,
    context_json JSONB NULL,
    UNIQUE(subscriber_id, decision_id)
);
CREATE INDEX idx_hce_routed ON human_close_escalations(routed_at);
CREATE INDEX idx_hce_outcome ON human_close_escalations(outcome) WHERE outcome IS NULL;
```

### 2.5 New table: `manual_action_log` (AP Lite trigger)

```sql
CREATE TABLE manual_action_log (
    id SERIAL PRIMARY KEY,
    subscriber_id INT NOT NULL REFERENCES subscribers(id),
    action_type VARCHAR(40) NOT NULL,  -- 'lead_unlock' | 'sms_send' | 'voicemail_drop' | 'skip_trace'
    week_start DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_mal_sub_week ON manual_action_log(subscriber_id, week_start);
```

---

## 3. Cross-Cutting: Event Bus Integration

All new flows emit events through existing supervisor:
`src/agents/supervisor.py:dispatch_event()`.

Add to `EVENT_TO_GRAPH` (`src/agents/router.py:47`):

```python
"subscriber_crossed_lock_threshold": GraphSpec(
    graph_name="wallet_to_lock_close",
    runner=_run_wallet_to_lock,
),
"subscriber_crossed_ap_lite_threshold": GraphSpec(
    graph_name="ap_lite_close",
    runner=_run_ap_lite_close,
),
"high_intent_no_convert": GraphSpec(
    graph_name="synthflow_voice_drop",
    runner=_run_voice_drop,
),
"escalate_to_human_closer": GraphSpec(
    graph_name="human_close_route",
    runner=_run_human_close,
),
"flash_scarcity_window_open": GraphSpec(
    graph_name="fomo",  # reuse existing FOMO graph
    runner=_run_fomo,
),
```

Idempotency key per event:

| Event | Idempotency key |
|---|---|
| `subscriber_crossed_lock_threshold` | `wal2lock:{subscriber_id}:{zip}:{YYYY-MM}` |
| `subscriber_crossed_ap_lite_threshold` | `aplite:{subscriber_id}:{ISO_week}` |
| `high_intent_no_convert` | `voicedrop:{subscriber_id}:{YYYYMMDD}` |
| `escalate_to_human_closer` | `humanclose:{subscriber_id}:{YYYYMMDD}` |
| `flash_scarcity_window_open` | `flashscar:{zip}:{vertical}:{YYYYMMDDHHMM rounded 10min}` |

Supervisor already enforces idempotency_key dedup (verify in `src/agents/supervisor.py`).

---

## 4. Implementation Priority

### Priority 1 — Revenue-critical (Phase A)
1. Wallet-to-Lock upgrade
2. AP Lite delivery
3. Thin Human Backup Close

### Priority 2 — Product completion (Phase B)
4. Partner tier
5. Save/Pause 60-day
6. ZIP Map UI

### Priority 3 — Automation (Phase C)
7. Dynamic Flash Scarcity prod trigger
8. Synthflow outbound voice drop trigger
9. Earlier retention cadence

---

## 5. Missing Flow 1: Wallet-to-Lock Upgrade

### 5.1 Code path

**New module:** `src/services/wallet_to_lock.py`

```python
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from src.core.models import Subscriber, ZipTerritory, WalletTransaction

LOCK_THRESHOLD_CREDITS = 40
WINDOW_DAYS = 30


@dataclass
class WalletToLockCandidate:
    subscriber_id: int
    zip_code: str
    credits_used: int
    window_start: datetime


def find_candidates(db: Session) -> List[WalletToLockCandidate]:
    """Group wallet debits by (subscriber, zip) over last 30d. Return rows >= 40 credits."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    rows = db.execute(
        select(
            WalletTransaction.subscriber_id,
            WalletTransaction.zip_code,
            func.sum(func.abs(WalletTransaction.amount)).label("credits"),
        )
        .where(
            WalletTransaction.txn_type == "debit",
            WalletTransaction.zip_code.is_not(None),
            WalletTransaction.created_at >= cutoff,
        )
        .group_by(WalletTransaction.subscriber_id, WalletTransaction.zip_code)
        .having(func.sum(func.abs(WalletTransaction.amount)) >= LOCK_THRESHOLD_CREDITS)
    ).all()
    return [WalletToLockCandidate(r.subscriber_id, r.zip_code, int(r.credits), cutoff) for r in rows]


def is_already_locked(db: Session, zip_code: str, subscriber_id: int) -> bool:
    """Skip if ZIP already locked OR subscriber already on a lock+ tier."""
    sub = db.get(Subscriber, subscriber_id)
    if sub and sub.tier in ("territory_lock", "autopilot_lite", "autopilot_pro", "annual_lock", "partner"):
        return True
    zt = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code == zip_code,
            ZipTerritory.status == "locked",
        )
    ).scalar_one_or_none()
    return zt is not None


def mark_lock_candidate(db: Session, subscriber_id: int, zip_code: str) -> None:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return
    sub.lock_candidate_zip = zip_code
    sub.lock_candidate_at = datetime.now(timezone.utc)
    db.flush()


def emit_event(subscriber_id: int, zip_code: str, credits_used: int) -> None:
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event
    yyyymm = datetime.now(timezone.utc).strftime("%Y-%m")
    evt = Event(
        event_type="subscriber_crossed_lock_threshold",
        subscriber_id=subscriber_id,
        payload={"zip_code": zip_code, "credits_used": credits_used, "window_days": WINDOW_DAYS},
        source="cron",
        idempotency_key=f"wal2lock:{subscriber_id}:{zip_code}:{yyyymm}",
    )
    dispatch_event(evt.to_dispatch_dict())
```

**New cron task:** `src/tasks/wallet_to_lock_sweep.py`

```python
def run_sweep(dry_run: bool = False) -> dict:
    results = {"candidates": 0, "skipped_already_locked": 0, "events_emitted": 0}
    with get_db_context() as db:
        for cand in find_candidates(db):
            if is_already_locked(db, cand.zip_code, cand.subscriber_id):
                results["skipped_already_locked"] += 1
                continue
            results["candidates"] += 1
            if not dry_run:
                mark_lock_candidate(db, cand.subscriber_id, cand.zip_code)
                emit_event(cand.subscriber_id, cand.zip_code, cand.credits_used)
                results["events_emitted"] += 1
    return results
```

**Cron:** `0 9 * * *` (9 AM UTC, after annual_push 8 AM, before proactive_save 10 AM).

### 5.2 Required schema changes
- `WalletTransaction.zip_code` (section 2.2) — required
- `Subscriber.lock_candidate_zip`, `lock_candidate_at` (section 2.1)

### 5.3 Wallet engine signature change
`wallet_engine.debit(subscriber_id, action, db, description, zip_code=None)` — add optional `zip_code` param. Caller path in `src/api/main.py` `/api/hot-lead-unlock` route must pass ZIP from the lead being unlocked.

### 5.4 New Cora graph
`src/agents/graphs/wallet_to_lock_close.py` — clone structure from `fomo.py`. Pulls subscriber + ZIP context, builds Lock CTA with checkout link, runs through 6-step decision_hierarchy, calls compose_and_send.

### 5.5 Acceptance criteria
- [ ] 40+ credits in single ZIP over 30d → candidate found
- [ ] Already-locked ZIP → skipped
- [ ] Subscriber already on `territory_lock` tier → skipped
- [ ] Same candidate same month → idempotency_key blocks duplicate event
- [ ] Cora event payload includes `zip_code`, `credits_used`, `window_days`
- [ ] decision_id logged in `agent_decisions` table

---

## 6. Missing Flow 2 (combined w/ Partial 1): AP Lite Delivery

Critical insight: AP Lite is sold to **lock holders** (`tier='territory_lock'`) doing 10+ manual actions/week. Definition of "manual action" = explicit user action that AP Lite would automate.

### 6.1 Manual action definition

```python
# config/ap_lite.py (new)
MANUAL_ACTION_TYPES = ("lead_unlock", "sms_send", "voicemail_drop", "skip_trace")
AP_LITE_THRESHOLD_PER_WEEK = 10
```

### 6.2 Counter wiring

Logger function:

```python
# src/services/manual_action_counter.py
from datetime import date, datetime, timezone
from src.core.models import Subscriber
# new model ManualActionLog (section 2.5)

def log_action(db, subscriber_id: int, action_type: str) -> None:
    week_start = _monday_of_week(datetime.now(timezone.utc).date())
    db.add(ManualActionLog(subscriber_id=subscriber_id, action_type=action_type, week_start=week_start))
    db.flush()


def count_this_week(db, subscriber_id: int) -> int:
    week_start = _monday_of_week(datetime.now(timezone.utc).date())
    return db.execute(
        select(func.count()).select_from(ManualActionLog)
        .where(ManualActionLog.subscriber_id == subscriber_id,
               ManualActionLog.week_start == week_start)
    ).scalar() or 0
```

Call sites (instrument these):
- `src/api/main.py` `/api/hot-lead-unlock` → `log_action(..., 'lead_unlock')`
- `src/services/sample_leads_sms.py` send paths → `log_action(..., 'sms_send')`
- Any voicemail drop endpoint → `log_action(..., 'voicemail_drop')`
- Skip-trace endpoint → `log_action(..., 'skip_trace')`

### 6.3 Sweep + event emission

`src/tasks/ap_lite_sweep.py` runs `0 14 * * MON` (Mon 2 PM UTC, after weekly reset).

```python
def run_sweep(dry_run: bool = False) -> dict:
    with get_db_context() as db:
        # Lock holders only
        subs = db.execute(
            select(Subscriber).where(Subscriber.tier == "territory_lock")
        ).scalars().all()
        for sub in subs:
            n = count_last_week(db, sub.id)  # previous Mon-Sun
            if n >= AP_LITE_THRESHOLD_PER_WEEK:
                _emit("subscriber_crossed_ap_lite_threshold", sub.id, {"actions": n})
```

### 6.4 AP Lite feature gate

`src/services/ap_lite_features.py`:

```python
def is_ap_lite_enabled(sub: Subscriber) -> bool:
    return sub.tier in ("autopilot_lite", "autopilot_pro")

def auto_skip_qualifies(sub, lead) -> bool:
    if not is_ap_lite_enabled(sub): return False
    return lead.score < 30  # auto-skip cold leads
```

Hook points (gate AP Lite-only behavior):
- Daily lead email path: if AP Lite, auto-skip cold + auto-text top-3
- 3-touch sequence in retention graph
- Weekly summary cadence (already exists for retention; differentiate by tier)

### 6.5 Upgrade route
Add `POST /api/upgrade/ap-lite` to `src/api/main.py`. Validates `sub.tier == 'territory_lock'`, calls `stripe_service.switch_subscription_plan(sub.stripe_subscription_id, settings.active_stripe_price('autopilot_lite'))`, sets `sub.tier='autopilot_lite'`.

### 6.6 Acceptance criteria
- [ ] Manual action logged at every instrumented endpoint
- [ ] Sweep finds Lock holders with ≥10/week previous week
- [ ] Cora event emitted once per ISO-week per subscriber
- [ ] Upgrade route flips tier + Stripe plan
- [ ] AP Lite features active only when tier is AP Lite/Pro
- [ ] No manual action counted twice (single insert per call)

---

## 7. Missing Flow 3: Thin Human Backup Close Routing

### 7.1 Detection

`src/services/human_close_routing.py`:

```python
@dataclass
class HumanCloseCandidate:
    subscriber_id: int
    revenue_signal_score: int
    interactions_count: int
    target_tier: str  # 'autopilot_pro' | 'partner' | 'annual_lock' | 'territory_lock'
    last_decision_id: str


def find_candidates(db: Session) -> list[HumanCloseCandidate]:
    """
    Criteria:
      revenue_signal_score >= 85
      AND >=3 agent_decisions sent in last 14d for same subscriber
      AND zero deal capture in last 14d
      AND no escalation in human_close_escalations within last 7d
      AND target_tier resolves to AP Pro/Partner/Annual/Lock by tier ladder
    """
    # SQL: join subscribers with revenue_signal_score view + agent_decisions count + leftjoin escalations
    ...


def build_context(db, sub_id: int) -> dict:
    return {
        "name": ..., "phone": ..., "email": ...,
        "current_tier": ..., "zip": ..., "score": ...,
        "last_5_messages": [...],
        "proposed_offer": "...",
        "recommended_action": "Call within 24h, lead with $X offer",
    }
```

### 7.2 Routing channel (Phase A: Slack only)

```python
def route_to_slack(candidate: HumanCloseCandidate, context: dict) -> bool:
    import requests
    payload = {
        "text": f"🚨 Human close needed: {context['name']} (score {candidate.revenue_signal_score})",
        "blocks": [...],  # rich block kit with context
    }
    r = requests.post(settings.slack_human_close_webhook, json=payload, timeout=10)
    return r.status_code == 200
```

Phase B: add GHL tag `human-close-needed` via existing `src/services/ghl_webhook.py`.

### 7.3 Persistence

Insert into `human_close_escalations` (section 2.4) BEFORE Slack call. UNIQUE constraint on `(subscriber_id, decision_id)` blocks dup. Set `subscriber.escalation_routed_at` and `escalation_channel`.

### 7.4 Cron + event

Add to `src/tasks/human_close_sweep.py`. Cron `0 13 * * 1-5` (1 PM UTC weekdays — closer working hours).

Also fire from inline Cora path: when `decision_hierarchy` returns `terminal_status='escalated'`, supervisor emits `escalate_to_human_closer` event. Sweep catches stragglers.

### 7.5 Acceptance criteria
- [ ] All 4 candidate criteria enforced
- [ ] No duplicate escalation in 7d window
- [ ] Slack webhook sent with full context block
- [ ] Row inserted into `human_close_escalations` before notification (so audit trail exists even if Slack fails)
- [ ] Slack failure logged but does not raise; retry logic exists
- [ ] Outcome captured via admin endpoint `POST /admin/human-close/{id}/outcome`

---

## 8. Partial Flow: ZIP Map UI

### 8.1 Component

`Forced-action-ui/src/components/landing/ZipTerritoryMap.jsx`. Use Mapbox GL JS or Leaflet (Leaflet free, no API key) — recommend **Leaflet** for zero-cost path.

Props:
```typescript
{
  county: string,
  vertical: string,
  highlightZip?: string,
  onZipClick: (zip: string) => void,
}
```

### 8.2 New API

`GET /api/territory-map?county_id=X&vertical=Y` returns:

```json
{
  "county_id": "florida_hillsborough",
  "vertical": "roofing",
  "zips": [
    {"zip": "33647", "status": "available", "active_viewers": 0, "lead_count": 12},
    {"zip": "33602", "status": "locked", "active_viewers": 3, "lead_count": 28},
    {"zip": "33606", "status": "grace", "grace_expires_at": "...", "waitlist_count": 4}
  ]
}
```

Backed by single query joining `ZipTerritory` + `urgency_engine.get_active_count()` + `Property` count. Cache 60s in Redis.

### 8.3 Color scheme
- Green: available
- Red: locked
- Yellow: grace
- Pulsing: active competition (active_viewers > 0)

### 8.4 Acceptance criteria
- [ ] Map renders all ZIPs in county
- [ ] Status colors match data
- [ ] Click on available ZIP → ZIP collector modal opens with that ZIP prefilled
- [ ] Click on locked ZIP → waitlist form
- [ ] Counter polling every 20s (match `SampleLeads.jsx` pattern)
- [ ] No regression to existing `ZipChecker.jsx` path

---

## 9. Partial Flow: Save / Pause 60-day

### 9.1 Stripe approach

Use `pause_collection` on subscription:

```python
import stripe
stripe.Subscription.modify(
    sub.stripe_subscription_id,
    pause_collection={"behavior": "void", "resumes_at": int(resume_ts)},
)
```

`behavior=void` → invoices skipped during pause. `resumes_at` Unix ts auto-resumes.

### 9.2 New service

`src/services/pause_subscription.py`:

```python
def pause_subscriber(db, subscriber_id: int, days: int = 60) -> bool:
    sub = db.get(Subscriber, subscriber_id)
    if sub.status != "active":
        return False
    resume_at = datetime.now(timezone.utc) + timedelta(days=days)
    # Stripe call (off_session)
    stripe.Subscription.modify(
        sub.stripe_subscription_id,
        pause_collection={"behavior": "void", "resumes_at": int(resume_at.timestamp())},
    )
    sub.status = "paused"
    sub.paused_at = datetime.now(timezone.utc)
    sub.pause_resume_at = resume_at
    db.flush()
    return True


def resume_subscriber(db, subscriber_id: int) -> bool:
    sub = db.get(Subscriber, subscriber_id)
    stripe.Subscription.modify(sub.stripe_subscription_id, pause_collection="")
    sub.status = "active"
    sub.paused_at = None
    sub.pause_resume_at = None
    db.flush()
    return True
```

### 9.3 SMS PAUSE wiring

`src/services/sms_commands.py:_handle_pause` → replace dashboard redirect with confirm flow:
1. First PAUSE → reply: "Reply YES to pause for 60 days. Reply NO to keep active."
2. YES within 5min → `pause_subscriber()` → reply confirmation
3. NO or timeout → no action

State stored Redis key `pause_pending:{sub.id}` with 5min TTL.

### 9.4 Reminder cron

`src/tasks/pause_resume_reminder.py` — 7 days before `pause_resume_at`, send reminder SMS. Cron `0 11 * * *`.

### 9.5 Lead suppression during pause

Add `Subscriber.status == "paused"` to skip filter in:
- `src/tasks/lead_email_daily.py` (verify exists)
- `src/agents/supervisor.py` (gate event dispatch — paused subs get no Cora SMS)

### 9.6 Acceptance criteria
- [ ] Pause sets status to `paused`, calls Stripe
- [ ] Stripe invoices voided during pause
- [ ] Resume restores collection
- [ ] Lead emails suppressed during pause
- [ ] Cora messages suppressed during pause
- [ ] Reminder fires 7d before resume
- [ ] PAUSE SMS confirm flow works
- [ ] Auto-resume at exact `pause_resume_at` (Stripe handles)

---

## 10. Missing Flow: Partner Tier

### 10.1 Self-serve eligibility

```python
# src/services/partner_tier.py
ELIGIBLE_TIERS = ("territory_lock", "autopilot_lite", "autopilot_pro", "annual_lock")
DEFAULT_MAX_ZIPS = 5

def is_eligible(sub: Subscriber) -> bool:
    return sub.tier in ELIGIBLE_TIERS and sub.status == "active"


def validate_zip_selection(db, county_id: str, vertical: str, zip_codes: list[str], max_zips: int) -> dict:
    if len(zip_codes) > max_zips:
        return {"ok": False, "reason": "max_zips_exceeded"}
    # check no ZIPs already locked by other subs
    rows = db.execute(
        select(ZipTerritory).where(
            ZipTerritory.zip_code.in_(zip_codes),
            ZipTerritory.vertical == vertical,
            ZipTerritory.status == "locked",
        )
    ).scalars().all()
    if rows:
        return {"ok": False, "reason": "zips_already_locked", "zips": [r.zip_code for r in rows]}
    return {"ok": True}
```

### 10.2 Checkout endpoint

`POST /api/upgrade/partner` body:
```json
{ "zip_codes": ["33647", "33602", "33606"], "vertical": "roofing" }
```

1. validate eligibility
2. validate ZIPs available
3. create Stripe checkout session w/ `metadata: {tier: 'partner', zip_codes: '...', vertical: '...'}`
4. on `_on_checkout_completed` → call `provision_partner_access()`

### 10.3 Provisioning

```python
def provision_partner_access(db, sub_id: int, zip_codes: list[str], vertical: str, county_id: str) -> None:
    sub = db.get(Subscriber, sub_id)
    sub.tier = "partner"
    db.add(PartnerSubscription(subscriber_id=sub_id, max_zips=len(zip_codes)))
    for zc in zip_codes:
        zt = db.execute(select(ZipTerritory).where(
            ZipTerritory.zip_code == zc,
            ZipTerritory.vertical == vertical,
            ZipTerritory.county_id == county_id,
        )).scalar_one_or_none()
        if not zt:
            zt = ZipTerritory(zip_code=zc, vertical=vertical, county_id=county_id)
            db.add(zt)
        zt.subscriber_id = sub_id
        zt.status = "locked"
        zt.locked_at = datetime.now(timezone.utc)
    db.flush()
```

Note: existing `ZipTerritory.subscriber_id` is single FK. Multiple ZIPs each FK to same subscriber — works without schema change.

### 10.4 Frontend page

`Forced-action-ui/src/pages/PartnerUpgradePage.jsx`. ZIP multi-select using `ZipTerritoryMap.jsx` (built in 8). Submit → `/api/upgrade/partner`.

### 10.5 Acceptance criteria
- [ ] Eligibility gate works
- [ ] ZIP validation rejects already-locked ZIPs
- [ ] Stripe checkout session created with `tier=partner` metadata
- [ ] Webhook activates: tier flipped, all ZIPs locked, partner_subscriptions row created
- [ ] Partner can see leads from all assigned ZIPs
- [ ] Non-partner cannot access partner-only behaviors (verify lead pool query filters by ZIP ownership)

---

## 11. Partial Flow: Dynamic Flash Scarcity prod trigger

### 11.1 Source: Gold lead spike

Detect: ≥3 new gold-scored leads (CDS score ≥80) in same ZIP within 60 minutes.

`src/services/flash_scarcity.py`:

```python
def detect_spike(db, zip_code: str, vertical: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    n = db.execute(
        select(func.count()).select_from(Property)
        .where(Property.zip_code == zip_code,
               Property.vertical == vertical,
               Property.cds_score >= 80,
               Property.created_at >= cutoff)
    ).scalar() or 0
    return n >= 3


def open_window_if_spike(db, lead_id: int, zip_code: str, vertical: str) -> bool:
    """Called from CDS scoring path when a new gold lead lands."""
    if not detect_spike(db, zip_code, vertical):
        return False
    # Skip if ZIP already locked
    zt = db.execute(select(ZipTerritory).where(
        ZipTerritory.zip_code == zip_code, ZipTerritory.status == "locked"
    )).scalar_one_or_none()
    if zt:
        return False
    # Idempotency: skip if window opened in last 30min
    if redis_get(f"flash_scar_lock:{zip_code}"):
        return False
    redis_set(f"flash_scar_lock:{zip_code}", "1", ttl_seconds=1800)
    create_window(lead_id, zip_code, vertical)  # existing fn
    _emit_event(zip_code, vertical, lead_id)
    return True
```

### 11.2 Wire into CDS scoring

`src/services/cds_engine.py` — find where new properties get scored. After score assigned and ≥80 gold, call `flash_scarcity.open_window_if_spike()`.

### 11.3 Cora event

`flash_scarcity_window_open` payload:
```json
{"zip_code": "33647", "vertical": "roofing", "lead_id": 12345, "expires_at": "..."}
```

Routes to FOMO graph (existing). FOMO graph already handles competing-viewer messaging.

### 11.4 Acceptance criteria
- [ ] 3+ gold leads in 60min → window opens
- [ ] Already-locked ZIP → skipped
- [ ] Window dedup 30min — same ZIP no double-fire
- [ ] `create_window()` called from production path (not just tests)
- [ ] FOMO event emitted for non-locked subscribers in ZIP

---

## 12. Partial Flow: Synthflow Outbound Voice Drop

### 12.1 Detector

`src/tasks/synthflow_voice_drop_sweep.py`:

```python
QUALIFY_SCORE = 70
MIN_HOURS_NO_CONVERT = 48


def find_candidates(db) -> list:
    """
    revenue_signal_score >= 70
    AND no deal capture in last 48h
    AND last engagement (sms read or feed view) within 48h (still warm)
    AND no voice drop in last 7d
    AND has phone number
    AND has TCPA opt-in
    """
    ...
```

### 12.2 Outbound trigger

Existing repo has webhook receiver only. Add outbound client:

`src/services/synthflow_client.py`:

```python
import requests
from config.settings import settings

def initiate_call(phone: str, agent_id: str, context: dict) -> str:
    """Returns Synthflow call_id."""
    r = requests.post(
        f"{settings.synthflow_api_base}/calls",
        headers={"Authorization": f"Bearer {settings.synthflow_api_key.get_secret_value()}"},
        json={"agent_id": agent_id, "phone": phone, "context": context},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["call_id"]
```

Settings additions:
- `synthflow_api_base`
- `synthflow_api_key` (SecretStr)
- `synthflow_outbound_agent_roofing`

### 12.3 Cron

`0 15 * * 1-5` (3 PM UTC weekdays — daytime calls). Already covers timezone for US prospects.

### 12.4 Idempotency
Insert row into `voice_drops` table (or reuse `manual_action_log` with action_type='voice_drop') with `created_at`. Skip if any in last 7d.

### 12.5 Compliance
Pre-flight `compliance_check()` from `src/agents/tools/gating_tools.py`. TCPA opt-in required for marketing voice drop.

### 12.6 Acceptance criteria
- [ ] Qualified candidates detected
- [ ] TCPA gate enforced
- [ ] One drop per 7d max
- [ ] Synthflow API called with correct context
- [ ] Existing webhook still processes outcome
- [ ] Failed call retries 1× with backoff

---

## 13. Partial Flow: Earlier Retention cadence

### 13.1 Find producer

Search `src/tasks/` for cron entries emitting `retention_summary_due`. If absent, create:

`src/tasks/retention_event_producer.py`:

```python
RETENTION_CADENCE = {
    "wallet": 3,
    "lock": 5,
    "autopilot_lite": 5,
    "autopilot_pro": 7,
}


def run(db) -> int:
    fired = 0
    for tier, days in RETENTION_CADENCE.items():
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        subs = db.execute(
            select(Subscriber).where(
                Subscriber.tier == tier,
                Subscriber.status == "active",
            )
        ).scalars().all()
        for sub in subs:
            last_engagement = _last_engagement_at(db, sub.id)
            if last_engagement and last_engagement < cutoff:
                _emit_retention_event(sub.id, tier, days)
                fired += 1
    return fired
```

Cron `0 16 * * *` (4 PM UTC).

Idempotency key: `retention:{sub_id}:{YYYYMMDD}`.

### 13.2 Move config to constants

`config/retention.py` (new):
```python
RETENTION_CADENCE_DAYS = {"wallet": 3, "lock": 5, "autopilot_lite": 5, "autopilot_pro": 7}
```

### 13.3 Acceptance criteria
- [ ] Each tier has explicit days
- [ ] Cadence config-driven
- [ ] Event idempotency 1/day max
- [ ] Existing retention graph receives events
- [ ] Tier-specific window_days passed in payload

---

## 14. Regression: Verify Implemented Flows Still Work

For each "Implemented" flow, run smoke test before+after this work. Add to CI as `tests/regression_phase2bv9.py`.

| Flow | File | Smoke check |
|---|---|---|
| Territory Lock | `src/services/stripe_webhooks.py` | Mock Stripe checkout webhook → ZipTerritory row locked, status=locked, locked_at set |
| Charter Annual Push | `src/tasks/annual_push.py` | Founding member day 7 → email sent, switch_to_annual offered |
| Data-Only Save | `src/tasks/proactive_save.py` | inactive 6d sub → email sent w/ data_only price |
| Cora Lock Close | `src/agents/graphs/fomo.py` | competitor_acted_on_lead event → Cora SMS composed, agent_decisions row written |
| Stripe Recovery | `src/services/stripe_webhooks.py` | invoice.payment_failed → email + GHL tag fired |
| What You Missed | `src/agents/graphs/retention.py` | retention_summary_due event → SMS w/ unclaimed_gold + competing_viewers |
| Gate Monitoring | `src/agents/tools/gating_tools.py` | guardrail_check w/ out-of-bounds → returns allowed=False |

### 14.1 Verification matrix (must pass before merge)

```bash
pytest tests/regression_phase2bv9.py -v
pytest tests/test_annual_push.py tests/test_proactive_save.py tests/agents/ -v
```

Manual checks:
- Stripe test mode: run checkout w/ partner metadata → confirm provisioning row
- Slack test channel: trigger human close → confirm message arrives
- Redis CLI: `ZCARD urgency_zips:33647` after gold lead spike → ≥1

---

## 15. Unit Test Plan (Detailed)

All tests use `pytest`, MagicMock for DB/Stripe (match `tests/test_proactive_save.py` style).

### 15.1 `tests/test_wallet_to_lock.py`

```python
class TestFindCandidates:
    def test_below_threshold_no_candidate(self): ...
    def test_at_threshold_returns_candidate(self): ...      # exactly 40 credits
    def test_above_threshold_returns_candidate(self): ...   # 41 credits
    def test_credits_split_across_zips_no_candidate(self): ...  # 25+25 in two ZIPs → none
    def test_window_excludes_old_txns(self): ...            # >30d ago not counted
    def test_only_debits_count_not_credits(self): ...
    def test_groups_by_subscriber_and_zip(self): ...

class TestIsAlreadyLocked:
    def test_zip_locked_by_other_returns_true(self): ...
    def test_subscriber_already_lock_tier_returns_true(self): ...   # tier=territory_lock
    def test_subscriber_partner_tier_returns_true(self): ...
    def test_zip_available_returns_false(self): ...

class TestEmitEvent:
    def test_idempotency_key_includes_yyyymm(self): ...
    def test_payload_contains_credits_and_zip(self): ...
    def test_event_type_correct(self): ...

class TestSweep:
    def test_dry_run_emits_zero_events(self): ...
    def test_skipped_already_locked_counted(self): ...
    def test_marks_lock_candidate_on_subscriber(self): ...
```

### 15.2 `tests/test_ap_lite.py`

```python
class TestManualActionCounter:
    def test_log_action_inserts_row(self): ...
    def test_log_action_uses_monday_week_start(self): ...
    def test_count_this_week_returns_correct(self): ...
    def test_count_excludes_other_weeks(self): ...

class TestApLiteSweep:
    def test_lock_holder_at_threshold_emits_event(self): ...
    def test_lock_holder_below_threshold_no_event(self): ...
    def test_non_lock_holder_skipped(self): ...   # tier=wallet → no event
    def test_idempotency_per_iso_week(self): ...

class TestApLiteFeatures:
    def test_auto_skip_only_when_ap_lite(self): ...
    def test_auto_skip_only_for_cold_leads(self): ...

class TestUpgradeRoute:
    def test_non_lock_tier_rejected(self): ...
    def test_stripe_called_with_correct_price(self): ...
    def test_tier_updated_on_success(self): ...
```

### 15.3 `tests/test_human_close.py`

```python
class TestFindCandidates:
    def test_score_below_85_skipped(self): ...
    def test_under_3_interactions_skipped(self): ...
    def test_recent_deal_capture_skipped(self): ...
    def test_recent_escalation_within_7d_skipped(self): ...
    def test_qualified_candidate_returned(self): ...

class TestRouteToSlack:
    def test_inserts_escalation_row_before_slack(self): ...
    def test_slack_failure_logged_no_raise(self): ...
    def test_unique_constraint_blocks_duplicate(self): ...

class TestBuildContext:
    def test_includes_last_5_messages(self): ...
    def test_includes_proposed_offer(self): ...
    def test_no_pii_leaked_to_log(self): ...
```

### 15.4 `tests/test_pause_subscription.py`

```python
class TestPause:
    def test_active_sub_can_pause(self): ...
    def test_already_paused_returns_false(self): ...
    def test_stripe_modify_called_with_void_behavior(self): ...
    def test_resume_at_60_days_from_now(self): ...
    def test_status_set_to_paused(self): ...

class TestResume:
    def test_clears_pause_collection(self): ...
    def test_status_back_to_active(self): ...
    def test_resume_dates_cleared(self): ...

class TestSmsPauseFlow:
    def test_first_pause_sends_confirm(self): ...
    def test_yes_within_5min_pauses(self): ...
    def test_yes_after_5min_no_action(self): ...
    def test_no_response_cancels(self): ...
    def test_redis_state_ttl_5min(self): ...

class TestPauseSuppression:
    def test_cora_skips_paused_subscriber(self): ...
    def test_lead_email_skips_paused(self): ...
```

### 15.5 `tests/test_partner_tier.py`

```python
class TestEligibility:
    def test_lock_holder_eligible(self): ...
    def test_wallet_user_not_eligible(self): ...
    def test_grace_status_not_eligible(self): ...

class TestZipValidation:
    def test_max_5_zips_default(self): ...
    def test_locked_zip_rejected(self): ...
    def test_available_zips_pass(self): ...
    def test_all_locked_returns_full_list(self): ...

class TestProvisioning:
    def test_creates_partner_subscription_row(self): ...
    def test_locks_all_zips_to_subscriber(self): ...
    def test_creates_zip_territory_if_missing(self): ...
    def test_tier_flipped_to_partner(self): ...

class TestCheckoutWebhook:
    def test_partner_metadata_triggers_provisioning(self): ...
    def test_non_partner_metadata_falls_through(self): ...
```

### 15.6 `tests/test_flash_scarcity.py`

```python
class TestDetectSpike:
    def test_2_gold_leads_no_spike(self): ...
    def test_3_gold_leads_returns_true(self): ...
    def test_3_silver_leads_no_spike(self): ...     # score <80
    def test_old_leads_excluded(self): ...           # >60min

class TestOpenWindow:
    def test_locked_zip_skipped(self): ...
    def test_redis_dedup_30min(self): ...
    def test_create_window_called(self): ...
    def test_event_emitted(self): ...
```

### 15.7 `tests/test_synthflow_voice_drop.py`

```python
class TestQualification:
    def test_score_below_70_skipped(self): ...
    def test_no_phone_skipped(self): ...
    def test_no_tcpa_optin_skipped(self): ...
    def test_recent_drop_within_7d_skipped(self): ...
    def test_qualified_returned(self): ...

class TestInitiateCall:
    def test_synthflow_api_called_with_context(self): ...
    def test_returns_call_id(self): ...
    def test_failure_retries_once(self): ...
```

### 15.8 `tests/test_retention_producer.py`

```python
class TestCadence:
    def test_wallet_3d_inactive_fires(self): ...
    def test_wallet_2d_inactive_no_fire(self): ...
    def test_lock_5d_fires(self): ...
    def test_autopilot_lite_5d_fires(self): ...
    def test_autopilot_pro_7d_fires(self): ...

class TestIdempotency:
    def test_same_day_same_sub_no_double_fire(self): ...
    def test_next_day_can_fire_again(self): ...

class TestPayload:
    def test_window_days_in_payload(self): ...
    def test_tier_in_payload(self): ...
```

### 15.9 `tests/test_zip_territory_map_api.py`

```python
class TestApiResponse:
    def test_returns_all_zips_in_county(self): ...
    def test_status_reflects_db(self): ...
    def test_active_viewers_from_redis(self): ...
    def test_lead_count_from_property_table(self): ...
    def test_cache_60s(self): ...
    def test_404_for_unknown_county(self): ...
```

### 15.10 `tests/regression_phase2bv9.py`

Smoke tests per section 14. One test per implemented flow. Run on every PR.

```python
def test_territory_lock_smoke(): ...
def test_annual_push_smoke(): ...
def test_proactive_save_smoke(): ...
def test_cora_fomo_smoke(): ...
def test_stripe_payment_failed_smoke(): ...
def test_retention_what_you_missed_smoke(): ...
def test_gating_tools_smoke(): ...
```

### 15.11 Test fixtures

Add to `tests/conftest.py`:

```python
@pytest.fixture
def make_sub():
    def _make(tier="starter", status="active", **kw):
        sub = MagicMock()
        sub.tier = tier
        sub.status = status
        for k, v in kw.items(): setattr(sub, k, v)
        return sub
    return _make


@pytest.fixture
def mock_redis():
    with patch("src.core.redis_client.redis_available", return_value=True), \
         patch("src.core.redis_client._get_client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def mock_stripe():
    with patch("stripe.Subscription.modify") as mod, \
         patch("stripe.PaymentIntent.create") as pi:
        yield {"modify": mod, "payment_intent": pi}
```

---

## 16. Open Questions — Recommended Answers

| # | Question | Recommended Answer | Rationale |
|---|---|---|---|
| 1 | Wallet→Lock immediate or daily sweep? | Daily sweep | Avoids hot path latency. 24h delay acceptable for Lock close. |
| 2 | Manual action source for AP Lite? | New `manual_action_log` table | Separates concerns, queryable, auditable |
| 3 | First human close channel? | Slack | Fastest to ship, single webhook secret |
| 4 | Partner ZIP limit? | 5 default, configurable per-sub | Prevents abuse, allows enterprise upsell |
| 5 | Pause method? | Stripe `pause_collection` w/ `behavior=void` | Native Stripe handling, auto-resume, no schedule mgmt |
| 6 | Synthflow trigger location? | Backend Python | Brings qualification logic into repo, observable |
| 7 | Retention cadences? | wallet=3d, lock=5d, AP Lite=5d, AP Pro=7d | Tier-aligned to engagement expectations |

---

## 17. Delivery Phases

### Phase A — Backend revenue (2 weeks)
- Wallet-to-Lock sweep + graph
- AP Lite counter + sweep + graph + upgrade route
- Human close routing (Slack only)
- Schema migration

### Phase B — Product completion (2 weeks)
- Partner tier (backend + frontend page)
- Save/Pause flow (Stripe + SMS confirm)
- ZIP map UI (Leaflet)

### Phase C — Automation (1 week)
- Flash scarcity prod trigger (CDS hook)
- Synthflow outbound trigger
- Retention cadence producer

### Phase D — Verification (1 week)
- Regression suite green
- Manual smoke pass on all 16 flows
- Production canary: 5% subs

---

## 18. Definition of Done

- All 3 missing flows implemented end-to-end with passing unit tests
- All 6 partial flows have closed gaps with passing unit tests
- All 7 implemented flows pass regression smoke tests
- Single Alembic migration applied to staging + prod
- `EVENT_TO_GRAPH` updated with 5 new event types
- Cron entries deployed and observed firing
- `agent_decisions` audit trail confirms each new event is logged
- Slack webhook received human close test message
- Stripe test-mode partner checkout completes provisioning
- Redis observed flash scarcity windows opened from prod path

---

## 19. Final Target State

```text
Wallet 40+ credits/zip/30d → Cora Lock close → Stripe checkout → ZipTerritory locked
Lock holder 10+ manual/wk → Cora AP Lite close → upgrade route → tier flipped
Score≥85 + 3 interactions + no convert → Slack alert to human closer
Multi-ZIP power user → Partner page → Stripe → multiple ZipTerritory rows locked
At-risk → save offer (data_only OR 60d pause) → Stripe pause_collection void
3+ gold leads/zip/60min → flash scarcity window → FOMO graph → SMS sent
Score≥70 + 48h no convert → Synthflow outbound API → call placed → outcome webhook
Tier-specific inactivity (3/5/5/7d) → retention event → "what you missed" SMS
Visual ZIP map shows live status: available/locked/grace/competing
```

---

## 20. File-by-File Change Index

### New files
- `src/services/wallet_to_lock.py`
- `src/services/manual_action_counter.py`
- `src/services/ap_lite_features.py`
- `src/services/human_close_routing.py`
- `src/services/pause_subscription.py`
- `src/services/partner_tier.py`
- `src/services/flash_scarcity.py`
- `src/services/synthflow_client.py`
- `src/tasks/wallet_to_lock_sweep.py`
- `src/tasks/ap_lite_sweep.py`
- `src/tasks/human_close_sweep.py`
- `src/tasks/pause_resume_reminder.py`
- `src/tasks/synthflow_voice_drop_sweep.py`
- `src/tasks/retention_event_producer.py`
- `src/agents/graphs/wallet_to_lock_close.py`
- `src/agents/graphs/ap_lite_close.py`
- `src/agents/graphs/human_close_route.py`
- `src/agents/graphs/synthflow_voice_drop.py`
- `config/ap_lite.py`
- `config/retention.py`
- `alembic/versions/phase2bv9_missing_flows.py`
- `Forced-action-ui/src/components/landing/ZipTerritoryMap.jsx`
- `Forced-action-ui/src/pages/PartnerUpgradePage.jsx`
- 11 new test files (section 15)

### Modified files
- `src/services/wallet_engine.py` — add `zip_code` param to `debit()`
- `src/services/sms_commands.py` — `_handle_pause` confirm flow
- `src/services/cds_engine.py` — call `flash_scarcity.open_window_if_spike()`
- `src/services/stripe_webhooks.py` — partner metadata branch in `_on_checkout_completed()`
- `src/agents/router.py` — 5 new EVENT_TO_GRAPH entries
- `src/agents/supervisor.py` — paused subs gate
- `src/api/main.py` — `POST /api/upgrade/ap-lite`, `POST /api/upgrade/partner`, `GET /api/territory-map`, `POST /admin/human-close/{id}/outcome`
- `src/core/models.py` — Subscriber columns, WalletTransaction.zip_code, new ManualActionLog/HumanCloseEscalation/PartnerSubscription models
- `config/settings.py` — `synthflow_api_base`, `synthflow_api_key`, `synthflow_outbound_agent_roofing`, `slack_human_close_webhook`
- `tests/conftest.py` — shared fixtures
