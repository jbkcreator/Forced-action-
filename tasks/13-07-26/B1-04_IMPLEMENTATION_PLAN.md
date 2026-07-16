# B1-04 — Abandoned-Checkout Founder Alert — Implementation Plan

**Block:** 1 (Floor / Storefront) · **Bid unit:** brief 1.7 · **Master ID:** `T-B1-04`
**Owner:** Dev 1 · **Wave:** 2 · **Priority:** P3
**Scope:** Backend only. No DB migration. No frontend change.
**Status:** Implemented.

---

## 1. Goal

Fire a founder SMS alert whenever a checkout (subscription or lead pack) is abandoned, so the founder can personally follow up. This is distinct from the automated buyer-facing recovery drip ("Task 7" / `CheckoutRecovery`), which already exists and nudges the *customer* — B1-04 notifies the *founder*.

## 2. Current state (verified by code read, superseding the stale note in `BLOCK_0_1_PLAN.md`)

**Already built — do not rebuild:**
- Full customer-facing abandoned-checkout recovery system, all three capture paths funneling through one entry point:
  - `pre_payment` — [`main.py:952`](../../src/api/main.py#L952), captured at `/api/checkout` session creation.
  - `session_expired` — [`stripe_webhooks.py:4178`](../../src/services/stripe_webhooks.py#L4178) (`_start_recovery_for_expired_checkout`), off the `checkout.session.expired` webhook.
  - `lead_pack` — [`main.py:3438`](../../src/api/main.py#L3438), gated by `checkout_recovery_lead_pack_enabled`.
  - All three call [`checkout_recovery.start_recovery()`](../../src/services/checkout_recovery.py#L115) → one `CheckoutRecovery` row per email (unique, idempotent).
  - [`checkout_recovery_sweep.py`](../../src/tasks/checkout_recovery_sweep.py) sends the buyer-facing email/SMS touches on a cadence.
- `_send_founder_alert()` — [`stripe_webhooks.py:3370`](../../src/services/stripe_webhooks.py#L3370), best-effort SMS to `settings.founder_phone`, already wired to refunds/disputes/chargebacks.
- `CheckoutRecovery` table (`models.py:5062`) already has every field needed: `email`, `phone`, `source`, `resume_context`.

**Missing (the actual gap):** `_send_founder_alert()` was never called from the abandonment path. No founder SMS fired on any abandoned checkout.

## 3. Decisions (confirmed with client/founder)

1. Alert fires for **both** subscription and lead-pack abandonment — per brief text "subscription (or pack)."
2. No batching/digest — per-event SMS is fine at current volume; noise is not a concern right now. (Upgrade path noted as a `ponytail:`-style deferral if volume ever makes this unwieldy — see §6.)

## 4. Implementation

**Single hook, not three.** All three abandonment sources already converge on `start_recovery()` — that is the one choke point to instrument, avoiding duplicated wiring at each of the three call sites and guaranteeing no future fourth source is missed.

### Backend changes

1. **[`src/services/checkout_recovery.py`](../../src/services/checkout_recovery.py)** — `start_recovery()`:
   - After `db.add(row)` (new-row path only — never on the "already active" no-op replay at the `existing is not None` early return), call a new `_alert_founder(email, source, phone)`.
   - `_alert_founder()` lazy-imports `_send_founder_alert` from `src.services.stripe_webhooks` inside the function body — mirrors the existing lazy-import pattern already used in `cora_attribution_rollback_check.py` and `ab_rollback_check.py`, avoiding a circular import (since `stripe_webhooks.py` also lazy-imports `checkout_recovery` at call time).
   - Message format follows the existing one-liner convention (`"REFUND: ...", "DISPUTE: ..."`):
     `f"ABANDONED CHECKOUT: {source} email={email}"` (+ `" phone={phone}"` if present). Truncated to 320 chars by `_send_founder_alert` itself.
   - Fires **unconditionally** — not gated by `checkout_recovery_enabled` / `checkout_recovery_lead_pack_enabled` (those flags control customer-facing sends only; the founder alert sends nothing to the buyer, and `_send_founder_alert` already no-ops safely if `FOUNDER_PHONE` isn't configured).

2. **[`tests/test_checkout_recovery.py`](../../tests/test_checkout_recovery.py)** — new test `test_start_recovery_alerts_founder_once`:
   - Monkeypatches `src.services.stripe_webhooks._send_founder_alert`.
   - Asserts exactly one alert fires on first `start_recovery()` call for a fresh email, containing the email and source.
   - Asserts a second `start_recovery()` call for the same (still-active) email does **not** re-alert (dedup via the existing idempotency check).

### DB changes

None. `CheckoutRecovery` already carries every field this task needs.

### Frontend changes

None. The alert is an SMS to the founder's personal phone, not a dashboard surface (an "abandoned checkout" queue entry belongs to Block 8's Operator Dashboard, out of scope here).

## 5. Definition of Done

- [x] An abandoned subscription checkout produces a founder alert.
- [x] An abandoned lead-pack checkout produces a founder alert.
- [x] A dedup'd replay (same email, still-active row) does not double-alert.
- [x] Test coverage proves both the fire and the no-double-fire cases.

## 6. Deferred / not built (explicitly out of scope)

- **Alert batching/digest** — per founder's confirmation, not needed now. Add if per-event SMS volume becomes noisy at scale (e.g., roll up into a periodic digest or route through Block 8's action queue instead of a live SMS).
- **Dashboard visibility of abandoned checkouts** — belongs to Block 8 (Operator Dashboard), not this task.
