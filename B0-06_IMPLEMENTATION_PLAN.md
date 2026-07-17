# B0-06 — Voice-Path Affirmative Opt-In Check — Implementation Plan

**Task:** Require a stored explicit PEWC voice opt-in before any automated AI voice call fires.
**Owner:** Dev 2 · Block 0 · P2 · master `T-B4-07`
**DoD:** No automated call can fire without a stored explicit voice opt-in.
**Decisions:** ADR 0030 · CONTEXT.md "Voice Consent (PEWC)". Spans two repos (`Forced-action-` backend, `Forced-action-ui` frontend).

---

## Why (one line)
Synthflow AI voice = "artificial voice" robocall under TCPA (FCC Feb-2024). PEWC (47 CFR 64.1200(f)(9)) needs a **separate, channel-specific** consent — the generic marketing checkbox does not satisfy it.

## Scope
- **In:** direct-dial path `synthflow_voice_drop` → `synthflow_client.initiate_call` (targets active subscribers).
- **Out:** Auto Mode owner-VM (`auto_mode_followup`) — dispatched inside a GHL workflow via contact tag, unreachable by in-code gate. Waitlist — never becomes a `subscribers` row, sweep can't target it.

---

## Backend — `Forced-action-`

### 1. Schema — new voice-consent cluster on `consent_acceptances`
Mirror the existing `tcpa_consent_*` fields. `models.py` (`ConsentAcceptance`, ~line 1088):
```python
voice_consent_at:      Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
voice_consent_text:    Mapped[Optional[str]]      = mapped_column(Text)
voice_consent_version: Mapped[Optional[str]]      = mapped_column(String(30))
```
Migration `migrations/apply_b0_06_voice_consent.py` — idempotent `ADD COLUMN IF NOT EXISTS` ×3 (ADR 0024, new scripts go in `migrations/`). No CHECK/enum change — deliberately NOT a `consent_scope` value (that column holds one value, already used by `'marketing'`).

### 2. Capture — write voice consent in the two reachable flows
Both write `ConsentAcceptance` already; add the 3 fields, gated on a new optional payload flag `voice_consent_accepted`:
- **checkout** `main.py:858` — add `voice_consent_at = now() if <flag> else None`, `voice_consent_text`/`_version` from payload.
- **free_signup** `main.py:5529` — same.
- Extend the Pydantic consent sub-models to accept `voice_consent_accepted: bool = False`, `voice_consent_text`, `voice_consent_version`. Default false → non-blocking, unchecked (legally required: never a condition of purchase).

### 3. Link — backfill `subscriber_id` onto the checkout consent row
Checkout writes the consent row before the subscriber exists (no `subscriber_id`, no `phone`). In `stripe_webhooks.py` at subscriber creation (~line 491), after commit:
```python
db.execute(text("""
    UPDATE consent_acceptances SET subscriber_id = :sid
    WHERE email = :email AND subscriber_id IS NULL
"""), {"sid": sub.id, "email": sub.email})
```
Idempotent (`subscriber_id IS NULL` guard). free_signup already sets `subscriber_id` — no change there.

### 4. Enforce — gate at the dispatch call site
`synthflow_voice_drop.py` — beside the existing `validate_outbound` block (~line 209), before `allotment_consume`/`initiate_call`:
```python
has_voice_consent = db.execute(text("""
    SELECT 1 FROM consent_acceptances
    WHERE subscriber_id = :sid AND voice_consent_at IS NOT NULL
    LIMIT 1
"""), {"sid": subscriber_id}).first()
if not has_voice_consent:
    return {"call_id": None, "sent": False, "terminal_status": "aborted",
            "failure_reason": "voice_consent_required"}
```
`sms_opt_ins` JOIN in the sweep stays (voice consent is additive). Revocation handled by existing universal suppression (`sms_opt_outs`, ADR 0028) — no voice-specific opt-out.

### 5. Tests (`tests/test_b0_06_voice_consent.py`)
- subscriber **with** `voice_consent_at` → dispatch proceeds (mock `initiate_call`).
- subscriber **without** → aborted, `failure_reason="voice_consent_required"`, `initiate_call` never called. **← proves DoD.**
- checkout capture writes the 3 fields when flag true; NULL when false/absent.
- Stripe-webhook link sets `subscriber_id` on the email-matched checkout consent row; idempotent on re-run.
- migration idempotent (apply twice, no error).

---

## Frontend — `Forced-action-ui`
Optional, unchecked-by-default voice-consent checkbox on the checkout + free-signup consent modals. On submit, pass `voice_consent_accepted` + the displayed disclosure `text`/`version` into the existing consent payload. Checkout/signup must succeed regardless of the box.

---

## External deliverable (blocks legal-completeness, not code)
**PEWC disclosure wording** — clear-and-conspicuous, names automated/AI/prerecorded calls, states "not a condition of purchase." Needs founder sign-off. Code ships with a marked placeholder + version string; swap copy when approved.

---

## Build order
1. Schema + migration → 2. models.py → 3. capture (both flows) + Pydantic → 4. webhook link → 5. call-site gate → 6. tests → 7. frontend checkbox.
Steps 1–6 are one backend PR; 7 is a frontend PR. DoD provable from backend test #2 alone.
