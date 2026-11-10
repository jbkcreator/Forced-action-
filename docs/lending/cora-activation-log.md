# Cora Activation (P0-P3) – Work Log

Source: *Cora Activation: Developer Build Brief* (Nov 10, 2026). Branch: `feat/cora-activation-p0` (cut from `origin/dev` @ `0e6a7fa`). One PR at the end of the phase.

---

## P0.1 – 8 AM to 8 PM hard call/SMS window clamp

**Date:** 2026-11-10 · **Status:** code done, tests pass; Slack proof pending

### Findings
- PR #410 (Nov 5) moved the dialer window to env (`LENDING_CALL_WINDOW_*`) with two guards: settings refused values outside 08:00-20:00, and `dial_window()` clamped the local window to the legal floor.
- PR #411 (Nov 5) removed both guards so testers could dial out of hours: any value was accepted and a wider window only logged a WARNING.
- Production runs `0e6a7fa` (#411), so this code was live there.
- Production `.env` values (checked on the server Nov 10), all equal to the defaults, so nothing was dialing out of hours:
  - `LENDING_CALL_WINDOW_LOCAL_START=08:00`
  - `LENDING_CALL_WINDOW_LOCAL_END=20:00`
  - `LENDING_CALL_WINDOW_ET_START=09:00`
  - `LENDING_CALL_WINDOW_ET_END=19:15`
- Texts were never affected: `text_window_open()` uses the fixed 08:00-20:00 constants, not env.

### Changes
- `src/lending/compliance.py`
  - `dial_window()`: hard clamp. Both the recipient-local and the ET window are held inside 08:00-20:00. Env can narrow, never widen. A wider value is clamped and logged at ERROR. A window entirely outside 08:00-20:00 blocks every call (fails closed).
  - `can_dial_now()`: logs `BLOCKED_HOURS call refused phone_hash=… at=…` when the window refuses a call.
  - `text_window_open()`: logs `BLOCKED_HOURS text refused phone_hash=… at=… zones=…` when the window refuses a text (covers Cora's send check, LendingFlow intro SMS and partner texts).
- `config/settings.py`, `.env.example`: comments updated (values can only narrow).
- `tests/lending/test_call_window_settings.py`: replaced the "wider window opens out of hours" test with clamp tests, plus 9:00 PM call and 9:00 PM text refusal tests that assert the `BLOCKED_HOURS` log and that no raw phone is logged.

### Decisions
- Stored reason code stays `OUTSIDE_CALL_WINDOW` (it is persisted in DB rows and used in config lists). `BLOCKED_HOURS` is the log tag the brief asks for.
- ET window is clamped to the legal 08:00-20:00 floor (as #410 did), not to the shipped 09:00-19:15, so Josh's hours can still be tuned from env inside the legal limit.
- No startup refusal of wide values (#410 had one). `AppSettings` is shared by every service (fa-api, Stripe webhooks…), so a lending typo would take them all down. The runtime clamp alone guarantees no out-of-hours dial.
- Batch `dial_blocks()` is not logged per phone (sweeps check thousands of numbers overnight).
- Log de-duplication (code-review follow-up): the clamp ERROR is logged once per distinct wide value per process (not once per phone in a sweep), and `BLOCKED_HOURS` is logged once per phone per Eastern day for calls and for texts (not on every sweep re-check). The de-dup set is capped at 10,000 entries and cleared when full.

### Tests
- `py -m pytest tests/lending/test_call_window_settings.py tests/lending/test_can_dial_now.py tests/lending/test_text_window.py tests/lending/test_dialer_events.py tests/lending/test_booking_messages.py tests/lending/test_partner_door.py tests/lending/test_routing.py`
  → **165 passed, 191 skipped** (skips = Postgres tests, no `TEST_DATABASE_URL` locally), 0 failed.
- `test_call_window_settings.py`: 32/32 passed (adds: clamp ERROR once for 500 checks, logged again for a new wide value, BLOCKED_HOURS once per phone per day, logged again next day).

### Still to do (deliver to Josh)
- [ ] Post the production `LENDING_CALL_WINDOW_*` values to #lending-ops (by the team; not sent by Claude).
- [ ] After deploy: capture the `BLOCKED_HOURS text refused` log line from a 9:00 PM test SMS and post it.
