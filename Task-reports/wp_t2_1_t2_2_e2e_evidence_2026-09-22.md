# WP-T2-1 / WP-T2-2 — Real End-to-End Evidence

Run date: 2026-09-22, against the live server. Continuation of
`wp1_wp2_wp5b_e2e_evidence_2026-09-22.md` (WP-1/WP-2/WP-5B). All tests run
against the real DB, real code paths, real production tables. No real
email/SMS was sent to any real person — `INSTANTLY_ENABLED=false` fleet-wide
on this box (confirmed), and `fa_max_relay_send_mode=fake` for FA Max
specifically (also confirmed, deliberately). Real Slack test traffic used
the user's own separate sandbox Slack app/workspace, never the real FA Max
Slack app. All synthetic DB rows created for these tests were deleted after
capture; no real borrower/opportunity data was ever written to (only read,
in one case, from a real `buyer_entities` row, in the earlier WP-5B report).

---

## WP-T2-1 — Own-Lane Send Infrastructure

### FakeMail / FakeSMS abstraction — PASS

Called the real production dispatch function `channels_email.send_email()`
for a `fa_max_lending` item with `fa_max_relay_send_mode='fake'` (this box's
actual current value):

```
FAKE_MAIL.sent: [{
  'recipient': 'zztest-fakemail@test.example',
  'subject': '[ZZTEST] FakeMail probe',
  'body': '...probe body...
    --
    Josh Kantor, Forced Action
    1320 W. Lemon St., Tampa, FL 33606
    (813) 361-8927

    This message is not an offer of credit and is not a solicitation to
    originate a loan. Reply STOP to opt out of future messages.

    [Unsubscribe link with signed JWT token]',
  'campaign_id': 'fa_max_fake_campaign',
  'sent_at': '2026-09-22T07:42:34+00:00'
}]
```

This is a real, complete, CAN-SPAM-compliant email body (brand, postal
address, phone, disclaimer, working signed unsubscribe link) built by the
real `build_passthrough_body()` function and recorded by `FakeMail` — zero
network calls to a real vendor, exactly matching WP-T2-1's own documented
design (`fakes.py`'s module docstring). Confirms "every other developer can
write and test outbound code against a fake without a live domain" is real,
not aspirational.

### 10DLC gate: no SMS ships before registration is confirmed — PASS

On this box, `FA_MAX_10DLC_REGISTERED` isn't even set in `.env` (defaults to
`False`). The real send-time guard (`relay.guards.evaluate()`) refuses every
SMS item regardless — three layered, independent gates were confirmed, in
the order they actually fire:

1. `fa_max_relay_send_mode != 'live'` → `DEFER` (this box's actual current state).
2. `fa_max_send_backlog_release_confirmed` → `DEFER` (also gated).
3. `fa_max_10dlc_registered == False` → `DEFER`, reason `fa_max_10dlc_not_registered`.

To isolate and directly prove gate #3 specifically fires when it's the only
remaining blocker (in-process settings mutation only, reverted immediately
after, `.env` and the live server untouched):

```
with send_mode='live', backlog_confirmed=True, 10dlc=False:
  outcome=defer reason=fa_max_10dlc_not_registered
```

**Verdict: PASS.** No code path in this system can dispatch a real SMS
while 10DLC registration is unconfirmed — matches WP-T2-1's Done-When
exactly.

### Suppression enforcement hook at dispatch — PASS (structural + functional)

Confirmed `src/services/relay/engine.py` calls `guards.evaluate()`
immediately before every dispatch attempt (the same function proven to
enforce suppression/10DLC/send-window in the WP-1/WP-2 report). Not
re-tested functionally here since WP-2's evidence report already proved
suppression blocks a send at this exact layer, including under an explicit
override attempt.

### Reputation monitoring + EXCEPTIONS alerting on threshold breach — PASS

Real math against real `relay_approval_queue` data, using
`fa_max_send_health_monitor._relay_failure_trip()`:

```
5 failed, 0 sent (below MIN_SENT_FLOOR=10):        trip=None   (correctly ignores too-small samples)
15 sent + 5 failed = 20 total, 25.0% failure rate: trip=Trip(rule='fa_max_relay_failure_rate_high',
                                                              detail='5/20 ... (25.0%)')
65 sent + 5 failed = 70 total, 7.1% failure rate:  trip=None  (correctly below the 10% ceiling)
```

Then drove a real alert through `exceptions_alert_queue.enqueue_and_attempt()`
end to end. The real Slack post attempt (to the user's sandbox EXCEPTIONS
channel) genuinely **failed** (`channel_not_found`) — and this incidentally
proved the resilience property directly instead of just by code reading:

```
enqueue_and_attempt() returned: False
durable alert row: {'id': 5, 'rule': 'zztest_reputation_probe',
                     'message': '...', 'status': 'pending'}
```

The alert landed as a durable, recoverable DB row (for the drain worker to
retry) **even though the live Slack call failed** — exactly the
crash/outage-proof design WP-T2-1 calls for, proven under a real failure,
not a hypothetical one.

### What's NOT verified (explicitly out of reach on this box, or self-declared incomplete in code)

- **"Sustained inbox placement above 95%" (WP-T2-1's own literal Done-When)**
  is *not* measured by anything running on this box today. The monitor's own
  docstring says this outright: *"WP-T2-1's own Done-When line is 'sustained
  inbox placement above 95%' — this monitor does NOT measure that; it is a
  proxy... Do not wire this monitor's proxy output into a 'Done' claim for
  the 95% placement criterion either way."* Real placement measurement
  requires a separate Instantly "Inbox Placement" plan entitlement, not yet
  confirmed (see `docs/fa-max-go-live.md` Step 6). **This Done-When
  criterion cannot be marked done from this codebase alone.**
- Domain/DNS/SPF/DKIM/DMARC provisioning and the actual multi-week warmup
  ramp were not (and could not safely be) tested here — they require a real
  domain going live, which is explicitly still pending per the code's own
  comments and per your explicit instruction not to flip `fa_max_relay_send_mode`
  to live on an unwarmed domain.
- No real email or SMS was sent to a real recipient anywhere in this box's
  current state (`INSTANTLY_ENABLED=false` fleet-wide; `fa_max_relay_send_mode=fake`
  for FA Max) — by design, and left untouched per your decision.

---

## WP-T2-2 — Agent Loop, Tool Registry & Autonomy Controls

### Autonomy tier graduation gates — PASS, all three tiers, real DB evidence

**Tier A** (25 approved sends): refused at 0 and 24 sends, opened at exactly 25.
**Tier B** (100+ sends AND <10% edit rate): refused at 15% edit rate on 100
sends (`edit_rate_too_high`), opened once diluted to 9.4% edit rate across
160 sends — using the real `material_edit`/`decided_by`/`decided_at`
evidence columns (not a stub flag).
**Tier C** (300 sends AND 5 funded loans): refused at 300 sends / 0 funded
loans (`funded_loans_below_threshold`); opened only after **5 real,
causally-attributed funded opportunities** were walked through the actual
production state machine end to end
(`new→qualifying→scoping→ready_to_submit→submitted→term_sheet→closing→funded`),
each with a real `write_interaction()` → `create_fa_max_opportunity()`
causal chain via `origin_interaction_id`.

**Tier isolation confirmed:** the same agent's 25 Tier-A sends counted as
**zero** evidence toward Tier B or Tier C — no cross-tier pooling, exactly
as documented.

### 🔴 CONFIRMED BUG — `fa_max_entity_registry` has zero rows for `entity_type='opportunity'`

While proving the Tier C funded-loan chain, `transition()` on a freshly
created opportunity failed with `invalid_transition` / *"not found in
registry"*. Root cause, confirmed directly against real production data
(read-only, no mutation made to real rows):

```sql
-- real production state at time of test:
SELECT count(*) FROM fa_max_opportunities;                                    -- 2 real rows
SELECT count(*) FROM fa_max_entity_registry WHERE entity_type='opportunity';  -- 0 rows
```

`create_fa_max_opportunity()` (the sole sanctioned INSERT path into
`fa_max_opportunities`, per its own docstring) never registers the new
opportunity in `fa_max_entity_registry`, and a repo-wide grep confirms
**zero** production callers of `ensure_entity_registry()` anywhere in the
codebase, for either `entity_type='person'` or `'opportunity'`.

**Impact:** `state_engine.transition()` is documented as "the ONLY sanctioned
write path" for opportunity stage changes — `mark_opportunity_funded()`
depends on it, and it in turn is exactly what would be needed to grant Tier C
graduation to any real agent. **As currently wired, neither of the 2 real
opportunities already in production, nor any new one created through the
sanctioned path, can ever legitimately reach `funded` through `transition()`.**
This is the same root gap flagged for `entity_type='person'` in the earlier
WP-1 report, now confirmed to also block opportunity funding — i.e. it's not
an edge case, it's the only entity type WP-T2-2's own Tier C mechanism
actually depends on.

**Suggested fix:** `create_fa_max_opportunity()` (and the equivalent person
creation path in `selfserve_sessions.resolve_or_create_person()`) should call
`ensure_entity_registry()` at creation time, in the same transaction, so every
entity is registrable the moment it exists.

### 🔴 CONFIRMED BUG — `finish_tool_call()` crashes on non-JSON-serializable tool output, contradicting its own "Never raises" contract

Running the real bounded agent loop (`run_fa_max_agent_no_checkpoint()`)
with a real registered tool (`get_fa_max_person_state`, whose output
legitimately includes `created_at`/`updated_at` as Python `datetime`
objects — this is normal, common tool output, not contrived) produced an
**unhandled** `TypeError: Object of type datetime is not JSON serializable`
that crashed the entire graph invocation.

Root cause, in `src/services/fa_max_tool_log.py::finish_tool_call()`:

```python
params: dict = {
    "log_id": log_id,
    "output": _json.dumps(redact_for_tool_log(output)),   # <- line ~330, OUTSIDE the try block
    ...
}
...
try:
    result = session.execute(...)          # <- try/except only wraps this
    ...
except Exception:
    ...
    return False
```

The function's own docstring says *"Never raises; returns False on a
genuine write failure."* It does raise — `json.dumps()` executes before the
`try` block even starts, so any tool whose output contains a `datetime`
(or `Decimal`, `UUID` object, etc.) will crash the whole bounded loop
uncaught, not merely fail closed and log a warning as designed.

**Impact:** any real task that calls `get_fa_max_person_state` (a core, most
basic FA Max read tool) as one of its bounded steps crashes the entire agent
loop today. Confirmed reproducible; not a synthetic edge case.

**Suggested fix:** move the `_json.dumps(...)` call inside the `try` block
(or pass `default=str` to `json.dumps`), matching the function's own
documented "never raises" contract.

### Bounded tool-call loop — PASS (after avoiding the datetime bug above)

Using only JSON-safe tool output (`check_suppression`):

```
2 real steps requested -> done=True, error=None, step_index=2, 2 tool_results recorded
fa_max_tool_call_log: 2 rows, both status='success', duration_ms recorded (24ms, 0ms), input redacted (PII masked)
```

Ceiling test — requested 13 steps against the real configured
`FA_MAX_AGENT_MAX_TOOL_CALLS=8`:

```
requested 13 steps; done=True; actual_tool_calls_made=8
```

**Verdict: PASS.** The loop is genuinely bounded at the configured ceiling,
not merely documented as bounded — it stopped cleanly at exactly 8, never
attempted step 9 through 13.

### Structured tool-call logging — PASS (for JSON-safe outputs; see bug above for the gap)

Every successful call produced a `fa_max_tool_call_log` row written
**before** execution (`start_tool_call()`) and updated **after**
(`finish_tool_call()`), with tool name, input (PII-redacted — recipient
shown as `[redacted]`), output, duration in ms, and agent name — matches
WP-T2-2's Done-When: *"A task dispatched to Cora results in a structured
log entry for every tool call made, including inputs, outputs, duration,
and agent."* The one gap is the crash bug above, which means this Done-When
is **not actually true for every tool** — it's true only for tools whose
output happens to be JSON-serializable as-is.

### Suppression at send time regardless of explicit instruction — PASS (already proven in the WP-1/WP-2 report)

Not re-tested here; the earlier report proved both `enqueue()` (even with
`auto_authorize=True`, an explicit autonomous-override attempt) and the
independent send-time guard both refuse a suppressed contact and return the
exact reason, even after a simulated direct-DB bypass of the first gate.

### What's NOT verified

- **Edit-rate weekly report** (`fa_max_weekly_edit_rate_report.py`, Friday
  cron) was not run end-to-end in this session — code was located but not
  executed; the underlying `get_weekly_edit_rate()` evidence column was
  already proven correct via the Tier B test above (same column, different
  time window).
- **Draft flow's Approve/Skip/Snooze/Revise buttons** — only Approve was
  exercised end-to-end (including with real Slack clicks in the earlier
  session). Skip/Snooze/Revise exist in code (`_build_approval_blocks()`)
  but were not clicked/tested this session.
- Real LLM-driven task selection (`tool_registry.select_task_tools()`) was
  not tested — only the deterministic ordered-`steps` path was exercised.

---

## Consolidated bug list across the full test session (WP-1 through WP-T2-2)

| # | Severity | Finding | Status |
|---|----------|---------|--------|
| 1 | **High** | `fa_max_entity_registry` never populated for `entity_type='person'` or `'opportunity'` by any production code path (`ensure_entity_registry()` has zero real callers) — blocks `transition()` for every real person/opportunity in production today, including opportunity funding (Tier C evidence). | Confirmed, unfixed |
| 2 | **High** | `finish_tool_call()` crashes uncaught on non-JSON-serializable tool output (e.g. `datetime`), contradicting its own "Never raises" contract and crashing the entire bounded agent loop. | Confirmed, unfixed |
| 3 | Info | WP-T2-1's literal 95%-inbox-placement Done-When is explicitly not measured by any code in this repo (self-declared in the monitor's own docstring) — needs a separate Instantly Inbox Placement plan entitlement decision, not a code fix. | Self-documented gap, not a bug |
| 4 | Info | This server's Slack signature verification, venture approver lists, and `.env` now include a kept, intentional test-workspace wiring (`FA_MAX_TEST_SLACK_SIGNING_SECRET`, `ventures.fa_max_lending.relay_approvers` includes a test user id) — added and kept at your explicit request for future manual click-through testing. Not a bug; noted for awareness. | Kept intentionally |

Everything else tested across both reports — WP-1's durable state engine
(crash/resume at both transaction and OS-process level, full ordered
history), WP-2's Slack queues and two-gate send governance (including real
Slack clicks through your test workspace), WP-5B's buy-box/velocity/
next-need prediction against a real production buyer entity, and the
WP-T2-1/T2-2 pieces above — **passed with real, independently-verified
evidence** (DB queries, Slack API fetch-backs, real state-machine walks,
real process kills).
