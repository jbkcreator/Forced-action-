# Synthflow Voice Flow — Manual QA Sign-Off

Date: 2026-06-01
Tester: automated (pytest) + local live DB run
Environment: local dev (real Postgres; Telnyx/Synthflow not reachable — sends suppressed / call mocked; Redis via fakeredis sandbox)

Legend: ✅ pass · ⚠️ pass with caveat (external vendor not reachable locally) · ⬜ pending real staging network

---

## Inbound

| Check | Result | Evidence |
|-------|--------|----------|
| Synthflow inbound path creates/resolves subscriber | ✅ | `onboard_inbound_caller` → sub 20316, `is_new=true` |
| SmsOptIn created — `source=synthflow_inbound` | ✅ | row present, `opt_in_created=true` |
| First Leads SMS sent as `marketing` | ✅ | `sms_send_logs.message_type='marketing'`, `task_type='first_leads'` |
| First Leads body contains lead content (3) + dashboard link | ✅ | `lead_count=3` |
| Welcome SMS fires independently | ✅ | `welcome_sent=true` |
| **SLA: consent/account → First Leads enqueue** | **✅ 5.07s** (< 60s) | `sms_send_logs.created_at − sms_opt_ins.opted_in_at` |
| SMS actually transmitted | ⚠️ suppressed | no live Telnyx key locally — outcome=`suppressed`, send path exercised |

## Inbound idempotency

| Check | Result | Evidence |
|-------|--------|----------|
| Same `call_id` replayed → duplicate no-op | ✅ | `{"status":"duplicate"}` via `already_logged` |
| No duplicate First Leads SMS | ✅ | `sms_send_logs` row count stayed 2 |

## Outbound

| Check | Result | Evidence |
|-------|--------|----------|
| High-intent non-converter detected by sweep | ✅ | sweep `candidates=1, dispatched=1` |
| LangGraph routed `high_intent_no_convert` | ✅ | supervisor route → `synthflow_voice_drop` |
| Kill-switch gate passes (green) | ✅ | after fix: `kill_switch_color=green`, `action_allowed=true` |
| `initiate_call` invoked + `sent=true` | ⚠️ mocked | Synthflow API unreachable locally; `initiate_call` mocked → `call_id` returned, `sent=true` |
| `voice_drop` audit row written | ✅ | `manual_action_log.action_type='voice_drop'` (today) |
| `agent_decisions` row written (after fix) | ✅ | `decision_id=proof_log_D`, summary populated |
| **SLA: dispatch → call initiation** | ⚠️ in-process < 1s | synchronous; real-network delta pending staging |

## Outbound idempotency

| Check | Result | Evidence |
|-------|--------|----------|
| `decision_id == idempotency_key` (storm fix) | ✅ | `test_idem_key_format` + live key `synthflow_drop:{id}:{date}` |
| 7-day dedup blocks re-dispatch | ✅ | re-dispatch aborted `voice_drop:dedup_7d` |

## Conversion exclusion

| Check | Result | Evidence |
|-------|--------|----------|
| Bundle purchase excluded (48h) | ✅ unit | SQL `NOT EXISTS bundle_purchases ... purchased_at > cutoff` |
| Wallet debit excluded (48h) | ✅ unit | SQL `NOT EXISTS wallet_transactions txn_type='debit'` |

---

## Bugs found & fixed during this run

All surfaced only against the real DB / real graph — not in mocked unit tests.

1. **DDL targeted `webhook_log`; real table is `webhook_events`** — apply script + migration fixed.
2. **`synthflow_inbound` rejected by `check_opt_in_source` CHECK** — constraint widened (model + DDL).
3. **SmsOptIn failure corrupted the session** (PendingRollback) → killed downstream First Leads — wrapped insert in `db.begin_nested()` savepoint.
4. **SmsOptIn dedup queried by `subscriber_id`** but the unique index is on `phone` — switched to phone lookup.
5. **Sweep SQL used `bundle_purchases.created_at`** — real column is `purchased_at`.
6. **Redis `ping()` had no `socket_timeout`** → hung forever on an unreachable/TLS-mismatched server, blocking every agent dispatch — added `socket_timeout=2` (THE blocking bug).
7. **Voice-drop gated on `synthflow_voice_drop`** (not in KILL_SWITCH config, no observed value) → always `unknown` → fail-safe RED → every dispatch aborted before calling. Re-pointed to `lock_conversion` + pass live metric (matches `fomo`).
8. **`created_at` string vs datetime** in `_node_initiate_drop` → `TypeError` — parse ISO string defensively.
9. **`synthflow_voice_drop` never wrote `agent_decisions`** (only graph without `log_decision`) → no audit trail / SLA query blind — added `log_decision` to finalize on all paths.

## Automated test run

```
pytest tests/test_synthflow_inbound_flow.py -q
11 passed in 0.90s  (2026-06-01)
```

## Still pending (real staging network)

- Live Telnyx send of First Leads + welcome (confirm delivery, not just enqueue).
- Live Synthflow outbound call initiation with real `synthflow_calls` row → real dispatch→initiation delta.
- End-to-end recent-payer exclusion with seeded purchase.

## Overall

- Inbound SLA: ✅ **5.07s** (< 60s)
- Inbound idempotency: ✅
- Outbound routing + gate + audit + dedup: ✅ (Synthflow call mocked locally)
- Automated tests: ✅ 11/11
- Blocking hang: ✅ root-caused (Redis no read-timeout) and fixed
