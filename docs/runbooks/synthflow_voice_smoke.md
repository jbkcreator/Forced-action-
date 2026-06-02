# Staging Smoke — Synthflow Voice Flow (DoD Layer B)

Proves both SLA legs with real network + real timestamps. Run this after
deploying the fa057 changes to staging.

**DoD:** inbound event → account + First Leads < 60s; outbound high-intent
event → Synthflow call initiation < 60s.

---

## Prerequisites

- `SYNTHFLOW_WEBHOOK_SECRET` set in staging `.env`
- `SYNTHFLOW_INBOUND_AGENT` configured (inbound Synthflow agent ID)
- `SYNTHFLOW_INBOUND_DID` configured (inbound phone number)
- `SYNTHFLOW_API_KEY` + at least `SYNTHFLOW_OUTBOUND_AGENT_ROOFING` set
- `TELNYX_*` keys set (SMS can be sent)
- DDL applied: `python scripts/apply_fa057_synthflow_inbound.py`
- Staging API server running

---

## Leg A — Inbound: Synthflow event → account + First Leads

### Step 1 — Post a synthetic inbound event

```bash
curl -s -X POST https://staging.forcedactionleads.com/webhooks/synthflow/inbound \
  -H "X-Synthflow-Secret: $SYNTHFLOW_WEBHOOK_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "smoke_test_001",
    "phone": "+18135550199",
    "zip_code": "33602",
    "vertical": "roofing"
  }'
```

Expected response:
```json
{"status": "ok", "is_new": true, "first_leads_sent": true, "lead_count": 3, "capture_complete": true}
```

### Step 2 — Read timestamps from DB

```sql
-- T0: webhook received
SELECT processed_at AS t0_webhook_received
FROM webhook_events
WHERE source = 'synthflow_inbound'
  AND source_event_id = 'smoke_test_001'
ORDER BY processed_at DESC LIMIT 1;

-- T1: First Leads SMS enqueued
SELECT mo.sent_at AS t1_first_leads_enqueued
FROM message_outcomes mo
JOIN subscribers s ON s.id = mo.subscriber_id
WHERE s.phone = '+18135550199'
  AND mo.task_type = 'first_leads'
ORDER BY mo.sent_at DESC LIMIT 1;

-- Delta
SELECT EXTRACT(EPOCH FROM (mo.sent_at - we.processed_at)) AS delta_seconds
FROM webhook_events we
JOIN subscribers s ON TRUE
JOIN message_outcomes mo ON mo.subscriber_id = s.id
WHERE we.source = 'synthflow_inbound'
  AND we.source_event_id = 'smoke_test_001'
  AND s.phone = '+18135550199'
  AND mo.task_type = 'first_leads'
LIMIT 1;
```

**Pass:** `delta_seconds < 60`

### Step 3 — Verify consent record

```sql
SELECT source, opt_in_message, opted_in_at
FROM sms_opt_ins
JOIN subscribers s ON s.id = subscriber_id
WHERE s.phone = '+18135550199';
```

**Pass:** row exists with `source = 'synthflow_inbound'` and `opt_in_message` containing `call_id=smoke_test_001`.

### Step 4 — Verify replay idempotency

Re-post the exact same payload (same `call_id`):

```bash
curl -s -X POST https://staging.forcedactionleads.com/webhooks/synthflow/inbound \
  -H "X-Synthflow-Secret: $SYNTHFLOW_WEBHOOK_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"call_id": "smoke_test_001", "phone": "+18135550199", "zip_code": "33602", "vertical": "roofing"}'
```

**Pass:** response is `{"status": "duplicate", "call_id": "smoke_test_001"}` and no second `message_outcomes` row for this subscriber.

### Step 5 — Verify auth rejection

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST \
  https://staging.forcedactionleads.com/webhooks/synthflow/inbound \
  -H "X-Synthflow-Secret: wrong_secret" \
  -H "Content-Type: application/json" \
  -d '{"call_id": "smoke_auth_test", "phone": "+18135550188"}'
```

**Pass:** HTTP 401.

---

## Leg B — Outbound: high-intent event → Synthflow call initiation

### Step 1 — Seed a high-intent non-converter

```sql
-- Insert (or update) a test subscriber with RSS >= 70 and no recent conversion.
-- Use a real phone number you can receive calls on.
UPDATE user_segments
SET revenue_signal_score = 75, revenue_signal_band = 'high'
WHERE subscriber_id = <YOUR_TEST_SUBSCRIBER_ID>;

-- Confirm no voice drop in last 7 days and no conversion in last 48h.
SELECT * FROM manual_action_log
WHERE subscriber_id = <YOUR_TEST_SUBSCRIBER_ID>
  AND action_type = 'voice_drop'
  AND created_at > NOW() - INTERVAL '7 days';
-- Should return 0 rows.
```

### Step 2 — Trigger the sweep manually

```bash
cd /root/Forced-action-
python -c "from src.tasks.synthflow_voice_drop_sweep import run; print(run())"
```

Expected: `{"dispatched": 1, "errors": 0, "candidates": 1}` (or more if other subs qualify)

### Step 3 — Read timestamps from DB

```sql
-- T0: event dispatch (agent_decisions row created)
SELECT created_at AS t0_dispatch
FROM agent_decisions
WHERE subscriber_id = <YOUR_TEST_SUBSCRIBER_ID>
  AND graph_name = 'synthflow_voice_drop'
ORDER BY created_at DESC LIMIT 1;

-- T1: Synthflow call initiated (synthflow_calls row)
SELECT created_at AS t1_call_initiated
FROM synthflow_calls
WHERE prospect_phone = '<YOUR_TEST_PHONE>'
ORDER BY created_at DESC LIMIT 1;

-- Delta
SELECT EXTRACT(EPOCH FROM (sc.created_at - ad.created_at)) AS delta_seconds
FROM agent_decisions ad
JOIN synthflow_calls sc ON TRUE
WHERE ad.subscriber_id = <YOUR_TEST_SUBSCRIBER_ID>
  AND ad.graph_name = 'synthflow_voice_drop'
  AND sc.prospect_phone = '<YOUR_TEST_PHONE>'
ORDER BY ad.created_at DESC LIMIT 1;
```

**Pass:** `delta_seconds < 60`

### Step 4 — Confirm no re-dispatch storm

Wait 4 minutes (2 sweep cycles), then:

```sql
SELECT COUNT(*) FROM agent_decisions
WHERE subscriber_id = <YOUR_TEST_SUBSCRIBER_ID>
  AND graph_name = 'synthflow_voice_drop'
  AND created_at > NOW() - INTERVAL '10 minutes';
```

**Pass:** count = 1 (idempotency key dedup prevents re-dispatch).

---

## Sign-off checklist

| Check | Result | Timestamp |
|-------|--------|-----------|
| Leg A delta_seconds < 60 | | |
| Leg A SmsOptIn created with call_id audit | | |
| Leg A replay = duplicate no-op | | |
| Leg A bad secret = 401 | | |
| Leg B delta_seconds < 60 | | |
| Leg B re-dispatch count = 1 after 4 min | | |

Record the actual timestamps in the sign-off row and attach to the deploy PR.
