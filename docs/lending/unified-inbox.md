# Unified inbox (R1 item 1.5)

Every inbound text, email and cold-email reply, and every missed inbound call, on the channels NDL uses lands in one
Slack stream (`LENDING_REPLIES_CHANNEL`, #lead-alerts) within 2 minutes, tagged **deal**, **question**, **objection** or
**dead**, with a drafted reply card in #approvals. Josh is mentioned on **deal** items only. Off until
`LENDING_UNIFIED_INBOX_ENABLED=true`.

## What lands in the stream

| Channel | Source | Table | Card (egress channel) |
|---|---|---|---|
| GHL text | `POST /webhooks/lending/ghl-reply` (inbound) | `lending.inbox_events` | `ghl_sms` |
| GHL email | same webhook, message type email | `lending.inbox_events` | `ghl_email` |
| Other GHL message (Facebook, Instagram, web chat) | same webhook | `lending.inbox_events` | none: triage line only |
| Instantly reply | `POST /webhooks/lending/instantly` (P1.3, unchanged) | `lending.instantly_replies` | `instantly_email` |
| Missed inbound call (GHL) | `POST /webhooks/lending/ghl-missed-call`, direction incoming | `lending.inbox_events` | `ghl_sms` with consent, else a `ghl_task` call-back |
| Missed inbound call (BatchDialer) | CDR poller, inbound zero-second call on a lending campaign | `lending.inbox_events` | same as above |

**Not connected, by the client's answer to question 1 (27 Nov):** HubSpot and Salesmsg (Backflip systems, never added),
Gmail and Hunter (not in NDL's setup yet). Adding one later is one adapter that builds an `InboundItem` and calls
`unified_triage.record_inbound`, plus one entry in `config/lending_unified_inbox.SOURCES`. The spec's "6 channels"
Done-when is shown on the channels above.

## Intents, cards and Josh's mention

- **deal**: has a deal or funding need and needs Josh's decision on pricing, terms or closing, or asks for a call.
  **question**: asks something we can answer without that decision. **objection** and **dead** as in P1.3. A missed
  call is a **deal** when the caller's phone has an open loan file (before FUNDED, not closed), else a **question**;
  no model call is made for it.
- **Josh is mentioned (`<@id>` for each `LENDING_CORA_APPROVER_USER_IDS`) on deal lines only**, Instantly included. No
  new Slack scope is needed; he must be a member of the replies channel for it to notify him.
- **Cards** are Approve / Revise / Reject drafts, never auto-sent: they carry no template version, so they never
  graduate to standing auto-send. Dead items get the line only. A stop request gets no card.
- A **stop** is opted out at once through `compliance.propagate_opt_out`: a stop phrase in a GHL email or text by the
  webhook, wording only the model catches by the sweep. A text that is just a stop word (STOP, "please stop") is left
  to GHL's own do-not-disturb. A stop that cannot be recorded after 5 attempts posts ":rotating_light: ... NOT
  suppressed: record it by hand".
- **No card** when: the contact is opted out; an after-hours conversation is open (the night responder answers); the
  GHL reply agent already answered the text; a person already replied in GHL; a test contact while
  `LENDING_SLACK_TEST_ENABLED` is off (skipped silently).
- **The GHL reply agent ("NDL Reply Agent")** answers texts only. A text waits 30 s (`AI_GRACE_SECONDS`) for the agent
  or a person. If the agent hands it to Josh (its three Stop Bot final messages, or a rate/terms or reschedule text), or
  nobody answers, Cora drafts the card. With the agent off (as now) every text gets a card.
- A rate/terms or reschedule text also still gets `reply_guard`'s handoff post and reply SLA, so it shows two lines.

## Setup, in order

1. Run `PYTHONPATH=. python migrations/apply_lending_unified_inbox.py` (creates `lending.inbox_events`, adds
   `question` to the `instantly_replies` intent check). **Before deploying the code.**
2. GHL, workflow "Customer replied": make sure it includes the **Email** channel. Turn on
   `LENDING_GHL_LOG_PAYLOAD_SHAPE=true`, send one test email and confirm the message-type key; if it is not one of
   `config.lending_unified_inbox.GHL_MESSAGE_TYPE_PATHS` / `GHL_EMAIL_TYPES`, update them. An absent type is treated as a text.
3. GHL, new workflow "NDL - Inbound missed call to backend": trigger Call details, direction **Incoming**, status
   no-answer / voicemail / busy. Action: Webhook POST `/webhooks/lending/ghl-missed-call` with `X-Webhook-Secret` and
   the body of the outbound workflow, but `phone` = `{{phoneCall.from}}` and `direction` = `incoming`. Capture the
   payload shape for one test call: if the status GHL sends for a missed inbound call is not in
   `GHL_MISSED_CALL_STATUSES` (for example `missed`), add it there.
4. **BatchDialer check** (needs the live key; done once): confirm lending campaigns report inbound zero-second calls.
   On the server:

   ```bash
   PYTHONPATH=. python -c "
   from collections import Counter; from datetime import date, timedelta
   from src.lending.dialer_port import get_http; from src.lending.cdr_poll import iter_day
   from src.lending.dispositions import parse_event
   http = get_http(); c = Counter()
   for d in range(7):
       for item in iter_day(http, date.today() - timedelta(days=d)):
           ev = parse_event(item)
           if ev: c[(ev.direction, (ev.duration or 0) == 0, ev.campaign_id)] += 1
   print(c)"
   ```

   No `('inbound', True, <lending campaign>)` rows means inbound calls ring the GHL number (656) 221-1100 and the
   GHL webhook covers every missed call: the CDR step is then simply idle (record "BatchDialer: none, checked <date>").
5. Set `LENDING_UNIFIED_INBOX_ENABLED=true`; restart `lending-api`, `fa-lending-cora` and `fa-lending-cdr-poller`.
   While it is off the adapters record nothing and the ghl-missed-call route answers `{"status": "disabled"}` for an
   inbound call.

## Reply agent checks (in GHL, once)

- The "Conversation AI sent a message" workflow posts to `/webhooks/lending/ghl-reply` with `direction: outbound` and no
  `userId`; a message a person typed carries `userId`.
- The agent's three Stop Bot final messages equal `REPLY_AGENT_HANDOFF_MESSAGES` (in `config/lending_unified_inbox.py`)
  word for word. If someone edits them in GHL, edit the config. A mismatch costs a card for non-rate, non-reschedule
  handoffs only; the triage line still shows the text.
- The agent runs on GPT 4.1, which GHL marks "Retires 15 Dec": pick a new model and re-run its six hard-rule tests.
  Separate from 1.5.

## Measuring the 2 minutes

```sql
SELECT channel, percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM triage_posted_at - received_at))
  FROM lending.inbox_events WHERE triage_posted_at IS NOT NULL GROUP BY channel;
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - received_at))
  FROM lending.instantly_replies WHERE processed_at IS NOT NULL;
```

Each item logs `[unified-inbox] item N intent=... latency_s=N`, with a warning over 120 s. A text includes its 30 s wait.

## Done-when rehearsal (the screen recording)

Record Mon-Fri between 9:00 AM and 7:15 PM ET (outside those hours an inbound text opens an after-hours conversation and
gets no card). Confirm Josh is in `LENDING_REPLIES_CHANNEL`. Use real non-test contacts, or `LENDING_SLACK_TEST_ENABLED=true`.
With a timestamp visible on screen:

1. A GHL text asking about rates: deal line with Josh's mention, an SMS card.
2. A GHL email asking what documents are needed: question line, no mention, an email card.
3. An Instantly reply with an objection: objection line, no mention, an Instantly card.
4. A call to (656) 221-1100 left unanswered: a line, and an SMS or a call-back task card.
5. If step 4 of the setup found BatchDialer inbound misses: one missed BatchDialer call.

Show each line arriving within 2 minutes and run the latency query.

## Decisions recorded (27 Nov 2026)

The missed-call text (`MISSED_CALL_REPLY_TEXT`), the triage wording, deal-only mentions, no auto-send graduation for
inbox cards, and the reply-agent rules above were approved. Rollback: set `LENDING_UNIFIED_INBOX_ENABLED=false` (new
items stop being recorded; Instantly triage keeps working; rows already in `inbox_events` stay).
