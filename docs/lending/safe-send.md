# Safe send (Release 1, item 0.4)

Channel circuit breaker, per-minute rate limits, and a check right before a send that the approved recipient is still
the right one. Code: `src/lending/dispatch/` (`circuit_breaker.py`, `revalidate.py`, `resume_command.py`) and the
`held` status in `packages/agent_core` (`pending_actions.py`, `relay.py`). Migration:
`migrations/apply_lending_channel_breakers.py`.

## The channel breaker

One breaker per sending channel: **GHL SMS** (`ghl_sms`), **GHL email** (`ghl_email`), **Instantly** (`instantly`, both
answers in a reply thread and cold email). The senders themselves call it, so every send on the channel counts, not only
approval cards: Cora's cards, missed-call text-backs, booking reminders, the night responder, partner notices.

| Event | What happens |
|---|---|
| A send fails (API error, provider refusal on the send, timeout, 5xx, a 2xx with no message id) | the channel's failure count goes up |
| A send succeeds | the count goes back to 0 |
| **5 failures in a row** | the breaker opens; every approved card on that channel is held; one Cora inbox item (#cora) and a post to `LENDING_OPS_CHANNEL` say which channel, the last error type and how many cards were held |
| Breaker open | `admit` refuses every send on the channel (`GhlChannelPaused` / `InstantlyChannelPaused`): cards are held for review; text-backs, reminders and the night responder see a refusal and use their own retry. Alerts to the team's own phone are never stopped |
| Breaker state cannot be read (table missing, database down) | treated as open: no send without knowing the channel is healthy |

What does **not** count as a channel failure: a recipient the provider declines (Instantly block list, duplicate lead),
a cold-lead or do-not-disturb refusal, a 4xx on a contact upsert (bad contact data), a pause or resume of the Instantly
campaign.

### Reopening a channel (manual only)

No test message is ever sent to a real borrower. A person reopens it:

* Slack, approvers only: `@Cora resume sms`, `@Cora resume email`, `@Cora resume instantly`
* CLI: `python -m src.lending.dispatch.circuit_breaker --status | --reset ghl_sms | --list-held | --release <action_id>`

Reopening also sends the cards that were held *for the breaker* back to `pending` with a fresh approval card. Nothing
sends until each is approved again, and every send-time check runs again.

## Per-minute rate limits (10DLC throughput)

A shared per-channel, per-minute counter in Postgres. Defaults, until NDL's real 10DLC throughput is known (client
question 7): `LENDING_SMS_PER_MINUTE=60`, `LENDING_EMAIL_PER_MINUTE=30`, `LENDING_INSTANTLY_PER_MINUTE=30`.

* Over the cap, a **card** goes back to `approved` and Cora's next minute sweep sends the next batch: 100 texts approved
  in one minute drip out over the following minutes.
* Cards use at most `cap - 10` of each minute, leaving 10 slots for live replies (a missed-call text-back is never
  crowded out by a bulk batch).
* Other senders get a "channel paused" refusal and use their own retry.

## Held cards (the spec's PENDING_REVIEW)

Status `held`, with `held_reason`:

| `held_reason` | Why |
|---|---|
| `breaker_open` | the channel breaker tripped (or was open when the send was tried) |
| `recipient_changed` | the lead's phone (text) or email (email) was edited after approval; or `lead_not_found` |
| `consent_revoked` | no text consent on record any more for the approved number |
| `uncertain_outcome` | the provider may or may not have sent (timeout, 5xx, no message id): never retried |

A held card keeps its place in history, its card in Slack says why, and it is **never retried automatically**. Reopen it
with `--release <action_id>` (fresh card, fresh expiry, new approval) or reject it. An opt-out or suppression-list hit
is still a hard `blocked`, never a hold.

## Re-validation right before the send

The card freezes the recipient when it is drafted. Just before dispatch, `revalidate.changed_recipient` compares that
frozen phone/email with the lead record the card names (`web_lead:`, `lendingflow_lead:`, `contact:` or `ghl:<id>`
`contact_ref`). Changed, or the lead is gone: held. A `contact_ref` we hold no lead record for (an Instantly reply, a
document chase, a GHL contact outside our leads) cannot be compared and is allowed; the opt-out and consent checks
still run on the frozen value.

## What it does not cover

* Carrier delivery failures that arrive after the API accepted the message are not counted (we receive no GHL
  message-status events today; client question 6).
* A double-tap on Approve already sent once (each state change is one conditional update); that is unchanged.

## Deploy

1. `PYTHONPATH=. python migrations/apply_lending_channel_breakers.py` (idempotent). It must run **before** the code:
   every send asks the breaker table first, and an unreadable breaker refuses the send.
2. Restart `fa-lending-cora`, `lending-api`, the call poller and the reminder worker.
3. Set `LENDING_OPS_CHANNEL` (alerts) if it is not set; set the per-minute caps once NDL's 10DLC throughput is known.
