# Instantly replies (P1.3)

Replies to Next Deal Lending's Instantly cold emails reach Cora, and a STOP or unsubscribe suppresses the contact
everywhere.

## What happens

| Event from Instantly | What we do | Where it shows |
|---|---|---|
| `reply_received`, the lead asks to stop ("STOP", "Please stop", "No thanks, stop", "unsubscribe", "remove me/us", "take us off your list", "stop all emails", "don't send me any more emails", ...) | Suppressed at once through `compliance.propagate_opt_out` (channel email): FA's email/SMS stores, `lending.suppression_list`, every phone that email is on file with in lending, the dialer, and GHL do-not-disturb (synced by the opt-out poller within 15 s). The address is added to the Instantly block list. No answer is drafted. | Notice in `LENDING_OPS_NOTICE_CHANNEL` (#dial-tasks) + Cora's inbox (`ops_notice`) |
| `lead_unsubscribed` (Instantly's unsubscribe link) | Same as a STOP reply | Same |
| `reply_received`, anything else | Stored; Cora's minute sweep classifies it: **stop**, **deal**, **objection** or **dead** | Triage post in `LENDING_REPLIES_CHANNEL` (#lead-alerts) + Cora's inbox (`replies`) |
| ...stop (a stop request in wording the phrase check missed) | Suppressed by the sweep exactly like a STOP reply (stores, dialer, GHL do-not-disturb, Instantly block list), within about a minute. No answer is drafted | Same as a STOP reply (no triage post) |
| ...deal or objection | Cora drafts an answer (no numbers: anything about pricing is for the call with Josh) onto an Approve / Revise / Reject card | #approvals. Approve sends it inside the Instantly thread, from the inbox the lead replied to |
| ...dead | Triage only. Never a suppression: only a STOP or unsubscribe suppresses | |
| any other event | Answered 200 and ignored | |

Only the lead's own words count for STOP: the quoted earlier email (which may say "Reply STOP to unsubscribe") is
ignored. A line that is only "stop" with a short lead-in ("Please stop", "No thanks, stop") counts; "can't stop
thinking about this deal" does not. The model is told to choose **stop** whenever a reply could be read as asking
not to be contacted.

A STOP whose opt-out cannot be written is retried by Cora's sweep every minute; after 5 failures ops is alerted to
record it by hand. A reply the model cannot classify is retried, then posted as **Unclassified** with no draft and a
note to read it in Instantly and opt the lead out by hand if they asked to stop.

## Setup

NDL runs as its own campaign, **NDL Cora Passthrough**, inside FA's Instantly workspace (not a separate workspace),
with its own API key. Instantly sends webhook events for the whole workspace unless a webhook is limited to one
campaign, so both NDL webhooks are limited to NDL Cora Passthrough: FA's campaign replies and unsubscribes never
reach Cora. The workspace has one block list, so an address blocked by an NDL opt-out is also blocked for FA's
Instantly campaigns, and the other way round.

1. Run the migration before deploying:
   `PYTHONPATH=. python migrations/apply_lending_instantly_replies.py`
2. Production `.env` (separate from FA's `INSTANTLY_*` settings):
   - `LENDING_INSTANTLY_API_KEY`: an API key created in the workspace for NDL ("NDL cora"; scopes: campaigns, leads,
     emails, block list entries). Without it, approved answers fail with "no executor registered" and nothing is
     sent.
   - `LENDING_INSTANTLY_WEBHOOK_SECRET`: a long random value we generate (`openssl rand -hex 32`), never FA's
     `INSTANTLY_WEBHOOK_SECRET`. The route answers 503 while it is unset.
   - `LENDING_INSTANTLY_CAMPAIGN_ID`: the NDL Cora Passthrough campaign id (P1.1's cold emails).
3. Deploy, or restart `lending-api` and `fa-lending-cora` after a `.env` change.
4. Register two webhooks in Instantly (Settings → Integrations → Webhooks), the same for both except the event:
   - URL: `https://api.nextdeallending.com/webhooks/lending/instantly`
   - Event type: **Reply Received** for one, **Lead Unsubscribed** for the other
   - Campaign: **NDL Cora Passthrough** only, never "All campaigns"
   - Header: `X-Instantly-Webhook-Secret`, value the same as `LENDING_INSTANTLY_WEBHOOK_SECRET`
5. Check the route with a wrong secret; `403` means it is live and the secret is loaded:
   `curl -s -o /dev/null -w "%{http_code}\n" -X POST https://api.nextdeallending.com/webhooks/lending/instantly -H "Content-Type: application/json" -H "X-Instantly-Webhook-Secret: wrong" -d '{}'`
6. Instantly's own calls show in `journalctl -u lending-api | grep -i instantly`: `200` is accepted, `403` means the
   webhook header does not match `.env`.

## Proof for Josh

1. Reply to a test Instantly email with a deal question: within 2 minutes the triage post appears in #lead-alerts and
   the answer card in #approvals.
2. Reply "STOP" (or click the unsubscribe link): the notice appears in #dial-tasks and the address is in
   `lending.suppression_list`; a phone on file for that email is in the dialer removal and gets GHL do-not-disturb.

## Unverified

Instantly's webhook field names (`lead_email`, `email_account`, `email_id`, `reply_text`, ...) and the reply call
(`POST /api/v2/emails/reply` with `eaccount`, `reply_to_uuid`, `subject`, `body`) follow Instantly's public
reference, not a captured delivery. Turn on `LENDING_GHL_LOG_PAYLOAD_SHAPE` for the first test reply to log the
field names (never values), and check the first approved answer lands in the thread.
