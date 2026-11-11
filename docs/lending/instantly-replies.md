# Instantly replies (P1.3)

Replies to Next Deal Lending's Instantly cold emails reach Cora, and a STOP or unsubscribe suppresses the contact
everywhere.

## What happens

| Event from Instantly | What we do | Where it shows |
|---|---|---|
| `reply_received`, the lead asks to stop ("STOP", "unsubscribe", "remove me", "take me off", "don't email me", ...) | Suppressed at once through `compliance.propagate_opt_out` (channel email): FA's email/SMS stores, `lending.suppression_list`, every phone that email is on file with in lending, the dialer, and GHL do-not-disturb (synced by the opt-out poller within 15 s). The address is added to the Instantly block list. No answer is drafted. | Notice in `LENDING_OPS_NOTICE_CHANNEL` (#dial-tasks) + Cora's inbox (`ops_notice`) |
| `lead_unsubscribed` (Instantly's unsubscribe link) | Same as a STOP reply | Same |
| `reply_received`, anything else | Stored; Cora's minute sweep classifies it: **deal**, **objection** or **dead** | Triage post in `LENDING_REPLIES_CHANNEL` (#lead-alerts) + Cora's inbox (`replies`) |
| ...deal or objection | Cora drafts an answer (no numbers: anything about pricing is for the call with Josh) onto an Approve / Revise / Reject card | #approvals. Approve sends it inside the Instantly thread, from the inbox the lead replied to |
| ...dead | Triage only. Never a suppression: only a STOP or unsubscribe suppresses | |
| any other event | Answered 200 and ignored | |

Only the lead's own words count for STOP: the quoted earlier email (which may say "Reply STOP to unsubscribe") is
ignored. A whole line that is only "stop" counts; "can't stop thinking about this deal" does not.

A STOP whose opt-out cannot be written is retried by Cora's sweep every minute; after 5 failures ops is alerted to
record it by hand. A reply the model cannot classify is retried, then posted as **Unclassified** with no draft.

## Setup

1. Run the migration before deploying:
   `PYTHONPATH=. python migrations/apply_lending_instantly_replies.py`
2. Production `.env` (NDL's own Instantly workspace, not FA's `INSTANTLY_*`):
   - `LENDING_INSTANTLY_WEBHOOK_SECRET`: any long random value. The route answers 503 while it is unset.
   - `LENDING_INSTANTLY_API_KEY`: from Muhammad, through the secure channel. Without it, approved answers fail with
     "no executor registered" and nothing is sent.
3. Deploy (restarts `lending-api` and `fa-lending-cora`).
4. Muhammad registers the webhook in NDL's Instantly workspace:
   - URL: `https://<our host>/webhooks/lending/instantly`
   - Events: `reply_received`, `lead_unsubscribed`
   - Custom header: `X-Instantly-Webhook-Secret: <the same value as LENDING_INSTANTLY_WEBHOOK_SECRET>`

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
