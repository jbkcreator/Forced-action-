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

**Decided in the webhook, not the sweep.** The webhook also treats any wording that could mean stop as a STOP on the
spot (`instantly_replies.could_mean_stop`: cease, unsubscribe, harass, "leave us alone", "stop this/it/now", "remove /
delete / take me|us|my ... name|email|number|from", "this is spam", "no longer want to receive", "never email me", on top
of the phrases above). The wording is specific on purpose, so ordinary replies ("we no longer have that property",
"please stop by Tuesday", "remove the prepayment penalty", an out-of-office) are not suppressed; they go to the model.
A possible STOP is suppressed within seconds and never waits for Cora's sweep or the AI model (it also works while the
model is down). If one is wrong, lift the suppression by hand.

**Which phones an email-only STOP reaches** (`compliance._with_linked_phones`): phones on lending contacts, website and
LendingFlow leads, partners, and the traced emails in the cold calling lists (`lending.calling_pool_staging`); then,
for a cold contact held only in GHL and the dialer, the GHL contact found by that email and every dialer load of that
GHL contact id (`ghl_email_lookup.py`, read only, one attempt per email, 4 s timeout, 30 s for the batch, at most 10 emails; every email is asked, even one already linked to a phone). Each phone
gets its own opt-out event, so the dialer removal retry and the GHL do-not-disturb sync cover it. If the GHL search
fails the email is still suppressed; only the phone link is missed (logged `[ghl-email-lookup] GHL search failed`) and
a `stop_phone_link_failed` item goes to Cora's inbox (no address in it), so a person checks the dialer and GHL for that
contact by hand.

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

## Deal replies become GHL contacts (off until `LENDING_INSTANTLY_GHL_HANDOFF_ENABLED=true`)

Cold emails and the first answer stay in Instantly. When the model reads a reply as **Deal** (and only then), Cora's sweep
hands the lead to the Next Deal Lending GHL account (`src/lending/instantly_ghl_handoff.py`):

- the GHL contact is **found by email** (a contact search, exact match) and used as it is; only when there is none is it created,
  from the email alone (no do-not-disturb, tags, source or phone in the create, so nothing an earlier STOP or the dialer set is
  overwritten);
- the tag `ndl-instantly-interested` is added. `ndl-cold` is removed (so the agents' later emails to this contact go through GHL)
  **only when GHL email is open for the contact**. A cold-imported contact carries Email do-not-disturb, which the GHL email rail
  refuses (`blocked_consent`), so it **keeps `ndl-cold` and its Instantly route** for follow-up emails; its do-not-disturb is never lifted
  here, and a note on the contact says so. The first answer is a Cora draft sent inside the Instantly thread, so it is not affected;
- a pipeline card is created at the new-lead stage (`LENDING_GHL_PIPELINE_ID`, `LENDING_GHL_STAGE_NEW_LEAD`) when the contact
  has none; a card already in any stage is never moved. Without those two settings the lead is contact-only;
- a note says the contact came from an Instantly reply;
- if the lead gives a phone number in the reply (their own words, not the quoted email), it is put on the contact, but only when no
  other GHL contact owns that number and this contact has no phone of its own; otherwise it is left off and the note says why (a
  shared office line can never redirect the hand-over to another contact). That number is not consent to text;
- nothing is written while the lending emergency halt is on.

An address that opted out is never handed over, and neither is a GHL contact that already carries the `lending-opt-out` tag or Call do-not-disturb. Do-not-disturb is never lifted: texts still need consent. A failed hand-over
never blocks the triage or the answer; it raises a `instantly_ghl_handoff_failed` item in Cora's inbox (no address in it) so
the team adds the contact by hand. Built from the team's recommendation for open question P1.3 #1: switch it on once Josh
confirms.

Limits to know before switching it on (read at Cora's start-up: restart `fa-lending-cora` to change the setting):

- The contact is matched by **email only**. A cold contact loaded into GHL by phone with no email on file is not found, so a second
  contact is created. Check the first few hand-overs.
- A failure part-way (for example the tag step) leaves a **partly built** contact. The reply is not retried; the inbox item names the
  step that failed, and the team finishes the contact, tag and card by hand.
- Moving a cold-imported contact fully to GHL email (removing `ndl-cold` and lifting its Email do-not-disturb) is **not done here**: it
  needs Josh's decision on the client rule that a cold lead gets no GHL email. Until then those contacts keep getting follow-up emails
  through Instantly. A contact whose GHL email is open does move to the GHL rail (the rail follows the tag, and the GHL rail does not add
  the cold-email footer).
- An address in `lending.suppression_list` or `email_opt_outs` is never handed over. A phone number the lead types in the reply is not checked against the
  suppression lists before it is put on the contact (the call gate tags suppressed numbers afterwards); several contacts sharing one email use the lowest id.
- The hand-over runs inside Cora's sweep and makes about eight GHL calls; a GHL outage can slow that sweep by up to a few minutes.

## Only NDL campaigns, the block list and the unsubscribe link

- **Only NDL campaigns reach Cora.** The Instantly workspace is shared with other campaigns. The webhook ignores (answers 200, stores
  nothing) an event whose `campaign_id` is not the NDL passthrough campaign (`LENDING_INSTANTLY_CAMPAIGN_ID`) or one of
  `LENDING_INSTANTLY_EXTRA_CAMPAIGN_IDS` (comma separated). An event that names no campaign is accepted, so a STOP is never dropped; with
  no NDL campaign configured nothing is filtered. **If NDL runs a second campaign, add its id to `LENDING_INSTANTLY_EXTRA_CAMPAIGN_IDS`: an opt-out from a campaign that is not listed is ignored.** Instantly's field names are unverified: check `campaign_id` in the first delivery's
  logged payload shape. Ids are compared case-insensitively; an ignored event is logged as a warning with the campaign id.
- **Every opt-out goes onto Instantly's block list.** Any opt-out that carries an email (the unsubscribe link, the opt-out mailbox, an
  Instantly STOP, an email opt-out from Forced Action) is added to the NDL workspace block list by `compliance._propagate`, best effort:
  at most 10 per batch and 15 s (each call can take up to 20 s), never raises; it runs inside the opt-out's transaction, so it also
  covers Forced Action unsubscribes (the shared-unsubscribe design). Test and internal addresses are not excluded. A phone-only opt-out has no address to add.
- **The unsubscribe link is on NDL's own host.** The cold-email footer link points at `LENDING_UNSUBSCRIBE_BASE_URL` +
  `/api/lending/email/unsubscribe?token=...` (served by `lending-api`: GET only shows an Unsubscribe button, POST opts out and is the RFC 8058 one-click target; a valid signed link is never rate limited, requests with an invalid token are, per IP), never at the Forced Action app. Cold email
  stays off until the setting is set (for example `https://api.nextdeallending.com`, which nginx already routes `/api/lending/` to
  lending-api). A valid link is an opt-out on every channel (`propagate_opt_out`). Instantly's own `List-Unsubscribe` header
  (`insert_unsubscribe_header`) stays on Instantly's domain; its unsubscribes arrive as `lead_unsubscribed` events.

**Block-list retry.** An opt-out does not call Instantly inside its own transaction any more: `compliance._propagate` queues each
opted-out address in `lending.instantly_block_queue` (idempotent per address; a failed write is logged and never breaks the opt-out),
and Cora's minute sweep (`BlockListSweep`) sends up to 10 pending addresses per pass to Instantly's block list. A failed attempt is
retried with a growing wait (1 minute doubling to 1 hour); an address Instantly already holds (HTTP 409) counts as done; after 12 failed
tries the row is marked `failed` and an `instantly_block_failed` item (no address) goes to Cora's inbox so a person adds it by hand.
A `failed` row, or one `done` more than a day ago, is queued again by a new opt-out for the same address. Rows stay pending while `LENDING_INSTANTLY_API_KEY` is unset. **Run `migrations/apply_lending_instantly_block_queue.py` before
deploying**: without the table an opt-out is still recorded, but its address is not queued.
