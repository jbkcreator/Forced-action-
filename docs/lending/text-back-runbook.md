# Missed-call text-back (WP-GL-9): runbook

Unanswered outbound lending calls are texted through GoHighLevel (GHL). Code: `src/lending/text_back.py`
(processor), `src/lending/ghl_sms.py` (GHL account + send), `config/lending_text_back.py` (templates, window).
It runs inside the single CDR poller (`python -m src.lending.cdr_poller`, systemd `fa-lending-cdr-poller`) and
consumes `lending.missed_call_events`. PR #320's `missed_call_poller` / `missed_call_texts` are superseded; do not
run them. Everything below the first section is the checklist that remains once GHL access arrives.

## 1. What it does and the rules

- An unanswered **outbound** call queues a `lending.missed_call_events` row. The text-back decides it once.
- The text must go out within **60 s** of the call ending; later than that is `skipped_late`.
- **Consented numbers only** (`src.lending.consent.has_text_consent`: `lending.text_consents`, with suppression / do-not-contact
  overriding). No consent: `skipped_no_consent`.
- **One text per contact per Eastern day.** A second unanswered call the same day is `duplicate_day`. Only `pending`, `sending`,
  `sent` and `send_unknown` hold the day's slot; a skipped, dry-run or failed text does not.
- Every text ends "Reply STOP to opt out." Wording is the client-approved set in `config/lending_text_back.py`.
- **GHL only.** Lending-engine SMS go through GoHighLevel; every other FA SMS (subscribers, alerts) stays on Telnyx via
  `sms_compliance.send_sms`.
- Safety window 8 am-8 pm ET (`skipped_quiet_hours` outside it). Calls already stop at 7:15 pm ET.
- **Off until the sending number is A2P Verified.** Until then the flag stays `false`, the line runs calls-only with live voicemail.
- The send is a **single attempt, never retried**: a possible duplicate text is worse than a missed one. An ambiguous outcome (timeout, 5xx, no message id) is recorded `send_unknown`, not `failed`.

| Status | Meaning |
|---|---|
| `pending` / `sending` | Queued / claimed by the processor. A claim stuck in `sending` over 5 min (crash) is marked `send_unknown` by the stale sweep: never retried or resent, and it keeps the day's slot |
| `sent` | GHL accepted the text; `provider_message_id` stored |
| `dry_run` | Every gate passed but `MISSED_CALL_TEXT_ENABLED=false`; nothing sent |
| `failed` | Nothing went out: render error, contact-upsert failure (single attempt), a 4xx on the send, or the 60 s send deadline passing before the send started. Frees the day's slot |
| `send_unknown` | Cannot prove nothing went out, so it keeps the day's slot and is never resent: crash after the claim (stale sweep), or an ambiguous GHL send (request timed out/errored, a 5xx, or a 2xx with no message id). Check GHL for a duplicate or missing text |
| `skipped_late` | More than 60 s after the call ended |
| `skipped_no_consent` | No text consent on file for the phone |
| `skipped_quiet_hours` | Outside 8 am-8 pm ET |
| `skipped_not_configured` | Flag on but no GHL account or no texting number |
| `blocked` | Number was suppressed (`lending.suppression_list` or do-not-contact) when the event was queued |
| `duplicate_day` | Another sendable event already holds this contact's ET day |

## 2. Environment (server `.env`, never committed)

Next Deal Lending is the only GHL account lending uses (texts, the opt-out DND sync and the DND backstop); there is no
fallback to the platform's `GHL_API_KEY` / `GHL_LOCATION_ID`.

- `LENDING_GHL_API_KEY` (Private Integration token of the Next Deal Lending sub-account) and `LENDING_GHL_LOCATION_ID`: set **both**.
  Restart `fa-api`, `fa-lending-cdr-poller` **and** `fa-lending-opt-out-poller` after changing them (settings are cached per process:
  the texts are sent by the CDR poller and the DND sync runs in the opt-out poller, so a process left on old values would keep
  using them). Setting only **one** of the two is a misconfiguration: texts and DND fail closed (no account is used) with an ERROR log.
- `LENDING_GHL_SMS_FROM_NUMBER`: E.164 of the **one number used for calling and texting**. It is also the number printed in
  "call or text me back at ...". There is no separate text-back number setting.
- `LENDING_GHL_WEBHOOK_SECRET`: shared secret for the `X-Webhook-Secret` header on the GHL webhooks (already exists).
- `MISSED_CALL_TEXT_ENABLED=false` until section 5(e).

## 3. GHL-side setup, in order

Client gives `hari@heu.ai` Agency Admin and a card first (client answers B2/B1).

1. Agency Settings -> Phone Integration -> Switch to LeadConnector Phone. Sub Account Settings -> Next Deal Lending -> Link to
   LeadConnector; wait for the green LC Phone badge. Add a card (Settings -> Billing).
2. Send the client the expected monthly cost (number rental, per-message, 10DLC one-time plus monthly campaign fee) **before**
   buying (client question 10).
3. Settings -> Phone Numbers -> Add Number: US, Local, SMS + Voice, area code 813 or 727. Complete identity verification. Record the
   number in `LENDING_GHL_SMS_FROM_NUMBER`. A number normally lives with one provider: confirm with the client how the same number
   serves BatchDialer calls and GHL SMS (v2 Q8).
4. Credentials. Create a Private Integration token with the scopes contacts write and conversations/messages write in the Next
   Deal Lending sub-account and set `LENDING_GHL_API_KEY` + `LENDING_GHL_LOCATION_ID` (the `--send-test` in section 5 proves it).
5. **10DLC (Trust Center)**, only after `nextdeallending.com` is public (WP-GL-11): Standard Brand, brand **Next Deal Lending**, legal
   entity HEU AI LLC, address 971 US Highway 202N, Ste N, Branchburg, NJ 08876 (client C3), website URL. Use case: missed-call
   text-back, booking confirmations, reminders, replies. Sample messages: the three templates in `config/lending_text_back.py` plus
   the three approved confirmation/reminder texts. Opt-in: website form checkbox (unchecked by default) plus verbal consent on the
   call ("Is it okay if we text you the confirmation?"). Opt-out: "Reply STOP". **Confirm with the client** whether the existing
   HEU AI LLC registration is updated or a new GHL registration is filed (open question H3 / v2 Q18), and whether HEU AI LLC is the
   legal entity or Next Deal Lending is a DBA.
6. Wait for approval. The number must show **A2P Verified** before any live text. If not approved by launch: calls-only with live
   voicemail; texting switches on the day it clears.

## 4. GHL workflows to create

Merge-field names below are the intended shape and must be **confirmed in the GHL UI**. All webhooks send header
`X-Webhook-Secret: <LENDING_GHL_WEBHOOK_SECRET>`.

- **Contact DND changed** -> Webhook `POST https://<fa-api host>/webhooks/lending/ghl-opt-out`, body
  `{"contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "email": "{{contact.email}}"}`. Built (PR #320). This workflow is
  the wiring behind the client's request "GHL wired into the #320 opt-out sync" (confirm with the client).
- **Customer replied (SMS)** -> Webhook `POST .../webhooks/lending/ghl-text-consent`, body
  `{"source": "inbound_text", "contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "message": "{{message.body}}"}`
  (confirm the message merge field). GHL's built-in STOP handling still sets DND for bare keywords (the **Contact DND changed** workflow covers that path).
  Consent rules (fail closed; **counsel should confirm both phrase lists** in `config/lending_text_back.py` before go-live):
  - empty or non-string message: nothing recorded.
  - **Hard opt-out**: the whole message is one of stop / stopall / unsubscribe / cancel / end / quit, or it contains stop, stopall,
    unsubscribe, optout, revoke or "opt out". The router revokes consent **and makes the opt-out durable itself**: the number goes into
    `lending.suppression_list`, is removed from the dialer, and the opt-out poller's DND sync writes it to GHL (channel `sms`, so
    `ghl_dnd_at` stays unset until that write succeeds). A later answered call cannot re-grant consent.
  - **Soft decline**: cancel / end / quit inside a longer message, or a phrase such as "wrong number", "remove me", "take me off",
    "no more", "don't text", "do not contact", "leave me alone", "not interested". Consent is revoked and nothing is recorded, but the
    number is **not** suppressed and **not** removed from the dialer; a later answered call can re-grant consent.
  - any other non-empty reply records `inbound_text` consent.
  - A hard opt-out is permanent and matches words anywhere, so a false positive (e.g. "bus stop", "please don't stop calling") suppresses the number and deletes it from the dialer, and it must be undone by hand: delete its row from `lending.suppression_list`, set `do_not_contact = false` on its `lending.contacts` row, delete its `sms_opt_outs` row (source `lending_dialer`, which is FA-wide and also blocks every other FA text), and, if the poller has already synced it (`ghl_dnd_at` set on its `lending.opt_out_events` row), clear the DND in GHL, since no code path ever clears it; the dialer removal deletes the BatchDialer contact and nothing restores it (`restore` only undoes calling-window holds), so the number comes back only on the next list build and that needs a manual check.
- **Form submitted** (website lead form, WP-GL-11) -> Webhook `POST .../webhooks/lending/ghl-text-consent`, body
  `{"source": "web_form", "contact_id": "{{contact.id}}", "phone": "{{contact.phone}}", "consent": "{{<consent checkbox custom field>}}"}`
  (confirm the custom-field merge name). The checkbox must be unchecked by default; only a checked value records consent.

**Which opt-outs are durable.** A hard opt-out caught by the consent webhook is made durable by the webhook itself (above). A soft
decline only flips `lending.text_consents`; a later answered call or CDR can re-grant `inbound_call` / `on_call_yes` while the BatchDialer
`text_consent` field is still `yes`. Bare keywords that GHL handles itself (its DND) reach us through the **Contact DND changed** workflow,
which posts to `/webhooks/lending/ghl-opt-out` and writes `lending.suppression_list` (`has_text_consent` checks it). That workflow stays a
hard go-live prerequisite (section 5). The DND sync only runs while `fa-lending-opt-out-poller` is running.

- **Pipeline stage changed** -> `.../webhooks/lending/ghl-stage` (scoreboard "Showed", PR #326; lives on that branch, not this one,
  until merged).

## 5. Go-live sequence

a. `PYTHONPATH=. python migrations/apply_lending_gl9_text_back.py` (idempotent; run after `apply_lending_call_dispositions_dialer.py`).
b. Set the env vars from section 2. **Hard prerequisite:** create the **Contact DND changed** workflow (section 4) and test it with a
   real STOP reply (contact goes DND in GHL, the phone appears in `lending.suppression_list`, `has_text_consent` is false). Texting must
   not be switched on until this passes.
c. `python -m src.lending.ghl_sms --send-test <your own phone>` (sends one REAL SMS to the phone you pass). Confirm it arrives from the calling/texting number. The request field
   names follow the public GHL v2 reference and are not yet confirmed live: fix `src/lending/ghl_sms.py` if GHL disagrees.
d. Restart `fa-lending-cdr-poller` (and `fa-api` and `fa-lending-opt-out-poller` if the webhook secret or LENDING_GHL_* changed). With the flag still off, make a test
   unanswered outbound call to a consented test contact and confirm the event ends `dry_run`.
e. When the number is A2P Verified, set `MISSED_CALL_TEXT_ENABLED=true` and restart `fa-lending-cdr-poller`.
f. Run the verification in section 6.

**Rollback:** set `MISSED_CALL_TEXT_ENABLED=false` and restart; new events return to `dry_run`.

## 8. Decisions taken (team lead, 2026-10-02)

- Next Deal Lending's GHL sub-account is the only GHL account lending uses (2026-10-08): no fallback to the shared `GHL_*` account.
- One number for calling and texting. Texting stays off until it is A2P Verified.
- Missing first name -> "Hi"; missing caller -> "our team"; Option 1/2 fall back to Option 3 without address/county.
- **Outbound calls only.** An unanswered inbound callback creates no automated text: inbound speed-to-lead is month-two scope, an
  unanswered inbound call is not consent evidence under #319, and the callback queue already rings the original caller, then any open
  caller, then voicemail (voicemails become next-morning calls).
- 8 am-8 pm ET safety window (Florida's texting law is a counsel question; this does not advise on it).
- Consent lives in `lending.text_consents` (+ BatchDialer `text_consent`), not a GHL contact field (client D4).

## 9. Not in WP-GL-9

Confirmation/reminder texts, reply agent, Conversation AI, email fallback for non-consenters (WP-GL-10); website and consent checkbox
(WP-GL-11); Deal Drop marketing opt-in checkbox (first drop Oct 12, week-two scope); counsel sign-off on cold-number texting
(client/counsel).
