# Dialer call disposition logging: setup runbook

Every dialer call becomes one row in `lending.call_dispositions`; unanswered calls also queue a
`lending.missed_call_events` row (no consumer; see the handoff section). The result reaches the Google Sheet and Slack `#dial-tasks`
within 30 seconds. This runbook is vendor-neutral; the BatchDialer-specific steps are marked.

Call results are ingested by **polling BatchDialer call records (CDRs)** with the service
`python -m src.lending.cdr_poller` (`deploy/systemd/fa-lending-cdr-poller.service`). BatchDialer has no
webhooks, so there is no dialer webhook route.

**Unverified until the live check in `docs/lending/batchdialer-api-findings.md` (Task 0, not yet done)
is filled in:** the CDR field strings (for example `"ANSWER"`), whether an unanswered call appears as a
CDR, and how a contact is held. Confirm them before go-live.

## 1. Order of setup

1. Migrations, in order (all idempotent):
   `apply_lending_compliance.py` → `apply_lending_call_dispositions.py` →
   `apply_lending_call_dispositions_dialer.py` → `apply_lending_pr319_client_feedback.py`.
2. `.env`:
   - `DATABASE_URL` (the shared database; lending has no separate DB URL)
   - `LENDING_DIALER_CAMPAIGN_IDS` (comma-separated BatchDialer **campaign ids**, from `GET /api/campaigns`; **empty ignores every call**)
   - `LENDING_SEAT_GROUPS` (`agentid:A,agentid:B`, keyed by BatchDialer **agent id**; a seat missing here gets no shift group)
   - `LENDING_DISPOSITION_MISSING_ALERT_MINUTES` (default 10)
   - `LENDING_SLACK_BOT_TOKEN`, `LENDING_DIAL_TASKS_CHANNEL`,
     `LENDING_SHEETS_SERVICE_ACCOUNT_KEY_PATH`, `LENDING_DISPOSITION_SHEET_ID` (+ `_TAB`)
   - `BATCHDIALER_API_KEY` (the API token: **User icon → Settings → Integrations → Custom Integration**; shared with the dialer load; `.env` only, never in chat or PRs)
3. Start `fa-lending-cdr-poller` (`deploy/systemd/fa-lending-cdr-poller.service`). Run **exactly one
   instance**: the `/v2/cdrs/last` watermark is per API key, so a second poller (or anything else using
   that endpoint with the same key) steals records. The GHL webhooks are served by `fa-api` (no separate lending server).
   Poller behavior to know:
   - Inbound calls are ignored, except calls disposed `DNC_REQUEST` (those are opted out; no attempt is counted).
   - The day rescan re-reads today and yesterday (UTC) every few minutes. After an outage longer than that,
     run `python -m src.lending.cdr_poller --backfill-days N` (rescans N days, then continues; with `--once` it
     rescans and exits). DNC rows whose opt-out failed are retried every cycle regardless of the window.
   - `--once` also advances the `/v2/cdrs/last` watermark for the API key, so it can consume calls that only
     the running poller's rescan will then recover. Prefer stopping the service first.
   - UNVERIFIED (Task 0, needs a live account check): the pagination parameter name `next_page` and the CDR
     status strings (`ANSWER` etc. in `CDR_ANSWERED_STATUSES`).
4. Install the crontab: delivery retry and missing-disposition alert (every 5 minutes), plus the two new lines
   `src.tasks.lending_recording_check` (every 10 minutes) and `src.tasks.lending_daily_scoreboard` (7:20pm ET).

## 2. What the dialer admin configures in the app (BatchDialer: Akrash)

- The disposition list, **exact spelling**, from `config/lending_dispositions.py` (version
  `2026-10-01`; the list is approved, 2026-09-30).
- No webhook is configured; the webhook is not used. We read call results by polling.
- `DNC_REQUEST` set to add the number to the dialer's DNC list and not redial (backup to our own removal).
- Campaigns named as in `config/lending_dialer.py` (`POOL_CAMPAIGN_TAGS`).
- **One line per seat**, max 3 attempts, campaign hours ending 7:15pm ET (backstop; the database rules
  are the enforcement), caller ID numbers, recording disclosure on every connect, and a saved
  screenshot of the disclosure setting (our database records the disclosure only if the dialer reports it).
- Disposition mandatory after each call, if the dialer supports it.
- A contact custom field named exactly `text_consent` (create it in BatchDialer; the code writes to it, it does not create it).
- The recording permission for the API key (see Recordings below).
- The caller-script line for consent capture (see Consent capture below).

## 3. Behaviour to know

- **Attempts:** every call writes a row with `call_ended_at`. A disposition-only event has no end time,
  so the end is derived from start + duration. `can_dial_now` (cap, 7:15pm stop, shift groups) counts these rows.
- **Unknown code:** stored in `disposition_raw`, `disposition` stays empty, one Slack warning.
- **Built-in dialer results** (No Answer, Busy, Answering Machine, Disconnected Number, Do Not Call, …)
  map to our codes without an alert (`SYSTEM_DISPOSITION_ALIASES`).

#### BatchDialer result names (as created)

| # | Our code | Result name in BatchDialer | Type / how it maps | Attached to campaigns |
|---|---|---|---|---|
| 1 | `NO_ANSWER` | **No Answer** | Built-in; the name normalises to `NO_ANSWER` (direct match) | Done |
| 2 | `LEFT_VOICEMAIL` | `LEFT_VOICEMAIL` | Custom; exact | Done |
| 3 | `BAD_NUMBER` | `BAD_NUMBER` | Custom; exact | Done |
| 4 | `CALL_FAILED` | `CALL_FAILED` | Custom; exact | Done |
| 5 | `WRONG_PERSON` | `WRONG_PERSON` | Custom; exact | Done |
| 6 | `NOT_DECISION_MAKER` | `NOT_DECISION_MAKER` | Custom; exact | Done |
| 7 | `REFERRED` | `REFERRED` | Custom; exact | Done |
| 8 | `DNC_REQUEST` | **Do Not Call** | Built-in; alias `DO_NOT_CALL` -> `DNC_REQUEST` | Done |
| 9 | `CONNECTED_NOT_INTERESTED` | `CONNECTED_NOT_INTERESTED` | Custom; exact | Done |
| 10 | `CALLBACK_REQUESTED` | **Call Back** | Built-in; alias `CALL_BACK` -> `CALLBACK_REQUESTED` | Done |
| 11 | `DATA_NURTURE_ONLY` | `DATA_NURTURE_ONLY` | Custom; exact | Done |
| 12 | `GATE_FAILED_NURTURE` | `GATE_FAILED_NURTURE` | Custom; exact | Done |
| 13 | `BOOKED` | `BOOKED` | Custom; exact | Done |

All 13 results exist in BatchDialer (group "Lending", created). Only the three built-ins keep their own names (No Answer, Do Not Call, Call Back); the code maps them, so they need no renaming. A test (`test_every_result_created_in_batchdialer_maps_to_its_code`) pins this table. The "Lending" call-results group is attached to every campaign (checked by the client).

- **Missed-call signal:** unanswered (no answer, voicemail, failed, or a ring-out with no disposition).
  Status `pending`; a second one the same Eastern day is `duplicate_day`; a suppressed number is `blocked`.
  The text itself is sent by the separate text-back task (see the handoff section); this row is not read by it.
- **BOOKED is caller-reported.** On a nurture-only list (Lists 2 and 4) it is flagged `booking_blocked`
  and not counted. Gate passed / held are added when the booking work writes them.
- **Unfunded cause** is a provisional default from the call result; the file owner sets the final value.
- **DNC:** opt-out is propagated once on all channels. If the dialer removal is not confirmed the
  opt-out stays `dialer_pending` (see the compliance floor) and must be watched.

### Consent capture

- Script line for the caller, when the borrower is on the phone: ask whether Next Deal Lending may text them, and record a yes.
- A yes is stored in `lending.text_consents` (`source` `on_call_yes` or `inbound_call`, plus `call_id`, `captured_by`, `captured_at`)
  and mirrored to the BatchDialer contact field `text_consent`. A failed consent write never blocks call ingestion or loses the call.
- Revoking or a suppression / do-not-contact entry makes `has_text_consent` false again.

### Handoff to the text-back task

- The sender is PR #320's WP-GL-9 pipeline: `src/lending/missed_call_text.py`, `missed_call_poller.py`, table `lending.missed_call_texts`,
  setting `missed_call_text_enabled`. It reads BatchDialer CDRs itself.
- Its `consent_gated_sender` uses FA `send_sms` (Telnyx) and currently blocks every send (`skipped_sms_gate`). The client wants GHL on a
  Next Deal Lending number, consented numbers only, and no text if 10DLC does not clear. That developer should replace the gate with
  `src.lending.consent.has_text_consent(db, phone)` and the sender with GHL. Nothing else from #319 is promised to the sender.
- `record_consent` re-grants a revoked consent because the BatchDialer field `text_consent=yes` stays on the contact, so whoever calls
  `revoke_consent()` must also clear that dialer field. Today STOP/DNC are enforced through suppression/`do_not_contact` inside
  `has_text_consent`, so nothing calls revoke yet.
- #319's `lending.missed_call_events` / `queue_missed_call()` is not read by that pipeline. Follow-up cleanup: remove it once #320 is merged and
  the text-back task is live (not deleted here; existing tests and the migration cover it).

### Recordings

- The disclosure plays at the start of every call (Florida requires everyone's consent). `recording_disclosure_logged`
  is the evidence; Akrash saves a screenshot of the in-app disclosure setting.
- BatchDialer holds the audio; the call row holds the link (`recording_ref`), shown in the Sheet-adjacent Slack post
  with the borrower and property.
- `recording_status`: `pending` (not checked yet or a 5xx), `readable`, `forbidden` (key lacks permission, rechecked every
  30 min, never given up), `missing` (404). Cron: `src.tasks.lending_recording_check` every 10 min.
- The permission fix is an account setting in BatchDialer (name it only after reading it off the Integrations screen).
  Once enabled, `forbidden` rows flip to `readable` within about 40 minutes with no code change.
- Audio download and S3 storage are not built here. For the week-two call-analysis owner, `readable` is the go signal.

### Daily scoreboard

- `src.tasks.lending_daily_scoreboard` posts to `LENDING_DAILY_CHANNEL` (channel ID) at 7:20pm ET, after the dialer stops at 7:15pm (UTC cron lines 23:20 and 00:20; only the one at 19:xx ET acts).
  Tables: by caller, by BatchDialer campaign (`dialer_campaign_id`, names from `GET /campaigns`), and by hook (`campaign_tag`).
- Definitions live in `config/lending_dispositions.py` (`LIVE_CONVERSATION_CODES`, `GATED_CODES`, `NURTURE_SENT_CODES`).
  Booked excludes `booking_blocked`; Showed comes from GHL stage events (see "GHL showed feed" below), not the log.
- The post goes out at 7:20pm ET, after the dialer stops at 7:15pm, so every call of the day is counted.
- A fourth table, by caller-ID number, shows dials, answered (`ANSWERED_CODES`: a person picked up) and answer rate, to spot numbers that drop or get spam-flagged.
- Not built: abandon rate per campaign. The dialer feed carries no abandoned-call signal and multi-line power dial is not live; add it when BatchDialer reports abandons.
- Test calls count like real ones. Run with `--force` only after the Friday test, or filter by a test campaign.

## 4. Acceptance check

1. A test call creates one row with phone, seat, `seat_group`, caller-ID number, start/end and disposition;
   replaying the event still gives one row.
2. Sheet row and `#dial-tasks` post appear within 30 seconds; a changed disposition updates the same row.
3. Three dials to one number in 24 hours: `can_dial_now` returns `ATTEMPT_CAP_REACHED`.
4. An unanswered call creates a `missed_call_events` row; a second the same day is `duplicate_day`.
5. `DNC_REQUEST` blocks the contact everywhere; a replay does not duplicate.
6. A connected call with no disposition after N minutes raises one Slack warning.
7. The poller logs `processed=N` within 20 s of a test call, and the call row appears; a non-lending campaign is ignored.
8. `pytest tests/lending/`.

## GHL "showed" feed for the 7pm scoreboard

1. Run `PYTHONPATH=. python migrations/apply_lending_ghl_stage_events.py` (idempotent), and set `LENDING_GHL_WEBHOOK_SECRET` and `LENDING_DAILY_CHANNEL` in `.env`.
2. In GHL (sub-account): Automation -> Workflows -> new workflow, trigger **Pipeline Stage Changed** on the booked pipeline (stage = the held / showed stage; names matched case-insensitively against `SHOWED_STAGE_KEYS` in `config/lending_dispositions.py`).
3. Action **Webhook** (POST) to `https://<api host>/webhooks/lending/ghl-stage` with header `X-Webhook-Secret: <the secret>` and JSON body: `opportunity_id` (`{{opportunity.id}}`), `stage_name` (`{{opportunity.pipleline_stage_name}}`), `pipeline_id`, `phone` (`{{contact.phone}}`), optional `booked_by` (the "Booked by" custom field). Use the GHL merge-field picker for the exact variable names.
4. Showed is credited to the caller / campaign of the latest BOOKED call to that phone; `booked_by` is only the fallback. A repeat delivery of the same opportunity + stage counts once.
