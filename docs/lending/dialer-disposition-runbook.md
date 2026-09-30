# Dialer call disposition logging: setup runbook

Every dialer call becomes one row in `lending.call_dispositions`; unanswered calls also queue a
`lending.missed_call_events` row. The result reaches the Google Sheet and Slack `#dial-tasks`
within 30 seconds. This runbook is vendor-neutral; the BatchDialer-specific steps are marked.

Call results are ingested by **polling BatchDialer call records (CDRs)** with the service
`python -m src.lending.cdr_poller` (`deploy/systemd/fa-lending-cdr-poller.service`). The webhook route
`/webhooks/lending/dialer` still exists but is **not used** unless BatchDialer's payload carries the CDR id.

**Unverified until the live check in `docs/lending/batchdialer-api-findings.md` (Task 0, not yet done)
is filled in:** the CDR field strings (for example `"ANSWER"`), whether an unanswered call appears as a
CDR, and how a contact is held. Confirm them before go-live.

## 1. Order of setup

1. Migrations, in order (all idempotent):
   `apply_lending_compliance.py` → `apply_lending_call_dispositions.py` →
   `apply_lending_call_dispositions_dialer.py` → `apply_lending_app_role.py` (needs a superuser,
   `LENDING_DB_PASSWORD`).
2. `.env`:
   - `LENDING_DATABASE_URL`
   - `LENDING_DIALER_CAMPAIGN_IDS` (comma-separated BatchDialer **campaign ids**, from `GET /api/campaigns`; **empty ignores every call**)
   - `LENDING_SEAT_GROUPS` (`agentid:A,agentid:B`, keyed by BatchDialer **agent id**; a seat missing here gets no shift group)
   - `LENDING_DISPOSITION_MISSING_ALERT_MINUTES` (default 10)
   - `LENDING_SLACK_BOT_TOKEN`, `LENDING_DIAL_TASKS_CHANNEL`,
     `LENDING_SHEETS_SERVICE_ACCOUNT_KEY_PATH`, `LENDING_DISPOSITION_SHEET_ID` (+ `_TAB`)
   - `BATCHDIALER_API_KEY` (the API token: **User icon → Settings → Integrations → Custom Integration**; shared with the dialer load; `.env` only, never in chat or PRs)
3. Start `fa-lending-cdr-poller` (`deploy/systemd/fa-lending-cdr-poller.service`). Run **exactly one
   instance**: the `/v2/cdrs/last` watermark is per API key, so a second poller (or anything else using
   that endpoint with the same key) steals records. `lending-api` and its Nginx block are not needed for ingestion.
   Poller behavior to know:
   - Inbound calls are ignored, except calls disposed `DNC_REQUEST` (those are opted out; no attempt is counted).
   - The day rescan re-reads today and yesterday (UTC) every few minutes. After an outage longer than that,
     run `python -m src.lending.cdr_poller --backfill-days N` (rescans N days, then continues; with `--once` it
     rescans and exits). DNC rows whose opt-out failed are retried every cycle regardless of the window.
   - `--once` also advances the `/v2/cdrs/last` watermark for the API key, so it can consume calls that only
     the running poller's rescan will then recover. Prefer stopping the service first.
   - UNVERIFIED (Task 0, needs a live account check): the pagination parameter name `next_page` and the CDR
     status strings (`ANSWER` etc. in `CDR_ANSWERED_STATUSES`).
4. Install the crontab (delivery retry and missing-disposition alert, both every 5 minutes).

## 2. What the dialer admin configures in the app (BatchDialer: Akrash)

- The disposition list, **exact spelling**, from `config/lending_dispositions.py` (version
  `2026-10-01`; the client approves it first). The codes marked *proposed* in that file need approval.
- No webhook is configured; the webhook is not used. We read call results by polling.
- `DNC_REQUEST` set to add the number to the dialer's DNC list and not redial (backup to our own removal).
- Campaigns named as in `config/lending_dialer.py` (`POOL_CAMPAIGN_TAGS`).
- **One line per seat**, max 3 attempts, campaign hours ending 7:15pm ET (backstop; the database rules
  are the enforcement), caller ID numbers, recording disclosure on every connect, and a saved
  screenshot of the disclosure setting (our database records the disclosure only if the dialer reports it).
- Disposition mandatory after each call, if the dialer supports it.

## 3. Behaviour to know

- **Attempts:** every call writes a row with `call_ended_at`. A disposition-only event has no end time,
  so the end is derived from start + duration. `can_dial_now` (cap, 7:15pm stop, shift groups) counts these rows.
- **Unknown code:** stored in `disposition_raw`, `disposition` stays empty, one Slack warning.
- **Built-in dialer results** (No Answer, Busy, Answering Machine, Disconnected Number, Do Not Call, …)
  map to our codes without an alert (`SYSTEM_DISPOSITION_ALIASES`).
- **Missed-call signal:** unanswered (no answer, voicemail, failed, or a ring-out with no disposition).
  Status `pending`; a second one the same Eastern day is `duplicate_day`; a suppressed number is `blocked`.
  The text itself is sent by a separate task.
- **BOOKED is caller-reported.** On a nurture-only list (Lists 2 and 4) it is flagged `booking_blocked`
  and not counted. Gate passed / held are added when the booking work writes them.
- **Unfunded cause** is a provisional default from the call result; the file owner sets the final value.
- **DNC:** opt-out is propagated once on all channels. If the dialer removal is not confirmed the
  opt-out stays `dialer_pending` (see the compliance floor) and must be watched.

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
