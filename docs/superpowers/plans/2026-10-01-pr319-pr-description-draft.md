> DRAFT: a team member must review this description (and the PR) before it is merged or shared with the client.

# Lending: call disposition logging, text consent, recordings, daily scoreboard

Stacked on #320; merge #320 first, then this PR.

## What this PR now does

- Logs every BatchDialer call as one row in `lending.call_dispositions` using the approved 13-code disposition list (`config/lending_dispositions.py`), delivered to the Sheet and Slack.
- Records the borrower's "OK to text" yes in `lending.text_consents` (from the caller-set BatchDialer contact field `text_consent`, and from answered inbound calls). `src.lending.consent.has_text_consent(db, phone)` is the gate the text-back task should use.
- Ties the recording link to the contact and tracks `recording_status` (`pending/readable/forbidden/missing`); `src.tasks.lending_recording_check` rechecks forbidden links so they flip to readable once the permission is granted.
- Posts a 7pm ET scoreboard per caller, per BatchDialer campaign and per hook to `LENDING_DIAL_TASKS_CHANNEL` (`src.tasks.lending_daily_scoreboard`).
- Runbook: `docs/lending/dialer-disposition-runbook.md` (consent capture, handoff, recordings, scoreboard).

## Migration order (all idempotent)

1. `apply_lending_compliance`
2. `apply_lending_call_dispositions`
3. `apply_lending_call_dispositions_dialer`
4. `apply_lending_pr319_client_feedback`
5. `apply_lending_app_role`

## BatchDialer results
The 13 results exist in BatchDialer (group "Lending"). Three are the built-ins and keep their own names: **No Answer** -> `NO_ANSWER` (direct), **Do Not Call** -> `DNC_REQUEST` (alias `DO_NOT_CALL`), **Call Back** -> `CALLBACK_REQUESTED` (alias `CALL_BACK`). The other 10 are custom and exact. A test pins all 13 names. Remaining admin work: attach the nine results not yet on the four campaigns (everything except LEFT_VOICEMAIL, BAD_NUMBER, CALL_FAILED, WRONG_PERSON).

## Admin steps

- Create the BatchDialer contact field `text_consent`.
- Grant the API key the recording permission in BatchDialer.
- Add the consent line to the caller script.
- Set `LENDING_DIAL_TASKS_CHANNEL`.
- Install the two new cron lines: `src.tasks.lending_recording_check` (every 10 min) and `src.tasks.lending_daily_scoreboard` (7:00pm ET).

## Not included

- The GHL missed-call text sender (#320's WP-GL-9 pipeline is the sender; it still uses FA `send_sms` and blocks every send until that developer swaps in `has_text_consent` and GHL).
- Audio download and storage.
- The Backflip conflict check remains off.
- The 7:00-7:15pm scoreboard gap: the dialer runs until 7:15pm ET, so calls in that window are missing from the 7:00pm post (open question for the client: post at 7:20pm).
- `lending.missed_call_events` has no consumer; removal is follow-up cleanup after #320 merges.

## Testing

Lending tests run against the dev DB; see CI/controller run for the full-suite result.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
