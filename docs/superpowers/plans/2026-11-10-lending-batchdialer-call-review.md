# BatchDialer Call Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the eleven issues found when David's BatchDialer calls (Nov 1–10, 2026) were cross-checked against our database, Slack, the Google Sheet and GHL.

**Architecture:** Every fix stays inside the existing lending call pipeline: `parse_event` → `record_dialer_event` (`src/lending/dispositions.py`) → `process_event` / `follow_up` (`src/lending/call_pipeline.py`) → delivery (`src/lending/disposition_delivery.py`), plus the reports that read `lending.call_dispositions`. One new column (`recording_view_url`), one migration, one data-fix script. No new services or crons; one cron is removed.

**Tech Stack:** Python 3.11, SQLAlchemy 2.0 `text()` queries, pytest, PostgreSQL (schema `lending`).

**Spec:** the call-review analysis of 2026-11-10 (this session). Its findings are summarised under "Background" below; that section is the spec.

## Background (the spec)

| # | Issue | Cause |
|---|---|---|
| 1 | All 370 BatchDialer calls have `recording_status = 'missing'` | `src/tasks/lending_recording_check.py` GETs `recording_ref` (`https://app.batchdialer.com/api/callrecording/{call_id}`); BatchDialer answers 404 for every call; `src/lending/recordings.py:29` maps 404 → `missing`. The CDR's `recordingenabled = 1` and `callRecordUrl` are present on every call, answered or not, so they carry no signal either. |
| 2 | The stored recording link opens nothing | BatchDialer has no direct recording URL. The contact page `https://app.batchdialer.com/contacts/view/{dialer_contact_id}` plays the contact's recordings. |
| 3 | Dials with no agent show `Caller: —` (Slack), blank (Sheet), `(unknown)` (scoreboard) | Predictive mode: BatchDialer attaches `agent` only when a call is bridged to a caller. 326 of 370 CDRs have `"agent": null`; no API endpoint our key can use returns the agent. Never assign a person to these dials. |
| 4 | 17 of 28 transcripts marked `failed` | Claude correctly returns an empty tool input `{}` for calls with nothing to extract; `src/lending/call_extraction.py:204` (`if not tool_input`) raises on `{}`; `failed` is never retried. |
| 5 | No alert when the caller leaves the result "Unknown" | Missing-result alert required `disposition_raw IS NULL`. **Already fixed on this branch (uncommitted).** |
| 6 | David's end-of-day summary leaves out BatchDialer | `src/lending/david_activity.py` reads only GHL conversations. |
| 7 | Voicemails count as talk time in David's summary | `count_messages` adds `call_duration_seconds` for every call, including inbound voicemails leads left him. |
| 8 | A DNC result on an inbound BatchDialer call can be ignored | Inbound CDRs carry no campaign; `cdr_poll.ingest` drops non-lending-campaign calls and only logs a warning for DNC. |
| 9 | `lending.first_contact_snapshots` never filled | `snapshot_first_contact` (`src/lending/first_contact.py:108`) has no caller. |
| 10 | 6 "Unknown" calls never posted to Slack or the Sheet (the 353/359 gap) | Delivery only posts a row with a `disposition`; "Unknown" maps to none. 4 of the 6 have 0 s talk time and get no alert either. |
| 11 | 191 machine hang-ups stored as `LEFT_VOICEMAIL` | `SYSTEM_DISPOSITION_ALIASES["ANSWERING_MACHINE"] = "LEFT_VOICEMAIL"` (`config/lending_dispositions.py:44`). These CDRs have `duration 0` and `voicemailid null`: no voicemail was left. |

### Decisions (defaults accepted 2026-11-10)

- **A (fix 11):** the default was a new result `ANSWERING_MACHINE`. **Changed in this plan to mapping it to `NO_ANSWER`**: `DISPOSITIONS` is the client-approved 10-code list shared with GHL, which allows at most 10 call dispositions (`config/lending_dispositions.py:10`), so an 11th code cannot be added without a client decision. The raw "Answering Machine" stays in `disposition_raw`, so it is still visible and countable. Behaviour is otherwise unchanged (both codes are in `UNANSWERED_CODES`, same default cause `contactability`). Confirm with Josh before merge.
- **B (fix 2):** the Google Sheet gets a recording-link column, appended as the last column.
- **C (fix 9):** first-contact snapshots only. `lead_call_confirmations` stays unwired until a source for confirmed maturity / decision maker is agreed. Load records hold a PropertyRadar ref (`pr_list:verified_maturity:P10BF0C7E`), not a `properties.id`, so snapshots are taken with `property_id=None` (the function supports this: "snapshotted with every input missing").

### Decisions from plan review (2026-11-10)

- **Existing tests are updated, not just new ones added**, so the suite matches the new behaviour: `tests/lending/test_dialer_events.py:170` (`test_new_recording_enters_the_check_and_no_recording_stays_null`), `tests/lending/test_attempt_log.py:104` (`test_call_rows_carry_the_recording_state`), plus any other test asserting `pending` / `readable` / `missing` recording states or `ANSWERING_MACHINE -> LEFT_VOICEMAIL`.
- **Sheet header is added by hand** (user, before deploy): Sheet `1OZPv8ez4FiCgCBd4sZ18KW9_vndBMJwWW5lrbHTZkWA`, tab `Dispositions`, column **N**, header text exactly `Recording (BatchDialer)`. The code only writes values; `sync_sheet` writes a header only on an empty sheet.
- **Old Slack posts and old Sheet rows are left as they are.** No re-posting, no batch of updates. Only calls ingested after deploy get the new link, label and results in Slack and the Sheet.
- **The data fix (Task 9) must not cause any Slack post or Sheet write.** It sets `slack_posted_disposition` and `sheet_synced_disposition` to the new disposition on every row it relabels, so the delivery retry cron sees nothing behind.
- **No production credentials in implementation or tests.** The Slack, Google Sheets, GHL and BatchDialer credentials in `.env` are production: never used by tests or by any verification run. All tests use fakes; DB tests use a throwaway `TEST_DATABASE_URL`.
- **Recording status uses BatchDialer's flag too:** `recorded` only when the CDR has `recordingenabled = 1` and talk time > 0.
- **`recording_checked_at`** is left in place, unused; `models.py` comment says so.
- **Old GHL notes** written for the 191 answering-machine calls ("Left voicemail") are not rewritten.

### Open (answer before implementation; plan shows the suggested default)

- **Q5:** post a connected call left "Unknown" to Slack/Sheet at once as "No result yet" (default: yes, Task 3 step 3b).
- **Q6:** also snapshot first contact for GHL calls in the GHL call import (default: yes, Task 8 step 3b).
- **Q7:** after deploy, Task 1's alert would post once about the two old Nov 4 "Unknown" calls (1223326317, 1223365079). Default: no post — Task 9 step 6 marks them alerted, in line with "post nothing for old calls".

## Global Constraints

- SQL through `sqlalchemy.text()` with named bind parameters only; no ORM queries (CLAUDE.md).
- Schema change = `src/lending/models.py` + idempotent `migrations/apply_<name>.py` (`ADD COLUMN IF NOT EXISTS`); never `scripts/`.
- Phones only through `src.services.phone_utils.normalize`; never log a phone, only `last4()`.
- Log exception class names only (`type(exc).__name__`), never messages (they embed phones).
- Never call BatchDialer `GET /v2/cdrs/last` outside the poller (server-side watermark per API key).
- Tests never use the live DB: pure tests with fakes; DB tests skip without `TEST_DATABASE_URL`.
- No commit is pushed and no PR opened until the user gives go/no-go after review. Local commits per task only.
- Never use the production Slack, Google Sheets, GHL or BatchDialer credentials from `.env` in tests or checks. Nothing in this work posts to Slack or writes the Sheet during implementation.
- The data fix never triggers a Slack post or Sheet write; only calls ingested after deploy are delivered with the new behaviour.
- Branch: `fix/lending-batchdialer-call-review`.

## Review Focus

1. **A rescan re-ingests an old CDR** (the poller rescans the last 2 days every 2 minutes): the upsert must not undo a backfilled value (`recording_status`, `recording_view_url`, the NO_ANSWER mapping). Owned by Task 2/3 tests `test_rescan_keeps_*`.
2. **"Unknown" arrives first with 0 s, then the caller sets a real result** (BatchDialer updates the CDR): the later real code must replace `NO_ANSWER`. Owned by Task 3 test `test_unknown_then_real_result_replaces_no_answer`.
3. **CDR with no `contact.id`** (malformed or inbound): `recording_view_url` stays NULL, no crash, no `.../view/None`. Owned by Task 2 test `test_no_contact_id_gives_no_view_url`.
4. **Inbound DNC from a number that is not one of our leads**: still ignored (we must not opt out strangers' numbers into our suppression list). Owned by Task 7 test `test_inbound_dnc_from_unknown_number_is_still_ignored`.
5. **Model returns `None` (paused / blocked)** after the empty-input change: must still raise, never store an empty extraction. Owned by Task 6 test `test_blocked_model_call_raises_instead_of_storing_empty` (existing, must stay green).

---

### Task 1: Commit the "Unknown" alert fix (fix 5)

Already implemented: `config/lending_dispositions.py` (`DIALER_NO_RESULT = "UNKNOWN"`), `src/tasks/lending_missing_disposition_alert.py` (`_BATCHDIALER` predicate + `no_result` param), `tests/lending/test_ghl_calls_in_delivery_and_alerts.py` (`test_missing_alert_covers_batchdialer_unknown_result`).

- [ ] **Step 1:** Run `pytest tests/lending/test_ghl_calls_in_delivery_and_alerts.py tests/lending/test_dispositions_config.py -q` — expected: all pass (DB tests skip).
- [ ] **Step 2: Commit**

```bash
git add config/lending_dispositions.py src/tasks/lending_missing_disposition_alert.py tests/lending/test_ghl_calls_in_delivery_and_alerts.py
git commit -m "fix(lending): alert when a connected BatchDialer call is left as Unknown"
```

---

### Task 2: Recording status from talk time + contact view link (fixes 1, 2)

**Files:**
- Modify: `config/lending_dispositions.py` — add constants; delete `RECORDING_RETRY_MINUTES`, `RECORDING_BATCH_LIMIT`; append `"Recording (BatchDialer)"` to `SHEET_COLUMNS`
- Modify: `src/lending/models.py:264` — add `recording_view_url: Mapped[Optional[str]] = mapped_column(String(500))`; update the `recording_status` comment to `recorded / none`
- Create: `migrations/apply_lending_recording_view_url.py`
- Modify: `src/lending/dispositions.py` (`record_dialer_event` insert/upsert)
- Modify: `src/lending/disposition_delivery.py` (`build_sheet_row`, `build_slack_message`)
- Modify: `src/lending/attempt_log.py` (`_call_attempt`, `_CALLS`, `CallRow`)
- Delete: `src/lending/recordings.py`, `src/tasks/lending_recording_check.py`, `tests/lending/test_recordings.py`, `tests/lending/test_recordings_locking.py`, `tests/lending/test_recording_status_getter.py`
- Modify: `scripts/cron/crontab.txt:1229` — remove the `lending_recording_check` line
- Modify: `src/lending/dispositions.py` `parse_event` / `DialerCallEvent` — add `recording_enabled: Optional[bool]` from CDR `recordingenabled` (`EVENT_FIELD_CANDIDATES["recording_enabled"] = ("recordingenabled", "recording_enabled")`)
- Test: `tests/lending/test_recording_view.py` (new, pure), `tests/lending/test_disposition_delivery.py`, `tests/lending/test_apply_lending_recording_view_url.py` (new, DB)
- Update existing tests: `tests/lending/test_dialer_events.py:170` (rename to `test_recording_status_follows_talk_time_and_the_dialer_flag`: talk 48 + enabled → `recorded`; talk 0 → `none`; a rescan with talk time upgrades `none` → `recorded`), `tests/lending/test_attempt_log.py:104` (`recorded` instead of `readable`, URL = `recording_view_url`), `tests/lending/test_dialer_parse.py` (`recording_enabled` parsed), and any `DialerCallEvent(...)` constructions in tests (`tests/lending/test_dialer_events.py:25` etc.) gain `recording_enabled=None`

**Interfaces:**
- Produces (config): `RECORDING_RECORDED = "recorded"`, `RECORDING_NONE = "none"`, `DIALER_CONTACT_VIEW_URL = "https://app.batchdialer.com/contacts/view/{contact_id}"`
- Produces (`src/lending/dispositions.py`): `contact_view_url(contact_id: Optional[str]) -> Optional[str]`; `recording_status_for(talk_seconds: Optional[int], recording_enabled: Optional[bool]) -> str` — `recorded` only when `recording_enabled is not False` and talk > 0 (a CDR without the flag is treated as enabled), else `none`
- Produces (column): `lending.call_dispositions.recording_view_url varchar(500) NULL`

- [ ] **Step 1: Write the failing tests** in `tests/lending/test_recording_view.py`

```python
from src.lending.dispositions import contact_view_url, recording_status_for

def test_view_url_uses_the_contact_id():
    assert contact_view_url("559662160") == "https://app.batchdialer.com/contacts/view/559662160"

def test_no_contact_id_gives_no_view_url():
    assert contact_view_url(None) is None and contact_view_url("") is None

def test_talk_time_means_recorded():
    assert recording_status_for(48, True) == "recorded" and recording_status_for(48, None) == "recorded"

def test_no_talk_time_means_none():
    assert recording_status_for(0, True) == "none" and recording_status_for(None, True) == "none"

def test_recording_switched_off_means_none():
    assert recording_status_for(48, False) == "none"
```

In `tests/lending/test_disposition_delivery.py` add:
- `test_slack_post_links_the_recording_when_recorded`: row with `talk_duration_sec=48`, `recording_status="recorded"`, `recording_view_url=".../contacts/view/559662160"`, `call_started_at` 2026-11-04 14:34 UTC → a block line contains `<https://app.batchdialer.com/contacts/view/559662160|Open in BatchDialer>` and `10:34 AM ET`.
- `test_slack_post_has_no_recording_line_without_talk_time`: `recording_status="none"` → no `Recording` line.
- `test_sheet_row_ends_with_the_view_url`: `build_sheet_row(row, now)[-1] == row["recording_view_url"]` and `len(...) == len(SHEET_COLUMNS)`.

- [ ] **Step 2:** Run `pytest tests/lending/test_recording_view.py tests/lending/test_disposition_delivery.py -q` — expected FAIL (`ImportError: contact_view_url`).

- [ ] **Step 3: Implement**
  - `contact_view_url` / `recording_status_for` in `dispositions.py` (top-level helpers, config constants).
  - In the `record_dialer_event` INSERT: write `recording_view_url = :view_url` and `recording_status = :rec_status` (from `recording_status_for(ev.duration)`). ON CONFLICT: `recording_view_url = COALESCE(EXCLUDED.recording_view_url, lending.call_dispositions.recording_view_url)`; `recording_status = CASE WHEN EXCLUDED.talk_duration_sec IS NOT NULL THEN EXCLUDED.recording_status ELSE lending.call_dispositions.recording_status END` (a later CDR with talk time upgrades `none` → `recorded`). `recording_ref` keeps its current upsert.
  - `build_slack_message`: when `row.get("recording_status") == RECORDING_RECORDED and row.get("recording_view_url")`, append `*Recording:* <url|Open in BatchDialer> (call at h:mm AM ET)`.
  - `build_sheet_row`: append `row.get("recording_view_url") or ""` last.
  - `attempt_log`: select `recording_view_url`; `_call_attempt` sets `recording_url = row.recording_view_url or ""` and `recording = row.recording_status or ""` for BatchDialer rows (GHL rows unchanged).
  - Delete the recording check modules, their tests, the two config constants and the cron line.

- [ ] **Step 4: Migration** `migrations/apply_lending_recording_view_url.py`, following the shape of `migrations/apply_lending_loan_files_borrower_phone_index.py` (psycopg2 from `DATABASE_URL`, 5 s `lock_timeout`, idempotent):
  - `ALTER TABLE lending.call_dispositions ADD COLUMN IF NOT EXISTS recording_view_url varchar(500)`
  - nothing else (backfill is Task 9).
  - Test `tests/lending/test_apply_lending_recording_view_url.py::test_column_added_and_rerun_is_safe` (pattern of `tests/lending/test_apply_lending_pr319_client_feedback.py`): column exists after one run; a second run raises nothing.
  - `models.py`: comment on `recording_checked_at` = "unused since 2026-11 (recording check removed)".

- [ ] **Step 5:** Run `pytest tests/lending -q -k "recording or delivery or attempt_log or dispositions"` — expected PASS; `grep -rn "lending_recording_check\|src.lending.recordings" src tests scripts config` — expected no output.

- [ ] **Step 6: Commit** `fix(lending): recording status from talk time and BatchDialer contact view link`

---

### Task 3: "Unknown" and answering-machine results (fixes 10, 11)

**Files:**
- Modify: `config/lending_dispositions.py:44` — `"ANSWERING_MACHINE": "NO_ANSWER"` with a comment: machine detected and hung up, no voicemail left; the raw result stays in `disposition_raw`.
- Modify: `src/lending/dispositions.py` — new `resolve_dialer_code`; `record_dialer_event` uses it and stores the raw "Unknown".
- Test: `tests/lending/test_dispositions_g2.py`, `tests/lending/test_dispositions_config.py`

**Interfaces:**
- Consumes: `normalize_code(raw) -> tuple[Optional[str], bool]`, `DIALER_NO_RESULT` (Task 1)
- Produces: `resolve_dialer_code(raw: Optional[str], talk_seconds: Optional[int]) -> tuple[Optional[str], bool]` — `("NO_ANSWER", True)` for "Unknown" with 0/None talk time; `(None, True)` for "Unknown" with talk time (known, so no unknown-code alert; Task 1's alert covers it); otherwise `normalize_code(raw)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_unknown_with_no_talk_time_is_no_answer():
    assert resolve_dialer_code("Unknown", 0) == ("NO_ANSWER", True)

def test_unknown_with_talk_time_has_no_code_and_is_not_an_unknown_code():
    assert resolve_dialer_code("Unknown", 288) == (None, True)

def test_answering_machine_is_no_answer():
    assert resolve_dialer_code("Answering Machine", 0) == ("NO_ANSWER", True)

def test_caller_picked_left_voicemail_is_kept():
    assert resolve_dialer_code("LEFT_VOICEMAIL", 48) == ("LEFT_VOICEMAIL", True)
```

DB-backed (skip without `TEST_DATABASE_URL`), in `test_dispositions_g2.py`:
- `test_unknown_with_talk_time_keeps_the_raw_result`: `record_dialer_event` with raw "Unknown", duration 288 → row has `disposition IS NULL`, `disposition_raw = 'Unknown'`, `RecordedCall.unknown_code is None`.
- `test_unknown_then_real_result_replaces_no_answer`: event "Unknown"/0 s then same call id with "CALLBACK_REQUESTED" → `disposition = 'CALLBACK_REQUESTED'`.
- `test_rescan_keeps_no_answer_for_unknown`: same "Unknown"/0 s event twice → one row, `disposition = 'NO_ANSWER'`.

- [ ] **Step 2:** Run `pytest tests/lending/test_dispositions_g2.py tests/lending/test_dispositions_config.py -q` — expected FAIL (`ImportError: resolve_dialer_code`).
- [ ] **Step 3: Implement** `resolve_dialer_code` next to `normalize_code` (match the raw with the same upper/underscore key rule). In `record_dialer_event` replace `normalize_code(ev.disposition_raw)` with `resolve_dialer_code(ev.disposition_raw, ev.duration)`, and add a branch: `known and code is None and ev.disposition_raw` → store `disposition_raw` (same UPDATE as the unknown branch, no alert).
- [ ] **Step 3b (Q5, default yes):** a connected call left "Unknown" is delivered at once. In `src/lending/disposition_delivery.py` `_deliver_locked`, a row with `disposition IS NULL` and raw "Unknown" and talk > 0 that has never been posted (`slack_ts IS NULL`) is posted with the title `:grey_question: No result yet — set the call result in BatchDialer`, and synced to the Sheet with `No result yet` in the Disposition column. Track it so it is posted once: post when `slack_ts IS NULL`; a later real result updates the same post (existing `slack_behind` path). Tests in `tests/lending/test_disposition_delivery.py`: `test_unknown_connected_call_is_posted_once_as_no_result_yet`, `test_real_result_updates_the_no_result_yet_post`.
- [ ] **Step 3c:** update existing tests that pin `ANSWERING_MACHINE -> LEFT_VOICEMAIL` (search `tests/lending` for `Answering Machine` / `ANSWERING_MACHINE`) to `NO_ANSWER`.
- [ ] **Step 4:** Run the same tests — expected PASS. Also run `pytest tests/lending -q -k "missed_call or text_back or alarm or attempt"` — expected PASS (NO_ANSWER and LEFT_VOICEMAIL are both in `UNANSWERED_CODES`).
- [ ] **Step 5: Commit** `fix(lending): Unknown with no talk time is no answer; machine hang-up is not a voicemail`

---

### Task 4: Auto-dialer label for dials with no caller (fix 3)

**Files:**
- Modify: `config/lending_dispositions.py` — `AUTO_DIALER_LABEL = "Auto-dialer ({campaign})"`, `AUTO_DIALER_UNKNOWN_CAMPAIGN = "unknown campaign"`
- Modify: `src/lending/disposition_delivery.py` — `caller_label(row: Mapping[str, Any]) -> str`, used by `build_slack_message` (Caller line) and `build_sheet_row` (Caller column)
- Modify: `src/lending/scoreboard.py:31` (`_caller_key`)
- Test: `tests/lending/test_disposition_delivery.py`, `tests/lending/test_scoreboard.py`

**Interfaces:**
- Produces: `caller_label(row) -> str` — `caller_name`, else `caller_seat`, else for `call_source == 'batchdialer'` `"Auto-dialer (<raw_event.campaign.name or dialer_campaign_id or 'unknown campaign'>)"`, else `"—"`.

- [ ] **Step 1: Failing tests**
  - `test_caller_label_names_the_campaign_for_a_dial_with_no_agent`: `{"caller_name": None, "caller_seat": None, "call_source": "batchdialer", "raw_event": {"campaign": {"name": "Verified maturity"}}}` → `"Auto-dialer (Verified maturity)"`.
  - `test_caller_label_prefers_the_agent`: `caller_name="David Willis"` → `"David Willis"`.
  - `test_caller_label_for_a_ghl_call_with_no_user_is_a_dash` → `"—"`.
  - In `test_scoreboard.py`: `test_caller_key_labels_agentless_batchdialer_dials` asserts the SQL from `_caller_key()` contains `'Auto-dialer ('` and `raw_event->'campaign'->>'name'`, and still falls back to `'(unknown)'` for non-BatchDialer rows.
- [ ] **Step 2:** Run them — expected FAIL.
- [ ] **Step 3: Implement.** `raw_event` may arrive as a dict or a JSON string from `SELECT *`; handle both. In `_caller_key()` the fallback becomes `CASE WHEN call_source = 'batchdialer' THEN 'Auto-dialer (' || COALESCE(NULLIF(raw_event->'campaign'->>'name',''), NULLIF(dialer_campaign_id,''), 'unknown campaign') || ')' ELSE '(unknown)' END` (config literals only). Never write a person's name onto these rows.
- [ ] **Step 4:** Run `pytest tests/lending/test_disposition_delivery.py tests/lending/test_scoreboard.py tests/lending/test_daily_scoreboard_task.py -q` — expected PASS.
- [ ] **Step 5: Commit** `fix(lending): label agentless dialer calls by campaign instead of unknown`

---

### Task 5: David's summary: BatchDialer calls, and voicemails out of talk time (fixes 6, 7)

**Files:**
- Modify: `config/settings.py` (near line 1033) — `lending_david_bd_agent_id: str = Field(default="", env="LENDING_DAVID_BD_AGENT_ID")` (David = `118924`; set in server `.env`)
- Modify: `src/lending/david_activity.py`
- Modify: `src/tasks/lending_david_summary.py:58` — pass `bd_agent_id=s.lending_david_bd_agent_id.strip()`
- Test: `tests/lending/test_david_activity.py`, `tests/lending/test_david_summary_task.py`

**Interfaces:**
- Produces on `DavidActivity`: `bd_dials: int = 0` (auto-dialer dials in campaigns David worked that day), `bd_connected: int = 0`, `bd_talk_seconds: int = 0`, `bd_voicemails_left: int = 0`, `bd_callbacks: int = 0`, `bd_booked_reported: int = 0`, `voicemails_received: int = 0`
- Produces: `batchdialer_counts(db, day: date, agent_id: str) -> dict[str, int]` (one SQL query over `lending.call_dispositions`, `call_source='batchdialer'`, `direction='outbound'`, ET day bounds from `day_bounds`); `build_activity(..., bd_agent_id: str = "")`.
- Rules: `bd_connected`, `bd_talk_seconds`, results counts = rows with `caller_seat = :agent_id`; `bd_dials` = all rows in the `dialer_campaign_id`s where David has at least one row that day (dials are the campaign's, shown as "in his campaigns"). Empty `agent_id` → section omitted. Query failure → savepoint, warning, zeros (same pattern as `stage_counts`).
- Fix 7: in `count_messages`, an inbound call in `GHL_VOICEMAIL_STATES` adds to `voicemails_received` and **not** to `talk_seconds`.

- [ ] **Step 1: Failing tests**
  - `test_inbound_voicemail_is_not_talk_time`: one inbound GHL call message, `callStatus` voicemail, 38 s, David's userId → `talk_seconds == 0`, `voicemails_received == 1`.
  - `test_batchdialer_counts_credit_only_davids_connected_calls` (fake db returning one row of counts) → fields set from the row; SQL contains `caller_seat = :agent_id` and `call_source = 'batchdialer'`.
  - `test_no_bd_agent_id_skips_the_batchdialer_section`: `build_activity(..., bd_agent_id="")` makes no BatchDialer query; `format_slack` has no "BatchDialer" line.
  - `test_slack_text_shows_batchdialer_line`: `bd_dials=298, bd_connected=22, bd_talk_seconds=3600` → text contains `BatchDialer · Dials in his campaigns 298 · Connected 22 · Talk time 1h 0m`, and `Voicemails received` appears when `voicemails_received > 0`.
- [ ] **Step 2:** Run `pytest tests/lending/test_david_activity.py tests/lending/test_david_summary_task.py -q` — expected FAIL.
- [ ] **Step 3: Implement** per Interfaces. Keep the existing GHL line wording, renamed to start with `GHL ·`; update the module docstring (now GHL + BatchDialer).
- [ ] **Step 4:** Run the same tests — expected PASS.
- [ ] **Step 5: Commit** `fix(lending): David's summary adds BatchDialer calls and leaves voicemails out of talk time`

---

### Task 6: Empty extraction is done, not failed (fix 4)

**Files:**
- Modify: `src/lending/call_extraction.py:203-205`
- Test: `tests/lending/test_call_extraction.py`, `tests/lending/test_transcript_ingest.py`

- [ ] **Step 1: Failing tests**
  - `test_empty_tool_input_is_an_empty_extraction`: `_extract({})[0] == CallExtraction()`.
  - In `test_transcript_ingest.py`: `test_empty_extraction_is_marked_done` — fake extractor returning `CallExtraction()` → store's `extract_and_mark_done` called, `mark_failed` not called.
  - Existing `test_blocked_model_call_raises_instead_of_storing_empty` (`_extract(None)` raises) must stay green.
- [ ] **Step 2:** Run `pytest tests/lending/test_call_extraction.py tests/lending/test_transcript_ingest.py -q` — expected FAIL on the new test.
- [ ] **Step 3: Implement:** raise only when `tool_input is None`; `{}` validates to an empty `CallExtraction`. Update the docstring.
- [ ] **Step 4:** Run again — expected PASS.
- [ ] **Step 5: Commit** `fix(lending): a call with nothing to extract is done, not failed`

---

### Task 7: Honour DNC on inbound calls from our leads (fix 8)

**Files:**
- Modify: `src/lending/cdr_poll.py` (`ingest`, lines 122-133)
- Test: `tests/lending/test_cdr_poll_inbound.py`

**Interfaces:**
- Produces: `known_lead_phones(db, phones: list[str]) -> set[str]` in `cdr_poll.py` — one query: normalized phones present in `lending.dialer_load_records.phone` or `lending.contacts.phone`.
- Rule: a parsed event whose code is `DNC_CODE`, campaign not in `lending_campaign_ids()`, and whose normalized phone is in `known_lead_phones` joins `events` and goes through `process_event` (the existing DNC path). All other non-campaign DNC events keep the existing warning.

- [ ] **Step 1: Failing tests** (pattern: `_cdr()` fixture, `MagicMock` db, monkeypatch)
  - `test_inbound_dnc_from_a_known_lead_is_processed`: `_cdr(8, direction="in", disposition="DNC_REQUEST", campaign={"id": 999})`, `known_lead_phones` patched to return `{"+18135550142"}`, `process_event` patched to record calls → called once with call id `"8"`.
  - `test_inbound_dnc_from_unknown_number_is_still_ignored`: same but `known_lead_phones` returns `set()` → `process_event` not called.
- [ ] **Step 2:** Run `pytest tests/lending/test_cdr_poll_inbound.py -q` — expected FAIL.
- [ ] **Step 3: Implement.** Call `known_lead_phones` once per `ingest` batch, only when such DNC events exist (no query otherwise).
- [ ] **Step 4:** Run `pytest tests/lending/test_cdr_poll_inbound.py tests/lending/test_cdr_poll.py -q` — expected PASS.
- [ ] **Step 5: Commit** `fix(lending): honour a DNC result on an inbound call from one of our leads`

---

### Task 8: First-contact snapshot on the first reached call (fix 9)

**Files:**
- Modify: `src/lending/call_pipeline.py` (`process_event`)
- Test: `tests/lending/test_call_record_entry_points.py`

**Interfaces:**
- Consumes: `snapshot_first_contact(session, *, phone, property_id, contacted_at, caller_seat, script_version, source_tag, queue, warm) -> bool` (`src/lending/first_contact.py:108`, does not commit), `lookup_load_record(db, contact_id, phone, started_at) -> Optional[dict]`.
- Produces: `_snapshot_first_contact(db, ev: DialerCallEvent, recorded: RecordedCall) -> None` in `call_pipeline.py`.
- Rule: only when `ev.direction != "inbound"`, `recorded.phone`, and `(ev.duration or 0) > 0`. Arguments: `property_id=None` (Decision C), `contacted_at=ev.started_at or ev.ended_at`, `caller_seat=recorded.caller_seat`, `script_version=None`, `source_tag` = load record `campaign_tag`, `queue` = load record `pool`, `warm = pool in {"lendingflow", "website"}`. Runs in `db.begin_nested()`, commits after; any exception is logged by class and never blocks the call.

- [ ] **Step 1: Failing tests**
  - `test_first_reached_call_is_snapshotted`: duration 48, outbound → `snapshot_first_contact` called once with `queue="verified_maturity"`, `warm=False`, `property_id=None`.
  - `test_unanswered_call_is_not_snapshotted`: duration 0 → not called.
  - `test_snapshot_failure_does_not_block_the_call`: snapshot raises `RuntimeError` → `process_event` still returns the `RecordedCall`.
- [ ] **Step 2:** Run `pytest tests/lending/test_call_record_entry_points.py -q` — expected FAIL.
- [ ] **Step 3: Implement** `_snapshot_first_contact`; call it in `process_event` after consent capture. Add `WARM_POOLS = frozenset({"lendingflow", "website"})` to `config/lending_queues.py`.
- [ ] **Step 3b (Q6, default yes):** the GHL call import (`src/lending/ghl_call_import.py`) snapshots the first outbound GHL call in a connected state (`GHL_CONNECTED_STATES`) for a phone: same arguments, `caller_seat` = the GHL caller name it stores, `queue`/`source_tag` from the lead's load record or `lendingflow` for a LendingFlow lead, `warm` per `WARM_POOLS`. Same savepoint + log-class-only pattern. Test in `tests/lending/test_ghl_call_import.py` (or the existing import test file): `test_first_connected_ghl_call_is_snapshotted`, `test_missed_ghl_call_is_not_snapshotted`.
- [ ] **Step 4:** Run again — expected PASS.
- [ ] **Step 5: Commit** `feat(lending): snapshot a lead at its first reached call`

---

### Task 9: Data fix for existing rows

**Files:**
- Create: `migrations/apply_lending_batchdialer_call_review_backfill.py` (dry run unless `--apply`; prints counts per step; idempotent; 5 s `lock_timeout`; one transaction; never logs phones)
- Test: `tests/lending/test_batchdialer_call_review_backfill.py` (DB-backed, skips without `TEST_DATABASE_URL`)

Steps, all limited to `call_source = 'batchdialer'`:
1. `recording_view_url = 'https://app.batchdialer.com/contacts/view/' || dialer_contact_id` where NULL and `dialer_contact_id` set.
2. `recording_status = CASE WHEN coalesce(talk_duration_sec,0) > 0 THEN 'recorded' ELSE 'none' END` where `recording_status IS DISTINCT FROM` that value.
3. `disposition = 'NO_ANSWER'` where `disposition IS NULL AND upper(btrim(disposition_raw)) = 'UNKNOWN' AND coalesce(talk_duration_sec,0) = 0` (expected 4).
4. `disposition = 'NO_ANSWER'` where `disposition = 'LEFT_VOICEMAIL' AND upper(btrim(disposition_raw)) = 'ANSWERING MACHINE' AND coalesce(talk_duration_sec,0) = 0` (expected 191).
5. `transcript_status = 'deferred', transcript_attempts = 0` where `transcript_status = 'failed'` (expected 18: 17 of David's calls plus one older test call).
6. (Q7, default) `disposition_missing_alerted_at = now()` where `disposition IS NULL AND upper(btrim(disposition_raw)) = 'UNKNOWN' AND coalesce(talk_duration_sec,0) > 0 AND disposition_missing_alerted_at IS NULL` (expected 4: the two Nov 4 calls plus two older test calls), so Task 1's alert posts nothing about old calls. The data fix runs **before** the code is deployed (Deploy order step 2). Re-extracting the 17 transcripts (step 5) posts nothing: the transcript worker has no Slack, Sheet or GHL output.

Steps 3–4 also set `slack_posted_disposition` and `sheet_synced_disposition` to the new disposition **only where they already equalled the old one** (`IS NOT DISTINCT FROM` the old value), so the delivery retry cron finds nothing behind and **no Slack post or Sheet write happens**. Old Slack posts and Sheet rows keep their old text by decision. `disposition_at` is left as is. GHL notes are not rewritten.

- [ ] **Step 1: Failing tests**
  - `test_backfill_dry_run_changes_nothing_and_apply_is_idempotent`: seed one row per step → dry run reports 1 per step and changes nothing; `--apply` changes them; a second `--apply` reports 0.
  - `test_backfill_leaves_nothing_for_the_delivery_retry`: after `--apply`, `src.tasks.lending_disposition_delivery_retry.behind_row_ids()` (pointed at the test DB) returns no backfilled row.
- [ ] **Step 2:** Run it — expected FAIL (module missing) or SKIP without `TEST_DATABASE_URL`.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run against a throwaway Postgres (`TEST_DATABASE_URL=...`) — expected PASS.
- [ ] **Step 5: Commit** `chore(lending): backfill BatchDialer call rows for the call-review fixes`

---

### Task 10: Docs and full verification

**Files:**
- Modify: `CLAUDE.md` — add `migrations/apply_lending_recording_view_url.py` and `apply_lending_batchdialer_call_review_backfill.py` lines; replace "`recording_status` (`pending/readable/forbidden/missing`) is kept by `src.tasks.lending_recording_check` (cron */10)" with "`recording_status` = `recorded` (talk time > 0) / `none`, set on ingest; `recording_view_url` = BatchDialer contact page"; note the Auto-dialer label and `LENDING_DAVID_BD_AGENT_ID`. Keep the file under its size rule.
- Modify: `docs/lending/text-back-runbook.md` / `docs/lending/dialer-disposition-runbook.md` where they describe the recording check or the answering-machine mapping.

- [ ] **Step 1:** `pytest tests/lending -q` — expected: all pass, DB tests skip.
- [ ] **Step 2:** With a throwaway DB: `TEST_DATABASE_URL=... pytest tests/lending -q` — expected all pass.
- [ ] **Step 3:** `grep -rn "recordings import\|lending_recording_check" src tests scripts config docs CLAUDE.md` — expected no hits except history notes.
- [ ] **Step 4: Commit** `docs(lending): call-review fixes`
- [ ] **Step 5:** Stop. Report to the user for go/no-go. No push, no PR.

## Deploy order (after go/no-go and merge)

0. User adds the Sheet header by hand: tab `Dispositions`, cell **N1** = `Recording (BatchDialer)`.
1. `PYTHONPATH=. python migrations/apply_lending_recording_view_url.py`
2. `PYTHONPATH=. python migrations/apply_lending_batchdialer_call_review_backfill.py` (dry run), then `--apply`. The data fix needs only the new column, so it runs before the code goes live; this keeps the new alert from posting about old calls.
3. Deploy code; set `LENDING_DAVID_BD_AGENT_ID=118924`; restart `fa-lending-cdr-poller`.
4. Remove the `lending_recording_check` line from the server crontab.
5. Expect: no Slack posts and no Sheet writes about old calls; the 17 transcripts re-extracted while BatchDialer still serves them; calls ingested from now on show the recording link, the Auto-dialer label and the corrected results. Old calls are only touched again if BatchDialer changes them inside the poller's 2-day rescan.

## Out of scope (round 2)

Callback alerts (BatchDialer and GHL inbound), BOOKED-without-appointment alert, morning callback list, after-hours voicemail workflow, CDR pickup latency (server logs), GHL stage/appointment webhook test, `lead_call_confirmations` source.
