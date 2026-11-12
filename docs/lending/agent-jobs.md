# Agent job queue (P1.1): runbook

Cora reads job files that the client's Claude agents drop in Google Drive, puts each one in front of Josh as a normal
#approvals card, sends it after he approves, and writes a receipt back to Drive. The agents never send anything
themselves.

```
Agents ── NDL Jobs/inbox/{created_at}_{agent}_{job_id}.json ──► Cora (every minute)
                                                                  │ checks: lane, Backflip, consent, caps, 8am-8pm
                                                                  ▼
                                               #approvals card ─► Josh: Approve / Revise / Reject
                                                                  ▼
                                    GHL (texts, warm email, stage, tag, note, task) or Instantly (cold email)
                                                                  ▼
Agents ◄── NDL Jobs/done/{job_id}.json (receipt) ◄────────────────┘
```

Code: `src/lending/agent_jobs/` (`job.py` parse, `checks.py` rules and rail, `drafts.py`, `drive.py`, `receipts.py`,
`worker.py`), `src/lending/instantly_email.py` (cold email), wired in `src/lending/cora_agent.py`. Table:
`lending.agent_jobs`. Plan and decisions:
`task_specs/Next-deal-lending/NDL_10-11-26/P1.1_Drive_Job_Queue_Implementation_Plan.md`.

## Settings

| Setting | Value |
|---|---|
| `LENDING_AGENT_JOBS_ENABLED` | `true` to run the worker inside `fa-lending-cora` (default `false`) |
| `LENDING_DRIVE_SERVICE_ACCOUNT_KEY_PATH` | Service-account key file. Today the same file as `LENDING_SHEETS_SERVICE_ACCOUNT_KEY_PATH` ("FA Lending Engine") |
| `LENDING_JOBS_INBOX_FOLDER_ID` | Drive folder id of `NDL Jobs/inbox` |
| `LENDING_JOBS_PROCESSED_FOLDER_ID` | Drive folder id of `NDL Jobs/inbox/processed` (Cora moves each file there once taken) |
| `LENDING_JOBS_DONE_FOLDER_ID` | Drive folder id of `NDL Jobs/done` |
| `LENDING_INSTANTLY_API_KEY` | NDL's Instantly workspace key, shared with P1.3 (Instantly replies); never Forced Action's `INSTANTLY_API_KEY` |
| `LENDING_INSTANTLY_SENDER_EMAIL` | NDL's cold sending inbox in Instantly |
| `LENDING_INSTANTLY_CAMPAIGN_ID` | Printed by the setup command below |
| `LENDING_EMAIL_FOOTER_POSTAL_ADDRESS` | NDL's postal address for the CAN-SPAM footer (**required**: cold email stays off without it) |
| `LENDING_EMAIL_FOOTER_BRAND` / `_PHONE` | Default "Next Deal Lending" / "(656) 221-1100" |

A folder id is the last part of the folder's Drive URL (`.../folders/<id>`).

## One-time setup

1. **Drive.**
   - Put `NDL Jobs` (with `inbox`, `inbox/processed`, `done`) in a **Shared Drive**. A service account has no storage of its own, so it cannot create receipts in a personal My Drive.
   - Add the service account's email (the `client_email` in the key file) as **Content manager**.
   - Give the agents' Google account access too.
   - Turn on the **Google Drive API** in the service account's Google Cloud project.
2. **Table.** `PYTHONPATH=. python migrations/apply_lending_agent_jobs.py`
3. **Check the sample files parse.** `PYTHONPATH=. python -m src.lending.agent_jobs.worker --dry-run --from-dir docs/lending/agent-jobs-samples` prints `received=8 carded=0 invalid=0` (one sample per job type, a warm and a cold email, and a partner Deal Drop).
4. **Cold email (once Muhammad gives the key and sending inbox, and the client gives the postal address).**
   - Set `LENDING_INSTANTLY_API_KEY` and `LENDING_INSTANTLY_SENDER_EMAIL`.
   - Run `PYTHONPATH=. python -m src.lending.instantly_email --setup-campaign`. It finds or creates the "NDL Cora Passthrough" campaign (one step: `{{ra_subject}}` / `{{ra_body}}`, sending 09:00–20:00 America/Detroit) and prints `LENDING_INSTANTLY_CAMPAIGN_ID`.
   - Set that id and `LENDING_EMAIL_FOOTER_POSTAL_ADDRESS`.
5. **Switch on.** After Josh approves, set `LENDING_AGENT_JOBS_ENABLED=true` and deploy (`deploy.sh` restarts `fa-lending-cora`). The log shows `agent job queue off` or `not configured` if something is missing.

## Job file format

The brief's section 6 format, plus three fields agreed in the plan:

| Field | Notes |
|---|---|
| `job_id` | UUID. A job_id is acted on once; a repeated one is ignored |
| `lane` | Must be `NDL`, else `blocked_lane` |
| `created_by`, `created_at`, `template_version`, `reason`, `contact_label` | Shown on the card; `created_at` needs a time zone |
| `type` | `send_sms`, `send_email`, `create_task`, `add_tag`, `move_stage`, `add_note` |
| `ghl_contact_id` | Contact in the NDL GHL sub-account. Phone and email are read from GHL, never from the file |
| `body` | The text to send; for `move_stage` the stage name, `add_tag` the tag, `add_note` the note, `create_task` the task title |
| `subject` | **Required for `send_email`** |
| `task_due_at`, `task_assignee` | Optional, `create_task` only (default due: 5 PM Eastern on the day the job was filed; unassigned) |
| `requires_approval` | `false` sends without a card only once the template has graduated (P1.4); until then every job gets a card |
| `not_before`, `expires_at` | With time zone. The card expires at `expires_at` |

Samples: `docs/lending/agent-jobs-samples/` (replace the `REPLACE_WITH_…` contact ids with test contacts before dropping them in the inbox).

**Deal Drop:** a job whose `template_version` starts with `DEAL-DROP` (any case) is the weekly Deal Drop; see the partner limit below.

## What Cora checks (first failure wins)

1. The contact exists in NDL's GHL (`error`).
2. Lane is `NDL`, and no "Backflip", backflip.com, (656) 209-6700 or (656) 207-6500 appears in the job or in the contact's phone or email (`blocked_lane`).
3. Not expired (`expired`).
4. Texts and emails only:
   - not suppressed, not do-not-contact, email not opted out (`blocked_consent`)
   - texts need text consent and no GHL SMS do-not-disturb
   - warm emails need no GHL email do-not-disturb
   - the contact has the phone or email the job needs (`error`)
   - no rates, fees or amounts in the text (`error`)
   - cold email at most once every 3 days and 4 times in 14 days (`blocked_cap`)
   - partners (GHL tag `ndl-partner`) at most twice in 7 days, plus the Deal Drop, which neither counts nor is stopped (`blocked_cap`)
5. `not_before` in the future → held until then.
6. Texts and emails outside 8am–8pm recipient time → held, re-checked every 15 minutes. `blocked_hours` if the job expires first.

**Which rail:** an email to a contact tagged `ndl-cold` in GHL goes through Instantly, with the CAN-SPAM footer and unsubscribe link added at send time. Everything else goes through GHL. The card's first line says which.

The relay's own send-time checks still run after Josh approves: opt-out, text consent, the 10DLC carrier approval (texts wait while `LENDING_SMS_CARRIER_APPROVED=false`), and the 8am–8pm window for texts and emails.

## Receipts (`NDL Jobs/done/{job_id}.json`)

```json
{"job_id": "…", "status": "edited_sent", "action_id": 1234, "provider_message_id": "…",
 "edited_body": "…", "rail": "ghl", "reason": null, "template_version": "LF-D2-SMS-v1",
 "created_by": "LendingFlow follow up", "timestamp": "2026-11-12T14:05:11-05:00"}
```

- **`status`:** `sent`, `edited_sent` (Josh revised it; `edited_body` is what went out), `rejected`, `blocked_consent`, `blocked_hours`, `blocked_lane`, `blocked_cap`, `expired` or `error` (`reason` says why).
- **Instantly emails:** `sent` means Instantly accepted the email for its next sending slot, and `provider_message_id` is the Instantly lead id.
- **No personal data:** no phone number or email address is ever written to a receipt.

A receipt that Drive refuses is retried every minute until it is written (`lending.agent_jobs.result_written_at`).

## Emergency halt

`@Cora emergency halt` stops all of Cora's sends. It also **pauses the NDL Instantly campaign**, so emails already handed to Instantly stop too. If the pause fails, the alert says to pause it by hand. `@Cora emergency resume` re-activates the campaign. Job cards still post while halted; nothing sends.

## Troubleshooting

```sql
-- jobs by state
SELECT state, result_status, count(*) FROM lending.agent_jobs GROUP BY 1, 2;
-- one job and its draft
SELECT j.job_id, j.state, j.rail, j.held_reason, j.release_at, a.status, a.error
  FROM lending.agent_jobs j LEFT JOIN lending.pending_actions a USING (action_id) WHERE j.job_id = '<job_id>';
-- receipts not yet in Drive
SELECT job_id, result_status FROM lending.agent_jobs WHERE state = 'done' AND result_written_at IS NULL;
```

- **A file stays in `inbox`.** The log shows `[agent-jobs] file … could not be taken` (usually Drive permissions).
- **A file has no readable `job_id`.** It is moved to `processed` without a receipt, and the log shows `could not be read as a job`.
- **An action stays `sending` for more than 15 minutes.** The outcome is unknown, and it is never retried (a duplicate send is worse than a missing one). Check GHL or Instantly by hand; the log line `has been sending for` names the action.
- **A repeat cold email** first removes the address's previous lead from the campaign (exact-email lookup), then adds the new one. An address on the Instantly block list is refused and the receipt says `error`.
