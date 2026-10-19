# Decision log: Dev 1 (Launch Protocol Runbook): Cora, send gate, kill switch, LendingFlow inbound, Cora acceptance test, evidence

Running log of what was done and what was decided for each Dev 1 task. Append a dated entry after every piece of work. Never rewrite earlier entries; add a correction entry instead.

Branch: `feat/dev1-cora-send-gate-kill-switch` (cut from `origin/dev`; local only, one PR for all Dev 1 tasks when asked).

Sources: `Launch_Protocol_Runbook_Dev_Split.md`, `Next_Deal_Lending_Launch_Protocol_Runbook 2.pdf`, `Next_Deal_Lending_Chunk2_Lead_to_Submission_Engineering_Spec.pdf`, `Chunk1_Addendum_R1_P1_Scope_and_Acceptance_Tests.pdf`, `Next_Deal_Lending-Questions_for_Josh_2026-10-08_deduplicated.md`.

## Related questions per task

| Task | Questions |
|---|---|
| T-01 | Q-34 |
| T-02 | Q-3, Q-12 |
| T-03 | Q-4 |

## Entry format

```
### YYYY-MM-DD, <what was done>
- Done: <changes, files, commit>
- Decision: <choice made, and why>
- Questions applied: <Q ids and the answer used>
- Assumed defaults: <anything assumed>
- Open: <still unanswered or blocked>
```

## T-01: Register the production email sender so approved drafts send

### 2026-10-19, built, tested, live test send
- Done: `src/lending/cora_agent.py` `build_egress_executors()` registers `ghl_email` with the existing `GhlEmailSender` (`ghl_email.py`) and passes it to the relay; `EGRESS_EXECUTORS` (empty) removed. `cora_tools.draft_email` payload now carries `to_phone` so GHL keeps the lead on one contact. Approve on a `draft_email` card sends through GHL and the action becomes `sent` with the GHL message id. Commit `8830521e`. Tests: `test_cora_conversation.py` (approved email sent exactly as approved; rejected never sent; suppressed blocked; GHL error failed, no retry), `test_cora_agent.py` (registered only when configured).
- Decision: registration lives in `cora_agent.py`, not `relay.py`/`cora_tools.py` as the runbook names: `packages/agent_core` must not import `src` or read env (`test_package_isolation.py`); `relay.py` already does claim, send-time suppression check, send and `mark_sent`.
- Questions applied: Q-34 (sender): user confirmed GHL is the only sender used; no Mailgun code exists.
- Assumed defaults: emails have no send-hour window (each is human-approved); ghl_sms and ghl_stage stay unregistered (runbook § 2 names only draft_email).
- Evidence: `python -m src.lending.ghl_email --send-test <own inbox>` accepted by GHL (message id F7QAbqBNMhwQt5PHcgaU) after adding `LENDING_GHL_EMAIL_FROM=hello@nextdeallending.com` to the local `.env`.
- Open: production `.env` needs `LENDING_GHL_EMAIL_FROM` and a `fa-lending-cora` restart; Laira's earlier held draft stays failed (redraft and approve); a GHL timeout shows "send failed" although the email may have gone (existing relay message, not changed); confirm with Hari that hello@ is the "verified sender" (Q-34).

## T-02: Cora service, morning digest and Slack operation

### 2026-10-19, PR #369 merged in, 7:30 AM digest built, kill switch test
- Done: merged `origin/feat/cora-main-thread` (#369, inbox digest + approver actions) into the branch (`39225ee1`), conflicts with T-01 resolved keeping both. New `src/lending/cora_morning_digest.py` + `lending.cora_morning_digests` (model, `migrations/apply_lending_cora_morning_digests.py`); posted by Cora's minute sweep (`run_sweep` → `make_morning_digest`) to Cora's channel once per ET day between 7:30 and noon; the day row is written in the same transaction as the post (no double post; failed post retried next minute); posts while halted. Kill switch end-to-end test (halt → approve held → resume → sent). Commits `4f475eae`, `6cb75cb5`, `8fb32cc9`.
- Decision: #369's digest is the per-minute inbox digest, not Line 4's 7:30 digest, so the 7:30 digest is new code. Section 7 items (Sonnet 5 loop, `<standing_rules>`, Approve/Revise/Reject cards) and the Socket Mode daemon already existed.
- Questions applied: Q-12 (digest content): Chunk 2 spec Task E5 "overdue actions, new high-score leads, and today's calling queues" (user: "do as per specs"). Sections: overdue actions (drafts waiting, confirmation calls due, open inbox items), new leads last 24h (LendingFlow, closer-priority then FULL_MACHINE first, plus web leads), today's calling queues (booked calls, active dialer contacts per campaign), Cora halted or running.
- Assumed defaults: every day (other lending Slack posts run every day); "high-score" = current intake classification until the Chunk 2 A0 1-10 score exists; no catch-up after noon.
- Open: Q-3 "Cora Task 7" undefined; on the server: run the migration, restart `fa-lending-cora`, live halt/resume, see the first 7:30 digest; close PR #369 once the Dev 1 PR covers it.

## T-03: Emergency halt ("@Cora emergency halt")

### 2026-10-19, built and tested
- Done: `src/lending/global_halt.py` + `lending.global_halt` (LENDING_GLOBAL_HALT, one row) and `lending.global_halt_events` (history) (models, `migrations/apply_lending_global_halt.py`). `SlackSession(command_hook=...)` (agent_core stays src-free) runs the command before the normal halt and the model. Approver-only `@Cora emergency halt` sets the flag, halts Cora, alerts `LENDING_PROGRESS_CHANNEL` (#next-deal-lending, new setting) and replies in thread. While set: `ghl_sms._post` refuses every GHL send (SMS and email), the LendingFlow intro-ready tag is not added or released, live dialer loads are refused. Unreadable flag = halted. `@Cora emergency resume` clears the flag only. Tests `test_global_halt.py` + Cora session test; `tests/lending/conftest.py` keeps the flag off for other tests (no shared-DB read). Commit `1056fba5`.
- Decision: BatchDialer's API has no campaign-pause call (`config/lending_dialer.py`), and GHL-internal workflows cannot be stopped from our code, so those two are done by hand and the alert says so. A refused text or reminder counts as failed and is not re-sent after resume.
- Questions applied: Chunk 2 spec § 1 "Any queued or active automated action must immediately halt upon receipt of an emergency kill command" confirms halting queued actions too.
- Assumed defaults: resume command `@Cora emergency resume`, approvers only; Cora's own halt still needs `resume`.
- Open: Q-4 "Section 8a" does not exist; Hari to accept manual BatchDialer/GHL pause; `#next-deal-lending` channel ID for `LENDING_PROGRESS_CHANNEL` and invite Cora's bot; run the migration on the server.

## Verification (T-01 to T-03)

### 2026-10-19, fakes + throwaway Postgres
- Done: 400 Cora/agent-core/GHL/halt/digest tests pass with fakes. Throwaway Postgres (Docker, port 58123, never the shared DB): both migrations create their tables and re-run cleanly; digest posts once, stores its row, shows the E5 sections; halt flag set/clear with 2 history rows. The same 10 test files on `origin/dev` and this branch fail identically (59 failed, 80 errors: the empty DB lacks the main app tables); none fail only on this branch, 16 more pass.
- Open: the existing text-back, reminder, intro-SMS and dialer-load Postgres tests could not run on an empty DB (fake-based tests pass); live checks on the server.

## T-04: LendingFlow real-time intake live (`POST /api/lending/lendingflow`)

### 2026-10-19, verified existing intake, duplicates go to a person
- Done: checked the existing intake (T-11, PR #346, on dev): route on lending-api `/api/lending/lendingflow`, `X-Webhook-Secret` check (401 wrong, 503 while off), dedupe on `vendor_lead_id` and SHA256(phone|email), certificate stored verbatim and append-only in `lending.lead_consent_certificates`, lead saved, GHL contact tagged `lendingflow` with a pipeline card when the pipeline/stage settings exist. New: `lendingflow._record_duplicate` raises a `lendingflow_possible_duplicate` item in Cora's inbox (target `dial_tasks`) when a delivery matches an existing lead on only one key; exact re-sends stay quiet. Tests `tests/lending/test_lendingflow_duplicates.py` (6, no DB).
- Decision: "ambiguous" = same phone+email with a different LendingFlow lead id, or same lead id with a different phone/email (Runbook § 1, Principle 3). Raised through Cora's inbox (her channel; one reply routes it to #dial-tasks-lending), with lead refs only, no phone or email. Inbox write is in a savepoint, so a failure never loses the lead.
- Questions applied: none answered in the four client docs for D-1 to D-4 (Questions for Josh lines 175-178 list them as requests to LendingFlow only).
- Assumed defaults: parser stays on the agreed test payload until David's real payload arrives.
- Open: David's real test payload (D-1 to D-4) for Line 2; local `.env` has `LENDING_GHL_PIPELINE_ID` but no `LENDING_GHL_STAGE_LENDINGFLOW_NEW`, `LENDING_LENDINGFLOW_ENABLED` or `LENDING_LENDINGFLOW_WEBHOOK_SECRET` (server `.env` not visible from here; Q-25); the LendingFlow Postgres tests need a fully migrated DB.

### 2026-10-19, correction: server settings
- Done: user confirmed `LENDING_GHL_STAGE_LENDINGFLOW_NEW`, `LENDING_LENDINGFLOW_ENABLED` and `LENDING_LENDINGFLOW_WEBHOOK_SECRET` are set on the server (only the local `.env` lacks them). Q-25 pipeline assignment is configured on the server.
- Open: David's real test payload (D-1 to D-4) only.

## T-05: LendingFlow direct-to-Josh path (instant SMS and mobile alert)

### 2026-10-19, arrival alert to Josh, alarms survive the emergency halt
- Done: new `src/lending/lead_arrival_alert.py` (`alert_josh`), subscribed to `LendingFlowLeadCreated` in lending-api (`src/lending/api.py`) and in the GHL retry sweep (`src/tasks/lending_lendingflow_sweep.py`), so every delivered lead alerts once whichever process gets it into GHL. It posts to `LENDING_OPS_CHANNEL` with an @-mention of each `LENDING_CORA_APPROVER_USER_IDS` (Josh): loan type, amount, state, last 4 of the phone, lead id. Off until `LENDING_LEAD_ARRIVAL_ALERT_ENABLED=true` (new setting). `GhlSmsSender(internal_alert=True)` / `get_sender(internal_alert=True)`, used only by `uncalled_alarm_worker`, skips the emergency-halt check. Tests `tests/lending/test_lead_arrival_alert.py` (12).
- How it works today (end to end): lead arrives → saved + certificate stored (T-04) → GHL contact, `lendingflow` tag, pipeline card → `lendingflow-intro-ready` tag starts the GHL intro-SMS workflow only with consent, inside 8am-8pm borrower time and `LENDING_INTRO_SMS_ENABLED` (off) → arrival alert @-mentions Josh in Slack (new, off) → 2 min no call: SMS to `LENDING_ALARM_SMS_TO`; 5 min: URGENT SMS + Slack post (`uncalled_alarm_worker`, off until `LENDING_UNCALLED_ALARMS_ENABLED`). A connected call stops the alarms.
- Decision (dev): alarm texts to Josh's own phone keep going during an emergency halt; the halt stops outbound to borrowers and a missed-lead alarm must not go quiet. A test pins that only the alarm worker uses the exempt sender.
- Decision (interim, Q-36): "high-priority push" = Slack @-mention (the Slack mobile app pushes it) until Josh asks for a dedicated push app (new vendor, his call).
- Questions applied: Client Actions § 4 note: the Slack part of the alarm does not depend on the carrier, so the Slack alert can go live before 10DLC.
- Assumed defaults: arrival alert goes to the alarm channel (`LENDING_OPS_CHANNEL`), not a new channel.
- Open (client): Q-36 push mechanism; R5 Josh's alarm mobile number; 10DLC approval and D1/D2 consent before any borrower text; Q-5 sending number. Test 1's 2m/5m steps are covered by the existing alarm tests, which need a fully migrated Postgres DB (skipped here).
- Suggestions:
  1. Switch on `LENDING_LEAD_ARRIVAL_ALERT_ENABLED` now: it is Slack only, needs no 10DLC or R5, and gives Josh a phone alert for every lead today.
  2. Until R5 and 10DLC land, the 2- and 5-minute alarms reach Josh only through the 5-minute Slack post; also @-mention him in that post so it pushes to his phone.
  3. Add a GHL contact link to the arrival alert so Josh can open the lead and call in one tap (the masked Slack text cannot carry the number); needs the GHL contact id passed in the event.
  4. Ask Josh to confirm the Slack app on his phone has notifications on for mentions in the ops channel, or the "push" will not ring.

### 2026-10-19, suggestion 2 done: 5-minute alarm post mentions Josh
- Done: `uncalled_alarm_worker.MentioningSlack` wraps the alarm worker's Slack client in `run_cycle`, prefixing every alarm post with @-mentions of `LENDING_CORA_APPROVER_USER_IDS` (Josh), so the 5-minute Slack alarm pushes to his phone through the Slack app. No approvers set = post unchanged. Tests in `tests/lending/test_lead_arrival_alert.py` (2 more).
- Decision: until R5 and 10DLC land, the 5-minute Slack post is the only alarm that reaches Josh, so it carries the mention.
- Open: suggestions 1 (switch on the arrival alert), 3 (GHL contact link), 4 (Josh's Slack mention notifications) remain.

### 2026-10-19, suggestion 3 done: GHL contact link in the arrival alert
- Done: `LendingFlowLeadCreated` gains optional `ghl_contact_id` (set by `lendingflow_events.build_event` from the delivered lead). The arrival alert adds "Open in GHL to call" linking to `LENDING_GHL_APP_URL/v2/location/<LENDING_GHL_LOCATION_ID>/contacts/detail/<contact id>`; without a contact id it posts without the link. New setting `LENDING_GHL_APP_URL` (default `https://app.gohighlevel.com`, change for a white-label domain). Tests in `test_lead_arrival_alert.py` (2 more).
- Decision: URL format confirmed from a contact page in the Next Deal Lending sub-account (standard GHL domain, no white label). No ids are hard-coded; nothing to configure in GHL.
- Open: suggestions 1 (switch on the arrival alert) and 4 (Josh's Slack mention notifications).

### 2026-10-19, suggestion 4: client notification check (handed to team lead)
- Done: drafted a client message (Josh is the client) for the team lead to review and send: arrival alerts and the 5-minute alarm tag @Josh in #lending-ops with an "Open in GHL to call" link; asks Josh to check (1) Slack Profile → Notifications → Mobile = "Direct messages, mentions & keywords" or "All new messages", (2) #lending-ops not muted, (3) phone allows Slack notifications with sound, including Focus / Do Not Disturb if he wants leads after hours; then we send one test alert.
- Team lead checks in Slack (no client action): Josh and the lending Slack bot are both members of #lending-ops.
- Decision: the message is a draft; the team lead reviews it before it goes to the client.
- Open: team lead sends the message; Josh confirms; set `LENDING_LEAD_ARRIVAL_ALERT_ENABLED=true` on the server and send one test lead to confirm the alert rings Josh's phone.

### 2026-10-19, correction: where each notification setting lives
- Phone settings (not Slack): notifications allowed with sound (iPhone: Settings → Notifications → Slack; Android: Settings → Apps → Slack → Notifications); Slack allowed during Focus / Do Not Disturb (iPhone: Settings → Focus → each Focus → Apps → add Slack; Android: Settings → Do Not Disturb → Exceptions → add Slack).
- Slack app: mobile notification level (Profile → Notifications → Mobile); #lending-ops not muted (channel name → Notifications); notification schedule / pause hours (Profile → Notifications → Notification schedule; desktop: Preferences → Notifications).
- The client message draft should name these two places separately before the team lead sends it.

## Answers from Hari (2026-10-19)

### 2026-10-19, Q-34 / Q-3 / T-03 manual steps / Q-4 answered
- Q-34 (T-01 sender): Hari: "yes proceed". GHL with hello@nextdeallending.com is the sender. T-01 unchanged.
- Q-3 (T-02 "Cora Task 7"): only the label on Runbook § 3 Line 4; build to Line 4's pass condition (service in Socket Mode with the bot token, kill switch tested, 7:30 AM digest). Hari is asking the client whether it means more. Note: the 7:30 AM digest IS built on this branch (`cora_morning_digest.py`, T-02 entries above, content per Chunk 2 Task E5); #369 alone has only the per-minute inbox digest.
- T-03 manual steps: Hari: not OK to assume. The runbook asks for BatchDialer and GHL to pause automatically, so the partial stop ships only if Hari and Josh accept the two manual steps IN WRITING, and the stop alert must show them as a checklist. Done: `global_halt.MANUAL_CHECKLIST` adds ":white_square: Pause the lending campaigns in BatchDialer" and ":white_square: Turn off GoHighLevel workflows and Conversation AI that send on their own" to the alert, under "Stopped automatically". Test updated (`test_global_halt.py`).
- Q-4 (Section 8a): Hari pointed to the § 7 bullets themselves; the three bullets are the spec. T-03 unchanged.
- Open: written acceptance from Hari and Josh of the two manual halt steps (blocks shipping T-03's partial stop); client answer on "Task 7".

### 2026-10-19, server settings update (T-03 / T-05)
- Done: `LENDING_OPS_CHANNEL` = `C0C7LFEKRDM` (#lending-ops) is set on the server (user confirmed); Josh and the Cora Lending app are in #lending-ops.
- Clarified: `LENDING_PROGRESS_CHANNEL` is our setting name for the runbook's "posts an alert to #next-deal-lending" (Runbook § 7); the client did not name it. Deploy step, no client approval: copy #next-deal-lending's channel ID into it on the server, restart `fa-lending-cora`, invite Cora's app to #next-deal-lending (a bot can only post in channels it is in). Without it the halt still works and the alert is only replied in Cora's channel.
