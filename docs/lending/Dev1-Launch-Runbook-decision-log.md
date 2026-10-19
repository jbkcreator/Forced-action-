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
