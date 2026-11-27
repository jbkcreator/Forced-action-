# David's dialer queue check and 2:45 PM scorecard (R1 1.6)

Two weekday posts to `LENDING_DAVID_CHANNEL` (#isa-daily), both off until their switch is on.

| Post | When (ET) | Code | Switch |
|---|---|---|---|
| Queue check | 6:xx AM | `src/tasks/lending_dialer_queue_check.py`, `src/lending/dialer/queue_manager.py` | `LENDING_DIALER_QUEUE_CHECK_ENABLED` |
| Scorecard + notes thread | 2:xx PM | `src/tasks/lending_caller_scorecard.py`, `src/lending/dialer/scorecard.py`, `src/lending/dialer/call_notes.py` | `LENDING_CALLER_SCORECARD_ENABLED` |

Each cron has two UTC lines (EST and EDT) and the task acts only in its ET hour, Monday to Friday.

## The post's count is the authoritative number

BatchDialer has no remove-from-campaign endpoint (`campaign_remove` is `None`), and only an opt-out or a hand-off to GHL
deletes a contact. So David's BatchDialer screen can show contacts the count leaves out. That is not padding: the dialer
sweep still blocks those contacts at dial time. If the two disagree, the post is right.

## 6 AM queue check

Read-only. It counts, alerts, and pushes nothing to BatchDialer; the loaders (`lending_dialer_load`,
`lending_wholesaler_load`, `lending_pr_dialer_autoload`, `lendingflow_dialer`) are unchanged.

- **Sources, in order:** David's callbacks, new LendingFlow leads, new website leads, then the dialer pools (partners,
  wholesalers, transaction ready, builders, verified maturity, LendingFlow, website). A phone is counted once, in the
  first source that has it.
- **Eligible** means none of these apply: opted out or suppressed; the stored DNC or litigator check failed; total
  attempt history used up (6 attempts in 10 business days, checked even when the phone is also at the 3-per-24h cap);
  the DNC scrub is too old. The call window and the 3-per-24h cap clear up within the day, so they are not counted
  against a 6 AM check. LendingFlow leads need no DNC scrub
  (client decision).
- **Never padded.** If 32 are eligible the post says 32. Under 50 the headline is "Only N of 50 eligible" and the post
  also goes to the ops notice channel. Zero reads "No eligible work today".
- **Dry** applies to dialer pools only: the pool had a load in the last 14 days (or has active rows), has nothing
  eligible, and has nothing staged for the 9:05 PropertyRadar load. A pool's readiness counts all its eligible active
  members, also those credited to an earlier source in the total ("N more counted above"), so a pool whose contacts are
  also callbacks or new leads is not dry. A pool with no recent loads shows "not in use".
  Callbacks and new leads are never dry (zero is a normal day).
- **Staged for 9:05** ("+N may load at 9:05") counts the PropertyRadar records staged for the autoload whose phone is not
  already loaded. The autoload's other gates run at load time, so this is an upper bound; the 9:05 run can load fewer.
  If the staged records cannot be read, no pool is called dry. While `LENDING_PR_DIALER_AUTOLOAD_ENABLED` is off,
  staged records are ignored: they will not load, so they never keep a pool from being called dry.
- **Not available** means a source could not be read. It is never reported as dry, and the total is built from the
  sources that did read. The two lead sources share one read, so one failure marks both.
- If no source can be read the headline says so instead of "No eligible work today". If some cannot be read, the
  headline reads "Partial count: at least N of 50 eligible"; "No eligible work today" needs every source read.
- If the whole check fails, ops gets a notice and no count is posted.
- **Delivery and retries.** David's post and the ops alert are each tried on every run and tracked separately for the
  ET day in `data/dialer_posts/` (gitignored). If either fails the run returns 1 and the cron retry (+15, +30 min) sends
  only the missing one, with the same text, even after 7:00 ET. An unset `LENDING_DAVID_CHANNEL` fails only David's
  post; the ops alert still goes. A finished day is not posted again, and overlapping runs wait on a per-day lock.

**0.6 reuse.** `read_queue(db, now, lead_days=..., autoload_enabled=...)` returns a `QueueReport`; 0.6's outage case should call it rather than
build a second eligibility rule or a second "no eligible work" alert.

## 2:45 PM scorecard

Numbers are David's BatchDialer seat (`LENDING_DAVID_BD_AGENT_ID`): dials (all dials in his campaigns), connects, 2+ minute
conversations, booked (GHL stage, all callers, next to what he reported; "not available" when that read fails), the top 10 connected calls (booked first, then
talk time), and "recorded calls with no disclosure reported by the dialer". BatchDialer plays the disclosure and controls
recording; the last line only counts recorded calls where the dialer did not report it, so it is not a compliance verdict.

**Quality score is provisional (`provisional_v1`).** One point each: a result was logged, 2+ minutes of talk, a next
action, the decision maker, and an objection, kill reason or deal status. A call with no transcript summary and no notes
is not scored and counts as "without notes". The client question is on the tracker; replace the rubric in
`config/lending_dialer_queue.py` once Josh answers.

**Notes form.** Under the post, each connected call with no recording and no notes gets an "Add notes" button. It opens a
Slack form that saves the call's extraction fields to `lending.call_extractions` with `source='caller_notes'` and
`entered_by` set. Notes never replace a transcript summary, and a later transcript replaces notes. Only
`LENDING_DAVID_SLACK_USER_ID` and `LENDING_CORA_APPROVER_USER_IDS` can open or submit it. Notes entered after 2:45 PM
count in the next day's quality figures for that call only if it is still in the day's window, so enter them the same day.

**Delivery and retries.** The rendered main post and notes thread are saved before either is sent, and the main post's
Slack timestamp as soon as it is sent (`data/dialer_posts/`). If the main post or the notes thread fails, the run
returns 1 and the cron retry (+15, +30 min, so after 3 PM) resumes from that saved payload without reading the database
again: the main post only if it was not sent, and the notes thread under the saved one. A finished day is not posted
again, and overlapping runs wait on a per-day lock.
`--force` and `--dry-run` neither read nor write this state, so a forced run always posts a fresh card.

## Setup

1. Run the migration before deploying: `PYTHONPATH=. python migrations/apply_lending_call_extraction_source.py`
   (idempotent). The transcript worker writes the new `source` column.
2. Slack app: Interactivity must point at `/webhooks/lending/slack-interactivity` with `LENDING_SLACK_SIGNING_SECRET` set.
   The scorecard is posted with `LENDING_SLACK_BOT_TOKEN`, so its buttons reach that URL.
3. Settings: `LENDING_DAVID_CHANNEL`, `LENDING_DAVID_BD_AGENT_ID`, `LENDING_DAVID_SLACK_USER_ID`,
   `LENDING_MORNING_LIST_LEAD_DAYS` (default 14), then the two switches.

## Checking it

Both tasks work with the switch off when `--dry-run` is used; nothing is posted and nothing is written:

```bash
python -m src.tasks.lending_dialer_queue_check --force --dry-run
python -m src.tasks.lending_caller_scorecard --force --dry-run
```

Phones never appear in either post (last four digits in the scorecard only).
