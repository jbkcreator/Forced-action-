# 14-day follow-up (Runbook § 4.1, Item 1)

Every LendingFlow and website lead gets a 14-day call, text and email cadence in GoHighLevel. The cadence is a GHL
workflow, built by hand (the GHL API cannot create workflows). The code side records every call attempt in
`lending.call_dispositions` and reports which LendingFlow leads qualify for replacement credit.

**Status of the wording below: DRAFT. Josh approves every message before the workflow is published.**

## 1. Before building

- [ ] Agency Admin access to the **Next Deal Lending** sub-account.
- [ ] The Private Integration token in `LENDING_GHL_API_KEY` has **`conversations.readonly`** and
      **`conversations/message.readonly`** (the GHL call import reads the call log with them).
- [ ] From Josh: his **personal voicemail recording** (Day 7) and the voicemail-drop method. He earlier said
      **no ringless voicemail**; the Runbook's "voicemail drop" on Day 0 and Day 7 must use a method he approves
      (for example the caller's in-call voicemail drop), or those steps are removed.
- [ ] Josh approves the message drafts in section 4.
- [ ] Josh is a user in the Next Deal Lending sub-account, so tasks can be assigned to him. As of 2026-10-19 the
      sub-account's only users are Hari K / Hari K. (admins); Josh, as agency owner, must add the sub-account to
      his own user. Until then tasks are assigned to **Hari K**, and switched to Josh afterwards.

## 2. The workflow: "NDL – 14-Day Follow-Up"

| Part | Setting |
|---|---|
| Triggers | **Contact Tag added: `lendingflow`** and **Contact Tag added: `web-lead`**. Our code adds these tags when a lead reaches GHL (`config/lending_lendingflow.py`, `config/lending_web.py`). |
| Re-entry | Off: a contact runs the sequence once. |
| Booking exit | A separate, **published** workflow **"NDL – Booked, stop follow-up"**: trigger **Customer Booked Appointment** on **"Josh's Calendar"** → action **Remove from Workflow: "NDL – 14-Day Follow-Up"**. Tested 2026-10-19: a booking removes the contact within seconds. The "Appointment booked" goal step (Appointment Status, New/Confirmed/Showed) is kept as the **last** step but does **not** react to a new booking on its own (tested): never rely on it alone, and never place it mid-sequence (a contact reaching it unmet is removed). |
| Calendar note | The Runbook calls it the "Booked Calls" calendar; in GHL the only calendar is "Josh's Calendar" (personal, the one behind Josh's booking link; checked through the GHL API on 2026-10-19). |
| Opt-out | A **"DND?"** If/Else at the start **and directly after every Wait step** (before the next task or message). The branch for "DND enabled channels is not empty" (or the fallback of an "is empty" condition) must contain **nothing: END**, never a Goto. GHL blocks texts and emails to DND contacts by itself, but not tasks, so without these checks Josh would keep getting call tasks for someone who opted out. Our opt-out handling sets GHL DND on every channel (`src/lending/ghl_dnd.py`); a STOP reply sets it as well. Tested 2026-10-19 with the real `set_ghl_dnd()`: no task or message after the opt-out. |
| Replies | A reply alone does **not** stop the sequence (the Runbook stops only on booking or opt-out). |
| Quiet hours | Texts only 8 AM–8 PM in the borrower's local time, the same rule as the intro text. |
| Sender | Josh's Next Deal Lending number and email; signed "Josh Kantor, Loan Officer"; **no company name**. |

### SMS steps stay paused until 10DLC clears
Every **Send SMS** step sits behind an **If/Else: contact has tag `ndl-sms-enabled`**. No contact has that tag, so
texts are skipped while call tasks and emails run. When the 10DLC campaign is approved, remove the If/Else
conditions (or tag contacts) and texts start. Nothing else changes.

The "no tag" branch holds a **Goto pointing at the step after that SMS** (never at the SMS itself, which would send
it anyway). This is the only place a Goto belongs; the opted-out branches of the DND checks have none.

### Steps (Runbook § 4.1, in order)

| Day | Steps |
|---|---|
| 0 | Task **"Call now – new lead"**, due immediately (LendingFlow leads go to Josh, Runbook Principle 2) → wait 10 min → task **"Callback"** → SMS 0 → Email 0. (No voicemail drop: see section 1.) A lead who books during the 10 minutes is removed by "NDL – Booked, stop follow-up" before the Callback task. |
| 1 | Wait until 9 AM → task "Morning call" → wait until 5 PM → task "Evening call" → SMS 1 |
| 2 | Wait until 12 PM → task "Midday call" → SMS 2 |
| 3 | Wait 1 day → task "Call" → Email 3 |
| 5 | Wait 2 days → task "Call" → SMS 5 |
| 7 | Wait 2 days → task **"Call – leave Josh's personal voicemail"** (a task, not a ringless voicemail action) |
| 10 | Wait 3 days → task "Call" → Email 10 |
| 14 | Wait 4 days → task "Final call" → SMS 14 ("Should I close your file?") → add tag **`ndl-nurture`** (starts the nurture track, Runbook § 4.2) |

Each day starts with the DND check, and every SMS sits behind the `ndl-sms-enabled` check. The time-of-day waits
use the contact's time zone with a window ending at **8:00 PM**: 09:00–20:00, 17:00–20:00, 12:00–20:00.

**Assignee:** every task goes to Josh Kantor once he is a user in the sub-account (section 1); until then, to
**Hari K**. Set **only** the custom user: a task step that also has "Assign to contact's assigned user" switched on
is skipped by GHL ("Task cannot be created with both assigned to contact's assigned user or custom assigned
user"). Rebuild such a step by hand if the option is not visible.

## 3. Call attempts and replacement credit

The Runbook asks that every attempt, timestamp and disposition is written to `lending.call_dispositions`, so that
LendingFlow leads with **5+ attempts over 5+ days** qualify for replacement credit.

- **BatchDialer calls** are recorded by the CDR poller (`src/lending/cdr_poll.py`), as before.
- **Calls made in GoHighLevel** are recorded by `src/lending/ghl_call_import.py` (cron every 15 minutes, on when
  `LENDING_GHL_CALL_IMPORT_ENABLED=true`): one row per outbound GHL call, `call_source = 'ghl'`,
  `dialer_call_id = 'ghl:<message id>'`, GHL's outcome in `disposition_raw`.
- **Calls made from a personal phone outside both systems are not recorded.** Make follow-up calls from BatchDialer
  or GHL.
- **Which leads qualify:** `python -m src.lending.replacement_eligibility [--eligible-only]` (attempts since the
  lead arrived, distinct Eastern call days, GHL attempts, connected calls, first and last attempt, `eligible`).
  A query, not a database view: the dialer migration re-applies column types and a view would block it.

## 4. Message drafts (for Josh's approval)

Rules for every message: signed "Josh Kantor, Loan Officer"; no company name; **no rate, term, point, fee, ratio,
payment, amount or range** (the same rule Cora and the reply agent enforce); texts end "Reply STOP to opt out.";
booking link `https://api.leadconnectorhq.com/widget/bookings/josh-kantor`.

**SMS 0:** Hi {{contact.first_name}}, this is Josh Kantor, loan officer. I just tried calling about your loan
request. Grab a time that works for you here: [booking link]. Reply STOP to opt out.

**SMS 1:** Hi {{contact.first_name}}, Josh Kantor again. Happy to walk through your project whenever it suits you:
[booking link]. Reply STOP to opt out.

**SMS 2:** Hi {{contact.first_name}}, do you have a property under contract yet? If you do, I can move quickly. Book
a short call here: [booking link]. Reply STOP to opt out.

**SMS 5:** Hi {{contact.first_name}}, I'd still like to help get your deal funded. A short call is all it takes to
start: [booking link]. Reply STOP to opt out.

**SMS 14:** Hi {{contact.first_name}}, should I close your file? If you'd still like help with your loan, reply here
or grab a time: [booking link]. Reply STOP to opt out.

**Email 0, subject "Your loan request":** Hi {{contact.first_name}}, thanks for your loan request. I tried to reach
you by phone and would like to learn about your project and timeline. You can pick a time that suits you here:
[booking link]. Josh Kantor, Loan Officer.

**Email 3, subject "Options for your project":** Hi {{contact.first_name}}, I work with several lenders and compare
their options so the loan fits your deal. On a short call I can go through your project and what a
pre-qualification would look like. Pick a time here: [booking link]. Josh Kantor, Loan Officer.
*Flag for Josh:* the Runbook asks this email to highlight "competitive rates and pre-approval offers". Rates and
approval wording are left out on purpose: borrower messages never quote rates, and "pre-approval" can read as a
commitment. Josh decides the final wording.

**Email 10, subject "Still looking to fund your deal?":** Hi {{contact.first_name}}, checking in on your loan
request. If your plans have changed, no problem. If you're still working on a deal, I'd be glad to help: [booking
link]. Josh Kantor, Loan Officer.

## 5. Checks after building (evidence)

- [ ] Screenshots: triggers, the full step list, one DND check, one SMS If/Else, and "NDL – Booked, stop follow-up".
- [x] Day 0 on the real workflow (2026-10-19): "Call now – new lead" and "Callback" tasks (Hari K), SMS 0 delivered,
      "Your loan request" email sent.
- [x] Full Days 0–14 on a fast copy with 1-minute waits (2026-10-19): all 10 tasks in order, SMS 0/1/2/5/14
      delivered, 3 emails sent, `ndl-nurture` added.
- [x] Booking exit: a booking on Josh's Calendar removed the contact; no later task or message (one task due in the
      same two seconds as the booking was created).
- [x] Opt-out exit: our real `set_ghl_dnd()` applied after Day 0; the next DND check ended the workflow, no later
      task or message.
- [x] SMS skipped without `ndl-sms-enabled`; sent with it.

## 6. Borrower nurture track (Runbook § 4.2, Item 2)

Leads that finish the 14 days without booking move to a low-frequency nurture track. Day 14 of the follow-up adds
the tag `ndl-nurture`, which starts it. Decisions (2026-10-19): one message **every 14 days**; the loop runs **until
the lead books or opts out** (no automatic end); a reply creates an urgent call task and the lead **stays** in
nurture. **Status of the wording: DRAFT, Josh approves it before publishing.**

### Workflows

| Workflow | Status | Setting |
|---|---|---|
| **"NDL – Borrower Nurture"** | draft | Trigger: Contact Tag Added `ndl-nurture`; re-entry off. DND? → Wait 14 days → DND? → email "Any deal under contract?" → SMS allowed? (`ndl-sms-enabled`): Yes → SMS → Go To "Wait 14 days"; No → Go To "Wait 14 days". Both DND? end branches have nothing after them. |
| **"NDL – Nurture reply → urgent call"** | draft | Trigger: Customer Replied (all channels), filter tag `ndl-nurture`; re-entry on. Wait 1 minute (so a STOP reply has set DND) → DND? (enabled → End) → task "URGENT: nurture lead replied – call back now", due immediately, assigned to Hari K only (Josh once he is a user). |
| **"NDL – Booked, stop follow-up"** | published | Also removes the contact from "NDL – Borrower Nurture" on a booking. |

The pipeline-stage workflow **"NDL - Nurture"** (trigger: "Booked Calls" stage Nurture; webhook to
`/webhooks/lending/ghl-nurture`) belongs to the booking-gate feature (`docs/lending/ghl-conversation-ai.md`) and is
**not** part of this track. It was left unchanged.

**Email (draft), subject "Any deal under contract?":** Hi {{contact.first_name}}, checking in. Do you have a property
under contract, or one you're looking at? Send me the details and I'll run a free deal analysis so you know where it
stands before you commit. Reply here or pick a time that suits you: [booking link]. Josh Kantor, Loan Officer

**SMS (draft, behind `ndl-sms-enabled` until 10DLC clears):** Hi {{contact.first_name}}, Josh Kantor here. Got a
property under contract or in your sights? I'll run a free deal analysis for you. Reply here or book a call:
[booking link] Reply STOP to opt out.

### Tests (2026-10-20, test contact, 1-minute copy "NDL – Nurture TEST (delete after)")

- [x] **Loop:** email + SMS at 05:59 UTC and again at 06:06 UTC; the execution log shows SMS → Go To → Wait. GHL ran
      the second 1-minute wait about 7 minutes late on the test run; immaterial for a 14-day wait.
- [x] **Reply → urgent task:** an email reply to the nurture email reached GHL as an inbound email (06:12:17 UTC)
      and created "URGENT: nurture lead replied – call back now" for Hari K, due 06:13 UTC.
- [x] **Opt-out:** DND set with our real `set_ghl_dnd()`; a second email reply reached GHL (06:21:18 UTC) and the run
      ended at the DND check, no task.
- [x] **Booking exit:** a booking on Josh's Calendar (06:33:58 UTC) removed the contact from the test copy; no
      nurture email or SMS afterwards.
- SMS replies were not tested: the test phone is not a US number. Email replies use the same trigger.
- [ ] Screenshots: both workflows, "NDL – Booked, stop follow-up", the two-round conversation, the urgent task, the
      DND-ended run, the booking removal.

### Not built
- **Deal Drop injection** (3–5 matching off-market deals in the nurture messages): Deal Drop is not live yet.

## 7. Known gaps

- **Day 0 "if unanswered":** the Runbook sends the Day 0 callback, text and email only if the first call is not
  answered. GHL cannot see whether Josh's call connected, so they go out every time. A fix (a "call answered" workflow
  adding a tag that the Day 0 steps check) is described but not built.
- **Booking before enrolment:** a lead who booked before its tag was added is not seen as booked.
- **Calls outside GHL and BatchDialer** (a personal phone) are not recorded.
- **Non-US phone numbers:** `phone_utils.normalize` returns nothing for them, so such a lead's calls are not stored.
  All lead phones are US today.
- **Connected-call duration:** not yet seen on a live GHL call (the only live call failed to connect); the reader
  handles `meta.call.duration` and `meta.callDuration`.
- **Before publishing:** Josh approves the messages; Josh becomes a user in the sub-account and the task assignee is
  switched to him; SMS stays off until 10DLC clears.
