# Speed-to-Lead (P2.1)

Every new website or LendingFlow lead gets an instant text (with consent), then Josh's phone rings and connects him to
the lead; if he misses it, David's rings; if nobody has connected at 2 minutes, a Voice AI calls the lead (consented
leads only, and it says it is an AI assistant for Next Deal Lending).

## Who does what

Our code decides, GoHighLevel texts and dials, the same split as the LendingFlow intro text (`intro-sms.md`). GHL
checks no consent, suppression or hours before a workflow step, so every gate runs in `src/lending/speed_to_lead.py`
and a workflow can only start through a tag our code adds. Calls come from the GHL number, the same one BatchDialer
shows.

| Time | What happens | Who |
|---|---|---|
| 0 s | Lead reaches GHL (LendingFlow webhook / website form) | our code |
| ~1-5 s | Intro text: LendingFlow `lendingflow-intro-ready` (T-09), website `web-intro-ready` | our code tags, GHL texts |
| ~1-5 s | `ndl-ai-voice-ok` set or cleared, then `ndl-speed-to-lead` added | our code |
| < 60 s | Josh's phone rings (whisper, press a key), then the lead is connected | GHL "Call" action |
| missed | David's phone rings, then the lead is connected | GHL "Call" action |
| 2 min | Nobody connected and the contact has `ndl-ai-voice-ok`: Voice AI calls the lead | GHL "Voice AI Outbound Call" |

## Targets

Each step has the client's **target**, which we aim for and report against. A miss is logged and marked on the timing
report. The numbers are in `config/lending_speed_to_lead.py`.

| Step | Target | When it cannot happen at once |
|---|---|---|
| Text to the lead | 5 s | Held for the lead's 8am-8pm window, sent up to 24 h after the form |
| Ring Josh, then David | 60 s | The lead waits in the **call queue** (below) |
| Voice AI | 2 min after that lead's cascade starts | Runs the same way for a queued lead |

## The call queue

A lead that may be called, but not right now, waits in the queue instead of being skipped:

| Why it waits | `queue_reason` |
|---|---|
| Outside the callers' hours: `LENDING_ALARM_HOURS_START` to `_END` (Eastern, default 09:00-19:15), Mon-Fri | `outside_hours` |
| Reached GHL more than 5 minutes after the form (a GHL or server delay) | `delayed` |
| Another cascade started less than 3 minutes ago | `waiting_turn` |
| A not-now gate reason: the lead's own 8am-8pm window, the attempt cap, a DNC scrub not back yet | the gate's code, e.g. `OUTSIDE_CALL_WINDOW` |
| The emergency halt was on when it arrived | `EMERGENCY_STOP` |

The minute job `python -m src.tasks.lending_speed_to_lead_queue` (cron `* * * * *`, one run at a time) starts **one
cascade at a time**, at least 3 minutes apart: fresh leads (reached GHL inside 5 minutes) first, then the oldest.
Before each release it checks every rule again:

* the call gate: suppression, DNC, reached in BatchDialer, attempt caps, the lead's window;
* the lead is not already handled in GHL;
* a live read of the lead's GHL calls. A call that got through since the lead arrived ends it. Any call to the number
  in the last 10 minutes (GHL or BatchDialer) makes it wait.

What happens next:

* A lasting reason (opted out, DNC, reached, handled, or any reason the code does not know) ends the lead's cascade
  for good.
* A not-now reason keeps it queued and checks it again after 10 minutes, so it never blocks the leads behind it.
* A failed GHL read or a failed GHL tag call puts that lead back for 2 minutes and the queue moves on. A run stops
  after 3 failed reads (a GHL outage).
* The fresh-lead start and the job share one Postgres advisory lock and never wait for it. A fresh lead that finds the
  lock taken waits its turn in the queue.
* The job writes its claim before adding any tag, so a crash never rings a lead twice. A claim left unfinished for 10
  minutes is reported as an interrupted release.

* **72 hours:** a lead still queued 72 hours after it arrived leaves the queue (`expired`). It is listed, with
  interrupted releases, in one post to the lead-alerts channel (`LENDING_OPS_CHANNEL`; resent next minute if the post
  fails; test leads are left out). It stays on the normal call paths: BatchDialer, the alarms and the next 08:55 list,
  counted from when it left the queue.
* **Switched off:** with `LENDING_SPEED_TO_LEAD_ENABLED=false` the job ends every queued lead (`off`), so the alarms
  and the 08:55 list treat them as ordinary leads again.
* **Notices** (lead-alerts channel, once per Eastern day each, in `lending.speed_to_lead_notices`; skipped while
  `LENDING_OPS_CHANNEL` is unset): a morning
  summary at the first minute of the callers' hours when leads are waiting, and a backlog alert when 10 or more are
  waiting or one has waited 30 minutes of calling hours.
* **Other paths on the same leads:**
  * **BatchDialer** keeps loading at once (team decision, Nov 13). A lead BatchDialer has reached is never
    released from the queue.
  * **Uncalled-lead alarms** skip a queued lead. After its release, the 2- and 5-minute alarms count from the
    release instead of the arrival.
  * **The 08:55 list** leaves out queued leads, because the queue rings them.
  * **GHL's 14-day follow-up Day 0 tasks** are set inside GHL and are not changed here.
* **Josh busy:** GHL's Call action rolls to David when Josh does not answer, declines or is busy.
* **Holidays:** the callers' hours follow Mon-Fri only; there is no holiday calendar.
* The emergency halt releases nothing while it is set. Leads queued before it, and leads that arrived during it, are
  released after `@Cora emergency resume`.

## Gates (all in our code)

* **Intro text, website** (`LENDING_INTRO_SMS_ENABLED` + `LENDING_GHL_CF_INTRO_NAME`, the same switch as T-09, and
  `LENDING_SMS_CARRIER_APPROVED=true`: no text before the 10DLC approval is confirmed): the
  text box was ticked, `consent.has_text_consent` (consent row, no opt-out, not do-not-contact), not halted, and
  8am-8pm for the borrower. Outside the window the text is held; the web-lead sweep (every 5 minutes) sends it once the
  window opens, for up to 24 hours. LendingFlow leads keep T-09's own rules.
* **Call cascade** (`LENDING_SPEED_TO_LEAD_ENABLED`): `ghl_call_gate.blocked_reasons` allows the number (suppression,
  DNC result, already reached, attempt caps, the 8am-8pm calling window, emergency halt), the lead is not handled in
  GHL, and nothing has to go first (call queue above). Website leads also need the call-consent box, and get a fresh
  DNC scrub first (1 Tracerfy credit); a scrub not back yet queues the lead. LendingFlow leads need no scrub (client
  decision).
* **Voice AI** (brief section 7: "only on consented leads"): `ndl-ai-voice-ok` only for a website lead who ticked the
  form's separate **AI call consent box** with exactly the approved wording (`config/lending_web.AI_CALL_CONSENT_TEXT`,
  a draft until Josh approves it; UI PR forced-action-ui #10) and is not suppressed. The call box alone is not
  consent to the AI call, and the AI box alone starts no calls (the AI only follows the Josh/David cascade). LendingFlow
  leads have no recorded AI-call consent, so they never get it (open question for Josh). The tag is removed from a
  returning contact that no longer qualifies.
* **Emergency halt**: no tag at all while it is set. `@Cora emergency halt` also removes `ndl-speed-to-lead` and
  `ndl-ai-voice-ok` from every lead whose cascade started in the last 5 minutes, so the workflow skips David's call
  and the Voice AI (steps 4 and 8 below); a phone already ringing at that moment cannot be recalled. The halt alert
  says how many it stopped. **Opt-out**: GHL do-not-disturb is synced within 15 seconds by
  the opt-out poller; the workflow must check DND before each call (below).
* **BatchDialer**: loading is unchanged; there is no hold while the cascade runs or the lead is queued (team
  decision, Nov 13: David works the BatchDialer list). A call Josh or David connects marks the lead handled in GHL, so
  BatchDialer removes or never loads it; a lead BatchDialer reached is not released from the queue.

**Never rung twice:** before any cascade tag is sent, the lead's claim and a "starting" mark (with the start time)
are committed in their own short transaction, apart from the delivery. If the delivery then fails to commit, a
retried delivery sees the lead as already started and skips it; the unfinished start is reported as an interrupted
release after 10 minutes.

Nothing here can fail a lead's delivery: an error is logged by lead id and the lead goes to BatchDialer as usual.

## GHL setup (Next Deal Lending sub-account)

1. **Users**: Josh and David are GHL users with their mobile numbers in their user profile (the Call action rings
   the assigned user's phone).
2. **Custom field** `intro_name` exists (id in `LENDING_GHL_CF_INTRO_NAME`).
3. **Workflow "NDL - Web Intro SMS"**: trigger *Contact Tag added* = `web-intro-ready` → *Send SMS* (approved intro
   wording, greeting `{{contact.intro_name}}`). No wait or time-window step: our code handles the window.
4. **Workflow "NDL - Speed to Lead"**: trigger *Contact Tag added* = `ndl-speed-to-lead`, then:
   1. *If/Else*: DND for calls is on → end.
   2. *Assign to user*: Josh → *Call* (whisper below, "Connect after keypress" on, timeout 25 s).
   3. *If/Else* on the call status: answered → end.
   4. *If/Else*: contact no longer has tag `ndl-speed-to-lead` (removed by the emergency halt) → end.
   5. *Assign to user*: David → *Call* (same whisper and timeout).
   6. *If/Else* on the call status: answered → end.
   7. *Wait* until 2 minutes after the trigger.
   8. *If/Else*: contact has tags `ndl-speed-to-lead` and `ndl-ai-voice-ok`, and DND for calls is off → *Voice AI
      Outbound Call* (agent below).
   9. *Remove tag* `ndl-speed-to-lead` (so a later inquiry starts it again; our code also removes and re-adds it).
5. **Voice AI agent "NDL Speed to Lead"**: outbound calling enabled for the location (HighLevel may require its
   business verification first). Its first line discloses it is an AI (below), and it follows the hard rules in
   `ghl-conversation-ai.md` (no rates, terms or approvals; book a call with Josh; honour stop requests at once).

The step names above follow HighLevel's help pages and are not verified in the sub-account yet; adjust labels to
what the builder shows, keeping the order and the conditions.

**Whisper (draft, for Josh to approve):** "New Next Deal Lending lead, {{contact.first_name}}. Press any key to
connect."

**Voice AI opening line (draft, for Josh to approve):** "Hi, this is an AI assistant calling for Next Deal Lending
about the loan inquiry you just sent us. Is now a good time?"

## Switching on

1. `PYTHONPATH=. python migrations/apply_lending_speed_to_lead.py`, then deploy.
2. Build the GHL workflows and the Voice AI agent above, and test them on a test contact.
3. `.env`: `LENDING_SMS_CARRIER_APPROVED=true` (10DLC approved), `LENDING_INTRO_SMS_ENABLED=true`,
   `LENDING_GHL_CF_INTRO_NAME=<id>`, `LENDING_SPEED_TO_LEAD_ENABLED=true`;
   restart `lending-api` and the cron-driven sweeps and the queue job pick it up on their next run.
4. Cron: the minute line `src.tasks.lending_speed_to_lead_queue` (in `scripts/cron/crontab.txt`).

## Timings for Josh

```bash
PYTHONPATH=. python -m src.lending.speed_to_lead --report web:123        # one lead
PYTHONPATH=. python -m src.lending.speed_to_lead --recent 10             # the last 10 leads
```

Each line shows when the lead arrived, the intro-text and call-cascade tag times (seconds after arrival) or why they
were withheld, when it was queued, why, and when it left the queue, whether Voice AI was allowed, and the GHL calls on that contact in the next 15 minutes, read live
from GHL (read-only; the stored GHL call import is used only if that read fails). No phone number is printed.

## Open questions for Josh

* The callers' hours (built: Mon-Fri 9:00-7:15 ET, the alarm hours), the 3-minute spacing, the 5-minute "fresh"
  limit, the 72-hour queue limit and the backlog alert (10 leads / 30 minutes): confirm or change.
* Holidays: should the queue skip any days besides weekends?
* Does LendingFlow's consent cover automated (AI) calls? Until then LendingFlow leads never get Voice AI.
* Approve the whisper and the Voice AI opening line above.
