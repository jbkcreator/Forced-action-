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

## Gates (all in our code)

* **Intro text, website** (`LENDING_INTRO_SMS_ENABLED` + `LENDING_GHL_CF_INTRO_NAME`, the same switch as T-09, and
  `LENDING_SMS_CARRIER_APPROVED=true`: no text before the 10DLC approval is confirmed): the
  text box was ticked, `consent.has_text_consent` (consent row, no opt-out, not do-not-contact), not halted, and
  8am-8pm for the borrower. Outside the window the text is held; the web-lead sweep (every 5 minutes) sends it once the
  window opens, for up to 24 hours. LendingFlow leads keep T-09's own rules.
* **Call cascade** (`LENDING_SPEED_TO_LEAD_ENABLED`): the lead reached GHL at most 100 seconds after it arrived (later: no call, BatchDialer as usual; the intro text is not affected), and
  `ghl_call_gate.blocked_reasons` allows the number now (suppression, DNC result, already reached, attempt caps, the
  8am-8pm calling window, emergency halt). Website leads also need the call-consent box, and get a fresh DNC scrub
  first (1 Tracerfy credit); a failed scrub means no call. LendingFlow leads need no scrub (client decision).
* **Voice AI** (brief section 7: "only on consented leads"): `ndl-ai-voice-ok` only for a website lead whose call box
  was ticked with the exact consent text (`config/lending_web.CALL_CONSENT_TEXT`) and is not suppressed. LendingFlow
  leads have no recorded call consent, so they never get it (open question for Josh). The tag is removed from a
  returning contact that no longer qualifies.
* **Emergency halt**: no tag at all while it is set. `@Cora emergency halt` also removes `ndl-speed-to-lead` and
  `ndl-ai-voice-ok` from every lead whose cascade started in the last 5 minutes, so the workflow skips David's call
  and the Voice AI (steps 4 and 8 below); a phone already ringing at that moment cannot be recalled. The halt alert
  says how many it stopped. **Opt-out**: GHL do-not-disturb is synced within 15 seconds by
  the opt-out poller; the workflow must check DND before each call (below).
* **BatchDialer**: loading is unchanged; there is no hold while the cascade runs (team decision, Nov 13: David works
  the BatchDialer list). A call Josh or David connects marks the lead handled in GHL, so BatchDialer removes or never
  loads it.

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
   restart `lending-api` and the cron-driven sweeps pick it up on their next run.

## Timings for Josh

```bash
PYTHONPATH=. python -m src.lending.speed_to_lead --report web:123        # one lead
PYTHONPATH=. python -m src.lending.speed_to_lead --recent 10             # the last 10 leads
```

Each line shows when the lead arrived, the intro-text and call-cascade tag times (seconds after arrival) or why they
were withheld, whether Voice AI was allowed, and the GHL calls on that contact in the next 15 minutes, read live
from GHL (read-only; the stored GHL call import is used only if that read fails). No phone number is printed.

## Open questions for Josh

* Who should the cascade ring outside his working hours (Mon-Fri 9:00-7:15 ET)? Today it runs whenever the lead's
  8am-8pm calling window is open.
* Does LendingFlow's consent cover automated (AI) calls? Until then LendingFlow leads never get Voice AI.
* Approve the whisper and the Voice AI opening line above.
