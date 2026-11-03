# GHL call-list gate and list scrub (G5, D1, D4)

Josh calls leads from eight GHL smart lists. Two jobs keep those lists inside the same calling rules as BatchDialer.

| Job | Module | Cron | Switch |
| --- | --- | --- | --- |
| Call-list gate | `src/lending/ghl_call_gate.py` | every minute | `LENDING_GHL_CALL_GATE_ENABLED` |
| List DNC scrub | `src/lending/ghl_list_scrub.py` | hourly at :20 | `LENDING_GHL_LIST_SCRUB_ENABLED` |

## How the gate works

Each smart list is one lead-type tag and excludes the tag `ndl-do-not-call-now`:

| Smart list | Lead-type tag |
| --- | --- |
| NDL – Verified Maturity | `ndl-verified-maturity` |
| NDL – Builders | `ndl-builder` |
| NDL – Nurture | `ndl-nurture` |
| NDL – LendingFlow | `lendingflow` |
| NDL – Web Leads | `web-lead` |
| NDL – Wholesalers | `ndl-wholesaler` |
| NDL – Partners | `ndl-partner` |
| NDL – Transaction Ready | `ndl-transaction-ready` |

Every minute the gate reads the listed contacts and adds `ndl-do-not-call-now` to each one that cannot be called now. It removes the tag once calling is allowed again. It writes only where the answer changed. It never adds or removes a lead-type tag, because removing and re-adding `lendingflow` re-fires the Speed to Lead and API Catch workflows.

A contact cannot be called now when:

- the emergency stop is on (`@Cora emergency halt`; an unreadable flag counts as on): every listed contact;
- its number is suppressed (opt-out, STOP, litigator);
- it has had 3 outbound calls in 24 hours, or 6 in 10 business days (GHL and BatchDialer calls count together);
- it is outside 8am-8pm in the recipient's timezone or outside 9:00-19:15 ET;
- for a number that is not a LendingFlow lead: its DNC scrub is missing or older than 7 days, or the scrub found it on a DNC or litigator list.

LendingFlow leads need no scrub: every LendingFlow lead counts as having agreed to calls and texts.

### What the gate cannot do

- It only empties the smart lists. Josh can still dial a blocked number from a contact page or a conversation.
- It does not stop calls a GHL workflow places itself, such as "NDL - LendingFlow Speed to Lead". Pause those workflows by hand during an emergency stop.
- GHL calls reach our database through the GHL call import (every 15 minutes, `LENDING_GHL_CALL_IMPORT_ENABLED`). A call Josh has just made counts toward the attempt limit only after the next import.

### Fail-safe behaviour

- If the rules cannot be read (database error), the run writes nothing.
- A failed tag write is retried by the next run.
- A contact with no usable phone is left as it is.
- A phone with no derivable timezone counts as outside the calling window, so it stays off the lists. The run log counts them (`no_timezone`).
- If no run has succeeded for 10 minutes, one alert goes to `LENDING_OPS_CHANNEL`. The next success clears it.

## How the list scrub works

Every hour the scrub reads the same eight lists. It sends Tracerfy every listed number whose newest scrub is missing or at least 6 days old, except LendingFlow leads and suppressed numbers.

- A new website lead is scrubbed within about an hour of reaching the Web Leads list. Until then the gate keeps it off Josh's list.
- Every listed number is rescrubbed before its 7-day window ends, so no lead drops off a list for an expired scrub.
- A DNC hit is stored in `lending.dnc_scrubs` and gets the nurture flag. A litigator is suppressed. The gate keeps both off the lists.
- Each number costs 1 Tracerfy credit. Each run stops before a batch that would exceed `LENDING_GHL_LIST_SCRUB_MAX_CREDITS` (exit code 4).
- A failed Tracerfy batch spends nothing and is retried next hour.

## Switching on

1. Run `PYTHONPATH=. python migrations/apply_lending_ghl_call_gate_state.py`.
2. In GHL, add `tags not_eq ndl-do-not-call-now` to all eight smart lists.
3. Check that `LENDING_GHL_CALL_IMPORT_ENABLED=true` is set, so GHL calls are counted.
4. Run `PYTHONPATH=. python -m src.lending.ghl_call_gate --dry-run`. It logs, per list, how many contacts would be blocked and why, and writes nothing. Share the counts with the team lead. (The GHL tag search itself was checked against the live sub-account on 2026-11-03.)
5. Run `PYTHONPATH=. python -m src.lending.ghl_list_scrub --dry-run` to see how many credits the first scrub run would use.
6. Set `LENDING_GHL_LIST_SCRUB_ENABLED=true` and let one hourly run scrub the lists.
7. Set `LENDING_GHL_CALL_GATE_ENABLED=true`.
8. Check one test contact: it should leave its smart list when the tag is added, come back when it is removed, and fire no workflow.

## Switching off

1. Set `LENDING_GHL_CALL_GATE_ENABLED=false`.
2. Run `PYTHONPATH=. python -m src.lending.ghl_call_gate --clear`.

Switching off alone leaves every tag in place, so Josh's lists would stay as they were at that moment.
