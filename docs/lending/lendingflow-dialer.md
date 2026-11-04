# LendingFlow leads in BatchDialer

LendingFlow leads still go to GHL first, where Josh calls them. Leads he has not handled are then loaded into the BatchDialer campaign "LendingFlow" for the ISA. A lead is removed from BatchDialer as soon as it is handled in GHL (booked, or a connected call). Every BatchDialer call result is written onto the lead's GHL contact.

Code: `src/lending/lendingflow_dialer.py` (load + removal) and `src/lending/bd_result_to_ghl.py` (results on the GHL contact). Both run from the 5-minute `lending_lendingflow_sweep`. The results are also written right after each call.

## How a lead moves

1. The lead is delivered to GHL, as before.
2. After `LENDING_LENDINGFLOW_DIALER_DELAY_MINUTES` (default **0**: the next 5-minute sweep; set minutes to give Josh a head start in GHL), the sweep re-reads the lead's GHL calls live. If that read fails, the lead is not loaded and is retried later.
3. If Josh already had a connected call with the lead, it is never loaded (`blocked` / `HANDLED_IN_GHL`).
4. Otherwise it goes through `run_dialer_load` alone with `dnc_scrub=False`. **Client decision: LendingFlow leads need no consent certificate and get no Tracerfy DNC scrub.** Still applied: opt-out / suppression, Georgia, Backflip, the attempt caps and the calling window. Loaded LendingFlow numbers are also skipped by the dialer sweep's stale-scrub hold and by the weekly rescrub (`compliance.scrub_exempt_phones`). Its card shows name, loan amount, loan type, state, request date and prior contact.
5. A lead stays `pending` for temporary reasons and is retried after 15 minutes: outside calling hours, attempt cap, halt, a failed GHL read, or a refused load. A lead that arrives at 8:30pm loads the next morning.
6. A lead is `blocked` for good for any of these reasons:
   - opt-out or suppression;
   - Georgia;
   - Backflip conflict;
   - attempt history exhausted;
   - already in another campaign;
   - handled in GHL.
7. One BatchDialer contact per phone: a second lead on the same phone becomes `duplicate`.
8. A booking (`/ghl-appointment` new/confirmed/booked, `/booking-confirmed`) or a connected GHL call removes the lead. A connected call is answered with at least `HANDLED_MIN_TALK_SECONDS` of talk: 60 s, a **placeholder until Josh decides**. Loaded leads' GHL calls are re-read every sweep. Removal deletes the BatchDialer contact and deactivates its load row. It never uses `dialer_holds`, so the dialer sweep cannot put the lead back. A failed removal stays `remove_pending` and is retried every sweep, even with loading switched off.

## Race conditions handled

| Race | How |
|---|---|
| Two sweeps at once | Advisory lock for the whole step; the claim uses `FOR UPDATE SKIP LOCKED` |
| Same phone twice | Duplicate marking before the claim; one lead per `run_dialer_load` call |
| Booked while the lead is being pushed | The loader commits the load row, then re-reads the stamp. The webhook commits the stamp, then looks for load rows. One of them always sees the other and removes the contact |
| A handled lead put back | No `dialer_holds` row; a stamped lead is never claimed again |
| Crash mid-run | A lead left `loading` becomes `loaded` (its load row exists), `unconfirmed` or `pending` on the next run |
| Create timed out | `unconfirmed`, never pushed again until `dialer_unconfirmed_creates` is resolved |
| Opt-out or halt mid-run | Existing `run_dialer_load` rechecks before every push |

**Remaining vendor limit:** BatchDialer confirmed that a delete does not cancel a dial already queued. Set the campaign's Retry Time to 2 h or more, so no next dial is queued minutes after a booking.

## Setup before switching on

1. Run `PYTHONPATH=. python migrations/apply_lending_lendingflow_dialer.py` before deploying. Existing leads become `preexisting` and are never loaded.
2. In BatchDialer, create the campaign **LendingFlow** (exact name), with Retry Time ≥ 2 h and Max Attempts per Record set. Create the card custom fields `loan_type`, `state` and `requested`, next to the existing ones.
3. Add the campaign's ID to `LENDING_DIALER_CAMPAIGN_IDS`. Without it the CDR poller ignores the ISA's LendingFlow calls: no disposition, no scoreboard, no GHL note.
4. Set `LENDING_GHL_CALL_IMPORT_ENABLED=true`. The import keeps Josh's GHL calls in `call_dispositions`; the loader also reads them live.
5. Have Josh approve the hook line (`CAMPAIGN_HOOKS["LendingFlow"]`, a placeholder), the delay, and the "connected" talk time.
6. Set `LENDING_LENDINGFLOW_DIALER_ENABLED=true`.
7. For the results in GHL: create a text custom field "Last BatchDialer call" and put its id in `LENDING_GHL_CF_LAST_BD_CALL`. Then set `LENDING_BD_RESULT_TO_GHL_ENABLED=true`. Tags used: `called-in-batchdialer`, `bd-connected`, `bd-callback`, `bd-booked`. A GHL workflow started by one of these tags runs on every BatchDialer call.
8. For GHL results on the BatchDialer card (the other direction): BatchDialer contacts have no tags, so each GHL call to a phone that is live in BatchDialer is written to the card custom field **`card_ghl_call`** (e.g. "2026-11-04 10:15 ET, no answer, 2 GHL calls"; rewritten when its result code arrives). Add `card_ghl_call` to the BatchDialer contact card, run the migration again (it adds `bd_note_*` to `call_dispositions`), then set `LENDING_GHL_RESULT_TO_BD_ENABLED=true`. Only that field changes: `set_custom_fields` reads the contact and sends every other field back as it was. Test it on one test contact first.

## Things Josh must agree to

- His GHL calls count toward the dialer's attempt limit: 3 in 24 h, 6 in 10 business days. After 3 calls by Josh, the lead waits 24 h before loading. After 6, it is never loaded.
- The LendingFlow leads in the dialer reverse the launch rule "LendingFlow → GHL + Josh only".
- No consent certificate and no DNC / litigator scrub for LendingFlow leads (client decision, Nov 3). The legal basis for auto-dialing these leads is the client's and their counsel's call.

## Checking a lead

```sql
SELECT id, dialer_status, dialer_block_reason, handled_in_ghl_reason, dialer_last_error
  FROM lending.lendingflow_leads WHERE phone = '+1XXXXXXXXXX';
```
