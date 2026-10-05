# Disposition Code List (version 2026-10-01): for Akrash

**Status: Approved by the client 2026-09-30.**
Akrash must create these **exactly as spelled** (Phone System → Call Results), because our database matches on these names. Source of truth: `config/lending_dispositions.py`.

## A. Call results: the caller picks one on every connected call

| Code | Meaning | Notes |
|---|---|---|
| `NO_ANSWER` | Rang, not answered | Triggers the missed-call text |
| `LEFT_VOICEMAIL` | Voicemail left | Counts as unanswered for the text (decided) |
| `BAD_NUMBER` | Wrong or disconnected number | |
| `CALL_FAILED` | Carrier or line error | Still counts as an attempt |
| `WRONG_PERSON` | Reached someone who is not the owner or borrower | |
| `NOT_DECISION_MAKER` | Right company, not the decision maker | Gate needs a decision maker |
| `REFERRED` | Gave a referral name | Brief §2.7 captures referral names |
| `DNC_REQUEST` | Asked not to be called | Blocks the contact on all channels. In BatchDialer also set the rule: Add to DNC + Do Not Redial |
| `CONNECTED_NOT_INTERESTED` | Spoke, no need | |
| `CALLBACK_REQUESTED` | Spoke, asked to be called later | Feeds third-seat callbacks |
| `DATA_NURTURE_ONLY` | List 2 or List 4 contact: data and nurture, no booking until December | |
| `GATE_FAILED_NURTURE` | Spoke, failed the 6-field gate | Routes to nurture, never the calendar |
| `BOOKED` | Caller reports a slot booked | Reported only; not counted on nurture-only lists |

Removed from the old list: `CONNECTED`, `QUALIFIED_APPOINTMENT`.

BatchDialer's built-in results are also understood and mapped: No Answer, Busy → `NO_ANSWER`; Answering Machine → `LEFT_VOICEMAIL`; Disconnected Number → `BAD_NUMBER`; Do Not Call → `DNC_REQUEST`; Not Interested → `CONNECTED_NOT_INTERESTED`; Call Back → `CALLBACK_REQUESTED`. Any other name arrives as an "unknown code" and raises a Slack warning.

### BatchDialer result names (as created)

| # | Our code | Result name in BatchDialer | Type / how it maps | Attached to campaigns |
|---|---|---|---|---|
| 1 | `NO_ANSWER` | **No Answer** | Built-in; the name normalises to `NO_ANSWER` (direct match) | Done |
| 2 | `LEFT_VOICEMAIL` | `LEFT_VOICEMAIL` | Custom; exact | Done |
| 3 | `BAD_NUMBER` | `BAD_NUMBER` | Custom; exact | Done |
| 4 | `CALL_FAILED` | `CALL_FAILED` | Custom; exact | Done |
| 5 | `WRONG_PERSON` | `WRONG_PERSON` | Custom; exact | Done |
| 6 | `NOT_DECISION_MAKER` | `NOT_DECISION_MAKER` | Custom; exact | Done |
| 7 | `REFERRED` | `REFERRED` | Custom; exact | Done |
| 8 | `DNC_REQUEST` | **Do Not Call** | Built-in; alias `DO_NOT_CALL` -> `DNC_REQUEST` | Done |
| 9 | `CONNECTED_NOT_INTERESTED` | `CONNECTED_NOT_INTERESTED` | Custom; exact | Done |
| 10 | `CALLBACK_REQUESTED` | **Call Back** | Built-in; alias `CALL_BACK` -> `CALLBACK_REQUESTED` | Done |
| 11 | `DATA_NURTURE_ONLY` | `DATA_NURTURE_ONLY` | Custom; exact | Done |
| 12 | `GATE_FAILED_NURTURE` | `GATE_FAILED_NURTURE` | Custom; exact | Done |
| 13 | `BOOKED` | `BOOKED` | Custom; exact | Done |

All 13 results exist in BatchDialer (group "Lending", created). Only the three built-ins keep their own names (No Answer, Do Not Call, Call Back); the code maps them, so they need no renaming. A test (`test_every_result_created_in_batchdialer_maps_to_its_code`) pins this table. The "Lending" call-results group is attached to every campaign (checked by the client).


## B. Unfunded cause (Brief §2.7)
`contactability`, `timing`, `fit`, `borrower_choice`, `lender_execution`, `our_execution`.
If BatchDialer can carry a second picklist per call, use these values there. Otherwise we set a **provisional** default from the call result and the file owner sets the final value.

## C. BatchDialer settings Akrash configures alongside the codes
- **One line per seat** (single line dialer).
- **Max 3 attempts per number per 24 hours across all campaigns**, and **campaign hours ending 7:15pm ET** (backstop; our database enforces it too).
- Recording disclosure on every connect (Florida is two party), plus a saved screenshot of the setting.
- Disposition mandatory after each call, if BatchDialer supports it.
- Campaigns named exactly: `Verified maturity`, `Transaction ready`, `Builders`.
- Caller ID numbers and seat-to-group assignment (Group A 9am–3pm ET, Group B 1pm–7:15pm ET); send us the BatchDialer agent id of each seat and the campaign ids.

## D. Client decisions
- List approved 2026-09-30, including all 13 codes.
- `WRONG_PERSON`, `NOT_DECISION_MAKER`, and `REFERRED` remain separate (not merged).
