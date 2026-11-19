# Standing Sends Tracker (P1.4)

Agent message types that Josh keeps approving unchanged start sending on their own.

## What is counted

Every agent job carries a **template version** (for example `LF-D2-SMS-v1`): the message type and the version of
its wording. The tracker counts **each template version separately**:

| Count | Meaning |
|---|---|
| Approved | Cards of this type Josh approved (sent, or approved and then stopped by a send check) |
| Edited | Of those, the ones he revised before approving |
| Rejected | Cards he rejected (shown only: they neither help nor block graduation, unless `LENDING_GRADUATION_COUNT_REJECTIONS=true`, below) |
| Sent automatically | Sent after graduation, without a card (not counted as approvals) |

A new wording means a new version (`...-v2`), which starts again from 0.

Cora's own message drafts also carry a template version (brief section 7: every send carries one): `CORA-SMS-v1`
and `CORA-EMAIL-v1` (texts and emails Josh asks Cora for) and `INSTANTLY-REPLY-v1` (answers to Instantly replies,
P1.3). They are counted on the tracker like any other type but **never graduate**: each is a message Josh asked for
or an answer to one person's reply, so it always gets an approval card. Only agent-job types can graduate.

## Graduation

A template version **graduates** the first time it has **25 approvals with under 10% edited** (10% exactly does not
graduate). Cora's minute sweep does it automatically, records the counts in `lending.template_graduations` and posts
a notice in #cora. Rule and numbers: `config/lending_standing_sends.py`.

After graduation, an agent job of that type that is marked `requires_approval: false` is sent without a card. Every
send-time check still runs: consent, the 8am-8pm window, opt-outs and suppression, cold-email caps, the lane and
Backflip check, and the emergency halt. A job marked `requires_approval: true` always gets a card.

## Edits

Every Revise on any approval card is stored in `lending.card_edits`: the message fields before and after (subject,
body, stage, tag, title; never the recipient), a line diff, Josh's instruction, who and when.

## Where to see it

- The 7:30 AM morning card, section "Standing sends": per message type, `18/25 approvals · 6% edited`, and
  whether it is ready, graduated or revoked.
- `PYTHONPATH=. python -m src.lending.standing_sends --list`

## Undo

```bash
PYTHONPATH=. python -m src.lending.standing_sends --revoke LF-D2-SMS-v1   # needs cards again
PYTHONPATH=. python -m src.lending.standing_sends --restore LF-D2-SMS-v1  # lift the revoke
```
A revoked type is never re-graduated automatically. The emergency halt still stops every send.

A revoke also covers messages of that type that were already approved automatically but not sent yet (waiting for
the 8am-8pm window, carrier approval or the emergency halt): they go back to "waiting for approval" and a fresh
approval card is posted, so none is sent without a person approving it. This is checked at three points: the revoke
itself, every Cora sweep before the relay sends anything, and the relay's send-time check. `--restore` does not
re-approve them; they stay on their cards.

## Setup

Run `PYTHONPATH=. python migrations/apply_lending_standing_sends.py` before deploying.

## Rejections count against graduation (off until Josh confirms)

The brief's rule ignores rejections, so a type with 25 approvals and 20 rejections could still graduate. With
`LENDING_GRADUATION_COUNT_REJECTIONS=true` (default false; restart `fa-lending-cora` and the sweep to change it) a type graduates
only when **(edited + rejected) / (approved + rejected) is under 10%**, still after 25 approvals. The per-channel send-time check
uses the same rule on that channel's own cards, and the morning card shows "edited or rejected". This is the team's
recommendation for open question P1.4 #1, not Josh's answer.

Turning the switch on does not revoke a type that already graduated: only the send-time check becomes stricter. A type that graduated
under the old rule but is over the limit once rejections count goes back to approval cards for new jobs (the tracker still shows it as
graduated until it is revoked with `--revoke`), and an action already approved automatically but not yet sent is held until its draft
expires. The rate stored with a graduation (`edit_rate`) is the rate the decision used.

## A graduated message sends only approved wording (off until Josh confirms)

With `LENDING_GRADUATED_APPROVED_WORDING_ONLY=true` (default false; restart `fa-lending-cora` to change) a graduated agent message goes
out without a card only when its wording matches wording Josh approved for the same template version and channel. The comparison
ignores case and spacing and takes out what an agent fills in: the contact's names and places (the words of the job's `contact_label`)
and every run of digits. Any other change of words, or no approved card to compare with (including cards approved before this
change, which carry no `contact_label`), gets a normal approval card, never a block. It looks at the 200 most recent person-approved cards (approved, sending, sent or failed). Digits are ignored, so a changed amount or term does not change the match: that is the point (amounts are filled in), and the reason this stays off until Josh agrees.
This is the team's recommendation for Q19, not Josh's answer.

The wording check also runs when the message is sent, not only when its draft is created: an automatically approved message that was
queued (for example waiting for its send window) before the switch was turned on is re-checked just before it goes out, and one whose
wording is not approved wording is sent back for a person's approval instead of sending: the relay has claimed it, the send check returns
it to pending with no decision, and the card retry posts a fresh approval card on the next sweep.
