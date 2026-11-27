# Contact spine (R1 1.1)

One canonical contact per real person, across GoHighLevel, BatchDialer and Instantly, and one card that shows every
touch with that person once. Cora reads it with the `get_contact_card` tool.

## What a contact is

- A contact is a row in `lending.contact_spine`. Its phones, emails, LLC names, GHL contact ids and BatchDialer contact
  ids are rows in `lending.contact_identifiers`; the source rows it was built from are in `lending.contact_sources`.
- Two records that share any **merge key** are the same contact. Merge keys are:
  - a phone (normalized), unless `lending.contacts.line_type` is `landline` (mobile, VoIP, unknown and unscrubbed
    numbers all merge: most LendingFlow numbers are never scrubbed);
  - an email (lower case);
  - an LLC name, matched exactly after normalization (`"Smith Holdings, LLC"` = `"SMITH HOLDINGS LLC"`), never fuzzy;
  - a GHL contact id;
  - a BatchDialer contact id. On a record with a known landline, the GHL and BatchDialer ids only link (both systems
    can key a contact on the phone itself, so the id may just be the office line).
- A known landline is kept on the card (`merge_key = false`) but never joins two people (an office line, a family
  number). A call or `lending.contacts` row whose only identifier is a landline makes no contact of its own; its touches
still reach every card that has that line. Two people who share one LLC become one contact, as the spec says.
- `lending.contacts` is not changed. It stays the per-phone compliance record (opt-outs, DNC scrub, line type, pins),
  so a merge never moves an opt-out to another number. The new tables join back to it by phone.

## Systems

GoHighLevel, BatchDialer and Instantly, on the client's 27 Nov answer: HubSpot and Salesmsg are Backflip systems and
are excluded completely. Gmail and Hunter have a named slot (`FUTURE_SYSTEMS` in `config/lending_contact_spine.py`)
and no connector. Adding one later means a new `TouchSource` in `src/lending/spine/touches.py` (and, if it holds
identities, a reader in `SOURCE_READERS` in `src/lending/spine/sources.py`); the resolver does not change.

## Sources read

`loan_files` (with `borrower_profiles` and `entities`), `lendingflow_leads`, `web_leads`, `after_hours_conversations`,
`dialer_load_records`, `instantly_replies`, `sunbiz_llc_contacts`, `call_dispositions` (rows with a phone) and
`contacts`. The display name comes from the best-ranked source that has one (a loan file first, see `NAME_RANK`).

## The sync

```bash
python -m src.tasks.lending_contact_spine_sync --dry-run   # counts only; works while the switch is off
python -m src.tasks.lending_contact_spine_sync             # needs LENDING_CONTACT_SPINE_ENABLED=true
```

- Cron every 15 minutes under `flock -n` (never two runs at once), and `run.sh` does not retry it: the next run is the
  retry.
- Each run reads every source, resolves them and writes only what changed. A second run with no new data writes
  nothing.
- **A source that cannot be read blocks the whole run**: nothing is written and the task exits 1 (the log names the
  source). A partial read could miss the link between two records.
- Contact ids are stable. When a new record joins two contacts, the one with the smaller id survives; the other gets
  `merged_into` = the survivor, and its identifiers and source links move. A chain of merges always points at the
  final survivor. Each merge is one row in `lending.contact_spine_merges` (ids only).

### Reading merges

```sql
SELECT m.merged_at, m.survivor_id, m.merged_id FROM lending.contact_spine_merges m ORDER BY m.merged_at DESC LIMIT 20;
```

### A wrong merge (by hand)

A phone (or a GHL/BatchDialer id on it) that was a merge key and is later found to be a landline stops merging from the
next run, but a merge already made through it is not undone: a shared office line seen before its scrub can join two
people. To split a contact: create a new `contact_spine` row, move the wrong source rows
(`contact_sources`) and their identifiers to it, and remove the bad shared identifier. Do it in one transaction, then
run the sync with `--dry-run` and check it proposes no merge back.

## The card

- Cora: `get_contact_card` with a phone, email, name or lead_ref (`lendingflow_lead:12`, `web_lead:3`, `contact:5`,
  `loan_file:8`). Several matches return a short list to pick from.
- CLI: `python -m src.lending.spine.card --query <phone|email|name|lead_ref>` logs the card as JSON.
- The timeline holds local touches (calls, sent missed-call texts, Cora's sent actions, Instantly replies, GHL stage
  entries, routing and loan status events) and live ones (GHL messages and notes for the last 365 days, Instantly
  emails). A touch stored in two places (a text Cora sent that GHL also holds) shows once, keyed on the provider id.
  A GHL call message has the same ref as the row `ghl_call_import` stores (`call:ghl:<message id>`), so it shows once
  (the local row, which has the disposition), and still shows while that import is off. Cora's cold emails and stage
  moves are left to the Instantly email list and `ghl_stage_events`, which hold the same touches.
- When GHL or Instantly cannot be read (or has no account set), the card still shows the local touches and lists the
  system under `unavailable`.
- UNVERIFIED: the GHL notes read (`GET /contacts/{id}/notes`) and the Instantly email list
  (`GET /api/v2/emails?lead=`) follow the public API references, not captured responses. If either fails, the card
  names the system as unavailable.

## Privacy

- Cards show the last 4 digits of a phone, never the full number.
- Timeline lines carry kind, direction, outcome and time, never message text.
- Merge audit rows store ids only.

## Setup

1. `PYTHONPATH=. python migrations/apply_lending_contact_spine.py` (idempotent; creates empty tables).
2. `python -m src.tasks.lending_contact_spine_sync --dry-run` and check the counts and that no source failed.
3. Set `LENDING_CONTACT_SPINE_ENABLED=true`, run the sync once by hand, then restart `fa-lending-cora` so Cora has the
   tool.
