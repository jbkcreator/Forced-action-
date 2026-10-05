# Unconfirmed dialer creates: runbook

## What it is
A dialer create (`POST /contacts`) that times out, returns a 5xx, or returns a 2xx with no contact
id may still have created the contact. We then hold no `dialer_contact_id` for it, so an opt-out
could not delete it. Each such failure is written to `lending.dialer_unconfirmed_creates`.

## What the system does meanwhile
- The create is never auto-retried (a retry after a server-side success would make a second,
  untracked contact).
- An opt-out for that phone deletes every contact we do hold an id for, then raises and stays
  **pending** (error logged every poll) instead of being marked complete.
- `lending_unconfirmed_create_alert` posts the open rows to #dial-tasks hourly.

## What to do for each open row
1. Find the phone's contacts in the dialer (search by phone and by `vendorcontactid` = the row's
   `source_record_ref`).
2. If a contact exists: delete it, or, if the person has not opted out, add its id to
   `lending.dialer_load_records` so it is tracked.
3. Close the row:

```sql
UPDATE lending.dialer_unconfirmed_creates
SET resolved_at = now(), resolution_note = '<what you found / did>'
WHERE phone = '<E.164>' AND resolved_at IS NULL;
```

The pending opt-out then completes on the next poll.

## Known limits
A hard kill (SIGKILL / out of memory) mid-load can still leave untracked contacts with no row
here; that needs a reconcile against the dialer and is tracked separately.
