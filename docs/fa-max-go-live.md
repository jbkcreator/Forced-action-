# FA Max WP-T2-1 go-live runbook

How to move the FA Max lending outreach lane (`venture_key=fa_max_lending`)
from the credential-free development contract to real live sending. This
runbook is the operational procedure the code's own go-live gates (see
`config/settings.py`'s `fa_max_relay_send_mode`, `fa_max_10dlc_registered`,
`fa_max_send_backlog_release_confirmed`) enforce but cannot themselves carry
out — flipping a flag is a human decision, not something automatable.

Added by WP-T2-1's go-live review (2026-09).

## Before you start: run the health check and the readiness gate

```bash
python -m src.services.relay --health --venture fa_max_lending
python -m src.services.relay --go-live-readiness --venture fa_max_lending
```

**These are two different commands answering two different questions**
(code-review finding, 2026-09 — an earlier version of this runbook
conflated them). `--health` is a pure scaffolding diagnostic (DB/table/
kill-switch/Slack reachable) — its exit code is **not** a go-live signal
and stays 0 regardless of gate state, including after a correct, intentional
go-live. `--go-live-readiness` is the actual scriptable go/no-go gate: it
exits 1 if an automatable pre-launch requirement isn't met while the send
gates are still closed, and exits 0 once gates are already open (nothing
further to check) or once every automatable requirement passes.

Read every line under `--health`'s "WP-T2-1 go-live checks:" carefully
regardless — `--go-live-readiness` only checks the subset that's
automatable. Each check reports one of three states, and the difference
matters:

- **verified** — this repo directly confirmed the fact (e.g. a real DNS
  query found the record).
- **configured** — a setting is present, but this repo cannot
  independently prove it's *correct* — treat as "needs a human to confirm."
- **missing / unknown / not_configured** — nothing there, or not
  checkable by this repo at all (DKIM — see below).

**A `configured` or `unknown` state is not a `verified` one.** Do not treat
"the health check didn't fail loudly" as proof of readiness for anything
it honestly reports as unverifiable — and do not treat `--go-live-readiness`
passing as covering DKIM or the approved-backlog review (Step 5), neither of
which it checks, since neither is automatable.

## Step 1 — Domain, mailbox, DNS

1. Confirm the dedicated sending domain and mailbox address with the
   client (`SOT.md` clarifications Q7/Q8 — still open as of this writing).
   Set `RELAY_INSTANTLY_SENDER_EMAIL` (or the venture row's
   `relay_instantly_sender_email`) once confirmed.
2. Re-run `--health`. SPF and DMARC are checked directly via live DNS —
   `[OK] verified` means this repo found and parsed a real record.
   `[WARN] missing` means it genuinely isn't there yet; get the sending
   provider (Instantly) to issue the exact records to add, then re-check.
3. **DKIM cannot be verified by this repo** — its selector is assigned by
   Instantly, not discoverable from the domain name alone. Verify DKIM
   status directly in Instantly's dashboard (Campaigns → Settings → Domain
   Authentication) or via the selector Instantly's own setup instructions
   specify. Record here, by hand, once confirmed:
   - [ ] DKIM verified in Instantly's dashboard on: `______` by: `______`

## Step 2 — Warmup

1. Confirm the target steady-state send volume with the client (SOT.md
   Q11 — still open).
2. Run Instantly's warmup ramp per their documented schedule. There is no
   code gate for warmup completion — this is entirely provider-side.
3. `--health`'s "monitor + drain worker cron registration" check confirms
   `fa_max_send_health_monitor` (daily proxy warmup/failure-rate check)
   and `fa_max_exceptions_alert_drain` (durable alert retry, */5 min) are
   both registered in `scripts/cron/crontab.txt`. It does **not** confirm
   they're actually executing on the live cron daemon — separately confirm
   the crontab is actually installed on the box that's supposed to run it.

## Step 3 — 10DLC (SMS only)

1. Complete Telnyx brand and campaign registration.
2. Confirm registration status directly with Telnyx — there is no
   automated check for this in the current codebase (a genuine gap; the
   `fa_max_10dlc_registered` flag is a manual attestation, not a verified
   API status check).
3. Only once genuinely confirmed:
   ```bash
   # .env or the deployment's secret store
   FA_MAX_10DLC_REGISTERED=true
   ```
4. No SMS dispatches before this flag is true, regardless of send mode —
   enforced in `src/services/relay/guards.py`.

## Step 4 — EXCEPTIONS alerting

1. Confirm `FA_MAX_SLACK_CHANNEL_EXCEPTIONS` is set and the Slack bot has
   been invited to that channel. `--health` checks the setting is present;
   it cannot confirm the bot is actually a channel member — check that by
   hand (post a test message).
2. Confirm the durable alert queue migration has run:
   ```bash
   PYTHONPATH=. python migrations/apply_fa_max_exceptions_alert_queue.py
   ```
   (idempotent — safe to run again if unsure).

## Step 5 — Review the approved backlog, THEN flip send mode

WP-T2-3 precondition: run `PYTHONPATH=. python migrations/apply_fa_max_wp_t2_3.py`
after the WP-T2-2 migrations. Import the latest verified active-campaign CSV
with `PYTHONPATH=. python scripts/import_backflip_suppression_csv.py --file
<export.csv>`. Check the import count and run the health monitor; an absent or
stale feed blocks sends and raises an EXCEPTIONS alert.

Import the operator-verified person-to-contact map before sending:
`PYTHONPATH=. python scripts/import_fa_max_contact_identifiers_csv.py --file
<contacts.csv>`. The CSV needs `person_id` plus `email` and/or `phone`. A
recipient not linked to that person is blocked at both gates. Importing a
contact identifier already assigned to another person fails for identity
review; do not reassign it automatically.

Review any older `approved` FA Max rows with `channel_split_source IS NULL`
before release. The send gate defers them with
`fa_max_opportunity_link_requires_review`; it does not silently dispatch or
terminally skip them. Verify the person's source and contact identifiers,
then reject and redraft under the new gate, or explicitly reconcile the row.
For a first touch, the canonical `fa_max_persons.source` must be one of
`deed`, `permit`, `distress`, `maturity`, or `partner`; the verified source is
stamped on the Relay row. The existing Tier C admin flow creates its
opportunity from the completed send and claims Forced Action attribution at
that point. Opportunities created before this migration with
`source='relay_outbound'` need source review before they can be used for
another outbound; do not relabel them without confirming the original lane.

The live feed format, Backflip employee roster, and conduit definition are
pending Backflip. Employee and conduit suppression cannot be asserted until
those sources exist.
The contact check considers every operator-verified email and phone linked to
the FA Max person. Before live sends, confirm the Backflip export covers the
identifiers used for active campaign contacts; an identifier missing from
both the export and the verified person map cannot be matched deterministically.

**Do this in order — flipping `fa_max_relay_send_mode` before reviewing
the backlog releases nothing by itself (see `guards.py`'s
`fa_max_send_backlog_release_confirmed` gate), but flipping BOTH flags
without having actually reviewed the backlog dispatches every row still
`approved`, oldest first, up to the daily ceiling, on the very next sweep
tick.**

1. List every FA Max row still in `approved` status:
   ```sql
   SELECT id, recipient, channel, decided_by, decided_at, payload
   FROM relay_approval_queue
   WHERE venture_key = 'fa_max_lending' AND status = 'approved'
   ORDER BY decided_at ASC;
   ```
2. For each row, check its content and `decided_at` date for staleness —
   an approval from weeks before the lane was ready may no longer reflect
   current context.
3. **If any approved item is stale, there is currently no way to remove
   it from the approved queue** — the existing Slack Approve/Reject
   buttons only act on `pending` rows (`relay_queue.record_decision()`'s
   `WHERE status = 'pending'` guard); both are silent no-ops on an
   already-approved row. **Keep `fa_max_send_backlog_release_confirmed`
   false until this gap is closed** (a real removal mechanism is out of
   WP-T2-1's scope by explicit decision — see the code-review thread this
   runbook was written from) or until the stale backlog is empty by some
   other means.
4. Record who ran this review and when — outside the flag itself, e.g. in
   this runbook's revision history or the go-live incident channel. The
   flag is a boolean; it is not an audit trail.
5. Only once the backlog is genuinely clean:
   ```bash
   FA_MAX_RELAY_SEND_MODE=live
   FA_MAX_SEND_BACKLOG_RELEASE_CONFIRMED=true
   ```
6. Re-run both `--health` and `--go-live-readiness` one final time.
   `--health`'s "send gates" line now reads `[WARN] open` — a loud,
   correct confirmation the flags took effect (`--health`'s own exit code
   stays 0; open gates are no longer treated as a scaffolding failure).
   `--go-live-readiness` reports `already_live` and exits 0 — its job is
   done once gates are open, it does not re-litigate readiness for a
   launch that already happened.

## Step 6 — Placement measurement

**Correction (code-review finding, 2026-09):** an earlier version of this
runbook said true placement measurement needs a separate seed-list vendor
(GlockApps, 250ok) and could not be built from inside this repo at all.
That was wrong — Instantly's own API documents a native "Inbox Placement
Analytics" group (`developer.instantly.ai/api-reference/groups/
inbox-placement-analytics`: `GET /inbox-placement-analytics`,
`stats-by-test-id`, `deliverability-insights`, `stats-by-date`) backing a
real product feature (Instantly Help Center: "Inbox Placement — One-time
& Automated Tests") that measures actual Inbox/Spam/Promotions folder
placement percentage, spam-filter score, and blacklist status, and
supports **automated recurring tests**, not just one-off checks.

**Still not built, and two things need confirming before it can be:**

1. **Account entitlement.** Instantly's docs describe a separate "Inbox
   Placement plan," shareable across sub-workspaces — this repo cannot
   confirm the live account has it. Confirm directly in the Instantly
   dashboard (or with Instantly support) before assuming the API is
   reachable.
   - [ ] Inbox Placement plan confirmed active on: `______` by: `______`
2. **Methodology fit against the SOT wording.** Like any seed-list
   approach (including GlockApps/250ok), Instantly's placement test sends
   to a set of seed/test addresses and measures where THOSE land — it
   does not literally inspect every production send's actual inbox
   placement. Whether that satisfies the Done-When line's exact wording
   ("sustained inbox placement above 95% **at target volume**") is a
   product judgment call, not a technical one this repo can resolve —
   confirm with the client whether seed-list-style sampling (from
   Instantly or any other vendor) is the intended measurement method.

`fa_max_send_health_monitor.py`'s daily check remains an honest proxy
(Instantly warmup score + Relay's own failure rate), not a placement
measurement, regardless of which provider ends up supplying real placement
data — do not use it to certify this Done-When line either way. Once (1)
and (2) are confirmed, wiring the Inbox Placement Analytics API into the
daily monitor (as a genuine `placement_pct` metric, not a proxy) is a
well-specified, buildable follow-up.

## Step 7 — Bounce/complaint ingestion (investigated, 2026-09)

Instantly's documented v2 webhook catalog
(https://developer.instantly.ai/guides/webhook-events) has an
`email_bounced` event but no distinct spam-complaint or feedback-loop event
type — confirmed against Instantly's own API specifically, not a gap in
this codebase's polling logic.

**This is narrower than "impossible," and an earlier version of this
runbook overstated it as a permanent blocker** (code-review finding,
2026-09). Complaint feedback loops (FBL) commonly exist independent of the
sending platform entirely — two concrete, uninvestigated paths:

- **Microsoft JMRP** — a per-complaint ARF report sent directly to a
  registered address for Outlook/Hotmail/Live/MSN complaints.
- **Gmail Postmaster Tools** — an aggregate daily spam-rate metric via
  Google's own API, independent of Instantly.

- [ ] Investigate JMRP/Postmaster Tools registration for the FA Max
      sending domain, on: `______` by: `______`

Until investigated, treat complaint auto-suppression as **unimplemented
and undecided** — not as a settled dead end, and not as something to
route around with a heuristic (do not infer complaints from bounce rate,
warmup score, or Relay's own failure rate — those measure different
things and would silently misclassify).

Hard-bounce suppression, currently a ~30-minute poll
(`suppression_sync.py`), could become immediate via that `email_bounced`
webhook — but Instantly's docs state webhooks require the Hypergrowth plan
($97/mo) or higher, which this repo cannot confirm against the live
account. Before building a webhook endpoint (mirroring
`src/api/main.py`'s existing Telnyx `message.finalized` handler):

- [ ] Confirm the live Instantly account's plan includes webhooks, on:
      `______` by: `______`
- [ ] Confirm a webhook can be registered for this account's FA Max
      passthrough campaign specifically (Instantly's webhooks may be
      account-wide or per-campaign — confirm which, since the passthrough
      campaign may be shared with other traffic).

## What `--health` cannot tell you

- Whether the sending provider account (Instantly/Telnyx) itself is in
  good standing, rate-limited, or suspended.
- Whether the Slack bot token is still valid (only whether one is set).
- Whether DKIM is actually configured (provider-side only — see Step 1).
- Whether a cron entry that's registered in `crontab.txt` is actually
  installed on the box that's supposed to run it.
- True inbox placement (Step 6).

Each of these needs a manual confirmation step; this runbook exists so
that confirmation happens deliberately, once, and is written down.
