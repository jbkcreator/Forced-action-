# Venture onboarding

How to stand up a second business on this agent fleet by **copying and updating
configuration values** — no new Relay code, no new scraper integrations.

Added by CLONE-v2.2 / CL3.

## What a venture is

A **venture** is one business running on this fleet. It owns:

| It owns | Where it lives | What reads it |
| --- | --- | --- |
| Relay sending identity — Slack approval channel, approvers, Instantly campaign, sender address, send window, daily ceiling, kill-switch key, brand name and postal address | `ventures` row | `src/services/relay/*` via `get_venture_config()` |
| Geography — state, bankruptcy court code, default division | `ventures` row | flood/insurance/storm scrapers (`state`), `bankruptcy_engine` (`court`), both via `get_county_config()` |
| Its counties, and each county's portal sources | `counties.venture_key` → `county_sources` | every scraper and loader |

Venture #1 is `hillsborough_distress`. Before CL3 all of the above were
single-valued env globals in `config/settings.py`, which is what made a second
venture impossible without code changes.

Read at runtime through **`src/utils/venture_config.py:get_venture_config()`**
(5-minute cache), never by querying `ventures` directly. If a venture has no
row, or a nullable column is unset, the resolver falls back to the matching
`config/settings.py` value — so venture #1 behaves exactly as it did before CL3
whether or not the migration has run.

## Onboarding sequence

### 1. Emit and fill a config

```bash
python -m src.services.venture_provisioning --emit-template > venture2.json
```

Every field marked `CHANGE ME` in `config/venture_template.py` needs a real
value. The validator (`validate_venture_config`) reports **all** gaps in one
pass, so run the dry-run early and often rather than fixing them one at a time.

The `counties` array takes one entry per county. `source_url_overrides` maps
`signal_type` → that county's portal URL. Override **every** signal you can:
anything you leave out inherits the template county's URL, which points at a
different county's portal. The dry-run reports what you missed as
`missing_urls`.

### 2. Dry-run, then apply

```bash
python -m src.services.venture_provisioning --config venture2.json --dry-run
python -m src.services.venture_provisioning --config venture2.json --apply
```

`--dry-run` (the default when neither flag is given) validates, executes every
write, prints the report, and rolls back — nothing is persisted. `--apply`
commits. Both are idempotent: `ventures` upserts on `venture_key`, `counties`
and `county_sources` insert with `ON CONFLICT DO NOTHING`, so a run interrupted
halfway is safe to repeat.

The report tells you what happened per county:

```json
{"venture_key": "venture_two",
 "counties": {"newco": {"county_created": true, "cloned": 7, "skipped": 0, "missing_urls": []}}}
```

### 3. Provision Relay's email channel

Once per venture:

```bash
python -m src.services.relay --setup-email-channel --venture venture_two
```

This finds or creates that venture's own Instantly passthrough campaign and
prints the `UPDATE ventures SET relay_instantly_campaign_id = ...` to run.

**Each venture needs its own campaign.** Instantly's duplicate-contact guard is
per-campaign (`docs/adr/0011`), so sharing one would make venture B's *first*
email to a prospect look like a repeat of venture A's and fail the send. Venture
#1 keeps the original unsuffixed campaign name, so re-running this command for
it still finds the live campaign rather than creating a second one.

### 4. Verify

```bash
python -m src.services.relay --health --venture venture_two
```

Confirms the resolved config: kill-switch colour, Slack channel, whether the
email channel is provisioned, and the send window / ceiling / brand actually in
force. Run it for `hillsborough_distress` too — its values must match the env
settings exactly, which is the check that nothing regressed for venture #1.

### 5. Add a sweep cron line

One `--sweep` run covers **one** venture, because the send window, ceiling,
Slack channel and kill-switch key all differ per venture. Add a line to
`scripts/cron/crontab.txt` alongside the existing one:

```cron
*/30 * * * * $PROJECT/scripts/cron/run.sh src.services.relay --sweep --venture venture_two
```

## What is deliberately NOT cloned

**`playwright_code` — never.** Cached Playwright selectors are written against
one specific portal's DOM. Carried to a different county's portal they would
scrape the wrong page *while looking like they worked*. Cloned sources whose
template mode was `playwright_only` or `playwright_then_ai` land on `ai_only`
so the AI path regenerates code against the real portal; approve the result
through the existing
`/api/admin/counties/{id}/sources/{sid}/playwright-code/approve` flow.

**Column mappings — opt-in.** A mapping encodes the portal's actual CSV
headers, so it only transfers when both counties run the same portal vendor
(two Accela counties, say). Pass `--include-column-mappings` when that is true.
Every clone lands `is_approved=false` and must be reviewed in the existing
mapping UI before a loader will use it.

## Deactivating a venture

Set `ventures.is_active = false`. The resolver then falls back to env settings
for that key rather than continuing to honour the row, which stops the venture
governing sends. Its `counties` rows are untouched — deactivate those
separately (`DELETE /api/admin/counties/{id}`) if the scrapers should stop too.

## Assigning an existing county to a venture

Through the admin API, no script needed:

```
PATCH /api/admin/counties/{county_id}   {"venture_key": "venture_two"}
```

An unknown or inactive `venture_key` returns 422 rather than a 500 — the column
is a real foreign key, so the endpoint checks it up front.

## Gotchas

- **`counties.venture_key` and `relay_approval_queue.venture_key` are foreign
  keys.** A typo is rejected at the DB, not silently stored.
- **The daily-ceiling Redis key is `relay_daily_sent:{venture}:{channel}:{date}`.**
  It gained the venture segment in CL3, so the first deploy resets that day's
  counter to zero. One-time and bounded; deploy outside the 11:00–18:00 ET send
  window to avoid even that.
- **A batch is homogeneous.** `execute_batch` applies one resolved
  `VentureConfig` to every item, so `run_sweep` filters the queue to a single
  venture. Never hand it a mixed batch.
- **`ventures.state` is two letters and feeds NWS/FEMA lookups.** A venture
  outside Florida also needs each county's `nws_zone` set, or the
  storm/flood/insurance scrapers have no zone to query.
