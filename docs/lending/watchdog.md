# NDL external watchdog (R1 0.3)

An independent process (`ndl-watchdog`, `python -m src.lending.watchdog.daemon`) that texts Josh and posts to `LENDING_OPS_CHANNEL` when the lending system looks down. It depends on neither lending-api nor Cora, so it still speaks when they do not. Off until `LENDING_WATCHDOG_ENABLED=true`.

An alert fires after `LENDING_WATCHDOG_FAIL_POLLS` consecutive failed polls (default 2, polled every `LENDING_WATCHDOG_POLL_SECONDS`, default 120), repeats every `LENDING_WATCHDOG_REPEAT_MINUTES` (default 120) while still down, and sends a "recovered" message when the check passes again. State is kept in `LENDING_WATCHDOG_STATE_PATH` (systemd `StateDirectory=ndl-watchdog`), so a restart neither forgets an open alert nor repeats it.

## Checks

| Check (alert key) | Reads | Settings |
|---|---|---|
| `unit:<name>` | `systemctl is-active` for each unit | `LENDING_WATCHDOG_UNITS` (comma-separated; empty = unit check off) |
| `health:lending-api` | `GET` of the lending-api health URL, expects 200 and `status: ok` | `LENDING_WATCHDOG_HEALTH_URL` (default `http://127.0.0.1:8010/health`) |
| `door:*` (ghl-stage, ghl-disposition, lendingflow, instantly, website) | Unauthenticated knock on each public inbound route through nginx; a refusal (401/403/405) means the route is up and guarding itself, nothing is written | `LENDING_WATCHDOG_PUBLIC_BASE_URL` (unset = door checks off); the doors are listed in `config/lending_watchdog.py` |
| `database` | `SELECT 1` through `lending_session`, with a 5 s statement timeout | none |
| `ghl_outcomes` (business hours) | GHL calls imported in the window versus GHL call outcomes received; many calls and zero outcomes = the GHL outcome webhook is not arriving | `LENDING_WATCHDOG_GHL_WINDOW_MINUTES` (240), `LENDING_WATCHDOG_GHL_MIN_CALLS` (5; `0` = off) |
| `cards` (business hours) | Oldest pending Cora approval card | `LENDING_WATCHDOG_CARD_AGE_MINUTES` (120) |
| `replies` (business hours) | Reply handoffs posted and past the reply SLA with no answer | `LENDING_WATCHDOG_REPLY_BACKLOG` (3) |
| `failures` (business hours) | Open webhook failures, or any that gave up in 24 h | `LENDING_WATCHDOG_OPEN_FAILURES` (5) |

Business-hours checks run only Mon-Fri inside the reply-SLA hours. A business-hours query that fails (for example a table that has not been migrated yet) is reported as `database` with the exception class name as the detail.

Recipients: `LENDING_ALARM_SMS_TO` plus optional `LENDING_WATCHDOG_BACKUP_SMS_TO`, normalized and deduplicated. Each text goes through GHL first; if that fails for any reason it falls through to Telnyx (below). If both fail, only the Slack post remains.

## Enabling it

0. **Check the unit list on the server.** Run `systemctl list-units 'fa-lending-*' 'lending-api*' --all` and set `LENDING_WATCHDOG_UNITS` to exactly the units that should be running. A unit listed but not installed alerts as down.
1. Set `LENDING_WATCHDOG_PUBLIC_BASE_URL` to the public lending domain (`https://...`, no trailing path). Confirm the website door through nginx once before relying on it: `GET /api/lending/web-leads` must return 405 from the public domain. If nginx answers differently, the door will alarm falsely.
2. Set `LENDING_WATCHDOG_BACKUP_SMS_TO` if there is a second phone.
3. Set up Telnyx (below).
4. Run `python -m src.lending.watchdog.daemon --once --dry-run` (works while the flag is still off). It logs `[watchdog] N checks, M failing, K alerts` and what it would send, and sends and saves nothing. Any failing check means a setting (unit list, base URL) is wrong.
5. Set `LENDING_WATCHDOG_ENABLED=true` and `systemctl restart ndl-watchdog`. `deploy.sh` installs and restarts the unit (warn-only); it is safe to install while the flag is off.

## Telnyx fallback number

The fallback exists so a GHL outage cannot silence the watchdog.

1. Buy an alerts-only number in the existing Telnyx account.
2. Register it for A2P 10DLC (a campaign whose use case is "internal system alerts to staff").
3. Set `LENDING_WATCHDOG_TELNYX_FROM_NUMBER` to that number (E.164) and, if it is on a different messaging profile, `LENDING_WATCHDOG_TELNYX_MESSAGING_PROFILE_ID` (otherwise `TELNYX_MESSAGING_PROFILE_ID` is used). `TELNYX_SMS_API_KEY` must be set.
4. Never flip `TELNYX_SMS_ENABLED` for this. That switch gates subscriber texting; the watchdog never reads it. The route is on whenever the number, key and profile id are present.

## Done-When verification (production, recorded)

- **A, unit down:** in a quiet window, start a visible clock and run `systemctl stop fa-lending-cora`. Josh gets an SMS and a Slack post within 10 minutes (design: about 4-5 minutes). `systemctl start fa-lending-cora` then produces a "recovered" SMS and post.
- **B, connector disabled:** during business hours set `LENDING_WATCHDOG_GHL_MIN_CALLS=1` and `LENDING_WATCHDOG_GHL_WINDOW_MINUTES=30` and restart the watchdog. Switch off the GHL "call disposition set -> webhook" workflow and make one test GHL call with an outcome. After the next GHL call import (up to 15 minutes) plus two polls, expect a separate `GHL call outcomes DOWN` alert and no Cora alert. Restore the workflow and both settings.

## Before merge (human step)

Ten watchdog tests need a database and skip without one. Run them once against a throwaway Postgres: `TEST_DATABASE_URL=<throwaway, name contains "test"> pytest tests/lending/test_watchdog_*.py`, and confirm none are skipped.

## Known gaps

- **Whole-server loss.** The watchdog runs on the same server, so a dead server silences it. Add an outside pinger on the public lending `/health`.
- **GHL and Telnyx both down.** Only the Slack post goes out.
- **A sender that stops sending while our door works.** The doors prove we accept inbound traffic, not that outbound sends leave; that is item 0.5 reconciliation.
- **No dead-letter queue until 0.5.** A failed webhook is visible through `failures`, but nothing replays it from here.
