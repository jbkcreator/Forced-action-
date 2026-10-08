# systemd unit files — Forced Action

Three long-running services on this server: `fa-api` (FastAPI/Uvicorn), `lifecycle` (LangGraph agents supervisor — retention/FOMO/abandonment/upsells), and `cora` (Cora, the Agent Lane cold-outreach worker — drafts-only, never sends, hands off to Relay).

Note: `cora.service` previously belonged to the pre-PR#177 unit for what is now `lifecycle.service`. `deploy.sh` used to stop+disable any `cora.service` it found on a box as part of that rename cleanup — that step has been removed now that `cora.service` is the current, intentional unit for the cold-outreach worker below (see `docs/cora_lifecycle_naming_audit.md` — Cora the product deliberately keeps its name).

## Install

Copy the unit files to `/etc/systemd/system/` on the server, reload, enable, start:

```bash
sudo cp deploy/systemd/fa-api.service        /etc/systemd/system/
sudo cp deploy/systemd/lifecycle.service     /etc/systemd/system/
sudo cp deploy/systemd/cora.service          /etc/systemd/system/

sudo systemctl daemon-reload

sudo systemctl enable  fa-api lifecycle cora
sudo systemctl start   fa-api lifecycle cora
```

## Lending compliance workers

`deploy.sh` installs, enables and restarts `fa-lending-opt-out-poller` and `fa-lending-dialer-sweep` on every deploy (client requirement: STOP propagation must run before callers dial). It is best-effort: a lending unit that fails to install or start prints a warning and never aborts or rolls back the deploy. `fa-lending-missed-call-poller` is not installed by `deploy.sh`; install it by hand if it is wanted (see below).

- `fa-lending-opt-out-poller` — every 15 s mirrors FA opt-outs (SMS/email) into `lending.suppression_list` and removes the number from the dialer (60 s stop SLA).
- `fa-lending-missed-call-poller` — SUPERSEDED by WP-GL-9: the text-back runs inside `fa-lending-cdr-poller`. Do not install; if it was installed, remove it: `sudo systemctl disable --now fa-lending-missed-call-poller`.
- `fa-lending-reminder-worker` — WP-GL-10: every 10 s sends the due booking confirmation / night-before / 90-minute messages from `lending.booking_messages` (texts via GHL only, after consent, 8am-8pm ET; nothing is sent while `BOOKING_REMINDER_TEXT_ENABLED` is false). Run `migrations/apply_lending_booking_messages.py` first. Install like the others: `sudo cp deploy/systemd/fa-lending-reminder-worker.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now fa-lending-reminder-worker`. It is NOT in `deploy.sh` (unlike the opt-out poller, dialer sweep and lending-api), so a deploy does not restart it: run `sudo systemctl restart fa-lending-reminder-worker` after every deploy that touches `src/lending/`, or it keeps running the old code.
- `fa-lending-dialer-sweep` — every 60 s pulls dialer contacts outside 09:00–19:15 ET / 8–20 local or at 3 attempts per 24 h, and restores them when allowed.

The opt-out poller (per cycle) and the CDR poller (for its lifetime) hold a Postgres advisory lock, so a second copy only skips cycles or exits; the dialer sweep also holds a per-cycle advisory lock, so a second copy just skips cycles. The opt-out poller also writes the GHL do-not-disturb, so restart it whenever `LENDING_GHL_*` changes. Until `BATCHDIALER_API_KEY` and the endpoints in `config/lending_dialer.py` are set, dialer removals are recorded as pending and complete on a later cycle.

The GoHighLevel opt-out sync (poller) and the 15-minute DND backstop (cron) use the **Next Deal Lending sub-account only**: set `LENDING_GHL_API_KEY` and `LENDING_GHL_LOCATION_ID` in the server `.env`. They never fall back to the platform's `GHL_*` account; until both are set, the GHL sync waits and the backstop logs an error and exits.

```bash
# smoke test one cycle each first
PYTHONPATH=. .venv/bin/python -m src.lending.opt_out_poller --once
PYTHONPATH=. .venv/bin/python -m src.lending.dialer_sweep --once

# both are installed and restarted by deploy.sh; the missed-call poller is superseded by the
# text-back in the CDR poller (do not install it)
sudo journalctl -u fa-lending-opt-out-poller -u fa-lending-dialer-sweep -f
```

`fa-lending-cdr-poller` (BatchDialer call log: 15 s fast poll of `/v2/cdrs/last`, 2 min rescan of today and yesterday) is installed the same way: `sudo cp deploy/systemd/fa-lending-cdr-poller.service /etc/systemd/system/`, then `sudo systemctl daemon-reload && sudo systemctl enable --now fa-lending-cdr-poller`. It needs `BATCHDIALER_API_KEY`, `DATABASE_URL` and `LENDING_DIALER_CAMPAIGN_IDS`. Run exactly one copy (a Postgres advisory lock enforces it): the `/last` watermark is server-side per API key, and `--once` advances it too.

## Lending API (lending-api)

Every `/webhooks/lending/*` route is served by `lending-api` (`src/lending/api.py`, gunicorn on `127.0.0.1:8010`), not by
`fa-api`, so a lending deploy or crash never touches the main API. Public webhook URLs do not change: nginx routes the
`/webhooks/lending/` prefix to port 8010 (`deploy/nginx/lending-api.conf.example`, placed above the generic `/webhooks/` block).

```bash
sudo cp deploy/systemd/lending-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now lending-api
curl -s http://127.0.0.1:8010/health           # {"status":"ok"}
# add the nginx location block, then:
sudo nginx -t && sudo systemctl reload nginx
sudo journalctl -u lending-api -f
```

`deploy.sh` installs, enables and restarts `lending-api` with the other lending units, then enforces two hard gates (the deploy fails and rolls back):
`deploy/verify_lending_routing.sh` (before `fa-api` restarts: nginx must have an active `location /webhooks/lending/` proxying to `127.0.0.1:8010`)
and a retrying `curl` of `http://127.0.0.1:8010/health` (after the restart). The nginx block is a one-time manual step, so **add it and reload nginx
before deploying this change**: `fa-api` no longer serves `/webhooks/lending/*`, and a dropped GHL opt-out is a do-not-contact compliance gap.
Rollback: remove the nginx block and deploy the previous `fa-api`.

## Prerequisites the units assume

- `/root/Forced-action-/` — the checked-out repo
- `/root/Forced-action-/.venv/` — Python virtualenv with requirements installed
- `/root/Forced-action-/.env` — the env file
- Both services run as `root` (matches this server's current setup)
- Redis + Postgres running on the same box (or reachable via DATABASE_URL / REDIS_URL)

## Deploying the repo

```bash
cd /root/Forced-action-
git pull
.venv/bin/pip install -r requirements.txt
# Schema changes: run any new migrations/apply_*.py or scripts/apply_*.py directly (Alembic is retired — ADR 0024)
sudo systemctl restart fa-api lifecycle cora
```

## Logs

```bash
# tail everything
sudo journalctl -u fa-api -u lifecycle -u cora -f

# just agents
sudo journalctl -u lifecycle -f

# just cora (cold-outreach worker + reply-mailbox poller + target producer)
sudo journalctl -u cora -f

# last 100 lines of API
sudo journalctl -u fa-api -n 100 --no-pager
```

## Health checks

```bash
systemctl status fa-api
systemctl status lifecycle
systemctl status cora
curl -s http://localhost:8000/docs     # should return 200
python -m src.agents.cora --status     # queue/DLQ depth + kill switch state
```

## Stopping

```bash
sudo systemctl stop fa-api lifecycle cora
```

## Kill-switch shortcut

If you need to halt Lifecycle autonomously without touching the service:

```bash
# Option 1: flip env flag + restart
sudo sed -i 's/^AGENTS_GLOBAL_KILL_SWITCH=.*/AGENTS_GLOBAL_KILL_SWITCH=true/' /root/Forced-action-/.env
sudo systemctl restart lifecycle

# Option 2: just stop the process (API keeps serving)
sudo systemctl stop lifecycle
```
