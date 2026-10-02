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

## Lending compliance workers (manual install — not in deploy.sh)

Two lending loops, installed by hand so a merge to `dev` never starts them unannounced:

- `fa-lending-opt-out-poller` — every 15 s mirrors FA opt-outs (SMS/email) into `lending.suppression_list` and removes the number from the dialer (60 s stop SLA).
- `fa-lending-missed-call-poller` — every 15 s reads BatchDialer call records and decides the missed-call text for each new no-answer. Sends only when `MISSED_CALL_TEXT_ENABLED=true`, through the consent-gated SMS path; otherwise logs `dry_run`.
- `fa-lending-dialer-sweep` — every 60 s pulls dialer contacts outside 09:00–19:15 ET / 8–20 local or at 3 attempts per 24 h, and restores them when allowed.

Both hold a Postgres advisory lock, so a second copy only skips cycles. Until `BATCHDIALER_API_KEY` and the endpoints in `config/lending_dialer.py` are set, dialer removals are recorded as pending and complete on a later cycle.

```bash
# smoke test one cycle each first
PYTHONPATH=. .venv/bin/python -m src.lending.opt_out_poller --once
PYTHONPATH=. .venv/bin/python -m src.lending.dialer_sweep --once

sudo cp deploy/systemd/fa-lending-opt-out-poller.service deploy/systemd/fa-lending-dialer-sweep.service deploy/systemd/fa-lending-missed-call-poller.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fa-lending-opt-out-poller fa-lending-dialer-sweep fa-lending-missed-call-poller
sudo journalctl -u fa-lending-opt-out-poller -u fa-lending-dialer-sweep -u fa-lending-missed-call-poller -f
```

`fa-lending-cdr-poller` (BatchDialer call log: 15 s fast poll of `/v2/cdrs/last`, 2 min rescan of today and yesterday) is installed the same way: `sudo cp deploy/systemd/fa-lending-cdr-poller.service /etc/systemd/system/`, then `sudo systemctl daemon-reload && sudo systemctl enable --now fa-lending-cdr-poller`. It needs `BATCHDIALER_API_KEY`, `DATABASE_URL` and `LENDING_DIALER_CAMPAIGN_IDS`. Run exactly one copy (a Postgres advisory lock enforces it): the `/last` watermark is server-side per API key, and `--once` advances it too.

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
