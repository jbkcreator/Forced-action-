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
