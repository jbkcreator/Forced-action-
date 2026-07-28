# systemd unit files — Forced Action

Two long-running services on this server: `fa-api` (FastAPI/Uvicorn) and `lifecycle` (LangGraph agents supervisor).

## Install

Copy the unit files to `/etc/systemd/system/` on the server, reload, enable, start:

```bash
sudo cp deploy/systemd/fa-api.service  /etc/systemd/system/
sudo cp deploy/systemd/lifecycle.service    /etc/systemd/system/

sudo systemctl daemon-reload

sudo systemctl enable  fa-api lifecycle
sudo systemctl start   fa-api lifecycle
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
sudo systemctl restart fa-api lifecycle
```

## Logs

```bash
# tail everything
sudo journalctl -u fa-api -u lifecycle -f

# just agents
sudo journalctl -u lifecycle -f

# last 100 lines of API
sudo journalctl -u fa-api -n 100 --no-pager
```

## Health checks

```bash
systemctl status fa-api
systemctl status lifecycle
curl -s http://localhost:8000/docs     # should return 200
```

## Stopping

```bash
sudo systemctl stop fa-api lifecycle
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
