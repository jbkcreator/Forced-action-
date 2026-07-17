# systemd unit files — Forced Action

Two long-running services on this server: `fa-api` (FastAPI/Uvicorn) and `cora` (LangGraph agents supervisor).

## Install

Copy the unit files to `/etc/systemd/system/` on the server, reload, enable, start:

```bash
sudo cp deploy/systemd/fa-api.service  /etc/systemd/system/
sudo cp deploy/systemd/cora.service    /etc/systemd/system/

sudo systemctl daemon-reload

sudo systemctl enable  fa-api cora
sudo systemctl start   fa-api cora
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
sudo systemctl restart fa-api cora
```

## Logs

```bash
# tail everything
sudo journalctl -u fa-api -u cora -f

# just agents
sudo journalctl -u cora -f

# last 100 lines of API
sudo journalctl -u fa-api -n 100 --no-pager
```

## Health checks

```bash
systemctl status fa-api
systemctl status cora
curl -s http://localhost:8000/docs     # should return 200
```

## Stopping

```bash
sudo systemctl stop fa-api cora
```

## Kill-switch shortcut

If you need to halt Cora autonomously without touching the service:

```bash
# Option 1: flip env flag + restart
sudo sed -i 's/^AGENTS_GLOBAL_KILL_SWITCH=.*/AGENTS_GLOBAL_KILL_SWITCH=true/' /root/Forced-action-/.env
sudo systemctl restart cora

# Option 2: just stop the process (API keeps serving)
sudo systemctl stop cora
```
