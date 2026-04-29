# systemd unit files — Forced Action

Two long-running services for Stage 1 server deployment.

## Install

Copy the unit files to `/etc/systemd/system/` on the server, reload, enable, start:

```bash
sudo cp deploy/systemd/fa-api.service     /etc/systemd/system/
sudo cp deploy/systemd/fa-agents.service  /etc/systemd/system/

sudo systemctl daemon-reload

sudo systemctl enable  fa-api fa-agents
sudo systemctl start   fa-api fa-agents
```

## Prerequisites the units assume

- `/opt/forced-action/` — the checked-out repo
- `/opt/forced-action/.venv/` — Python virtualenv with requirements installed
- `/etc/forced-action/env` — the env file (mode 0640, owned by root:forcedaction)
- `forcedaction` system user + group
- `/var/log/forced-action/` — created, writable by the user
- Redis + Postgres running on the same box (or reachable via DATABASE_URL / REDIS_URL)

Create user + dirs:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin forcedaction
sudo mkdir -p /opt/forced-action /etc/forced-action /var/log/forced-action
sudo chown -R forcedaction:forcedaction /opt/forced-action /var/log/forced-action
sudo chmod 0750 /etc/forced-action
```

## Deploying the repo

```bash
# First deploy
sudo -u forcedaction git clone <repo-url> /opt/forced-action
cd /opt/forced-action
sudo -u forcedaction python3 -m venv .venv
sudo -u forcedaction .venv/bin/pip install -r requirements.txt

# Subsequent deploys
cd /opt/forced-action
sudo -u forcedaction git pull
sudo -u forcedaction .venv/bin/pip install -r requirements.txt
sudo -u forcedaction .venv/bin/python -m alembic upgrade head
sudo systemctl restart fa-api fa-agents
```

## Logs

```bash
# tail everything
sudo journalctl -u fa-api -u fa-agents -f

# just agents
sudo journalctl -u fa-agents -f

# last 100 lines of API
sudo journalctl -u fa-api -n 100 --no-pager
```

## Health checks

```bash
systemctl status fa-api
systemctl status fa-agents
curl -s http://localhost:8000/         # should respond
```

## Stopping

```bash
sudo systemctl stop fa-api fa-agents
```

## Kill-switch shortcut

If you need to halt Cora autonomously without touching the service:

```bash
# Option 1: flip env flag + restart
sudo sed -i 's/^AGENTS_GLOBAL_KILL_SWITCH=.*/AGENTS_GLOBAL_KILL_SWITCH=true/' /etc/forced-action/env
sudo systemctl restart fa-agents

# Option 2: just stop the process (API keeps serving)
sudo systemctl stop fa-agents
```
