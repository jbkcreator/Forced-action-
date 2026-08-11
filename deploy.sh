#!/bin/bash
# Single-command prod deploy. Safe to run twice — a no-op pull means no
# migrations re-run and services just restart clean.
# Usage (on the prod server): bash /root/Forced-action-/deploy.sh
set -euo pipefail

PROJECT_DIR="/root/Forced-action-"
VENV="$PROJECT_DIR/.venv/bin"
LAST_GOOD_FILE="$PROJECT_DIR/.last-good-deploy"
LIFECYCLE_UNIT_SRC="$PROJECT_DIR/deploy/systemd/lifecycle.service"
LIFECYCLE_UNIT_DST="/etc/systemd/system/lifecycle.service"
# Cora (Agent Lane cold-outreach worker) — installed as cora.service. This
# filename previously belonged to the pre-PR#177 Lifecycle unit, which this
# script used to actively stop+disable on every deploy (a "retire legacy
# cora unit" step). That retirement step is gone now that cora.service is
# the current, intentional name for the cold-outreach worker — the rename
# migration has already run against prod (lifecycle_* tables confirmed live),
# so the old pre-rename unit this step targeted should not exist anymore.
CORA_UNIT_SRC="$PROJECT_DIR/deploy/systemd/cora.service"
CORA_UNIT_DST="/etc/systemd/system/cora.service"
THROUGHPUT_UNIT_SRC="$PROJECT_DIR/deploy/systemd/cora_throughput.service"
THROUGHPUT_UNIT_DST="/etc/systemd/system/cora_throughput.service"

cd "$PROJECT_DIR"

# Failsafe: export .env into this script's own shell. Standard practice is
# still config/settings.py reading .env itself (works regardless of caller),
# but deploy.sh is a bare SSH shell, not a systemd unit with EnvironmentFile=,
# so anything that reads os.environ directly (in violation of that standard,
# but a mistake that will recur) would otherwise see nothing and fail the
# deploy — see migrations/apply_add_subscriber_is_test.py incident 2026-08-11.
#
# Parsed with python-dotenv (what config/settings.py's env_file loading uses)
# and re-quoted with shlex, NOT plain `source` — several values (e.g.
# RELAY_APPROVERS=["U0..."]) contain quotes bash's own quote-removal would
# strip, corrupting them, and pydantic-settings gives real env vars priority
# over the .env file, so a corrupted export would silently win over the
# correct value config/settings.py would otherwise parse.
if [ -f "$PROJECT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    eval "$("$VENV/python" -c '
import shlex
from dotenv import dotenv_values
for k, v in dotenv_values(".env").items():
    if v is not None:
        print(f"export {k}={shlex.quote(v)}")
')"
    set +a
else
    echo "WARNING: $PROJECT_DIR/.env not found — scripts reading os.environ directly will fail" >&2
fi

# Restarts whichever agent-runtime unit is actually installed on this box —
# "lifecycle" post-rename, "cora" pre-rename — so a rollback that checks out
# a pre-rename commit doesn't fail trying to restart a unit name that was
# never installed under systemd.
restart_agent_service() {
    if systemctl list-unit-files lifecycle.service &>/dev/null; then
        systemctl restart lifecycle
    elif systemctl list-unit-files cora.service &>/dev/null; then
        systemctl restart cora
    else
        echo "restart_agent_service: neither lifecycle.service nor cora.service is installed" >&2
        return 1
    fi
}

# Rolls back to the last commit that completed a full successful deploy
# (tracked in $LAST_GOOD_FILE, not just "whatever was checked out before this
# pull") — so a failure on the fix-forward deploy can't roll back onto the
# still-broken commit from the previous failed run.
rollback() {
    if [ ! -f "$LAST_GOOD_FILE" ]; then
        echo "" >&2
        echo "No last-good commit on record — nothing to roll back to. Manual recovery required." >&2
        return
    fi
    local good_sha
    good_sha=$(cat "$LAST_GOOD_FILE")
    echo "" >&2
    echo "== ROLLING BACK to last known-good commit $good_sha ==" >&2
    git checkout "$good_sha" || { echo "ROLLBACK FAILED: git checkout $good_sha" >&2; return; }
    "$VENV/pip" install -q -r requirements.txt || echo "ROLLBACK WARNING: pip install failed on rollback" >&2
    systemctl restart fa-api || echo "ROLLBACK WARNING: fa-api restart failed" >&2
    restart_agent_service || echo "ROLLBACK WARNING: agent service restart failed" >&2
    # Best-effort: a rollback target predating today's cora.service (cold-outreach
    # worker) content won't have this unit yet, which is expected and not a
    # rollback failure.
    if systemctl list-unit-files cora.service &>/dev/null; then
        systemctl restart cora || echo "ROLLBACK WARNING: cora restart failed" >&2
    fi
    echo "== ROLLBACK COMPLETE — prod running $good_sha ==" >&2
    echo "NOTE: if $good_sha predates the cora->lifecycle DB rename, schema and code are now mismatched — this deploy cannot undo a completed DB rename. Manual DB recovery required." >&2
}

fail() {
    echo "" >&2
    echo "DEPLOY ABORTED — step failed: $1" >&2
    rollback
    exit 1
}

echo "== 1/7 pull dev =="
git checkout dev || fail "git checkout dev"
BEFORE=$(git rev-parse HEAD)
git pull origin dev || fail "git pull origin dev"
AFTER=$(git rev-parse HEAD)

echo "== 2/7 install deps =="
"$VENV/pip" install -q -r requirements.txt || fail "pip install -r requirements.txt"

echo "== 3/7 install/enable lifecycle + cora systemd units =="
# Must succeed BEFORE any DB migration runs: the rename migration below drops
# cora_* schema objects, and if the lifecycle.service unit isn't installed the
# restart in step 5 fails against a database that no longer matches the old
# code/service. Failing here aborts before the DB is touched.
if [ ! -f "$LIFECYCLE_UNIT_SRC" ]; then
    fail "lifecycle.service unit file not found at $LIFECYCLE_UNIT_SRC"
fi
if ! cmp -s "$LIFECYCLE_UNIT_SRC" "$LIFECYCLE_UNIT_DST" 2>/dev/null; then
    cp "$LIFECYCLE_UNIT_SRC" "$LIFECYCLE_UNIT_DST" || fail "install lifecycle.service"
    systemctl daemon-reload || fail "systemctl daemon-reload"
fi
systemctl enable lifecycle || fail "systemctl enable lifecycle"

# Cora cold-outreach worker — separate long-running daemon, own unit file.
if [ ! -f "$CORA_UNIT_SRC" ]; then
    fail "cora.service unit file not found at $CORA_UNIT_SRC"
fi
if ! cmp -s "$CORA_UNIT_SRC" "$CORA_UNIT_DST" 2>/dev/null; then
    cp "$CORA_UNIT_SRC" "$CORA_UNIT_DST" || fail "install cora.service"
    systemctl daemon-reload || fail "systemctl daemon-reload"
fi
systemctl enable cora || fail "systemctl enable cora"

echo "== 4/7 run pending migrations =="
# ponytail: "pending" = migration files newly added by this pull (ADR 0024 keeps
# no ledger table). Re-running an already-applied script is harmless per the
# ADR's idempotency guarantee, but scoping to new files keeps normal deploys fast.
PENDING=()
if [ "$BEFORE" != "$AFTER" ]; then
    while IFS= read -r f; do
        [ -n "$f" ] && PENDING+=("$f")
    done < <(git diff --name-only --diff-filter=A "$BEFORE" "$AFTER" -- migrations/ | grep -E '^migrations/apply_.*\.py$' | sort)
fi

for script in "${PENDING[@]:-}"; do
    [ -z "$script" ] && continue
    echo "   -> $script"
    PYTHONPATH="$PROJECT_DIR" "$VENV/python" "$script" || fail "migration $script"
done

echo "== 5/7 install cron + restart services =="
bash scripts/cron/install_cron.sh > /dev/null || fail "install_cron.sh"

# Install cora_throughput unit if not already present or changed
if [ ! -f "$THROUGHPUT_UNIT_SRC" ]; then
    fail "cora_throughput.service unit file not found at $THROUGHPUT_UNIT_SRC"
fi
if ! cmp -s "$THROUGHPUT_UNIT_SRC" "$THROUGHPUT_UNIT_DST" 2>/dev/null; then
    cp "$THROUGHPUT_UNIT_SRC" "$THROUGHPUT_UNIT_DST" || fail "install cora_throughput.service"
    systemctl daemon-reload || fail "systemctl daemon-reload (cora_throughput)"
fi
systemctl enable cora_throughput || fail "systemctl enable cora_throughput"

systemctl restart fa-api || fail "systemctl restart fa-api"
systemctl restart lifecycle || fail "systemctl restart lifecycle"
systemctl restart cora || fail "systemctl restart cora"

RESTART_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "== 6/7 verify service health =="
sleep 2
systemctl is-active --quiet lifecycle || fail "lifecycle service not active after restart"
systemctl is-active --quiet cora || fail "cora service not active after restart"
systemctl restart cora_throughput || fail "systemctl restart cora_throughput"
sleep 2
systemctl is-active --quiet cora_throughput || fail "cora_throughput service not active after restart"

RESTART_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "== 7/7 refresh Prometheus/Alertmanager config (if installed) =="
# Best-effort — only runs on boxes where Prometheus is actually deployed.
# Metric names in src/api/metrics_router.py changed cora_* -> lifecycle_*, so
# stale on-disk rules would keep evaluating against removed metric names.
if [ -d /etc/prometheus ]; then
    mkdir -p /etc/prometheus/rules
    cp deploy/prometheus/prometheus.yml /etc/prometheus/prometheus.yml || fail "copy prometheus.yml"
    cp deploy/prometheus/alert_rules.yml /etc/prometheus/rules/alert_rules.yml || fail "copy alert_rules.yml"
    if command -v promtool &>/dev/null; then
        promtool check config /etc/prometheus/prometheus.yml || fail "promtool check config"
        promtool check rules /etc/prometheus/rules/alert_rules.yml || fail "promtool check rules"
    fi
    systemctl reload prometheus || echo "WARNING: prometheus reload failed — config updated on disk but service needs manual reload" >&2
fi
if [ -d /etc/alertmanager ]; then
    # Render, don't copy: alertmanager.yml carries placeholder tokens for the
    # webhook auth secret and the Slack incoming-webhook URL. A plain cp would
    # overwrite the live config with those git-committed placeholders on every
    # deploy, silently downgrading webhook auth and killing Slack delivery.
    AM_WEBHOOK_SECRET=$(grep -E '^PROMETHEUS_ALERT_WEBHOOK_SECRET=' "$PROJECT_DIR/.env" | cut -d'=' -f2- | tr -d '"' | tr -d "'" || true)
    AM_SLACK_WEBHOOK_URL=$(grep -E '^ALERTMANAGER_SLACK_WEBHOOK_URL=' "$PROJECT_DIR/.env" | cut -d'=' -f2- | tr -d '"' | tr -d "'" || true)
    if [ -z "$AM_WEBHOOK_SECRET" ] || [ -z "$AM_SLACK_WEBHOOK_URL" ]; then
        echo "WARNING: alertmanager secrets missing from .env — skipping alertmanager config render" >&2
    else
        sed -e "s|prom-wh-s3cr3t-fa-stage10|${AM_WEBHOOK_SECRET}|g" \
            -e "s|https://hooks.slack.com/services/REPLACE_ME|${AM_SLACK_WEBHOOK_URL}|g" \
            deploy/prometheus/alertmanager.yml > /etc/alertmanager/alertmanager.yml \
            || echo "WARNING: render alertmanager.yml failed" >&2
        systemctl reload alertmanager || echo "WARNING: alertmanager reload failed — config updated on disk but service needs manual reload" >&2
    fi
fi

echo "$AFTER" > "$LAST_GOOD_FILE"

echo ""
echo "================ DEPLOY RECEIPT ================"
echo "Commit:       $AFTER"
echo "Restarted:    $RESTART_TS"
echo "Migrations applied this run:"
if [ "${#PENDING[@]}" -eq 0 ]; then
    echo "  (none)"
else
    printf '  %s\n' "${PENDING[@]}"
fi
echo "Live crontab:"
crontab -l 2>/dev/null || echo "  (no crontab installed)"
echo "=================================================="
