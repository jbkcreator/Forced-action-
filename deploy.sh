#!/bin/bash
# Single-command prod deploy. Safe to run twice — a no-op pull means no
# migrations re-run and services just restart clean.
# Usage (on the prod server): bash /root/Forced-action-/deploy.sh
set -euo pipefail

PROJECT_DIR="/root/Forced-action-"
VENV="$PROJECT_DIR/.venv/bin"

cd "$PROJECT_DIR"

fail() {
    echo "" >&2
    echo "DEPLOY ABORTED — step failed: $1" >&2
    exit 1
}

echo "== 1/4 pull dev =="
git checkout dev || fail "git checkout dev"
BEFORE=$(git rev-parse HEAD)
git pull origin dev || fail "git pull origin dev"
AFTER=$(git rev-parse HEAD)

echo "== 2/4 install deps =="
"$VENV/pip" install -q -r requirements.txt || fail "pip install -r requirements.txt"

echo "== 3/4 run pending migrations =="
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

echo "== 4/4 install cron + restart services =="
bash scripts/cron/install_cron.sh > /dev/null || fail "install_cron.sh"
systemctl restart fa-api || fail "systemctl restart fa-api"
systemctl restart cora || fail "systemctl restart cora"

RESTART_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

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
crontab -l
echo "=================================================="
