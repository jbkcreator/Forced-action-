#!/bin/bash
# ============================================================
# Scraper Runner — activates venv, executes a scraper module,
# retries up to 3 times on failure (15 min, 30 min delays).
#
# Usage: run.sh <python_module> [args...]
# Example: run.sh src.scrappers.violations.violation_engine --load-to-db
# ============================================================

PROJECT_DIR="/root/Forced-action-"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="$PROJECT_DIR/logs/cron"
STATUS_DIR="$LOG_DIR/status"
ALERT_EMAIL="root"

MAX_ATTEMPTS=3
RETRY_DELAYS=(0 900 1800)   # seconds before attempt 1, 2, 3 (0 = no pre-delay)
TIMEOUT=3600                # hard kill after 60 min (prevents stale processes)

# Tax delinquency scraper gets extra retries (browser-heavy, weekly run)
if [[ "$1" == *"tax_delinquent_engine"* ]]; then
    MAX_ATTEMPTS=5
    RETRY_DELAYS=(0 900 1800 2700 3600)   # 0, 15m, 30m, 45m, 60m
    TIMEOUT=7200                           # 2 hours for tax scraper
fi

mkdir -p "$LOG_DIR" "$STATUS_DIR"


MODULE="$1"
shift

if [ -z "$MODULE" ]; then
    echo "ERROR: No module specified"
    exit 1
fi

# Parse optional --log-name <name> from remaining args (stripped before passing to Python)
LOG_NAME_OVERRIDE=""
REMAINING_ARGS=()
skip_next=0
for arg in "$@"; do
    if [ "$skip_next" -eq 1 ]; then
        LOG_NAME_OVERRIDE="$arg"
        skip_next=0
    elif [ "$arg" = "--log-name" ]; then
        skip_next=1
    else
        REMAINING_ARGS+=("$arg")
    fi
done
BASE_ARGS="${REMAINING_ARGS[*]}"

LOG_NAME=$(echo "$MODULE" | awk -F. '{print $NF}')
if [ -n "$LOG_NAME_OVERRIDE" ]; then
    LOG_NAME="$LOG_NAME_OVERRIDE"
fi
LOG_FILE="$LOG_DIR/${LOG_NAME}.log"

# Rotate log file if it exceeds 5MB (keep 1 backup)
if [ -f "$LOG_FILE" ] && [ "$(stat -c%s \"$LOG_FILE\" 2>/dev/null || echo 0)" -gt 5242880 ]; then
    mv "$LOG_FILE" "${LOG_FILE}.1"
fi
TODAY=$(date +%Y-%m-%d)
STATUS_FILE="$STATUS_DIR/${LOG_NAME}.status"

# ── Retry loop ────────────────────────────────────────────────
EXIT_CODE=1

for attempt in $(seq 1 $MAX_ATTEMPTS); do

    # ── Per-scraper arg adjustments on retry ──────────────────
    ARGS="$BASE_ARGS"

    if [ $attempt -gt 1 ]; then
        # foreclosure_engine: increase --wait by 10s per retry
        # (gives the browser AI agent more time to complete)
        if [[ "$MODULE" == *"foreclosure_engine"* ]]; then
            WAIT_VAL=$(( 10 * attempt ))
            ARGS=$(echo "$BASE_ARGS" | sed -E 's/--wait [0-9]+//g' | tr -s ' ' | sed 's/^ //;s/ $//')
            ARGS="$ARGS --wait $WAIT_VAL"
        fi

        # lien_engine: never add --all on retry — sequential is safer
        # (--all is not in cron args anyway, just guard against manual override)
        if [[ "$MODULE" == *"lien_engine"* ]]; then
            ARGS=$(echo "$BASE_ARGS" | sed -E 's/--all//g' | tr -s ' ' | sed 's/^ //;s/ $//')
        fi
    fi

    cd "$PROJECT_DIR" || exit 1

    timeout --kill-after=60 $TIMEOUT "$VENV_PYTHON" -m "$MODULE" $ARGS 2>&1 | grep -E " - (WARNING|ERROR|CRITICAL) - " >> "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
    [ $EXIT_CODE -eq 124 ] && echo "WARNING: $LOG_NAME timed out after ${TIMEOUT}s (attempt $attempt) $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

    if [ $EXIT_CODE -eq 0 ]; then
        echo "$TODAY SUCCESS attempt=$attempt ts=$(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"
        exit 0
    fi

    echo "WARNING: $LOG_NAME failed (exit $EXIT_CODE, attempt $attempt/$MAX_ATTEMPTS) $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

    # Sleep before next retry — no sleep after the last attempt
    if [ $attempt -lt $MAX_ATTEMPTS ]; then
        DELAY=${RETRY_DELAYS[$attempt]:-1800}
        sleep $DELAY
    fi

done

# ── All attempts failed ──────────────────────────────────────
echo "ERROR: $LOG_NAME failed after $MAX_ATTEMPTS attempts $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "$TODAY FAILED attempts=$MAX_ATTEMPTS ts=$(date '+%Y-%m-%d %H:%M:%S')" >> "$STATUS_FILE"

# Send alert
ALERT_SUBJECT="[ALERT] Scraper failed: $LOG_NAME ($TODAY)"
ALERT_BODY="Scraper FAILED after $MAX_ATTEMPTS attempts.

Module : $MODULE
Args   : $BASE_ARGS
Date   : $TODAY
Log    : $LOG_FILE

Check the log for details."

if command -v mail &>/dev/null; then
    echo "$ALERT_BODY" | mail -s "$ALERT_SUBJECT" "$ALERT_EMAIL"
else
    echo "ERROR: mail not available — alert not sent for $LOG_NAME" >> "$LOG_FILE"
fi

exit $EXIT_CODE
