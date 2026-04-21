#!/bin/bash
# ============================================================
# Install or update all scraper cron jobs
# Usage: bash scripts/cron/install_cron.sh
# ============================================================

PROJECT_DIR="/root/Forced-action-"
CRONTAB_FILE="$PROJECT_DIR/scripts/cron/crontab.txt"
RUNNER="$PROJECT_DIR/scripts/cron/run.sh"

# Make runner executable
chmod +x "$RUNNER"

# Strip any previous platform block using BEGIN/END markers, then append fresh copy.
# This prevents duplicate entries on repeated installs regardless of variable expansion.
EXISTING=$(crontab -l 2>/dev/null | \
    sed '/=== BEGIN FORCED-ACTION-PLATFORM ===/,/=== END FORCED-ACTION-PLATFORM ===/d')

# Write merged crontab (existing non-platform jobs + fresh platform block)
(
    echo "$EXISTING"
    echo ""
    cat "$CRONTAB_FILE"
) | crontab -

echo ""
echo "✓ Cron jobs installed. Current crontab:"
echo "--------------------------------------------"
crontab -l
echo "--------------------------------------------"
echo ""
echo "Logs will be written to: $PROJECT_DIR/logs/cron/"
