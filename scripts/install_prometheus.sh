#!/bin/bash
# ============================================================
# Forced Action — Prometheus + Alertmanager install script
#
# Installs Prometheus 2.51 + Alertmanager 0.27 as systemd services,
# copies the Stage 10 config files, and starts both services.
#
# Must be run as root on the same server that runs the FastAPI app.
#
# Usage:
#   chmod +x scripts/install_prometheus.sh
#   sudo bash scripts/install_prometheus.sh
#
# Re-runnable: safe to run again — will overwrite binaries + configs
# and restart services, but won't break existing data.
# ============================================================

set -euo pipefail

PROJECT_DIR="/root/Forced-action-"
PROM_VERSION="2.51.2"
AM_VERSION="0.27.0"
PROM_USER="prometheus"
PROM_DATA_DIR="/var/lib/prometheus"
PROM_CONFIG_DIR="/etc/prometheus"
AM_CONFIG_DIR="/etc/alertmanager"
AM_DATA_DIR="/var/lib/alertmanager"
ARCH="linux-amd64"

# ── Load PROMETHEUS_ALERT_WEBHOOK_SECRET from .env ────────────────────────────
WEBHOOK_SECRET=""
if [ -f "$PROJECT_DIR/.env" ]; then
    WEBHOOK_SECRET=$(grep -E '^PROMETHEUS_ALERT_WEBHOOK_SECRET=' "$PROJECT_DIR/.env" | cut -d'=' -f2- | tr -d '"' | tr -d "'")
fi
if [ -z "$WEBHOOK_SECRET" ]; then
    echo "ERROR: PROMETHEUS_ALERT_WEBHOOK_SECRET not found in $PROJECT_DIR/.env"
    exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Forced Action — Prometheus + Alertmanager setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Prometheus  : v${PROM_VERSION}"
echo " Alertmanager: v${AM_VERSION}"
echo " Config dir  : ${PROM_CONFIG_DIR}"
echo " Data dir    : ${PROM_DATA_DIR}"
echo ""

# ── 1. Create service user ─────────────────────────────────────────────────────
echo "[1/9] Creating prometheus system user..."
id "$PROM_USER" &>/dev/null || \
    useradd --system --no-create-home --shell /bin/false "$PROM_USER"

# ── 2. Create directories ──────────────────────────────────────────────────────
echo "[2/9] Creating directories..."
mkdir -p "$PROM_DATA_DIR" "$PROM_CONFIG_DIR/rules" "$AM_CONFIG_DIR" "$AM_DATA_DIR"
chown -R "$PROM_USER:$PROM_USER" "$PROM_DATA_DIR" "$PROM_CONFIG_DIR" "$AM_DATA_DIR" "$AM_CONFIG_DIR"

# ── 3. Download + install Prometheus ──────────────────────────────────────────
echo "[3/9] Downloading Prometheus v${PROM_VERSION}..."
PROM_TAR="prometheus-${PROM_VERSION}.${ARCH}.tar.gz"
PROM_URL="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${PROM_TAR}"
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

wget -q --show-progress -O "$TMP_DIR/$PROM_TAR" "$PROM_URL"
tar -xzf "$TMP_DIR/$PROM_TAR" -C "$TMP_DIR"
PROM_DIR="$TMP_DIR/prometheus-${PROM_VERSION}.${ARCH}"

echo "[4/9] Installing Prometheus binaries..."
cp "$PROM_DIR/prometheus" "$PROM_DIR/promtool" /usr/local/bin/
chown "$PROM_USER:$PROM_USER" /usr/local/bin/prometheus /usr/local/bin/promtool

# ── 4. Download + install Alertmanager ────────────────────────────────────────
echo "[4b/9] Downloading Alertmanager v${AM_VERSION}..."
AM_TAR="alertmanager-${AM_VERSION}.${ARCH}.tar.gz"
AM_URL="https://github.com/prometheus/alertmanager/releases/download/v${AM_VERSION}/${AM_TAR}"

wget -q --show-progress -O "$TMP_DIR/$AM_TAR" "$AM_URL"
tar -xzf "$TMP_DIR/$AM_TAR" -C "$TMP_DIR"
AM_DIR="$TMP_DIR/alertmanager-${AM_VERSION}.${ARCH}"

echo "[5/9] Installing Alertmanager binaries..."
cp "$AM_DIR/alertmanager" "$AM_DIR/amtool" /usr/local/bin/
chown "$PROM_USER:$PROM_USER" /usr/local/bin/alertmanager /usr/local/bin/amtool

# ── 5. Copy Prometheus config + alert rules ────────────────────────────────────
echo "[6/9] Writing Prometheus config + alert rules..."
cp "$PROJECT_DIR/deploy/prometheus/prometheus.yml" "$PROM_CONFIG_DIR/prometheus.yml"
cp "$PROJECT_DIR/deploy/prometheus/alert_rules.yml" "$PROM_CONFIG_DIR/rules/alert_rules.yml"
chown -R "$PROM_USER:$PROM_USER" "$PROM_CONFIG_DIR"

# ── 6. Write Alertmanager config (inject webhook secret) ──────────────────────
echo "[7/9] Writing Alertmanager config (with webhook secret)..."
sed "s|prom-wh-s3cr3t-fa-stage10|${WEBHOOK_SECRET}|g" \
    "$PROJECT_DIR/deploy/prometheus/alertmanager.yml" > "$AM_CONFIG_DIR/alertmanager.yml"
chown "$PROM_USER:$PROM_USER" "$AM_CONFIG_DIR/alertmanager.yml"

# ── 7. Install systemd units ───────────────────────────────────────────────────
echo "[8/9] Installing systemd service units..."
cp "$PROJECT_DIR/deploy/systemd/prometheus.service" /etc/systemd/system/prometheus.service
cp "$PROJECT_DIR/deploy/systemd/alertmanager.service" /etc/systemd/system/alertmanager.service
systemctl daemon-reload

# ── 8. Enable + start services ────────────────────────────────────────────────
echo "[9/9] Enabling and starting services..."
systemctl enable prometheus alertmanager
systemctl restart prometheus
sleep 2
systemctl restart alertmanager
sleep 2

# ── 9. Verify ─────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Service status"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
systemctl is-active prometheus  && echo " ✓ prometheus   is active" || echo " ✗ prometheus   FAILED"
systemctl is-active alertmanager && echo " ✓ alertmanager is active" || echo " ✗ alertmanager FAILED"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Validate config"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
promtool check config "$PROM_CONFIG_DIR/prometheus.yml" && echo " ✓ prometheus.yml valid"
amtool check-config "$AM_CONFIG_DIR/alertmanager.yml"   && echo " ✓ alertmanager.yml valid"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Endpoints (confirm from the API server):"
echo "   Prometheus UI   → http://localhost:9090"
echo "   Alertmanager UI → http://localhost:9093"
echo "   /metrics        → http://localhost:8000/metrics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Next: install/update cron jobs:"
echo "  bash scripts/cron/install_cron.sh"
echo ""
