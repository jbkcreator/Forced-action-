#!/bin/bash
# Pre-flight for deploy.sh: exit non-zero unless nginx proxies /webhooks/lending/ to
# lending-api (127.0.0.1:8010). fa-api no longer serves that prefix, so a deploy without this
# route drops GHL opt-out webhooks (404 from the generic /webhooks/ block, or 502 if
# lending-api is down). Commented-out blocks do not count; the block must hit port 8010.
set -euo pipefail

if nginx -T 2>/dev/null | awk '
    /^[[:space:]]*location[[:space:]]+\/webhooks\/lending\/[[:space:]]*\{/ { in_block = 1 }
    in_block && /^[[:space:]]*proxy_pass[[:space:]]+http:\/\/127\.0\.0\.1:8010/ { ok = 1 }
    in_block && /\}/ { in_block = 0 }
    END { exit !ok }'; then
    exit 0
fi

echo "nginx has no active 'location /webhooks/lending/' proxying to http://127.0.0.1:8010 (or nginx -T failed)." >&2
echo "Add deploy/nginx/lending-api.conf.example to the server block, then: nginx -t && systemctl reload nginx" >&2
exit 1
