"""Cora inside lending-api: one session per worker process, fed by the Slack HTTP endpoints.

Built lazily on the first Slack request a worker receives, then reused. A failed build is
remembered so a busy channel does not trigger a Slack ``auth.test`` on every message: for five
minutes when settings are missing (nothing will change until someone sets them), but only for a
few seconds when Slack could not be reached, so a network blip does not silence Cora for long.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from src.lending.cora_agent import CoraServices, build_config, build_live_services, missing_settings

logger = logging.getLogger(__name__)

RETRY_UNCONFIGURED_SECONDS = 300
RETRY_UNREACHABLE_SECONDS = 15

_lock = threading.Lock()
_services: Optional[CoraServices] = None
_unavailable_until = 0.0


def cora_services() -> Optional[CoraServices]:
    global _services, _unavailable_until
    if _services is not None:
        return _services
    with _lock:
        if _services is not None:
            return _services
        if time.monotonic() < _unavailable_until:
            return None
        from config.settings import get_settings

        settings = get_settings()
        _services = build_live_services(settings)
        if _services is None:
            unconfigured = bool(missing_settings(build_config(settings)))
            _unavailable_until = time.monotonic() + (
                RETRY_UNCONFIGURED_SECONDS if unconfigured else RETRY_UNREACHABLE_SECONDS)
        return _services


def reset() -> None:
    """Forget the cached session (tests, and after a settings change in a long-lived process)."""
    global _services, _unavailable_until
    with _lock:
        _services, _unavailable_until = None, 0.0


def handle_slack_request(request_type: str, payload: dict[str, Any], envelope_id: str = "") -> None:
    """Background task for a Slack event or click. Never raises: Slack already has its 200."""
    try:
        services = cora_services()
        if services is None:
            logger.warning("[cora] Slack %s ignored: Cora is not configured", request_type)
            return
        services.session.handle_request(request_type, payload, envelope_id)
    except Exception as exc:
        logger.error("[cora] Slack %s handling crashed (%s)", request_type, type(exc).__name__)
