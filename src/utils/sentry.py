"""Sentry error reporting for the errors code chooses to send, nothing else.

Unset ``SENTRY_DSN`` (or a missing ``sentry-sdk``) means nothing is sent and every call is a no-op. Sentry is
initialised without its default integrations, so it does not capture every ERROR log line or uncaught exception
(many lines elsewhere carry borrower data); only ``capture_error`` sends, with a message and tags the caller chose.
Never raises.
"""
from __future__ import annotations

from typing import Mapping, Optional, Sequence

from config.settings import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_ready: Optional[bool] = None


def _sentry_ready() -> bool:
    """Initialise Sentry once per process; False when there is no DSN or the SDK cannot start."""
    global _ready
    if _ready is not None:
        return _ready
    settings = get_settings()
    dsn = settings.sentry_dsn.get_secret_value() if settings.sentry_dsn else ""
    if not dsn:
        _ready = False
        return _ready
    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=dsn,
            environment=settings.sentry_environment,
            default_integrations=False,
            send_default_pii=False,
        )
        _ready = True
    except Exception as exc:  # class only: an SDK error can echo the DSN
        logger.warning("[sentry] not started: %s", type(exc).__name__)
        _ready = False
    return _ready


def capture_error(message: str, *, tags: Mapping[str, str], fingerprint: Sequence[str]) -> bool:
    """Send one error-level event. ``fingerprint`` groups repeats into one Sentry issue. True when handed to Sentry."""
    if not _sentry_ready():
        return False
    try:
        import sentry_sdk

        with sentry_sdk.new_scope() as scope:
            for key, value in tags.items():
                scope.set_tag(key, value)
            scope.fingerprint = list(fingerprint)
            sentry_sdk.capture_message(message, level="error")
        return True
    except Exception as exc:  # class only
        logger.warning("[sentry] event not sent: %s", type(exc).__name__)
        return False


def reset_for_tests() -> None:
    """Forget the initialisation result so a test can change SENTRY_DSN."""
    global _ready
    _ready = None
