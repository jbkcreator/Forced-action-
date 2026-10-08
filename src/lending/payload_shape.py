"""Log the shape of an incoming GoHighLevel webhook body, never its values (WP-GL-10).

The GHL webhook field names in ``booking_webhook`` and ``reply_guard`` follow GHL's public reference and have not
been seen in a real event. With ``LENDING_GHL_LOG_PAYLOAD_SHAPE=true`` each event logs its dotted key paths and the
type of every value, so one real test event shows which names GHL actually sends. Values (phone numbers, names,
message text) are never logged. Turn it off once the names are confirmed.
"""
from __future__ import annotations

import logging
from typing import Any, Iterator, Mapping

from config.settings import get_settings

logger = logging.getLogger(__name__)

MAX_PATHS = 80
MAX_DEPTH = 5


def key_paths(body: Any, prefix: str = "", depth: int = 0) -> Iterator[str]:
    """Dotted paths of a JSON-like body with the type of each leaf, e.g. ``contact.phone:str``. Lists show the
    type of their first element only."""
    if isinstance(body, Mapping) and depth < MAX_DEPTH:
        for key in sorted(body):
            yield from key_paths(body[key], f"{prefix}.{key}" if prefix else str(key), depth + 1)
    elif isinstance(body, list) and body and depth < MAX_DEPTH:
        yield from key_paths(body[0], f"{prefix}[]", depth + 1)
    else:
        yield f"{prefix}:{type(body).__name__}"


def log_shape(source: str, body: Any) -> None:
    """Log the key paths of ``body`` for ``source`` when the setting is on. Never raises."""
    try:
        if not get_settings().lending_ghl_log_payload_shape:
            return
        paths = list(key_paths(body))
        extra = f" (+{len(paths) - MAX_PATHS} more)" if len(paths) > MAX_PATHS else ""
        logger.info("[ghl-payload-shape] %s: %s%s", source, ", ".join(paths[:MAX_PATHS]), extra)
    except Exception as exc:
        logger.warning("[ghl-payload-shape] %s: could not log shape (%s)", source, type(exc).__name__)
