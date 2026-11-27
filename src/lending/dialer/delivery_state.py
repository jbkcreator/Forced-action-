"""Per-ET-day delivery state for the R1 1.6 Slack posts, so a cron retry sends only what is still missing.

One small JSON file per post per day under ``data/dialer_posts/`` (gitignored, like ``data/pr_dialer_autoload/``).
An unreadable file counts as no state: the retry then re-sends rather than dropping a post.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3] / "data" / "dialer_posts"


def _path(name: str, day: date) -> Path:
    return ROOT / f"{name}-{day.isoformat()}.json"


def load(name: str, day: date) -> Optional[dict[str, Any]]:
    try:
        return json.loads(_path(name, day).read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        logger.warning("[dialer-posts] %s state for %s unreadable (%s); treated as none", name, day, type(exc).__name__)
        return None


def save(name: str, day: date, state: dict[str, Any]) -> None:
    path = _path(name, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(path)
