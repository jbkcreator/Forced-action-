"""
Knowledge base loader for Concierge Chat.

The chat is grounded in a single Markdown file (config/knowledge/forced_action.md).
The file is read once on first access and cached for the process lifetime.
Set CHAT_KNOWLEDGE_PATH to override the default path.
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# FAQ shortcuts — checked before cache + Claude. Each entry: (compiled regex, reply).
# Add high-traffic Qs here once observed in chat_messages logs.
_FAQ: list[tuple[re.Pattern, str]] = [
    (
        re.compile(r"\b(price|pricing|cost|how much)\b", re.I),
        "Pricing depends on the plan and number of ZIPs. "
        "See the pricing page on forcedaction.ai, or email support@forcedaction.ai "
        "and we'll send current rates.",
    ),
    (
        re.compile(r"\b(support|contact|email|help desk)\b", re.I),
        "You can reach support at support@forcedaction.ai. "
        "Include your account email and a short description and we'll get back to you.",
    ),
    (
        re.compile(r"\b(refund|cancel|cancellation)\b", re.I),
        "For refunds or cancellation, email support@forcedaction.ai with your "
        "account email and we'll handle it.",
    ),
]


def faq_lookup(user_text: str) -> Optional[str]:
    """Return a canned FAQ reply if the user message matches a shortcut, else None."""
    if not user_text:
        return None
    for pattern, reply in _FAQ:
        if pattern.search(user_text):
            return reply
    return None

_DEFAULT_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "config" / "knowledge" / "forced_action.md"
)

_cache: Optional[str] = None
_loaded: bool = False


def get_knowledge() -> Optional[str]:
    """Return the cached knowledge text, or None if unavailable."""
    global _cache, _loaded
    if _loaded:
        return _cache

    path = Path(os.environ.get("CHAT_KNOWLEDGE_PATH", str(_DEFAULT_PATH)))
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            logger.warning("chat_knowledge: file is empty at %s", path)
            _cache = None
        else:
            logger.info("chat_knowledge: loaded %d chars from %s", len(text), path)
            _cache = text
    except FileNotFoundError:
        logger.warning("chat_knowledge: file not found at %s", path)
        _cache = None
    except Exception as exc:
        logger.warning("chat_knowledge: failed to read %s: %s", path, exc)
        _cache = None

    _loaded = True
    return _cache


def reset_cache() -> None:
    """Test hook — force re-read on next get_knowledge() call."""
    global _cache, _loaded
    _cache = None
    _loaded = False
