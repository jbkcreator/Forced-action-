"""
Forward pack renderer — weekly Claude-generated share copy per buyer vertical.

render_weekly(db)          — called by Monday 03:00 UTC cron; writes one row
                             per vertical into referral_forward_copy.
get_current_copy(v, db)    — read path; returns current week's cached body
                             or the most recent prior week as fallback.
"""

import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.models import ReferralForwardCopy

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "config" / "prompts" / "referral_forward"
VERTICALS = ["attorneys", "fix_flip", "public_adjusters", "restoration", "roofing", "wholesalers"]

MIN_BODY_LEN = 60
MAX_BODY_LEN = 160
_BANNED_PATTERNS = [
    re.compile(r"^(yo|hey|hi|sup|what's up|whats up|howdy)\b", re.IGNORECASE),
    re.compile(r"#\w+"),                              # hashtags
    re.compile(r"[\U0001F300-\U0001FAFF☀-➿]"),  # emojis / dingbats
    re.compile(r"\b(game[- ]changer|level up|crush it|revolutionary)\b", re.IGNORECASE),
]


def _validate_body(body: str) -> tuple[bool, str]:
    """Return (ok, reason). reason is empty when ok=True."""
    if not body:
        return False, "empty"
    if len(body) < MIN_BODY_LEN:
        return False, f"too_short({len(body)})"
    if len(body) > MAX_BODY_LEN:
        return False, f"too_long({len(body)})"
    if body[-1] not in ".?!":
        return False, f"no_terminal_punctuation(ends={body[-1]!r})"
    # Last token must be a real word — guards against mid-word truncation
    # like "...someone else d".
    last_word = re.split(r"\s+", body.rstrip(".?!"))[-1]
    if len(last_word) < 3:
        return False, f"truncated_last_word({last_word!r})"
    for pat in _BANNED_PATTERNS:
        m = pat.search(body)
        if m:
            return False, f"banned_pattern({m.group(0)!r})"
    if body.startswith(("\"", "'", "“", "‘")):
        return False, "quoted_output"
    return True, ""


def _current_week_start() -> date:
    """Return the ISO Monday for the current week."""
    today = date.today()
    return today - __import__("datetime").timedelta(days=today.weekday())


def _load_prompt(vertical: str) -> tuple[str, str]:
    """Load system + user prompts from the vertical's YAML file."""
    path = PROMPTS_DIR / f"{vertical}.yaml"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("system", ""), data.get("user", "")


def render_weekly(db: Session) -> dict[str, str]:
    """
    Generate and cache one share-copy blurb per vertical for the current ISO week.
    Idempotent: re-running on the same Monday is a no-op (UNIQUE(vertical, week_start)).
    Returns a dict mapping vertical → generated body.
    """
    from src.services.claude_router import call_claude

    week_start = _current_week_start()
    results: dict[str, str] = {}

    for vertical in VERTICALS:
        try:
            system_prompt, user_prompt = _load_prompt(vertical)

            # Up to 2 attempts: first try, then a stricter retry on validation failure.
            body: Optional[str] = None
            reject_reason: str = ""
            for attempt in (1, 2):
                user_msg = user_prompt
                if attempt == 2:
                    user_msg += (
                        f"\n\nThe previous output was rejected ({reject_reason}). "
                        "Re-issue a fresh message that obeys every rule in the system prompt: "
                        "professional tone, no greeting words, no slang, no emojis, no hashtags, "
                        "between 90 and 160 characters, complete sentence ending with '.', '?' or '!'."
                    )
                raw = call_claude(
                    task_type="sms_copy",
                    messages=[{"role": "user", "content": user_msg}],
                    system=system_prompt,
                    max_tokens=200,
                )
                candidate = (raw or "").strip().strip('"').strip("'")
                ok, reject_reason = _validate_body(candidate)
                if ok:
                    body = candidate
                    break
                logger.warning(
                    "[ForwardPack] %s attempt=%d rejected: %s | body=%r",
                    vertical, attempt, reject_reason, candidate,
                )

            if body is None:
                logger.error(
                    "[ForwardPack] %s skipped — no valid copy after retries (last_reason=%s)",
                    vertical, reject_reason,
                )
                continue

            stmt = pg_insert(ReferralForwardCopy).values(
                vertical=vertical,
                week_start=week_start,
                body=body,
                generated_at=datetime.now(timezone.utc),
            ).on_conflict_do_nothing(
                constraint="uq_referral_forward_copy_vertical_week"
            )
            db.execute(stmt)
            results[vertical] = body
            logger.info("[ForwardPack] rendered %s week=%s len=%d", vertical, week_start, len(body))
        except Exception as exc:
            logger.error("[ForwardPack] failed for %s: %s", vertical, exc)

    db.flush()
    return results


def get_current_copy(vertical: str, db: Session) -> Optional[str]:
    """
    Return the cached blurb for the current ISO week, falling back to the
    most recent prior week if the current week hasn't been generated yet.
    """
    week_start = _current_week_start()

    row = db.execute(
        select(ReferralForwardCopy).where(
            ReferralForwardCopy.vertical == vertical,
            ReferralForwardCopy.week_start == week_start,
        )
    ).scalar_one_or_none()

    if row:
        return row.body

    # Fallback: most recent prior week
    fallback = db.execute(
        select(ReferralForwardCopy)
        .where(
            ReferralForwardCopy.vertical == vertical,
            ReferralForwardCopy.week_start < week_start,
        )
        .order_by(ReferralForwardCopy.week_start.desc())
        .limit(1)
    ).scalar_one_or_none()

    if fallback:
        logger.debug("[ForwardPack] using fallback copy for %s (week=%s)", vertical, fallback.week_start)
        return fallback.body

    return None
