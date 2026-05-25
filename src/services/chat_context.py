"""
Context assembler for Concierge Chat system prompts.

Builds the cached system block injected into each Claude call.
Pre-signup: static product/pricing/coverage text from YAML.
Post-signup: subscriber-scoped data (wallet, ZIPs, billing) — no PII.
"""

import logging
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_PROMPTS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "prompts" / "chat_concierge.yaml"


def _load_prompts() -> dict:
    with open(_PROMPTS_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_PROMPTS: dict = _load_prompts()


def pre_signup_context() -> str:
    """Return the cached system prompt for anonymous/pre-signup sessions."""
    return _PROMPTS.get("pre_signup", "")


def post_signup_context(subscriber_id: int, db: Session) -> str:
    """
    Return the cached system prompt for an identified subscriber.
    Injects wallet balance, owned ZIPs, available bundles, billing status.
    Never returns skip-traced phone or email.
    """
    from src.core.models import Subscriber, WalletBalance, ZipTerritory
    from sqlalchemy import and_

    base = _PROMPTS.get("post_signup", "")

    try:
        subscriber = db.execute(
            select(Subscriber).where(Subscriber.id == subscriber_id)
        ).scalar_one_or_none()

        if not subscriber:
            return base

        wallet = db.execute(
            select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
        ).scalar_one_or_none()

        locked_zips = db.execute(
            select(ZipTerritory.zip_code).where(
                and_(
                    ZipTerritory.subscriber_id == subscriber_id,
                    ZipTerritory.status.in_(["locked", "grace"]),
                )
            )
        ).scalars().all()

        wallet_balance = wallet.credits_remaining if wallet else 0
        wallet_tier = wallet.wallet_tier if wallet else "none"

        context_block = f"""
## Subscriber context (current session)
- Tier: {subscriber.tier}
- Status: {subscriber.status}
- Wallet balance: {wallet_balance} credits ({wallet_tier} tier)
- Locked ZIPs: {', '.join(locked_zips) if locked_zips else 'none'}
- Vertical: {subscriber.vertical}
- County: {subscriber.county_id}
"""
        return base + context_block

    except Exception as exc:
        logger.warning("chat_context.post_signup_context failed for sub %s: %s", subscriber_id, exc)
        return base
