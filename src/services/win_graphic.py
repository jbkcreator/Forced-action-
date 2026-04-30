"""
Deal-Win Graphic — Stage 5.

Generates a 1200x630 OG-friendly PNG for a confirmed deal win, anonymized
to protect subscriber + owner PII (deal *bucket* + vertical + county only —
no exact dollar amount, no address, no name).

Pillow is imported lazily so the module can be imported even when Pillow
is not installed in the environment (graceful degradation — generate()
just returns None and the deal-capture path proceeds without the graphic).

Output path: `data/win_graphics/<deal_outcome_id>.png`. The file is served
by `GET /api/win-graphic/{deal_outcome_id}` (defined in api/main.py).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import DealOutcome, Subscriber

logger = logging.getLogger(__name__)


_OUTPUT_DIR = Path("data") / "win_graphics"


_BUCKET_LABELS = {
    "5_10k":     "Closed $5K–$10K",
    "10_25k":    "Closed $10K–$25K",
    "25k_plus":  "Closed $25K+",
    "skip":      "Deal Closed",
}


def _ensure_output_dir() -> Path:
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return _OUTPUT_DIR


def output_path(deal_outcome_id: int) -> Path:
    return _OUTPUT_DIR / f"{deal_outcome_id}.png"


def generate(deal_outcome_id: int, db: Session) -> Optional[Path]:
    """
    Render the win-graphic PNG. Returns the file path on success, None if
    generation could not happen (Pillow missing, deal not found, etc.).
    Idempotent — returns existing file path if already rendered.
    """
    out = output_path(deal_outcome_id)
    if out.exists():
        return out

    try:
        from PIL import Image, ImageDraw, ImageFont   # type: ignore
    except ImportError:
        logger.warning("[WinGraphic] Pillow not installed - graphic generation skipped")
        return None

    deal = db.get(DealOutcome, deal_outcome_id)
    if not deal:
        logger.warning("[WinGraphic] DealOutcome %d not found", deal_outcome_id)
        return None

    sub = db.get(Subscriber, deal.subscriber_id)
    vertical = (sub.vertical if sub else "").replace("_", " ").title()
    county = (sub.county_id if sub else "").replace("_", " ").title()
    bucket_label = _BUCKET_LABELS.get(deal.deal_size_bucket, "Deal Closed")

    _ensure_output_dir()
    img = Image.new("RGB", (1200, 630), (15, 23, 42))      # slate-900
    draw = ImageDraw.Draw(img)

    # Try a real font — fall back to default if not available.
    try:
        font_xl  = ImageFont.truetype("arial.ttf", 84)
        font_lg  = ImageFont.truetype("arial.ttf", 48)
        font_md  = ImageFont.truetype("arial.ttf", 32)
    except (IOError, OSError):
        font_xl = ImageFont.load_default()
        font_lg = ImageFont.load_default()
        font_md = ImageFont.load_default()

    # Border accent
    draw.rectangle([0, 0, 1200, 8], fill=(251, 191, 36))   # amber bar

    # Headline
    draw.text((80, 140), bucket_label, fill=(251, 191, 36), font=font_xl)
    # Sub-line
    line2_parts = [p for p in [vertical, county] if p]
    line2 = " · ".join(line2_parts) if line2_parts else "Forced Action"
    draw.text((80, 280), line2, fill=(226, 232, 240), font=font_lg)
    # Brand watermark
    draw.text((80, 540), "via Forced Action", fill=(148, 163, 184), font=font_md)
    # Date
    from datetime import timezone as _tz
    today = datetime.now(_tz.utc).strftime("%b %Y")
    draw.text((80, 470), today, fill=(148, 163, 184), font=font_md)

    img.save(out, "PNG", optimize=True)
    logger.info("[WinGraphic] generated: deal=%d path=%s", deal_outcome_id, out)
    return out


def proof_wall_payload(db: Session, limit: int = 50) -> list[dict]:
    """
    Anonymized recent deal wins for the public Social Proof Wall.
    Returns list of dicts with bucket / vertical / county_id / days_ago / graphic_url.
    No subscriber name, no exact amount, no address.
    """
    from datetime import timezone as _tz
    today = datetime.now(_tz.utc).date()
    rows = db.execute(
        select(DealOutcome, Subscriber)
        .join(Subscriber, Subscriber.id == DealOutcome.subscriber_id)
        .where(DealOutcome.deal_size_bucket != "skip")
        .order_by(DealOutcome.created_at.desc())
        .limit(limit)
    ).all()

    payload: list[dict] = []
    for deal, sub in rows:
        days_ago = (today - deal.deal_date).days if deal.deal_date else None
        payload.append({
            "deal_outcome_id": deal.id,
            "deal_size_bucket": deal.deal_size_bucket,
            "label": _BUCKET_LABELS.get(deal.deal_size_bucket, "Deal Closed"),
            "vertical": sub.vertical if sub else None,
            "county_id": sub.county_id if sub else None,
            "days_ago": days_ago,
            "graphic_url": f"/api/win-graphic/{deal.id}",
        })
    return payload
