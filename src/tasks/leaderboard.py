"""
Referral leaderboard — Stage 5.

Weekly aggregation that ranks subscribers by confirmed referrals over the
last 7 days within each (county_id, vertical) cohort. The output is written
to `data/leaderboards/<iso_date>.json` and exposed by `GET /api/leaderboard`
(filterable by county + vertical).

The leaderboard is also injected into the Monday digest (see
`weekly_one_pager.py` — Stage 5 hook reads the latest snapshot file).

Subscriber identity is anonymized to `first_name + last_initial` so member
PII never leaks across cohorts.

Run via `python -m src.tasks.leaderboard` (Mondays from cron).
"""

from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import ReferralEvent, Subscriber

logger = logging.getLogger(__name__)


_OUTPUT_DIR = Path("data") / "leaderboards"
_TOP_N = 5


def _handle(sub: Subscriber) -> str:
    """First name + last initial. Falls back to 'Member <id>' if no name."""
    if not sub.name:
        return f"Member {sub.id}"
    parts = sub.name.strip().split()
    first = parts[0]
    last_initial = (parts[1][0] if len(parts) > 1 else "").upper()
    if last_initial:
        return f"{first} {last_initial}."
    return first


def _badge(refs_this_week: int) -> Optional[str]:
    if refs_this_week >= 5:
        return "team_unlocker"        # earned a team — meets the 5-ref milestone too
    if refs_this_week >= 3:
        return "rising_star"
    if refs_this_week >= 1:
        return "contributor"
    return None


def build(db: Session, today: Optional[date] = None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)

    # Per-subscriber confirmed referral count this week
    rows = db.execute(
        select(
            ReferralEvent.referrer_subscriber_id,
            func.count(ReferralEvent.id),
        )
        .where(
            ReferralEvent.status.in_(("confirmed", "rewarded")),
            ReferralEvent.confirmed_at >= week_ago,
        )
        .group_by(ReferralEvent.referrer_subscriber_id)
    ).all()

    if not rows:
        return {"as_of": today.isoformat(), "leaderboards": []}

    # Lifetime totals (used for tie-breakers and the "refs_total" display)
    totals = dict(db.execute(
        select(
            ReferralEvent.referrer_subscriber_id,
            func.count(ReferralEvent.id),
        )
        .where(ReferralEvent.status.in_(("confirmed", "rewarded")))
        .group_by(ReferralEvent.referrer_subscriber_id)
    ).all())

    # Group subscribers by (county_id, vertical)
    sub_ids = [r[0] for r in rows]
    subs = db.execute(
        select(Subscriber).where(Subscriber.id.in_(sub_ids))
    ).scalars().all()
    by_id = {s.id: s for s in subs}

    cohorts: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for sub_id, refs_week in rows:
        sub = by_id.get(sub_id)
        if not sub:
            continue
        cohorts[(sub.county_id, sub.vertical)].append({
            "subscriber_id": sub.id,
            "handle": _handle(sub),
            "refs_this_week": int(refs_week),
            "refs_total": int(totals.get(sub_id, refs_week)),
        })

    leaderboards = []
    for (county, vertical), members in cohorts.items():
        members.sort(
            key=lambda m: (m["refs_this_week"], m["refs_total"]),
            reverse=True,
        )
        ranked = []
        for i, m in enumerate(members[:_TOP_N], start=1):
            ranked.append({
                "rank": i,
                **m,
                "badge": _badge(m["refs_this_week"]),
            })
        leaderboards.append({
            "county_id": county,
            "vertical": vertical,
            "leaderboard": ranked,
        })

    return {"as_of": today.isoformat(), "leaderboards": leaderboards}


def write_snapshot(payload: dict) -> Path:
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = _OUTPUT_DIR / f"{payload['as_of']}.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    # Also write/update a `latest.json` symlink-style copy
    (_OUTPUT_DIR / "latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def latest_snapshot() -> Optional[dict]:
    latest = _OUTPUT_DIR / "latest.json"
    if not latest.exists():
        return None
    try:
        return json.loads(latest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def run() -> dict:
    with get_db_context() as db:
        payload = build(db)
    out = write_snapshot(payload)
    logger.info("[Leaderboard] written: %s cohorts=%d", out, len(payload.get("leaderboards", [])))
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run(), indent=2))
