"""P2.5: the pipeline part of the 7:20pm closing scoreboard: active deals, deals per GHL stage, and the daily blockers.

Read-only. Stages come from ``lending.ghl_opportunity_stage`` (P2.4: each deal's current stage, kept by the GHL stage
webhook and backfilled once from ``ghl_stage_events``), so the counts are only as complete as the stage moves GHL posts.
Active deals = P2.4's deal stages (``ACTIVE_STAGE_KEYS``: Application, Ready to fund, Submitted to lender; leads in
New lead / Booked / Held / No show are not deals); the stage line still shows every stage. Blockers are P2.4's
``stuck_deals()`` (active for more than 3 days).
Test deals (a phone flagged ``is_test`` on a contact or lead) are left out, as on every scoreboard.

A failed read (P2.4's tables not migrated yet, a query error) returns ``PipelineData(available=False)``: the board still
posts, with the pipeline marked unavailable, the same rule as the Showed column.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from config.lending_closing import ACTIVE_STAGE_KEYS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StageCount:
    stage_key: str
    stage_name: str
    deals: int


@dataclass(frozen=True)
class Blocker:
    """One stuck deal as the board shows it: first name and the last 4 digits only."""
    first_name: Optional[str]
    phone_last4: Optional[str]
    stage_name: str
    days: int
    owner: str
    missing: int


@dataclass(frozen=True)
class PipelineData:
    available: bool = True
    by_stage: list[StageCount] = field(default_factory=list)
    blockers: list[Blocker] = field(default_factory=list)

    @property
    def active(self) -> int:
        return sum(s.deals for s in self.by_stage if s.stage_key in ACTIVE_STAGE_KEYS)

    @property
    def total(self) -> int:
        return sum(s.deals for s in self.by_stage)

    @property
    def blockers_by_owner(self) -> list[tuple[str, int]]:
        counts = Counter(b.owner for b in self.blockers)
        return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


# A phone that is not a test contact or test lead (``is_test``: CLAUDE.md, scoreboards leave test rows out). ``{p}`` is a
# column or alias name from this module, never input. P2.4's own 7 AM card keeps test deals (its proof uses a test file).
_NOT_TEST = """
    NOT EXISTS (SELECT 1 FROM lending.contacts c WHERE c.is_test AND c.phone = {p})
    AND NOT EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.is_test AND w.phone = {p})
    AND NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.is_test AND l.phone = {p})
"""
_BY_STAGE = text(f"""
    SELECT s.stage_key, min(s.stage_name) AS stage_name, count(*) AS deals
      FROM lending.ghl_opportunity_stage s
     WHERE s.phone IS NULL OR ({_NOT_TEST.format(p="s.phone")})
     GROUP BY s.stage_key
     ORDER BY deals DESC, s.stage_key
""")
_TEST_PHONES = text(f"""
    SELECT p FROM unnest(CAST(:phones AS text[])) AS p WHERE NOT ({_NOT_TEST.format(p="p")})
""")


def _blockers(db, now: datetime) -> list[Blocker]:
    """P2.4's stuck deals without the test deals, reduced to what the board shows."""
    from src.lending.closing.blockers import stuck_deals

    deals = stuck_deals(db, now)
    phones = sorted({d.phone for d in deals if d.phone})
    test = {row[0] for row in db.execute(_TEST_PHONES, {"phones": phones})} if phones else set()
    return [Blocker(first_name=d.first_name, phone_last4=d.phone_last4, stage_name=d.stage_name, days=d.days,
                    owner=d.owner, missing=len(d.missing))
            for d in deals if d.phone not in test]


def build_pipeline(db, now: datetime) -> PipelineData:
    """Never raises. The reads run in a savepoint so a failure leaves the caller's transaction usable."""
    try:
        with db.begin_nested():
            by_stage = [StageCount(r["stage_key"], r["stage_name"], int(r["deals"]))
                        for r in db.execute(_BY_STAGE).mappings()]
            blockers = _blockers(db, now)
    except Exception as exc:  # class only: SQL errors embed bound values
        logger.warning("[lending-scoreboard] pipeline read failed, board posts without it: %s", type(exc).__name__)
        return PipelineData(available=False)
    return PipelineData(by_stage=by_stage, blockers=blockers)
