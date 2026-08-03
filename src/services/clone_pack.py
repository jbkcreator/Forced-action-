"""
Clone-Pack assembly — CLONE-v2.2 / CL4.

"Clone-Pack" is the collective name for what CL1–CL3 produced: the venture
template, the provisioning routine, the config resolver, the per-venture Relay
identity, and the cloned counties/county_sources. Up to now it existed only as
a set of modules and a manual checklist — there was no artifact you could hold
and assert on, which is why `--apply` returning `{"cloned": 7}` has never been
the same thing as "this venture can send".

This module gives it a shape. `assemble()` is one read-only pass that answers
every question the spin-up rung asks, and `is_complete()` is the PASS/FAIL.
The acceptance harness asserts against it; the ladder's spin_up gates and any
admin surface read the same object.

Read-only by design: nothing here writes, provisions or repairs. A Clone-Pack
reports gaps; closing them is venture_provisioning's job.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.venture_template import DEFAULT_VENTURE_KEY, REQUIRED_SIGNAL_TYPES
from src.utils.venture_config import VentureConfig, get_venture_config

logger = logging.getLogger(__name__)

CRONTAB_PATH = Path("scripts/cron/crontab.txt")

_RELAY_SWEEP_MODULE = "src.services.relay --sweep"


@dataclass(frozen=True)
class ClonePack:
    """Everything that must be true for a venture to actually run."""

    venture_key: str
    venture: VentureConfig
    ladder_stage: str
    is_active: bool
    counties: tuple[str, ...]
    # county_id -> required signal types with no county-specific URL. Empty
    # list means fully covered.
    source_coverage: dict[str, list[str]]
    relay_ready: bool
    relay_gaps: tuple[str, ...]
    cron_line_present: bool
    gaps: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.gaps


def is_complete(pack: ClonePack) -> bool:
    """True when the pack has no gaps — the harness's PASS condition."""
    return pack.complete


# Reads the ventures ROW, not the resolved VentureConfig. The CL3 resolver
# substitutes config/settings.py for any NULL column, so a venture with no
# Instantly campaign of its own resolves to venture #1's campaign and would
# report ready while being able to send only into another business's sequence.
_SELECT_ROW = """
SELECT venture_key, ladder_stage, is_active, template_county_id,
       NULLIF(TRIM(COALESCE(relay_instantly_campaign_id, '')), '')   AS campaign_id,
       NULLIF(TRIM(COALESCE(relay_instantly_sender_email, '')), '')  AS sender_email,
       NULLIF(TRIM(COALESCE(relay_slack_channel, '')), '')           AS slack_channel,
       NULLIF(TRIM(COALESCE(kill_switch_feature, '')), '')           AS kill_switch,
       COALESCE(jsonb_array_length(relay_approvers), 0)              AS approver_count
FROM ventures
WHERE venture_key = :key
"""

# Per (county, signal_type) coverage. A URL still equal to the template
# county's is INHERITED, not configured — clone_county_sources() reports those
# as `missing_urls` at provisioning time, and a scraper that inherits one
# silently hits the wrong county's portal. That is the failure this catches.
_SELECT_COVERAGE = """
WITH required AS (
    SELECT unnest(CAST(:required AS text[])) AS signal_type
),
template_sources AS (
    SELECT signal_type, url
    FROM county_sources
    WHERE county_id = :template_county AND is_active = true
),
pairs AS (
    SELECT c.county_id, r.signal_type
    FROM counties c
    CROSS JOIN required r
    WHERE c.venture_key = :key
)
SELECT
    p.county_id,
    p.signal_type,
    BOOL_OR(
        cs.url IS NOT NULL
        AND cs.url <> ''
        AND (
            t.url IS NULL
            OR p.county_id = :template_county
            OR cs.url <> t.url
        )
    ) AS covered
FROM pairs p
LEFT JOIN county_sources cs
       ON cs.county_id = p.county_id
      AND cs.signal_type = p.signal_type
      AND cs.is_active = true
LEFT JOIN template_sources t ON t.signal_type = p.signal_type
GROUP BY p.county_id, p.signal_type
ORDER BY p.county_id, p.signal_type
"""


def source_coverage(
    db: Session, venture_key: str, *, template_county_id: Optional[str]
) -> dict[str, list[str]]:
    """county_id -> required signal types with no county-specific URL.

    THE single source of coverage truth. src/services/venture_ladder.py's probe
    rung calls this rather than carrying its own copy of the query: the gate
    that decides whether a venture may advance and the Clone-Pack that reports
    whether it can run must never be able to disagree about what "covered"
    means.

    An empty list for a county means fully covered. A county with no
    county_sources rows at all still appears, with every required type listed.
    """
    rows = db.execute(
        text(_SELECT_COVERAGE),
        {
            "key": venture_key,
            "required": list(REQUIRED_SIGNAL_TYPES),
            "template_county": template_county_id,
        },
    ).fetchall()

    coverage: dict[str, list[str]] = {}
    for row in rows:
        coverage.setdefault(row.county_id, [])
        if not row.covered:
            coverage[row.county_id].append(row.signal_type)
    return coverage


def cron_line_present(venture_key: str, *, crontab_path: Optional[Path] = None) -> bool:
    """True when this venture has its own uncommented Relay sweep cron line.

    Checks the repo's crontab rather than a recorded claim, because the line
    either exists or it does not and a stale evidence row would assert the
    wrong thing. One sweep run = one venture (CL3), so a second venture that
    inherits no cron line silently never sends — the exact failure mode a
    Clone-Pack has to catch.

    Venture #1's line carries no `--venture` flag (it predates CL3 and defaults
    to that key); every other venture needs an explicit one.
    """
    path = crontab_path or CRONTAB_PATH
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        logger.warning(
            "[clone_pack] could not read %s — reporting the cron line as absent",
            path, exc_info=True,
        )
        return False

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or _RELAY_SWEEP_MODULE not in line:
            continue
        if venture_key == DEFAULT_VENTURE_KEY:
            if "--venture" not in line:
                return True
        elif f"--venture {venture_key}" in line:
            return True
    return False


def _relay_gaps(row) -> list[str]:
    gaps: list[str] = []
    if not row.campaign_id:
        gaps.append(
            "no relay_instantly_campaign_id of its own — run "
            "`python -m src.services.relay --setup-email-channel` (two ventures "
            "sharing one campaign cross-contaminate Instantly's duplicate guard, "
            "see docs/adr/0011)"
        )
    if not row.sender_email:
        gaps.append("no relay_instantly_sender_email — the email channel has no from-address")
    if not row.slack_channel:
        gaps.append("no relay_slack_channel — approvals would be ambiguous across ventures")
    if not row.kill_switch:
        gaps.append("no kill_switch_feature — this venture cannot be stopped independently")
    if not row.approver_count:
        gaps.append("no relay_approvers — nobody can approve this venture's queue items")
    return gaps


def assemble(
    db: Session, venture_key: str, *, crontab_path: Optional[Path] = None
) -> ClonePack:
    """Assemble the full Clone-Pack for a venture. Read-only.

    Raises LookupError if no `ventures` row exists — a radar candidate is still
    a row (is_active=false), so its absence means nothing has been provisioned
    at all.

    `crontab_path` overrides which crontab the cron-line check reads. Production
    callers leave it unset; the acceptance harness points it at a temp file so a
    stub venture can prove the check works without an edit to the real crontab.
    """
    row = db.execute(text(_SELECT_ROW), {"key": venture_key}).first()
    if row is None:
        raise LookupError(
            f"no ventures row for {venture_key!r} — nothing to assemble; "
            "start with `python -m src.services.venture_provisioning --emit-template`"
        )

    coverage = source_coverage(
        db, venture_key, template_county_id=row.template_county_id
    )
    counties = tuple(sorted(coverage))
    relay_gaps = _relay_gaps(row)
    has_cron = cron_line_present(venture_key, crontab_path=crontab_path)

    gaps: list[str] = []
    if not counties:
        gaps.append("no counties attached — this venture has nothing to scrape")
    for county_id in counties:
        missing = coverage[county_id]
        if missing:
            gaps.append(
                f"{county_id}: {len(missing)} required signal type(s) without a "
                f"county-specific URL ({', '.join(missing)})"
            )
    gaps.extend(relay_gaps)
    if not has_cron:
        gaps.append(
            "no Relay sweep cron line — add "
            f"`src.services.relay --sweep --venture {venture_key}` to "
            f"{CRONTAB_PATH.as_posix()} (one sweep run = one venture)"
        )

    return ClonePack(
        venture_key=venture_key,
        venture=get_venture_config(venture_key, session=db),
        ladder_stage=row.ladder_stage,
        is_active=bool(row.is_active),
        counties=counties,
        source_coverage=coverage,
        relay_ready=not relay_gaps,
        relay_gaps=tuple(relay_gaps),
        cron_line_present=has_cron,
        gaps=tuple(gaps),
    )
