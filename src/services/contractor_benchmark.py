"""
Stage 12 — Contractor Benchmark Engine.

Computes per-contractor performance metrics and compares each contractor to
their trade/county peer group. All DB I/O uses raw SQL via sa_text.

Definitions
-----------
contractor   = any active Subscriber whose vertical is in CONTRACTOR_VERTICALS
benchmark    = the peer group median/average for the same (vertical, county_id)
window_days  = look-back window for deal and message data (default 90 days)

Metrics per contractor
----------------------
total_leads          — rows in sent_leads in window
closed_deals         — deal_outcomes with pipeline_stage='closed_won' in window
close_rate           — closed_deals / total_leads  (0 if no leads)
avg_days_to_close    — mean days_to_close on closed_won rows
avg_deal_size        — mean deal_amount on closed_won rows
total_revenue        — sum deal_amount on closed_won rows in window
sms_reply_rate       — message_outcomes replies / sends in window
revenue_signal_score — live score from subscribers.revenue_signal_score

Benchmark flags (relative to peer group average close_rate)
------------------------------------------------------------
above_benchmark      — close_rate >= 1.2 × group_avg
at_benchmark         — 0.8 ≤ close_rate < 1.2 × group_avg
below_benchmark      — close_rate < 0.8 × group_avg
ap_upsell_candidate  — below_benchmark (needs AP automation to close the gap)
ap_pro_candidate     — above_benchmark AND tier NOT IN ('autopilot_pro','annual_lock')

Minimum group size: 2 contractors. Groups with fewer are flagged as
'insufficient_peers' and not benchmarked — individual metrics still computed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CONTRACTOR_VERTICALS = (
    "roofing",
    "restoration",
    "fix_flip",
    "wholesalers",
    "attorneys",
    "public_adjusters",
)

ABOVE_BENCHMARK_THRESHOLD = 1.20   # 20% above avg → above_benchmark
BELOW_BENCHMARK_THRESHOLD = 0.80   # 20% below avg → below_benchmark
MIN_GROUP_SIZE = 2                  # peer groups smaller than this = no benchmark
AP_UPSELL_TIERS = frozenset(["starter", "pro"])   # eligible for AP upsell
AP_PRO_EXCLUSION = frozenset(["autopilot_pro", "annual_lock"])  # already at the top


# ── Data containers ───────────────────────────────────────────────────────────

@dataclass
class ContractorMetrics:
    subscriber_id: int
    name: str
    email: str
    vertical: str
    county_id: str
    tier: str
    plan_price: float
    revenue_signal_score: int

    total_leads: int = 0
    closed_deals: int = 0
    close_rate: float = 0.0
    avg_days_to_close: float = 0.0
    avg_deal_size: float = 0.0
    total_revenue: float = 0.0
    sms_reply_rate: float = 0.0
    total_messages_sent: int = 0

    # Set after benchmark computation
    benchmark_close_rate: float = 0.0
    close_rate_vs_benchmark: float = 0.0   # ratio: 1.0 = at benchmark
    benchmark_status: str = "no_peers"     # above / at / below / no_peers
    ap_upsell_candidate: bool = False
    ap_pro_candidate: bool = False


@dataclass
class GroupBenchmark:
    vertical: str
    county_id: str
    contractor_count: int
    avg_close_rate: float = 0.0
    avg_days_to_close: float = 0.0
    avg_deal_size: float = 0.0
    avg_reply_rate: float = 0.0
    total_leads: int = 0
    total_closed_deals: int = 0
    total_revenue: float = 0.0
    sufficient_peers: bool = True


@dataclass
class BenchmarkReport:
    generated_at: datetime
    window_days: int
    contractors: list[ContractorMetrics] = field(default_factory=list)
    benchmarks: list[GroupBenchmark] = field(default_factory=list)

    @property
    def below_benchmark(self) -> list[ContractorMetrics]:
        return [c for c in self.contractors if c.benchmark_status == "below"]

    @property
    def above_benchmark(self) -> list[ContractorMetrics]:
        return [c for c in self.contractors if c.benchmark_status == "above"]

    @property
    def ap_upsell_candidates(self) -> list[ContractorMetrics]:
        return [c for c in self.contractors if c.ap_upsell_candidate]

    @property
    def ap_pro_candidates(self) -> list[ContractorMetrics]:
        return [c for c in self.contractors if c.ap_pro_candidate]


# ── Core queries ──────────────────────────────────────────────────────────────

def _fetch_contractor_rows(
    db: Session,
    window_days: int,
    vertical_filter: Optional[tuple] = None,
    county_filter: Optional[str] = None,
) -> list[dict]:
    """Pull per-contractor metrics in one CTE query."""
    since = datetime.now(timezone.utc) - timedelta(days=window_days)
    verticals = list(vertical_filter or CONTRACTOR_VERTICALS)

    rows = db.execute(sa_text("""
        WITH sent AS (
            SELECT sl.subscriber_id,
                   COUNT(*) AS total_leads
            FROM sent_leads sl
            JOIN subscribers s ON s.id = sl.subscriber_id
            WHERE sl.sent_at >= :since
              AND s.vertical = ANY(:verticals)
            GROUP BY sl.subscriber_id
        ),
        deals AS (
            SELECT d.subscriber_id,
                   COUNT(*) FILTER (WHERE d.pipeline_stage = 'closed_won')      AS closed_deals,
                   COALESCE(AVG(d.days_to_close)
                     FILTER (WHERE d.pipeline_stage = 'closed_won'), 0)         AS avg_days_to_close,
                   COALESCE(AVG(d.deal_amount)
                     FILTER (WHERE d.pipeline_stage = 'closed_won'), 0)         AS avg_deal_size,
                   COALESCE(SUM(d.deal_amount)
                     FILTER (WHERE d.pipeline_stage = 'closed_won'), 0)         AS total_revenue
            FROM deal_outcomes d
            WHERE d.created_at >= :since
            GROUP BY d.subscriber_id
        ),
        msgs AS (
            SELECT m.subscriber_id,
                   COUNT(*)                                                       AS total_sent,
                   COUNT(*) FILTER (WHERE m.replied_at IS NOT NULL)              AS replies
            FROM message_outcomes m
            WHERE m.sent_at >= :since
              AND m.message_type = 'sms'
            GROUP BY m.subscriber_id
        )
        SELECT
            s.id                                                          AS subscriber_id,
            COALESCE(s.name, '')                                          AS name,
            COALESCE(s.email, '')                                         AS email,
            s.vertical                                                    AS vertical,
            s.county_id                                                   AS county_id,
            s.tier                                                        AS tier,
            COALESCE(s.plan_price, 0)::FLOAT                             AS plan_price,
            COALESCE(s.revenue_signal_score, 0)                          AS revenue_signal_score,
            COALESCE(sent.total_leads, 0)                                AS total_leads,
            COALESCE(deals.closed_deals, 0)                              AS closed_deals,
            COALESCE(deals.avg_days_to_close, 0)::FLOAT                  AS avg_days_to_close,
            COALESCE(deals.avg_deal_size, 0)::FLOAT                      AS avg_deal_size,
            COALESCE(deals.total_revenue, 0)::FLOAT                      AS total_revenue,
            COALESCE(msgs.total_sent, 0)                                 AS total_messages_sent,
            COALESCE(msgs.replies, 0)                                    AS replies,
            CASE
                WHEN COALESCE(sent.total_leads, 0) > 0
                THEN COALESCE(deals.closed_deals, 0)::FLOAT / sent.total_leads
                ELSE 0.0
            END                                                          AS close_rate,
            CASE
                WHEN COALESCE(msgs.total_sent, 0) > 0
                THEN COALESCE(msgs.replies, 0)::FLOAT / msgs.total_sent
                ELSE 0.0
            END                                                          AS sms_reply_rate
        FROM subscribers s
        LEFT JOIN sent   ON sent.subscriber_id   = s.id
        LEFT JOIN deals  ON deals.subscriber_id  = s.id
        LEFT JOIN msgs   ON msgs.subscriber_id   = s.id
        WHERE s.status = 'active'
          AND s.vertical = ANY(:verticals)
          AND (:county IS NULL OR s.county_id = :county)
        ORDER BY s.vertical, s.county_id, close_rate DESC
    """), {
        "since": since,
        "verticals": verticals,
        "county": county_filter,
    }).mappings().fetchall()

    return [dict(r) for r in rows]


def _compute_group_benchmarks(contractor_rows: list[dict]) -> dict[tuple, GroupBenchmark]:
    """Aggregate per-(vertical, county_id) benchmarks from raw contractor rows."""
    groups: dict[tuple, list[dict]] = {}
    for row in contractor_rows:
        key = (row["vertical"], row["county_id"])
        groups.setdefault(key, []).append(row)

    benchmarks: dict[tuple, GroupBenchmark] = {}
    for (vertical, county_id), members in groups.items():
        n = len(members)
        sufficient = n >= MIN_GROUP_SIZE
        gb = GroupBenchmark(
            vertical=vertical,
            county_id=county_id,
            contractor_count=n,
            sufficient_peers=sufficient,
        )
        if sufficient:
            gb.avg_close_rate = sum(m["close_rate"] for m in members) / n
            gb.avg_days_to_close = sum(m["avg_days_to_close"] for m in members) / n
            gb.avg_deal_size = sum(m["avg_deal_size"] for m in members) / n
            gb.avg_reply_rate = sum(m["sms_reply_rate"] for m in members) / n
            gb.total_leads = sum(m["total_leads"] for m in members)
            gb.total_closed_deals = sum(m["closed_deals"] for m in members)
            gb.total_revenue = sum(m["total_revenue"] for m in members)
        benchmarks[(vertical, county_id)] = gb
    return benchmarks


def _apply_benchmark_flags(
    contractor: ContractorMetrics,
    group: GroupBenchmark,
) -> None:
    """Mutate contractor in-place: set benchmark_status and upsell flags."""
    if not group.sufficient_peers:
        contractor.benchmark_status = "no_peers"
        contractor.benchmark_close_rate = 0.0
        contractor.close_rate_vs_benchmark = 0.0
        return

    avg = group.avg_close_rate
    contractor.benchmark_close_rate = avg

    if avg == 0:
        contractor.benchmark_status = "at"
        contractor.close_rate_vs_benchmark = 1.0
    else:
        ratio = contractor.close_rate / avg
        contractor.close_rate_vs_benchmark = round(ratio, 4)
        if ratio >= ABOVE_BENCHMARK_THRESHOLD:
            contractor.benchmark_status = "above"
        elif ratio < BELOW_BENCHMARK_THRESHOLD:
            contractor.benchmark_status = "below"
        else:
            contractor.benchmark_status = "at"

    contractor.ap_upsell_candidate = (
        contractor.benchmark_status == "below"
        and contractor.tier in AP_UPSELL_TIERS
    )
    contractor.ap_pro_candidate = (
        contractor.benchmark_status == "above"
        and contractor.tier not in AP_PRO_EXCLUSION
    )


# ── Public API ────────────────────────────────────────────────────────────────

def compute_benchmark_report(
    db: Session,
    *,
    window_days: int = 90,
    vertical_filter: Optional[tuple] = None,
    county_filter: Optional[str] = None,
) -> BenchmarkReport:
    """Run the full benchmark pipeline and return a BenchmarkReport.

    This is the single entry point used by the report task and tests.
    Pure computation — no side effects, no file writes.
    """
    report = BenchmarkReport(
        generated_at=datetime.now(timezone.utc),
        window_days=window_days,
    )

    raw_rows = _fetch_contractor_rows(db, window_days, vertical_filter, county_filter)
    if not raw_rows:
        logger.info("[benchmark] no contractor rows found for window=%dd", window_days)
        return report

    group_benchmarks = _compute_group_benchmarks(raw_rows)
    report.benchmarks = list(group_benchmarks.values())

    for row in raw_rows:
        c = ContractorMetrics(
            subscriber_id=row["subscriber_id"],
            name=row["name"],
            email=row["email"],
            vertical=row["vertical"],
            county_id=row["county_id"],
            tier=row["tier"],
            plan_price=float(row["plan_price"]),
            revenue_signal_score=int(row["revenue_signal_score"]),
            total_leads=int(row["total_leads"]),
            closed_deals=int(row["closed_deals"]),
            close_rate=float(row["close_rate"]),
            avg_days_to_close=float(row["avg_days_to_close"]),
            avg_deal_size=float(row["avg_deal_size"]),
            total_revenue=float(row["total_revenue"]),
            sms_reply_rate=float(row["sms_reply_rate"]),
            total_messages_sent=int(row["total_messages_sent"]),
        )
        group = group_benchmarks.get((c.vertical, c.county_id))
        if group:
            _apply_benchmark_flags(c, group)
        report.contractors.append(c)

    logger.info(
        "[benchmark] computed: %d contractors, %d groups, "
        "%d below benchmark, %d ap_upsell, %d ap_pro",
        len(report.contractors),
        len(report.benchmarks),
        len(report.below_benchmark),
        len(report.ap_upsell_candidates),
        len(report.ap_pro_candidates),
    )
    return report
