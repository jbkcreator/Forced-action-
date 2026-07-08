"""
Registry of Cora Data Engine outcome connectors.

An "outcome connector" mines an already-ingested, already-matched public
record (foreclosure auction, tax-deed auction, appraiser sale, etc.) for a
labeled event and stages it as an OutcomeCandidate (src/connectors/outcomes.py).
This module is pure static metadata — no DB dependency, no scheduling logic.
Scheduling itself stays in scripts/cron/crontab.txt (see CLAUDE.md: every
scraper/task already runs as its own cron line / OS process, which already
gives failure isolation — this registry does not replace that).

Mirrors the plain-dict precedent of src/loaders/macro_signals/source_registry.py
rather than a class hierarchy, since connector metadata is enumerable static
data, not polymorphic behavior.

Each entry's source_type is the identifier a connector passes to
run_connector() (src/connectors/runner.py) and, once a connector goes live,
to record_scraper_stats() and heartbeat_monitor.py's HEARTBEAT_SLAS/
SOURCE_OFF_DAYS (that wiring happens at each connector's own go-live, not here
— see sla_minutes/off_days below, which are the single source of truth wave-1
copies from so the number never drifts between files).

Wave-1 connectors (foreclosure_outcomes, tax_deed_outcomes,
appraiser_sale_outcomes) are `enabled=True` — their modules are deployed and
running. dor_sale_outcomes stays `enabled=False`: it depends on a DOR raw
ingestion pipeline that doesn't exist yet.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ConnectorSpec:
    source_type: str            # identifier passed to run_connector() / ScraperRunStats / heartbeat_monitor
    description: str
    reads_table: str            # informational: existing table this connector mines for outcomes
    module: str                 # dotted path this connector will live at (may not exist yet)
    cadence: str                # human-readable; scripts/cron/crontab.txt remains the executable source of truth
    sla_minutes: int            # copied into heartbeat_monitor.HEARTBEAT_SLAS at go-live
    off_days: frozenset = field(default_factory=frozenset)   # copied into heartbeat_monitor.SOURCE_OFF_DAYS at go-live
    enabled: bool = False        # flips True once the connector module is deployed


OUTCOME_CONNECTORS: dict[str, ConnectorSpec] = {
    "foreclosure_outcomes": ConnectorSpec(
        source_type="foreclosure_outcomes",
        description="Foreclosure auction results — sold to third party vs. reverted to lender.",
        reads_table="foreclosures",
        module="src.connectors.foreclosure_outcomes",
        cadence="daily, after the foreclosure scraper",
        sla_minutes=1500,
        off_days=frozenset(),
        enabled=True,
    ),
    "tax_deed_outcomes": ConnectorSpec(
        source_type="tax_deed_outcomes",
        description="Tax-deed auction results — sold amount, sold-to, status.",
        reads_table="tax_deed_auctions",
        module="src.connectors.tax_deed_outcomes",
        cadence="daily, after the tax-deed scraper",
        sla_minutes=1500,
        off_days=frozenset({6}),
        enabled=True,
    ),
    "appraiser_sale_outcomes": ConnectorSpec(
        source_type="appraiser_sale_outcomes",
        description="Property-appraiser qualified/unqualified sales.",
        reads_table="financials",
        module="src.connectors.appraiser_sale_outcomes",
        cadence="weekly, after the appraiser bulk refresh",
        sla_minutes=10_140,
        off_days=frozenset(),
        enabled=True,
    ),
    "dor_sale_outcomes": ConnectorSpec(
        source_type="dor_sale_outcomes",
        description="Florida DOR statewide sales file — cross-county normalized sales.",
        reads_table="(new DOR raw ingestion — not yet built)",
        module="src.connectors.dor_sale_outcomes",
        cadence="quarterly, per DOR file release",
        sla_minutes=131_040,  # ~91 days + grace
        off_days=frozenset(),
        enabled=False,
    ),
}


def get_spec(source_type: str) -> ConnectorSpec:
    """Look up a connector's static metadata. Raises KeyError with a clear message if unregistered."""
    try:
        return OUTCOME_CONNECTORS[source_type]
    except KeyError:
        raise KeyError(
            f"'{source_type}' is not a registered outcome connector. "
            f"Known source types: {sorted(OUTCOME_CONNECTORS)}"
        ) from None


def enabled_connectors() -> dict[str, ConnectorSpec]:
    """Connectors whose module is actually deployed and running."""
    return {k: v for k, v in OUTCOME_CONNECTORS.items() if v.enabled}
