"""
Registry of Lifecycle Data Engine outcome connectors.

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
running. dor_sales (CDE-07 raw ingestion — 160k+ rows, 99.8% matched) already
exists and is loaded; dor_sale_outcomes (the outcome connector itself, see
src/connectors/dor_sale_outcomes.py) ran end-to-end for both counties on
2026-07-16 (`scraper_run_stats` ids 9804/9854, run_success=True, 78,622 rows
staged into outcome_candidates) and is now `enabled=True`.
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
    "deed_flip_outcomes": ConnectorSpec(
        source_type="deed_flip_outcomes",
        description="Distressed-acquire-then-resell deed chains — flip margin and hold time.",
        reads_table="deeds",
        module="src.connectors.deed_flip_outcomes",
        cadence="weekly, after the deed loader",
        sla_minutes=10_140,
        off_days=frozenset(),
        enabled=True,
    ),
    "lis_pendens_outcomes": ConnectorSpec(
        source_type="lis_pendens_outcomes",
        description="Lis-pendens filings that never reached auction — resolved by a subsequent deed sale.",
        reads_table="foreclosures",
        module="src.connectors.lis_pendens_outcomes",
        # Deeds + lis-pendens both load Mon-Sat 05:00 (crontab.txt item 5) —
        # daily, not weekly; corrected after review caught the mismatch.
        cadence="daily Mon-Sat, after the liens/deeds/judgments loader",
        sla_minutes=1500,
        off_days=frozenset({6}),
        enabled=True,
    ),
    "probate_lien_outcomes": ConnectorSpec(
        source_type="probate_lien_outcomes",
        description="Probate and code-enforcement-lien lifecycles resolved by a subsequent deed sale.",
        reads_table="legal_proceedings, legal_and_liens, code_violations",
        module="src.connectors.probate_lien_outcomes",
        cadence="weekly, after the probate/lien loaders",
        sla_minutes=10_140,
        off_days=frozenset(),
        enabled=True,
    ),
    "outcome_label_layer": ConnectorSpec(
        source_type="outcome_label_layer",
        description="Label layer — promotes staged OutcomeCandidate rows into DealOutcome (CDE-10).",
        reads_table="outcome_candidates",
        module="src.connectors.label_layer",
        cadence="daily, after all outcome connectors have staged",
        sla_minutes=1500,
        off_days=frozenset(),
        enabled=True,
    ),
    "dor_sale_outcomes": ConnectorSpec(
        source_type="dor_sale_outcomes",
        description="Florida DOR statewide sales file — cross-county normalized qualified sales.",
        reads_table="dor_sales",
        module="src.connectors.dor_sale_outcomes",
        cadence="monthly, after the DOR SDF download (2nd of month)",
        sla_minutes=131_040,  # ~91 days + grace — DOR posts 3 rolls/year
        off_days=frozenset(),
        enabled=True,
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
