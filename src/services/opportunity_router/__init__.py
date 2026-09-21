"""WP-T2-11 — Green/Yellow/Red Opportunity Router.

Thin consumer layer that collapses Lender Box eligibility, borrower ledger
state, scenario-builder ARV, and send-layer suppression into one GYR routing
decision per opportunity. No new revenue math; no new suppression store.

Public API:
    run_sweep(db, as_of)        — nightly full sweep (all open opportunities)
    reevaluate(ids, db)         — event-driven subset re-evaluation
"""
from .router import reevaluate, run_staleness_pass, run_sweep

__all__ = ["run_sweep", "reevaluate", "run_staleness_pass"]
