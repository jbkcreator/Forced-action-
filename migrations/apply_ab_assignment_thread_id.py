"""SUPERSEDED — do not run. See migrations/apply_agent_lane_experiments.py
and migrations/apply_agent_lane_experiment_separation_cleanup.py instead.

Originally added ab_assignments.opportunity_thread_id (REVINT-v2.2 I3
review fix) so get_price_variant() could assign a price-band arm to a cold
Cora prospect before they're a subscriber. That coupled Agent Lane's
price-band tests to Lifecycle's ab_assignments table/blast radius. The
thread-keyed path now lives on its own table, agent_lane_experiment_
assignments (created by apply_agent_lane_experiments.py); the columns/
constraints this script added to ab_assignments are removed by
apply_agent_lane_experiment_separation_cleanup.py once the code redirect
(src/services/agent_lane_experiment_engine.py) is confirmed live.

Left in place, DDL emptied, for history — do not re-add this file's
original DDL; it's what's being undone.
"""
from __future__ import annotations

import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL: list[str] = []


def main() -> None:
    logger.info(
        "apply_ab_assignment_thread_id: superseded, no-op. See "
        "apply_agent_lane_experiments.py / "
        "apply_agent_lane_experiment_separation_cleanup.py."
    )


if __name__ == "__main__":
    main()
