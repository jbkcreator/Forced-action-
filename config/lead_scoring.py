"""Deterministic 1-10 lead score for the lending caller queues.

Rules only: no trained model until enough held outcomes exist. The go-live
brief defines rank 10 (verified maturity inside 90 days, borrowing entity in
good standing on Sunbiz, known equity over 35 percent, decision maker
confirmed) and asks that loan size and repeat-operator signals lift builders
and portfolio owners above one-property flippers. The ladder below rank 10 and
the two thresholds are the team's proposal, pending client confirmation.
"""
from __future__ import annotations

from decimal import Decimal

TOP_RANK = 10
MAX_RANK_BELOW_TOP = 9
MIN_RANK = 1

MATURITY_WINDOW_DAYS = 90
EQUITY_THRESHOLD_PCT = Decimal("35")

# Base rank by how many of the four rank-10 signals are known and met (0-3).
BASE_RANK_BY_SIGNALS_MET = (1, 3, 5, 7)

# Each bonus applies once, only when its signal is known.
REPEAT_OPERATOR_BONUS = 1
LARGE_LOAN_BONUS = 1

# Two or more properties held by the same Sunbiz entity, or permits pulled on
# them in the lookback window, marks a repeat operator.
REPEAT_OPERATOR_MIN_PROPERTIES = 2
REPEAT_OPERATOR_MIN_PERMITS = 2
REPEAT_OPERATOR_PERMIT_LOOKBACK_DAYS = 730

# The brief's planning figure for an average loan.
LARGE_LOAN_THRESHOLD = Decimal("300000")

SUNBIZ_GOOD_STANDING = "ACTIVE"

# Share of each caller queue drawn from below the top of the ranking, so the
# rules keep being tested against records they rate lower.
EXPLORATION_SHARE = 0.10
