"""Borrower lead score 0–10 (Runbook § 4.3, Item 3): src/lending/borrower_score.py.

Runbook § 4.3 points: under contract with a live contract in hand +4; 3+ completed flips or ground-up builds in the
trailing 36 months +3; property in Florida or Georgia +2; requested loan of $300K or more +1.
"""
from __future__ import annotations

UNDER_CONTRACT_POINTS = 4
EXPERIENCE_POINTS = 3
EXPERIENCE_MIN_PROJECTS = 3  # completed flips / ground-up builds in the trailing 36 months
TARGET_STATE_POINTS = 2
TARGET_STATES = frozenset({"FL", "GA"})
LOAN_SIZE_POINTS = 1
LOAN_SIZE_MIN = 300_000

MAX_SCORE = UNDER_CONTRACT_POINTS + EXPERIENCE_POINTS + TARGET_STATE_POINTS + LOAN_SIZE_POINTS
