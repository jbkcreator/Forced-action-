"""90-day cohort and batch tracking (Runbook § 4.6, Item 6).

A batch is one LendingFlow purchase (lending.lendingflow_batches: name, purchase date, lead count). Delivered leads
fill the batches in arrival order. Report: python -m src.lending.cohort_report
"""
from __future__ import annotations

# Funded conversion is counted this many days after a lead was delivered (lendingflow_leads.received_at).
FUNDED_WINDOWS_DAYS = (30, 60, 90)

# GHL pipeline stage names (lower-cased) that mean "loan funded", read from lending.ghl_stage_events. The "Booked
# Calls" pipeline has a stage named "Funded" (GHL API, 2026-10-19); Josh still confirms it is the one he uses.
FUNDED_STAGE_KEYS = frozenset({"funded"})

# Field of the /webhooks/lending/ghl-stage body holding the origination fee: the opportunity's monetary value, sent
# by the GHL stage webhook as {{opportunity.monetary_value}}. Josh enters the fee there when the loan funds.
FEE_FIELD = "monetary_value"

# lending.lendingflow_replacements.status
REPLACEMENT_REQUESTED = "requested"
REPLACEMENT_CREDITED = "credited"
REPLACEMENT_DENIED = "denied"
REPLACEMENT_STATUSES = (REPLACEMENT_REQUESTED, REPLACEMENT_CREDITED, REPLACEMENT_DENIED)
