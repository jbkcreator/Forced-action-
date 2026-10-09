"""In-process hand-off of ``LendingFlowLeadCreated`` to its consumers (T-09 intro SMS, T-10 alarms, T-12 card).

There is no event bus in lending, so consumers register a handler with ``subscribe``. ``emit`` is
called once per deduplicated, non-suppressed lead after its GHL contact exists. A handler that
raises is logged by lead id and never blocks the others. The event carries phone and email in
plaintext: log it with ``model_dump(exclude={"phone", "email"})`` or the lead id only.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Callable, Mapping

from src.lending.contracts import LendingFlowLeadCreated, LoanType

logger = logging.getLogger(__name__)

Handler = Callable[[LendingFlowLeadCreated], None]
_handlers: list[Handler] = []


def subscribe(handler: Handler) -> None:
    if handler not in _handlers:
        _handlers.append(handler)


def _clear_for_tests() -> None:
    _handlers.clear()


def build_event(lead: Mapping[str, Any]) -> LendingFlowLeadCreated:
    loan_type = lead.get("loan_type")
    amount = lead.get("loan_amount")
    return LendingFlowLeadCreated(
        lead_id=lead["lead_uuid"],
        phone=lead["phone"],
        email=lead.get("email") or "",
        created_at=lead["received_at"],
        credit_band_min_fico=lead.get("credit_band_min_fico"),
        loan_amount=Decimal(amount) if amount is not None else None,
        state=lead.get("property_state"),
        loan_type=LoanType(loan_type) if loan_type else None,
    )


def emit(event: LendingFlowLeadCreated) -> None:
    for handler in list(_handlers):
        try:
            handler(event)
        except Exception as exc:
            logger.error("[lendingflow] event handler %s failed lead=%s: %s",
                         getattr(handler, "__name__", "handler"), event.lead_id, type(exc).__name__)
