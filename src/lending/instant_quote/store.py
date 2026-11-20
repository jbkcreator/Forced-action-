"""Persist each instant quote as an audit row (``lending.instant_quotes``)."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Optional, Protocol

from sqlalchemy import text

from src.lending.db import lending_session


@dataclass(frozen=True)
class QuoteRecord:
    status: str
    reason: Optional[str]
    request: dict[str, Any]
    result: dict[str, Any]
    rules_fired: list[str]
    unconfirmed: bool
    api_key_label: str


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def audit_record(request: Any, result: Any, caller_label: str) -> QuoteRecord:
    """The audit row for one quote: the request and result as JSON, and who asked (an API key bucket, or "cora")."""
    return QuoteRecord(
        status=result.status,
        reason=result.reason,
        request=_json_safe(asdict(request)),
        result=_json_safe(asdict(result)),
        rules_fired=list(result.rules_fired),
        unconfirmed=result.unconfirmed,
        api_key_label=caller_label,
    )


class QuoteStore(Protocol):
    def save(self, record: QuoteRecord) -> str:
        """Store the record; return its quote_id."""
        ...


class SqlQuoteStore:
    """Commits each record in its own transaction before returning, so no quote leaves unaudited."""

    def save(self, record: QuoteRecord) -> str:
        with lending_session() as db:
            quote_id = db.execute(
                text(
                    """
                    INSERT INTO lending.instant_quotes
                        (status, reason, request, result, rules_fired, unconfirmed, api_key_label)
                    VALUES (:status, :reason, CAST(:request AS jsonb), CAST(:result AS jsonb),
                            CAST(:rules_fired AS jsonb), :unconfirmed, :api_key_label)
                    RETURNING id
                    """
                ),
                {
                    "status": record.status,
                    "reason": record.reason,
                    "request": json.dumps(record.request),
                    "result": json.dumps(record.result),
                    "rules_fired": json.dumps(record.rules_fired),
                    "unconfirmed": record.unconfirmed,
                    "api_key_label": record.api_key_label,
                },
            ).scalar_one()
        return str(quote_id)
