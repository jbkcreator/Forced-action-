"""Persist each instant quote as an audit row (``lending.instant_quotes``)."""
from __future__ import annotations

import json
from dataclasses import dataclass
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
