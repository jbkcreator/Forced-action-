"""Broker admin service — Layer 1 of Broker State Machine.

Covers broker identity: create, list, get, activate/deactivate.
Lane assignment and transitions are Layer 2 (broker_transitions).
"""
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.core.models import Broker


class BrokerAlreadyExists(Exception):
    """Raised when a broker with the given email already exists."""


class BrokerNotFound(Exception):
    """Raised when no broker matches the given broker_id."""


def create_broker(
    db: Session,
    email: str,
    name: str,
    actor: str = "admin",
) -> Broker:
    email = email.strip().lower()
    name = name.strip()

    existing = db.execute(
        sa_text("SELECT broker_id FROM brokers WHERE email = :email"),
        {"email": email},
    ).first()
    if existing is not None:
        raise BrokerAlreadyExists(f"Broker with email {email!r} already exists.")

    broker = Broker(
        email=email,
        name=name,
        role="broker",
        is_active=True,
        reset_token=secrets.token_urlsafe(32),
        reset_token_expires_at=datetime.now(timezone.utc) + timedelta(days=30),
    )
    db.add(broker)
    db.flush()
    return broker


def list_brokers(db: Session, include_inactive: bool = False) -> list[Any]:
    sql = "SELECT * FROM brokers"
    if not include_inactive:
        sql += " WHERE is_active = true"
    sql += " ORDER BY created_at"
    return db.execute(sa_text(sql)).fetchall()


def get_broker(db: Session, broker_id: str) -> Any:
    row = db.execute(
        sa_text("SELECT * FROM brokers WHERE broker_id = :id"),
        {"id": broker_id},
    ).first()
    if row is None:
        raise BrokerNotFound(f"Broker {broker_id!r} not found.")
    return row


def set_broker_active(
    db: Session,
    broker_id: str,
    is_active: bool,
    actor: str = "admin",
) -> Any:
    row = db.execute(
        sa_text(
            "UPDATE brokers SET is_active = :v, updated_at = NOW()"
            " WHERE broker_id = :id RETURNING *"
        ),
        {"v": is_active, "id": broker_id},
    ).first()
    if row is None:
        raise BrokerNotFound(f"Broker {broker_id!r} not found.")
    db.flush()
    return row
