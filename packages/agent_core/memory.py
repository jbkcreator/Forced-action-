"""Relational heuristic memory spine: standing rules in ``agent_memory``.

Active rules are compiled into ``<standing_rules>`` on every turn (see governance), so a rule
given in one Slack thread governs every later conversation. Rules are deactivated, never deleted.

The cap on active rules is enforced when a rule is saved, never when rules are loaded: every
active rule is always applied, and a save beyond the cap is refused so nobody believes a rule is
in force when it is not.
"""
from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text

from .store import AgentStore, utc_now

MAX_ACTIVE_RULES = 50


class MemoryFull(RuntimeError):
    """Saving would exceed the active-rule cap; a rule has to be deactivated first."""


@dataclass(frozen=True)
class StandingRule:
    memory_id: str
    category: str
    rule_text: str
    source_thread_ts: str | None
    is_active: bool


def _row_to_rule(row: Mapping[str, Any]) -> StandingRule:
    return StandingRule(memory_id=str(row["memory_id"]), category=row["category"], rule_text=row["rule_text"],
                        source_thread_ts=row["source_thread_ts"], is_active=bool(row["is_active"]))


def _same_text(rule_text: str) -> str:
    return " ".join(rule_text.lower().split())


class AgentMemory:
    def __init__(self, store: AgentStore, clock: Callable[[], datetime] = utc_now) -> None:
        self._store = store
        self._table = store.table("agent_memory")
        self._clock = clock

    def _active(self, conn) -> list[StandingRule]:
        rows = conn.execute(
            text(f"SELECT * FROM {self._table} WHERE is_active ORDER BY created_at, memory_id")
        ).mappings().all()
        return [_row_to_rule(row) for row in rows]

    def active_rules(self) -> list[StandingRule]:
        """Every active rule, oldest first. No limit: the cap is enforced in :meth:`save_rule`."""
        with self._store.transaction() as conn:
            return self._active(conn)

    def save_rule(self, *, category: str, rule_text: str, source_thread_ts: str | None) -> tuple[StandingRule, bool]:
        """Store a rule; an identical active rule is returned instead of duplicated. Returns (rule, created).

        Raises :class:`MemoryFull` when :data:`MAX_ACTIVE_RULES` rules are already active.
        """
        cleaned = " ".join(rule_text.split())
        memory_id = str(uuid.uuid4())
        with self._store.transaction() as conn:
            active = self._active(conn)
            for existing in active:
                if _same_text(existing.rule_text) == _same_text(cleaned):
                    return existing, False
            if len(active) >= MAX_ACTIVE_RULES:
                raise MemoryFull(f"{len(active)} standing rules are already active (limit {MAX_ACTIVE_RULES}); "
                                 "deactivate one before adding another")
            conn.execute(
                text(f"INSERT INTO {self._table} (memory_id, category, rule_text, source_thread_ts, is_active, created_at) "
                     "VALUES (:memory_id, :category, :rule_text, :source_thread_ts, :is_active, :created_at)"),
                {"memory_id": memory_id, "category": category, "rule_text": cleaned,
                 "source_thread_ts": source_thread_ts, "is_active": True, "created_at": self._clock()},
            )
        return StandingRule(memory_id, category, cleaned, source_thread_ts, True), True

    def deactivate_rule(self, memory_id: str) -> bool:
        with self._store.transaction() as conn:
            result = conn.execute(
                text(f"UPDATE {self._table} SET is_active = :inactive WHERE memory_id = :memory_id AND is_active"),
                {"inactive": False, "memory_id": memory_id},
            )
        return result.rowcount == 1
