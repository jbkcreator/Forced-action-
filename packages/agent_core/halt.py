"""Kill switch: a persisted halt flag every process checks before acting.

The flag lives in the database, not in process memory. The Slack session and the relay can run
in different processes, and a halt set in one must stop sends in the other. It also survives a
restart on purpose: a deploy or crash must not quietly resume work the operator stopped. Only an
approver's resume command clears it.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import datetime

from sqlalchemy import text

from .store import AgentStore, utc_now

logger = logging.getLogger(__name__)

_HALT_ROW_ID = 1

# A missed halt is the one unsafe outcome, so matching is broad and typo tolerant. A stop
# word followed by anything other than global filler ("stop texting Smith") is not a halt.
_STOP_TOKENS = frozenset({"stop", "stpo", "stahp", "halt", "hlt", "pause", "paus", "kill",
                          "cease", "shutdown", "abort"})
_UNHALT_TOKENS = frozenset({"resume", "unhalt", "unpause", "continue", "reactivate", "restart"})
_GLOBAL_FILLER = frozenset({"all", "everything", "evrything", "everythin", "everythng", "now",
                            "please", "the", "bots", "bot", "agent", "jobs", "job", "it",
                            "immediately", "right", "asap", "already", "just", "and", "completely",
                            "sending", "sends"})
_UNHALT_PHRASES = frozenset({"start again", "turn back on"})
_WORD = re.compile(r"[a-z]+")


class AgentHalted(RuntimeError):
    """Raised by :meth:`HaltSwitch.check` while the halt flag is set."""


def _is_global_command(text_value: str, tokens: frozenset[str], filler: frozenset[str]) -> bool:
    words = _WORD.findall((text_value or "").lower())
    index = next((i for i, word in enumerate(words) if word in tokens), None)
    if index is None:
        return False
    return all(word in filler for word in words[index + 1:])


class HaltSwitch:
    def __init__(self, store: AgentStore, agent_name: str, clock: Callable[[], datetime] = utc_now) -> None:
        self._store = store
        self._table = store.table("agent_halt_state")
        self._clock = clock
        # Lets "stop cora" and "resume cora" read as global commands.
        self._filler = _GLOBAL_FILLER | {agent_name.lower()}
        self._agent_name = agent_name

    def _read(self) -> tuple[bool, str]:
        try:
            with self._store.transaction() as conn:
                row = conn.execute(
                    text(f"SELECT halted, reason FROM {self._table} WHERE id = :id"), {"id": _HALT_ROW_ID}
                ).mappings().first()
        except Exception:
            # Fail safe: if the flag cannot be read, sending cannot be proven allowed.
            logger.exception("halt state unreadable; treating the agent as halted")
            return True, "halt state unreadable"
        if row is None:
            return False, ""
        return bool(row["halted"]), row["reason"] or ""

    def is_halted(self) -> bool:
        return self._read()[0]

    def reason(self) -> str:
        return self._read()[1]

    def check(self) -> None:
        halted, reason = self._read()
        if halted:
            raise AgentHalted(f"{self._agent_name} is halted ({reason}); an approver must resume it")

    def _write(self, halted: bool, reason: str, user_id: str) -> None:
        with self._store.transaction() as conn:
            conn.execute(
                text(
                    f"INSERT INTO {self._table} (id, halted, reason, set_by, set_at) "
                    "VALUES (:id, :halted, :reason, :set_by, :set_at) "
                    "ON CONFLICT (id) DO UPDATE SET halted = excluded.halted, reason = excluded.reason, "
                    "set_by = excluded.set_by, set_at = excluded.set_at"
                ),
                {"id": _HALT_ROW_ID, "halted": halted, "reason": reason, "set_by": user_id,
                 "set_at": self._clock()},
            )
        logger.warning("%s halt flag set to %s by %s", self._agent_name, halted, user_id)

    def set(self, reason: str, user_id: str) -> None:
        self._write(True, reason, user_id)

    def clear(self, user_id: str) -> None:
        self._write(False, "", user_id)

    def is_halt_command(self, message: str) -> bool:
        return _is_global_command(message, _STOP_TOKENS, self._filler)

    def is_resume_command(self, message: str) -> bool:
        if (message or "").strip().lower().rstrip("!. ") in _UNHALT_PHRASES:
            return True
        return _is_global_command(message, _UNHALT_TOKENS, self._filler)
