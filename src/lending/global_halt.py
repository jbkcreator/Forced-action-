"""Lending emergency halt (Runbook § 7, Centralized Emergency Kill Switch): ``LENDING_GLOBAL_HALT`` in Postgres.

An approver types ``@Cora emergency halt`` in a channel Cora's bot is in. That sets the flag in
``lending.global_halt`` (with a row in ``lending.global_halt_events``), halts Cora's own sends, and posts an alert
to ``#next-deal-lending`` (``LENDING_PROGRESS_CHANNEL``). ``@Cora emergency resume`` clears the flag only; Cora's
own halt still needs ``resume``.

While the flag is set, every GoHighLevel SMS and email this code sends is refused (``ghl_sms`` send path), the
LendingFlow intro-SMS tag that starts the GHL workflow is not added, and no new contacts are loaded into the
dialer. Two things cannot be stopped from here and the alert says so: calls already queued in BatchDialer (its
API has no campaign-pause call) and automations that run inside GHL itself. If the flag cannot be read, sends are
treated as halted.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Callable, Collection
from typing import Any, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_READ = text("SELECT halted FROM lending.global_halt WHERE id = 1")
_UPSERT = text("""
    INSERT INTO lending.global_halt (id, halted, reason, set_by, set_at) VALUES (1, :halted, :reason, :set_by, CURRENT_TIMESTAMP)
    ON CONFLICT (id) DO UPDATE SET halted = excluded.halted, reason = excluded.reason, set_by = excluded.set_by,
                                   set_at = excluded.set_at
""")
_EVENT = text("INSERT INTO lending.global_halt_events (halted, reason, set_by) VALUES (:halted, :reason, :set_by)")

_MENTION = re.compile(r"<@[^>]+>")
_WORD = re.compile(r"[a-z]+")
_HALT_WORDS = frozenset({"halt", "stop"})
_RESUME_WORDS = frozenset({"resume"})
_FILLER = frozenset({"cora", "please", "now"})

NOT_STOPPED_HERE = ("Still to do by hand: pause the lending campaigns in BatchDialer (its API has no pause call) "
                    "and turn off any GoHighLevel workflows or Conversation AI that send on their own.")


def _default_session():
    from src.lending.db import lending_session

    return lending_session()


# Read at call time so tests can swap in a fake and never touch the shared database.
_session_factory: Callable[[], Any] = _default_session


def is_globally_halted() -> bool:
    """True while the emergency halt is on, or when the flag cannot be read (fail closed)."""
    try:
        with _session_factory() as db:
            return bool(db.execute(_READ).scalar())
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[global-halt] flag unreadable (%s); treating lending as halted", type(exc).__name__)
        return True


def set_global_halt(halted: bool, *, reason: str, set_by: str) -> None:
    """Set or clear the flag and record the change, in one transaction. Raises if it cannot be written."""
    params = {"halted": halted, "reason": reason, "set_by": set_by}
    with _session_factory() as db:
        db.execute(_UPSERT, params)
        db.execute(_EVENT, params)
    logger.warning("[global-halt] lending emergency halt set to %s by %s", halted, set_by)


def emergency_command(message_text: str) -> Optional[str]:
    """``"halt"`` or ``"resume"`` for "@Cora emergency halt" / "@Cora emergency resume", else None. Only the
    exact command (with an optional mention, "cora", "please" or "now") counts, so a question about it does not."""
    words = _WORD.findall(_MENTION.sub(" ", message_text or "").lower())
    words = [word for word in words if word not in _FILLER]
    if len(words) != 2 or words[0] != "emergency":
        return None
    if words[1] in _HALT_WORDS:
        return "halt"
    if words[1] in _RESUME_WORDS:
        return "resume"
    return None


class EmergencyHalt:
    """The Slack command, run by Cora's session before anything else sees the message."""

    def __init__(self, *, chat: Any, cora_halt: Any, approver_ids: Collection[str],
                 alert_channel: Optional[str]) -> None:
        self._chat = chat
        self._cora_halt = cora_halt
        self._approvers = frozenset(approver_ids)
        self._alert_channel = alert_channel

    def __call__(self, message: Any) -> bool:
        """Handle the message if it is an emergency command; True when it was one."""
        command = emergency_command(message.text)
        if command is None:
            return False
        if message.user_id not in self._approvers:
            self._reply(message, "Only an approver can use the emergency halt.")
            return True
        if command == "halt":
            self._halt(message)
        else:
            self._resume(message)
        return True

    def _halt(self, message: Any) -> None:
        reason = f"emergency halt from Slack by {message.user_id}"
        try:
            set_global_halt(True, reason=reason, set_by=message.user_id)
        except Exception as exc:
            logger.error("[global-halt] could not set the emergency halt (%s)", type(exc).__name__)
            self._cora_halt.set(reason=reason, user_id=message.user_id)
            self._reply(message, ":rotating_light: The lending-wide halt could NOT be saved; only Cora is halted. "
                                 "Pause BatchDialer and GoHighLevel by hand now.")
            return
        self._cora_halt.set(reason=reason, user_id=message.user_id)
        alert = (f":rotating_light: *Lending emergency halt* by <@{message.user_id}>. Stopped: every GoHighLevel "
                 "text and email this system sends, the LendingFlow intro text, new dialer loads and Cora's sends. "
                 f"{NOT_STOPPED_HERE} Say `@Cora emergency resume` to lift it.")
        posted = self._alert_channel and self._chat.post(text=alert, channel=self._alert_channel).ok
        if not posted:
            logger.error("[global-halt] alert not posted to the progress channel")
        self._reply(message, alert if posted else alert + "\n(The alert could not be posted to #next-deal-lending.)")

    def _resume(self, message: Any) -> None:
        try:
            set_global_halt(False, reason=f"emergency resume from Slack by {message.user_id}", set_by=message.user_id)
        except Exception as exc:
            logger.error("[global-halt] could not clear the emergency halt (%s)", type(exc).__name__)
            self._reply(message, "The emergency halt could not be lifted; it is still on. Try again.")
            return
        text_ = (f":arrow_forward: Lending emergency halt lifted by <@{message.user_id}>. Texts, emails, intro texts "
                 "and dialer loads may run again. Cora stays halted until an approver says `resume`; BatchDialer and "
                 "GoHighLevel must be turned back on by hand.")
        if self._alert_channel:
            self._chat.post(text=text_, channel=self._alert_channel)
        self._reply(message, text_)

    def _reply(self, message: Any, reply_text: str) -> None:
        self._chat.post(text=reply_text, channel=message.channel, thread_ts=message.reply_thread_ts)
