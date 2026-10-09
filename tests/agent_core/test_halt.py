from __future__ import annotations

import pytest
from sqlalchemy import text

from packages.agent_core.halt import AgentHalted, HaltSwitch
from packages.agent_core.store import AgentStore


def test_not_halted_until_set(halt: HaltSwitch) -> None:
    assert not halt.is_halted()
    halt.check()


def test_set_and_clear_persist_across_instances(store: AgentStore, halt: HaltSwitch) -> None:
    halt.set("operator stop", "U1")
    other_process = HaltSwitch(store, "Cora")
    assert other_process.is_halted()
    assert other_process.reason() == "operator stop"
    with pytest.raises(AgentHalted):
        other_process.check()
    other_process.clear("U1")
    assert not halt.is_halted()


def test_unreadable_state_counts_as_halted(store: AgentStore, halt: HaltSwitch) -> None:
    with store.transaction() as conn:
        conn.execute(text("DROP TABLE agent_halt_state"))
    assert halt.is_halted()
    with pytest.raises(AgentHalted):
        halt.check()


@pytest.mark.parametrize("message", ["stop all", "STOP ALL!", "stop cora", "please stop everything now",
                                     "halt", "kill it", "stpo all", "pause sending"])
def test_global_halt_phrasings(halt: HaltSwitch, message: str) -> None:
    assert halt.is_halt_command(message)


@pytest.mark.parametrize("message", ["stop texting Smith", "pause the Smith follow-up", "what's the stop rate?",
                                     "how many leads today"])
def test_targeted_or_unrelated_messages_are_not_halts(halt: HaltSwitch, message: str) -> None:
    assert not halt.is_halt_command(message)


@pytest.mark.parametrize("message", ["resume", "resume cora", "Resume everything!", "start again", "unpause"])
def test_global_resume_phrasings(halt: HaltSwitch, message: str) -> None:
    assert halt.is_resume_command(message)


@pytest.mark.parametrize("message", ["resume texting Smith", "continue the Smith thread"])
def test_targeted_resume_is_not_global(halt: HaltSwitch, message: str) -> None:
    assert not halt.is_resume_command(message)
