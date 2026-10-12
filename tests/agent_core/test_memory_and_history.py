from __future__ import annotations

import pytest
from sqlalchemy import text

from packages.agent_core.memory import MAX_ACTIVE_RULES, AgentMemory, MemoryFull
from packages.agent_core.store import AgentStore
from packages.agent_core.thread_history import build_history

from .conftest import BOT


def test_rules_are_saved_listed_and_deactivated(store: AgentStore) -> None:
    memory = AgentMemory(store)
    rule, created = memory.save_rule(category="messaging",
                                     rule_text="Always emphasize   100% rehab funding for flippers",
                                     source_thread_ts="111.1")
    assert created and rule.rule_text == "Always emphasize 100% rehab funding for flippers"
    assert [r.memory_id for r in memory.active_rules()] == [rule.memory_id]
    assert memory.active_rules()[0].source_thread_ts == "111.1"

    assert memory.deactivate_rule(rule.memory_id)
    assert not memory.deactivate_rule(rule.memory_id)
    assert memory.active_rules() == []


def test_identical_rule_is_not_duplicated(store: AgentStore) -> None:
    memory = AgentMemory(store)
    first, _ = memory.save_rule(category="messaging", rule_text="Never text before 9am", source_thread_ts=None)
    second, created = memory.save_rule(category="scheduling", rule_text="never TEXT before 9am ", source_thread_ts="2")
    assert not created and second.memory_id == first.memory_id
    assert len(memory.active_rules()) == 1


def test_every_active_rule_is_loaded_even_past_the_cap(store: AgentStore) -> None:
    """Rules that got in some other way (or before the cap existed) are never silently dropped."""
    with store.transaction() as conn:
        for number in range(MAX_ACTIVE_RULES + 10):
            conn.execute(text("INSERT INTO agent_memory (memory_id, category, rule_text, is_active, created_at) "
                              "VALUES (:id, 'messaging', :rule, 1, :at)"),
                         {"id": f"00000000-0000-0000-0000-{number:012d}", "rule": f"Rule {number}",
                          "at": f"2026-10-01 00:{number // 60:02d}:{number % 60:02d}"})
    rules = AgentMemory(store).active_rules()
    assert len(rules) == MAX_ACTIVE_RULES + 10
    assert rules[-1].rule_text == f"Rule {MAX_ACTIVE_RULES + 9}"


def test_saving_past_the_cap_is_refused_not_silently_ignored(store: AgentStore) -> None:
    memory = AgentMemory(store)
    for number in range(MAX_ACTIVE_RULES):
        memory.save_rule(category="messaging", rule_text=f"Rule {number}", source_thread_ts=None)
    with pytest.raises(MemoryFull, match="deactivate one"):
        memory.save_rule(category="messaging", rule_text="One too many", source_thread_ts=None)
    assert "One too many" not in [r.rule_text for r in memory.active_rules()]

    _, created = memory.save_rule(category="messaging", rule_text=f"rule {MAX_ACTIVE_RULES - 1}", source_thread_ts=None)
    assert not created  # a duplicate of the newest rule is still recognised at the cap

    memory.deactivate_rule(memory.active_rules()[0].memory_id)
    _, created = memory.save_rule(category="messaging", rule_text="One too many", source_thread_ts=None)
    assert created


def test_rule_survives_into_a_new_memory_instance(store: AgentStore) -> None:
    AgentMemory(store).save_rule(category="messaging", rule_text="Sign texts as Josh", source_thread_ts="1")
    assert [r.rule_text for r in AgentMemory(store).active_rules()] == ["Sign texts as Josh"]


def _message(ts: str, text: str, user: str = "U_JOSH", **extra) -> dict:
    return {"ts": ts, "text": text, "user": user, **extra}


def test_history_maps_roles_and_skips_noise() -> None:
    replies = [
        _message("1.0", "how many leads today?"),
        _message("1.1", "Working on it…", user=BOT, bot_id="B1"),
        _message("1.2", "3 web leads today.", user=BOT, bot_id="B1"),
        _message("1.3", "deploy finished", user="U_OTHERBOT", bot_id="B2"),
        _message("1.4", "joined", subtype="channel_join"),
        _message("1.5", "and yesterday?"),
        _message("1.6", "this is the message being answered"),
    ]
    history = build_history(replies, bot_user_id=BOT, before_ts="1.6", skip_texts=frozenset({"Working on it…"}))
    assert history == [
        {"role": "user", "content": "<@U_JOSH>: how many leads today?"},
        {"role": "assistant", "content": "3 web leads today."},
        {"role": "user", "content": "<@U_JOSH>: and yesterday?"},
    ]


def test_history_is_capped_and_opens_with_a_user_turn() -> None:
    replies = [_message(f"{i}.0", f"q{i}") if i % 2 == 0 else _message(f"{i}.0", f"a{i}", user=BOT) for i in range(1, 30)]
    history = build_history(replies, bot_user_id=BOT, before_ts="99.0", limit=5)
    assert len(history) <= 5
    assert history[0]["role"] == "user"
    assert history[-1]["content"] == "a29"


def test_empty_thread_gives_empty_history() -> None:
    assert build_history([], bot_user_id=BOT, before_ts="1.0") == []
