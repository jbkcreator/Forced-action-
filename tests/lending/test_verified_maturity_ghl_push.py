"""One-off GHL push of loaded Verified maturity numbers."""
from __future__ import annotations

from src.lending.wholesaler_lists import push_to_ghl
from src.tasks import lending_verified_maturity_ghl_push as task


def _row(phone: str, **kw):
    base = {"source_record_ref": f"pr_list:verified_maturity:{phone}", "phone": phone, "borrower_name": "A B",
            "entity_name": None, "state": "FL", "warm": False, "suppressed": False}
    return {**base, **kw}


def test_tags_carry_list_cold_and_state():
    assert task.maturity_tags({"state": "GA"}) == ["ndl-verified-maturity", "ndl-cold", "ndl-state-ga"]
    assert task.maturity_tags({"state": None}) == ["ndl-verified-maturity", "ndl-cold"]


def test_select_skips_warm_and_suppressed():
    records, skipped = task.select_records([_row("+1"), _row("+2", warm=True), _row("+3", suppressed=True)])
    assert [r["phone"] for r in records] == ["+1"]
    assert dict(skipped) == {"WARM_LEAD": 1, "SUPPRESSED": 1}


def test_push_uses_given_tags():
    pushed = []

    class Sink:
        def push(self, record, tags):
            pushed.append(tags)

    records, _ = task.select_records([_row("+1", state="GA")])
    result = push_to_ghl(records, Sink(), tags_for=task.maturity_tags)
    assert result.pushed == 1 and pushed == [["ndl-verified-maturity", "ndl-cold", "ndl-state-ga"]]


def test_live_refuses_without_matching_confirm(monkeypatch):
    class Ctx:
        def __enter__(self):
            class S:
                def execute(self, *a, **k):
                    class R:
                        def mappings(self):
                            return [_row("+1")]
                    return R()
            return S()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(task, "get_db_context", Ctx)
    monkeypatch.setattr(task, "ghl_sink", lambda: (_ for _ in ()).throw(AssertionError("no push")))
    assert task.main(["--live", "--confirm", "5"]) == task.EXIT_NOT_CONFIRMED
