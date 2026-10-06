"""PR 318 re-review #8: the warm-network CSV loader must not silently read zero rows. No database."""
from __future__ import annotations

import pytest

from src.tasks import lending_warm_network_suppress as mod


def _csv(tmp_path, text, encoding="utf-8"):
    path = tmp_path / "warm.csv"
    path.write_bytes(text.encode(encoding))
    return path


@pytest.mark.parametrize("content,encoding", [
    ("name,phone\nJane,(813) 555-9901\n", "utf-8"),
    ("phone,name\n(813) 555-9901,Jane\n", "utf-8-sig"),      # Excel "CSV UTF-8": BOM on the header
    ("Name,Phone\nJane,(813) 555-9901\n", "utf-8"),           # header case
    ("PHONE\n(813) 555-9901\n", "utf-8-sig"),
])
def test_phones_are_read_whatever_the_header_case_or_bom(tmp_path, content, encoding):
    assert mod._read_phones(_csv(tmp_path, content, encoding)) == ["(813) 555-9901"]


def test_a_file_without_a_phone_column_reads_nothing(tmp_path):
    assert mod._read_phones(_csv(tmp_path, "name,mobile\nJane,8135559901\n")) == []


@pytest.mark.parametrize("content", ["name,mobile\nJane,8135559901\n", "phone\nnot-a-phone\n", "phone\n"])
def test_a_run_that_finds_no_valid_phone_exits_non_zero_and_never_opens_the_db(tmp_path, monkeypatch, content):
    monkeypatch.setattr(mod, "get_db_context", lambda: (_ for _ in ()).throw(AssertionError("opened the DB")))
    assert mod.main(["--input", str(_csv(tmp_path, content)), "--apply"]) == 1
