"""A fake SQLAlchemy session for the packaging tests: answers by SQL fragment, records every statement."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterable, Union


class Row(dict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __iter__(self):
        return iter(self.values())

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return dict.__getitem__(self, key)


class Result:
    def __init__(self, rows: Iterable[dict], rowcount: int = 1) -> None:
        self._rows = [Row(r) for r in rows]
        self.rowcount = rowcount

    def mappings(self) -> "Result":
        return self

    def all(self) -> list[Row]:
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def one(self):
        return self._rows[0]

    def scalar(self):
        return self._rows[0][0] if self._rows else None

    def scalar_one(self):
        return self._rows[0][0]

    def scalar_one_or_none(self):
        return self.scalar()

    def scalars(self):
        return [r[0] for r in self._rows]

    def __iter__(self):
        return iter(self._rows)


Answer = Union[list, Callable[[dict], Any]]


class FakeSession:
    """``answers`` maps a SQL fragment to rows (list of dicts) or to a function of the bound params."""

    def __init__(self, answers: dict[str, Answer] | None = None) -> None:
        self.answers = dict(answers or {})
        self.executed: list[tuple[str, dict]] = []
        self.commits = 0

    def execute(self, statement, params=None):
        sql = str(statement)
        params = dict(params or {})
        self.executed.append((sql, params))
        for fragment, answer in self.answers.items():
            if fragment in sql:
                value = answer(params) if callable(answer) else answer
                return value if isinstance(value, Result) else Result(value)
        return Result([], rowcount=1)

    def ran(self, fragment: str) -> list[dict]:
        return [params for sql, params in self.executed if fragment in sql]

    @contextmanager
    def begin_nested(self):
        yield self

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass
