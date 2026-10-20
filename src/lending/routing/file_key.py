"""Which file the router is working on: a LendingFlow lead or a partner file.

The routing tables carry one key per file, ``lead_id`` (lending.lendingflow_leads) or ``loan_file_id``
(lending.loan_files, written by the Partner Door).  ``FileKey`` is that choice as a value, so the router,
the decline and outcome handlers and the admin API treat both the same way.

``column`` comes from a closed two-value mapping, never from request data, which is why it is safe to
place in SQL as an identifier; every value still goes through a bind parameter.
"""
from __future__ import annotations

from dataclasses import dataclass

LEAD = "lead"
LOAN_FILE = "loan_file"

_COLUMN = {LEAD: "lead_id", LOAN_FILE: "loan_file_id"}
_SOURCE = {LEAD: "lendingflow", LOAN_FILE: "partner"}
_KIND_OF_SOURCE = {source: kind for kind, source in _SOURCE.items()}


@dataclass(frozen=True)
class FileKey:
    kind: str
    id: int

    def __post_init__(self) -> None:
        if self.kind not in _COLUMN:
            raise ValueError(f"unknown file kind {self.kind!r}")

    @property
    def column(self) -> str:
        return _COLUMN[self.kind]

    @property
    def source(self) -> str:
        """The name the admin API and screen use: ``lendingflow`` or ``partner``."""
        return _SOURCE[self.kind]

    @property
    def label(self) -> str:
        return f"File #{self.id}" if self.kind == LEAD else f"Partner file #{self.id}"


def lead_key(lead_id: int) -> FileKey:
    return FileKey(LEAD, int(lead_id))


def loan_file_key(loan_file_id: int) -> FileKey:
    return FileKey(LOAN_FILE, int(loan_file_id))


def key_from_source(source: str, file_id: int) -> FileKey:
    """``FileKey`` for an API ``source`` value; raises ValueError for an unknown one."""
    if source not in _KIND_OF_SOURCE:
        raise ValueError(f"source must be one of {sorted(_KIND_OF_SOURCE)}")
    return FileKey(_KIND_OF_SOURCE[source], int(file_id))


def key_from_row(row) -> FileKey:
    """``FileKey`` for a routing or event row (either ``lead_id`` or ``loan_file_id`` is set)."""
    if row["lead_id"] is not None:
        return lead_key(row["lead_id"])
    return loan_file_key(row["loan_file_id"])
