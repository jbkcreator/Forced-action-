"""Per-phone serialization for the "last call" fields written to the other platform.

``bd_result_to_ghl`` and ``ghl_result_to_bd`` write a "last result" that must always be the phone's newest call. Two
writers can run for one phone at once (the CDR follow-up thread for a new call, the sweep retrying an older one), and
a check of "is this call the latest?" made before the external write is not enough on its own: the older writer could
pass it, the newer one write, then the older one finish last and overwrite it.

``lock_phone_and_check_latest`` closes that gap. It takes a transaction-level advisory lock for the phone (one lock
space per direction), waiting if another writer holds it, and only then reads the phone's latest call. The lock is held
until the caller's transaction ends, i.e. after the external write and the row update are committed. So whichever
writer comes second sees the first one's work: an older call either writes before the newer one (which then overwrites
it) or finds it is no longer the latest and leaves the field alone. Callers lock phones in sorted order, so two batches
can never deadlock on each other.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

# One advisory-lock space per direction (two-int form: space, hashtext(phone)), next to the other lending lock keys.
BD_TO_GHL_LOCK_SPACE = 7_302_812
GHL_TO_BD_LOCK_SPACE = 7_302_813

_LOCK = text("SELECT pg_advisory_xact_lock(:space, hashtext(:phone))")
_LATEST = text(
    "SELECT id FROM lending.call_dispositions "
    "WHERE phone = :phone AND call_source = :source AND call_ended_at IS NOT NULL "
    "ORDER BY call_ended_at DESC, id DESC LIMIT 1"
)


def lock_phone_and_check_latest(db: Session, *, space: int, phone: str, source: str, row_id: int) -> bool:
    """Wait for the phone's write lock (held until the transaction ends), then whether ``row_id`` is its latest call
    from ``source``. Does not commit."""
    db.execute(_LOCK, {"space": space, "phone": phone})
    return db.execute(_LATEST, {"phone": phone, "source": source}).scalar() == row_id
