"""
In-memory exact-match response cache for Concierge Chat.

Key: SHA-256 of (normalized user text + knowledge fingerprint).
Bounded LRU; capacity in MAX_ENTRIES. Process-local, lost on restart.
"""

import hashlib
import re
import threading
from collections import OrderedDict
from typing import Any, Optional

MAX_ENTRIES = 500

_lock = threading.Lock()
_store: "OrderedDict[str, Any]" = OrderedDict()

_WS_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    return _WS_RE.sub(" ", text.strip().lower())


def _key(user_text: str, knowledge: str) -> str:
    kb_fp = hashlib.sha256(knowledge.encode("utf-8")).hexdigest()[:16]
    payload = f"{kb_fp}|{_normalize(user_text)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get(user_text: str, knowledge: str) -> Optional[Any]:
    k = _key(user_text, knowledge)
    with _lock:
        if k in _store:
            _store.move_to_end(k)
            return _store[k]
    return None


def put(user_text: str, knowledge: str, value: Any) -> None:
    k = _key(user_text, knowledge)
    with _lock:
        _store[k] = value
        _store.move_to_end(k)
        while len(_store) > MAX_ENTRIES:
            _store.popitem(last=False)


def clear() -> None:
    with _lock:
        _store.clear()
