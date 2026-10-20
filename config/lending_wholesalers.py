"""Split T-11 (Section 0 line 10): FL wholesaler lists for the Partners queue.

"Hunter" is not yet defined by the client (open Q-32); it is accepted as a CSV source
label only, with no API pull. Pass condition: >= 1,500 dialable numbers loaded.
"""

# CSV ``source`` value -> source tag (mapped to the Partners queue in config/lending_queues.py).
WHOLESALER_SOURCE_TAGS: dict[str, str] = {
    "sunbiz": "wholesaler_sunbiz",
    "hunter": "wholesaler_hunter",
}
WHOLESALER_STATES: frozenset[str] = frozenset({"FL"})
REQUIRED_COLUMNS: frozenset[str] = frozenset({"source", "phone", "state"})
DIALABLE_TARGET = 1500

# GHL tags (added through the add-tags call; the GHL upsert REPLACES tags).
GHL_TAG_WHOLESALER = "ndl-wholesaler"
GHL_TAG_SOURCE_PREFIX = "ndl-src-"
GHL_TAG_STATE_PREFIX = "ndl-state-"
