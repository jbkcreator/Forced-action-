"""Broker State Machine — Layer 2: work-state rules and pure validation.

States are deterministic and replayable. Every transition requires a reason_code.
No DB access. No side effects. Import freely from any layer.

STATE MACHINE
─────────────
  unassigned ──► assigned ──► working ──► quoted ──► committed ──► closed_won
                     │            │           │            │
                     └────────────┴───────────┴────────────┴──► closed_lost
"""

# ── States ────────────────────────────────────────────────────────────────────

BROKER_STATES: frozenset[str] = frozenset({
    "unassigned",
    "assigned",
    "working",
    "quoted",
    "committed",
    "closed_won",
    "closed_lost",
})

TERMINAL_STATES: frozenset[str] = frozenset({
    "closed_won",
    "closed_lost",
})

# ── Allowed transitions ───────────────────────────────────────────────────────
# Maps each from-state to the set of legal to-states.
# Terminal states map to empty tuples — they cannot transition.

ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "unassigned":  ("assigned",),
    "assigned":    ("working", "closed_lost"),
    "working":     ("quoted", "closed_lost"),
    "quoted":      ("committed", "closed_lost"),
    "committed":   ("closed_won", "closed_lost"),
    "closed_won":  (),
    "closed_lost": (),
}

# ── Reason codes ──────────────────────────────────────────────────────────────

REASON_CODES: frozenset[str] = frozenset({
    "no_contact",
    "not_interested",
    "price",
    "qualified",
    "docs_received",
    "funded",
    "lost_other",
})

# ── Exceptions ────────────────────────────────────────────────────────────────


class InvalidBrokerState(ValueError):
    """Raised when a state string is not in BROKER_STATES."""


class InvalidBrokerReasonCode(ValueError):
    """Raised when a reason_code string is not in REASON_CODES."""


class IllegalBrokerTransition(ValueError):
    """Raised when a from→to transition is not in ALLOWED_TRANSITIONS."""


# ── Pure helpers ──────────────────────────────────────────────────────────────


def get_allowed_next_states(from_state: str) -> tuple[str, ...]:
    """Return the tuple of states reachable from *from_state*.

    Raises InvalidBrokerState if *from_state* is unknown.
    """
    if from_state not in BROKER_STATES:
        raise InvalidBrokerState(f"Unknown broker state: {from_state!r}")
    return ALLOWED_TRANSITIONS[from_state]


def is_valid_state(state: str) -> bool:
    return state in BROKER_STATES


def is_terminal_state(state: str) -> bool:
    return state in TERMINAL_STATES


def is_valid_transition(from_state: str, to_state: str) -> bool:
    """Return True if the transition is in ALLOWED_TRANSITIONS.

    Returns False (not raises) for unknown states — use validate_* for errors.
    """
    return to_state in ALLOWED_TRANSITIONS.get(from_state, ())


def validate_state(state: str) -> None:
    """Raise InvalidBrokerState if *state* is not a known broker state."""
    if state not in BROKER_STATES:
        raise InvalidBrokerState(f"Unknown broker state: {state!r}")


def validate_reason_code(reason_code: str) -> None:
    """Raise InvalidBrokerReasonCode if *reason_code* is not recognised."""
    if reason_code not in REASON_CODES:
        raise InvalidBrokerReasonCode(f"Unknown reason code: {reason_code!r}")


def validate_transition(from_state: str, to_state: str, reason_code: str) -> None:
    """Full guard for a proposed transition.

    Raises:
        InvalidBrokerState      — unknown from_state or to_state
        InvalidBrokerReasonCode — unknown reason_code
        IllegalBrokerTransition — transition not in ALLOWED_TRANSITIONS
    """
    validate_state(from_state)
    validate_state(to_state)
    validate_reason_code(reason_code)
    if to_state not in ALLOWED_TRANSITIONS[from_state]:
        raise IllegalBrokerTransition(
            f"Transition {from_state!r} → {to_state!r} is not allowed."
        )


def requires_close_payload(to_state: str) -> bool:
    """Return True when the transition target requires a commission payload.

    Only closed_won requires one. Payload validation is Layer 3's responsibility.
    """
    return to_state == "closed_won"
