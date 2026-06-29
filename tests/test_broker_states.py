"""Tests for Broker State Machine Layer 2 — pure state/validation logic."""
import pytest

from config.broker_states import (
    ALLOWED_TRANSITIONS,
    BROKER_STATES,
    REASON_CODES,
    TERMINAL_STATES,
    IllegalBrokerTransition,
    InvalidBrokerReasonCode,
    InvalidBrokerState,
    get_allowed_next_states,
    is_terminal_state,
    is_valid_state,
    is_valid_transition,
    requires_close_payload,
    validate_reason_code,
    validate_state,
    validate_transition,
)


# ── State membership ──────────────────────────────────────────────────────────

class TestStates:
    def test_all_expected_states_are_valid(self):
        for state in ("unassigned", "assigned", "working", "quoted", "committed",
                      "closed_won", "closed_lost"):
            assert is_valid_state(state), f"{state!r} should be valid"

    def test_unknown_state_is_invalid(self):
        assert not is_valid_state("pending")
        assert not is_valid_state("")
        assert not is_valid_state("ASSIGNED")


# ── Reason codes ──────────────────────────────────────────────────────────────

class TestReasonCodes:
    def test_all_expected_reason_codes_are_valid(self):
        for code in ("no_contact", "not_interested", "price", "qualified",
                     "docs_received", "funded", "lost_other"):
            assert code in REASON_CODES, f"{code!r} should be a valid reason code"

    def test_unknown_reason_code_raises(self):
        with pytest.raises(InvalidBrokerReasonCode):
            validate_reason_code("random_code")

    def test_empty_reason_code_raises(self):
        with pytest.raises(InvalidBrokerReasonCode):
            validate_reason_code("")


# ── Terminal states ───────────────────────────────────────────────────────────

class TestTerminalStates:
    def test_closed_won_is_terminal(self):
        assert is_terminal_state("closed_won")

    def test_closed_lost_is_terminal(self):
        assert is_terminal_state("closed_lost")

    def test_working_is_not_terminal(self):
        assert not is_terminal_state("working")

    def test_committed_is_not_terminal(self):
        assert not is_terminal_state("committed")

    def test_terminal_states_have_no_allowed_transitions(self):
        for state in TERMINAL_STATES:
            assert ALLOWED_TRANSITIONS[state] == ()


# ── Legal transitions ─────────────────────────────────────────────────────────

class TestLegalTransitions:
    def test_unassigned_to_assigned(self):
        assert is_valid_transition("unassigned", "assigned")

    def test_assigned_to_working(self):
        assert is_valid_transition("assigned", "working")

    def test_working_to_quoted(self):
        assert is_valid_transition("working", "quoted")

    def test_quoted_to_committed(self):
        assert is_valid_transition("quoted", "committed")

    def test_committed_to_closed_won(self):
        assert is_valid_transition("committed", "closed_won")

    def test_committed_to_closed_lost(self):
        assert is_valid_transition("committed", "closed_lost")


# ── Illegal transitions ───────────────────────────────────────────────────────

class TestIllegalTransitions:
    def test_assigned_to_closed_won_is_illegal(self):
        assert not is_valid_transition("assigned", "closed_won")

    def test_working_to_committed_is_illegal(self):
        assert not is_valid_transition("working", "committed")

    def test_quoted_to_closed_won_is_illegal(self):
        assert not is_valid_transition("quoted", "closed_won")

    def test_closed_won_to_working_is_illegal(self):
        assert not is_valid_transition("closed_won", "working")

    def test_closed_lost_to_working_is_illegal(self):
        assert not is_valid_transition("closed_lost", "working")


# ── get_allowed_next_states ───────────────────────────────────────────────────

class TestGetAllowedNextStates:
    def test_unassigned_allows_only_assigned(self):
        assert get_allowed_next_states("unassigned") == ("assigned",)

    def test_assigned_allows_working_and_closed_lost(self):
        result = get_allowed_next_states("assigned")
        assert set(result) == {"working", "closed_lost"}

    def test_closed_won_allows_nothing(self):
        assert get_allowed_next_states("closed_won") == ()

    def test_closed_lost_allows_nothing(self):
        assert get_allowed_next_states("closed_lost") == ()

    def test_unknown_state_raises(self):
        with pytest.raises(InvalidBrokerState):
            get_allowed_next_states("nonexistent")


# ── validate_state ────────────────────────────────────────────────────────────

class TestValidateState:
    def test_valid_state_passes(self):
        validate_state("working")  # no exception

    def test_invalid_state_raises(self):
        with pytest.raises(InvalidBrokerState):
            validate_state("bogus")


# ── validate_transition ───────────────────────────────────────────────────────

class TestValidateTransition:
    def test_legal_transition_passes(self):
        validate_transition("working", "quoted", "qualified")  # no exception

    def test_illegal_transition_raises(self):
        with pytest.raises(IllegalBrokerTransition):
            validate_transition("assigned", "closed_won", "funded")

    def test_unknown_from_state_raises_invalid_state(self):
        with pytest.raises(InvalidBrokerState):
            validate_transition("bogus", "working", "qualified")

    def test_unknown_to_state_raises_invalid_state(self):
        with pytest.raises(InvalidBrokerState):
            validate_transition("working", "bogus", "qualified")

    def test_unknown_reason_code_raises(self):
        with pytest.raises(InvalidBrokerReasonCode):
            validate_transition("working", "quoted", "bad_code")

    def test_terminal_from_state_raises_illegal_transition(self):
        with pytest.raises(IllegalBrokerTransition):
            validate_transition("closed_won", "working", "qualified")


# ── requires_close_payload ────────────────────────────────────────────────────

class TestRequiresClosePayload:
    def test_closed_won_requires_payload(self):
        assert requires_close_payload("closed_won") is True

    def test_closed_lost_does_not_require_payload(self):
        assert requires_close_payload("closed_lost") is False

    def test_working_does_not_require_payload(self):
        assert requires_close_payload("working") is False
