"""
Name-parse resolution for Tracerfy traces (fix/enrichment-cascade-trace-type).

Covers the FIRST [MIDDLE] LAST assessor format and the Spanish two-surname
case that previously mis-traced under the wrong surname. Ambiguous names are
routed to the Tracerfy Address-Only pass (is_traceable=False).
"""
import inspect
from types import SimpleNamespace

import pytest

from src.services.tracerfy_fallback import (
    _resolve_trace_subject,
    _split_individual_name,
    run_tracerfy_fallback,
)


def _owner(name, otype="Individual"):
    return SimpleNamespace(
        owner_name=name,
        owner_type=otype,
        managing_members=None,
        registered_agent_name=None,
        registered_agent_address=None,
    )


class TestSplitIndividualName:
    @pytest.mark.parametrize("raw,first,last", [
        ("PHILLIP FINDLEY", "PHILLIP", "FINDLEY"),          # 2 tokens
        ("RICHARD JAMES FORSYTH", "RICHARD", "FORSYTH"),    # 3 tokens: drop middle
        ("PATRICK A FURLONG", "PATRICK", "FURLONG"),        # 3 tokens: middle initial
        ("SMITH, JOHN A", "JOHN", "SMITH"),                 # comma form
        ("CHER", "", "CHER"),                               # single token
    ])
    def test_traceable_by_name(self, raw, first, last):
        f, l, traceable = _split_individual_name(raw)
        assert traceable is True
        assert (f, l) == (first, last)

    @pytest.mark.parametrize("raw", [
        "LUIS D LOPEZ MORENO AURIOLES",   # 5 tokens
        "MARCOS ALFREDO ROSARIO GARCIA",  # 4 tokens
        "JUAN MIGUEL GONZALEZ SUAREZ",    # 4 tokens
        "DENNIS RENATO DE LEON MEJIA",    # particle DE
        "JASMINE DEL ROCIO PROANO",       # particle DEL
    ])
    def test_ambiguous_routes_to_address_only(self, raw):
        f, l, traceable = _split_individual_name(raw)
        assert traceable is False
        assert (f, l) == ("", "")


class TestResolveTraceSubject:
    def test_and_owner_borrows_shared_last(self):
        f, l, _addr, traceable = _resolve_trace_subject(_owner("JEFFERY AND PATRICIA SEVIGNY"))
        assert (f, l, traceable) == ("JEFFERY", "SEVIGNY", True)

    def test_and_owner_with_middle_initial(self):
        f, l, _addr, traceable = _resolve_trace_subject(_owner("GREG L AND BARBIE J BOOTH"))
        assert (f, l, traceable) == ("GREG", "BOOTH", True)

    def test_et_al_stripped(self):
        f, l, _addr, traceable = _resolve_trace_subject(_owner("AMANDA R AMESBURY ET AL"))
        assert (f, l, traceable) == ("AMANDA", "AMESBURY", True)

    def test_entity_without_individual_is_address_only(self):
        f, l, _addr, traceable = _resolve_trace_subject(_owner("MCNEIL MANAGEMENT SERVICES, INC", "Corporate"))
        assert traceable is False

    def test_spanish_name_is_address_only(self):
        f, l, _addr, traceable = _resolve_trace_subject(_owner("MARCOS ALFREDO ROSARIO GARCIA"))
        assert traceable is False


def test_run_tracerfy_fallback_accepts_trace_type():
    """Regression guard: run_cascade() calls this with trace_type=... ."""
    assert "trace_type" in inspect.signature(run_tracerfy_fallback).parameters
