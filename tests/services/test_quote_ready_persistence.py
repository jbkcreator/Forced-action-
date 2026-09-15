"""
WP-8A Quote Ready — persistence core (pure parts) unit tests.
No DB, no network. The spine-bound write/publish is out of scope until WP-1.
"""
from decimal import Decimal
from uuid import UUID

from src.services.quote_ready import compute_quote_ready, QuoteReadyInput
from src.services.quote_ready.persistence import (
    QUOTE_READY_CALC_VERSION,
    ExistingResult,
    build_idempotency_key,
    build_result_row,
    compute_input_hash,
    decide_persistence,
    derive_status,
    domain_event_for,
    overall_confidence,
)

OPP = UUID("00000000-0000-0000-0000-000000000001")


def _full_input(**over):
    base = dict(
        opportunity_id=OPP,
        property_id=1,
        purchase_price=Decimal("200000"),
        rehab_estimate=Decimal("50000"),
        arv=Decimal("320000"),
        arv_source="wp8b_comparable_sales",
        arv_confidence="high",
        max_ltc=Decimal("0.80"),
        max_ltv=Decimal("0.70"),
    )
    base.update(over)
    return QuoteReadyInput(**base)


# --- input_hash ---

def test_input_hash_deterministic():
    inp = _full_input()
    assert compute_input_hash(inp) == compute_input_hash(_full_input())


def test_input_hash_changes_with_effective_input():
    a = compute_input_hash(_full_input(rehab_estimate=Decimal("50000")))
    b = compute_input_hash(_full_input(rehab_estimate=Decimal("60000")))
    assert a != b


def test_input_hash_ignores_opportunity_id():
    other = _full_input(opportunity_id=UUID("00000000-0000-0000-0000-000000000099"))
    assert compute_input_hash(_full_input()) == compute_input_hash(other)


def test_input_hash_canonicalizes_equal_decimals():
    # 0.80 == 0.8 and 320000 == 320000.00 must hash identically
    a = _full_input(max_ltc=Decimal("0.80"), arv=Decimal("320000"))
    b = _full_input(max_ltc=Decimal("0.8"), arv=Decimal("320000.00"))
    assert compute_input_hash(a) == compute_input_hash(b)


def test_input_hash_zero_scale_and_sign_collapse():
    a = _full_input(rehab_estimate=Decimal("0"))
    b = _full_input(rehab_estimate=Decimal("0.00"))
    assert compute_input_hash(a) == compute_input_hash(b)


# --- status ---

def test_status_computed_when_whole():
    result = compute_quote_ready(_full_input())
    assert derive_status(result) == "computed"


def test_status_incomplete_when_missing():
    result = compute_quote_ready(_full_input(arv=None))
    assert derive_status(result) == "incomplete"


# --- overall confidence ---

def test_overall_confidence_is_weakest_present():
    # assessed basis (low) drags everything low
    result = compute_quote_ready(
        _full_input(purchase_price=None, assessed_value_mkt=Decimal("100000"))
    )
    assert overall_confidence(result) == "low"


def test_overall_confidence_unknown_when_no_figures():
    result = compute_quote_ready(_full_input(purchase_price=Decimal("0"), rehab_estimate=Decimal("0")))
    assert overall_confidence(result) == "unknown"


# --- supersede decision ---

def test_decide_insert_when_no_prior():
    d = decide_persistence(None, "h1", QUOTE_READY_CALC_VERSION)
    assert d.action == "insert"


def test_decide_noop_when_identical():
    latest = ExistingResult(result_id="r1", input_hash="h1", calculation_version=QUOTE_READY_CALC_VERSION)
    d = decide_persistence(latest, "h1", QUOTE_READY_CALC_VERSION)
    assert d.action == "noop"
    assert d.existing_result_id == "r1"


def test_decide_supersede_when_hash_differs():
    latest = ExistingResult(result_id="r1", input_hash="h1", calculation_version=QUOTE_READY_CALC_VERSION)
    d = decide_persistence(latest, "h2", QUOTE_READY_CALC_VERSION)
    assert d.action == "insert_supersede"
    assert d.supersedes_result_id == "r1"


def test_decide_supersede_when_version_differs():
    latest = ExistingResult(result_id="r1", input_hash="h1", calculation_version="wp8a-0.9.0")
    d = decide_persistence(latest, "h1", QUOTE_READY_CALC_VERSION)
    assert d.action == "insert_supersede"


# --- row assembly ---

def test_build_row_shape_and_provenance():
    inp = _full_input()
    result = compute_quote_ready(inp)
    row = build_result_row(inp, result, computed_by="system:quote_ready")

    assert row["opportunity_id"] == str(OPP)
    assert row["property_id"] == 1
    assert row["calculation_version"] == QUOTE_READY_CALC_VERSION
    assert row["input_hash"] == compute_input_hash(inp)
    assert row["status"] == "computed"
    assert row["missing_inputs"] == []
    assert row["computed_by"] == "system:quote_ready"
    # provenance carries the ARV source through
    assert row["provenance"]["arv"]["source"] == "wp8b_comparable_sales"
    # outputs are JSON-safe (Decimals serialized)
    assert isinstance(row["outputs"]["project_cost"]["raw"], str)
    # basis high + rehab job_estimator (medium) → cost medium → overall medium
    assert row["confidence"]["overall"] == "medium"


def test_build_row_persists_effective_rehab_confidence():
    # rehab_confidence omitted → default from source (job_estimator → medium)
    inp = _full_input(rehab_source="job_estimator")
    assert inp.rehab_confidence is None
    row = build_result_row(inp, compute_quote_ready(inp), computed_by="system:quote_ready")
    # provenance records the confidence actually used, not null
    assert row["provenance"]["rehab"]["confidence"] == "medium"


def test_build_row_carries_supersedes():
    inp = _full_input()
    result = compute_quote_ready(inp)
    row = build_result_row(inp, result, computed_by="system:quote_ready", supersedes_result_id="r1")
    assert row["supersedes_result_id"] == "r1"


# --- events ---

def test_event_name_computed_vs_incomplete():
    assert domain_event_for(compute_quote_ready(_full_input())) == "fa_max.quote_ready.computed"
    assert domain_event_for(compute_quote_ready(_full_input(arv=None))) == "fa_max.quote_ready.incomplete"


def test_idempotency_key_shape():
    inp = _full_input()
    key = build_idempotency_key(inp)
    assert key == f"quote-ready:{OPP}:{compute_input_hash(inp)}:{QUOTE_READY_CALC_VERSION}"
