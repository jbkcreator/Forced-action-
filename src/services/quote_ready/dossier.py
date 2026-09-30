"""WP-8B — Quote Ready Slack dossier.

Assembles the "fast Slack review dossier" SOT.md §17 describes: purchase
price/estimated value, rehab estimate, ARV range with comparables, proposed
loan amount, resulting LTC/LTV, likely Backflip program fit, and an
explicit list of what is still missing — posted with Approve / Modify /
Reject buttons so Josh gets a 3-minute review instead of 30-60 minutes of
manual deal structuring (SOT.md §17, confirmed).

Internal analysis only (SOT.md Part 1 compliance boundary, reconfirmed at
§17): this dossier never states a rate, term, or commitment, and is never
sent to a borrower — it posts to the internal MONEY Slack lane only.

Decisions write into fa_max_quote_ready_results.review_status/reviewed_by/
reviewed_at — columns that already existed in the schema (see
src/services/quote_ready/persistence.py:ResultStatus) but had no writer
until this module and its admin_router.py button handlers.

Reads (never writes) the WP-8B ARV projection via get_published_arv() and
the WP-8A program match via lender_box.evaluate() — this module owns
assembly and posting only, not computation.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services.lender_box import DealInput, evaluate as evaluate_lender_box
from src.services.quote_ready.arv_persistence import get_published_arv

logger = logging.getLogger(__name__)

_FA_MAX_VENTURE = "fa_max_lending"


def _resolve_bot_token(settings):
    return settings.fa_max_slack_bot_token


def _resolve_channel(settings) -> str:
    return settings.fa_max_slack_channel_money


_SELECT_RESULT_SQL = text(
    """
    SELECT result_id, opportunity_id, property_id, status, inputs, outputs,
           provenance, confidence, missing_inputs, computed_by, computed_at,
           review_status, reviewed_by, reviewed_at
    FROM fa_max_quote_ready_results
    WHERE result_id = :result_id ::uuid
    """
)

_DECIDE_SQL = text(
    """
    UPDATE fa_max_quote_ready_results
    SET review_status = :review_status, reviewed_by = :reviewed_by, reviewed_at = now()
    WHERE result_id = :result_id ::uuid AND status = 'computed'
      AND review_status IS NULL
    RETURNING result_id
    """
)


def _fmt_money(value: Optional[str]) -> str:
    if value is None:
        return "unknown"
    try:
        return f"${Decimal(value):,.0f}"
    except Exception:
        return str(value)


def _fmt_pct(value: Optional[str]) -> str:
    if value is None:
        return "unknown"
    try:
        return f"{Decimal(value) * 100:.1f}%"
    except Exception:
        return str(value)


def build_dossier_text(row: dict[str, Any]) -> str:
    """Plain-text dossier body — the same content the Slack blocks render,
    kept separate so it can be unit-tested without a live Slack app."""
    outputs = row["outputs"] if isinstance(row["outputs"], dict) else json.loads(row["outputs"])
    inputs = row["inputs"] if isinstance(row["inputs"], dict) else json.loads(row["inputs"])
    missing = row["missing_inputs"] if isinstance(row["missing_inputs"], list) else json.loads(row["missing_inputs"])

    project_cost = (outputs.get("project_cost") or {}).get("display", "unknown")
    proposed_loan = (outputs.get("proposed_loan") or {}).get("display", "unknown")
    ltc = outputs.get("ltc") or {}
    ltv = outputs.get("ltv") or {}

    lines = [
        f"*Quote Ready — Opportunity {row['opportunity_id']}*",
        "",
        f"Purchase/estimated value: {_fmt_money(inputs.get('purchase_price') or inputs.get('estimated_value'))}",
        f"Rehab estimate: {_fmt_money(inputs.get('rehab_estimate'))}",
        f"Project cost: {project_cost}",
        f"Proposed loan: {proposed_loan}",
        f"LTC: {ltc.get('display', 'unknown')} (confidence: {ltc.get('confidence', 'unknown')})",
        f"LTV: {ltv.get('display', 'unknown')} (confidence: {ltv.get('confidence', 'unknown')})",
    ]

    if row.get("arv"):
        arv = row["arv"]
        overridden_note = f" (reviewer override — {arv.overridden_by})" if arv.overridden else ""
        lines.append(
            f"ARV range: {_fmt_money(str(arv.low))} – {_fmt_money(str(arv.high))} "
            f"(point {_fmt_money(str(arv.point))}, {arv.comp_count} comps, "
            f"{'WEAK' if arv.weak_comp else 'strong'} evidence, confidence={arv.confidence}){overridden_note}"
        )
    else:
        lines.append("ARV range: not yet computed (WP-8B)")

    if row.get("lender_box"):
        lb = row["lender_box"]
        lines.append(f"Program match: {lb.summary()}")
    else:
        lines.append("Program match: not evaluated")

    if missing:
        lines.append(f"Missing: {', '.join(missing)}")

    lines.append("")
    lines.append(
        "Internal analysis only — no rate, term, or commitment issued here. "
        "Review: Approve, Modify, or Reject."
    )
    return "\n".join(lines)


def _build_dossier_blocks(row: dict[str, Any], text_body: str) -> list:
    result_id = str(row["result_id"])
    elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Approve"},
            "style": "primary",
            "action_id": "quote_ready_approve",
            "value": json.dumps({"result_id": result_id}),
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Modify"},
            "action_id": "quote_ready_modify",
            "value": json.dumps({"result_id": result_id}),
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Reject"},
            "style": "danger",
            "action_id": "quote_ready_reject",
            "value": json.dumps({"result_id": result_id}),
        },
    ]
    # Override ARV (WP-8B "allow reviewed manual override with audit trail")
    # only makes sense when there's a published ARV row to override — an
    # opportunity whose property has no comps yet has nothing for
    # override_arv_result() to target. Wires the previously-orphaned
    # override_arv_result() into a real Slack surface: see
    # _handle_quote_ready_override_arv_open()/_submission() in admin_router.py.
    arv = row.get("arv")
    if arv is not None:
        elements.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Override ARV"},
            "action_id": "quote_ready_override_arv",
            "value": json.dumps({"result_id": result_id, "arv_result_id": arv.arv_result_id}),
        })
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": text_body}},
        {"type": "actions", "elements": elements},
    ]


def assemble_dossier_row(session: Session, result_id: str) -> Optional[dict[str, Any]]:
    """Load one fa_max_quote_ready_results row plus its WP-8B ARV projection
    and WP-8A program match, for rendering. Read-only."""
    row = session.execute(_SELECT_RESULT_SQL, {"result_id": result_id}).mappings().first()
    if row is None:
        return None
    row = dict(row)

    arv = get_published_arv(session, row["property_id"]) if row.get("property_id") else None
    row["arv"] = arv

    inputs = row["inputs"] if isinstance(row["inputs"], dict) else json.loads(row["inputs"])
    outputs = row["outputs"] if isinstance(row["outputs"], dict) else json.loads(row["outputs"])
    proposed_loan = (outputs.get("proposed_loan") or {}).get("raw")
    lender_box_result = None
    if proposed_loan is not None:
        deal = DealInput(
            property_type=inputs.get("property_type", "single_family"),
            state="FL",
            proposed_loan_amount=Decimal(str(proposed_loan)),
            purchase_price=Decimal(str(inputs["purchase_price"])) if inputs.get("purchase_price") else None,
            rehab_estimate=Decimal(str(inputs["rehab_estimate"])) if inputs.get("rehab_estimate") else None,
            arv=Decimal(str(arv.point)) if arv and arv.point else None,
            ref=f"quote-ready:{result_id}",
        )
        lender_box_result = evaluate_lender_box(deal, session)
    row["lender_box"] = lender_box_result

    return row


def post_quote_ready_dossier(session: Session, result_id: str, *, delivery_id: Optional[str] = None) -> Optional[str]:
    """Post the dossier for one fa_max_quote_ready_results row to the
    internal MONEY Slack lane. Returns the Slack message ts, or None if
    Slack isn't configured or the row doesn't exist (never raises — a
    developer running compute_quote_ready() locally with no Slack app
    configured must not crash)."""
    row = assemble_dossier_row(session, result_id)
    if row is None:
        logger.warning("post_quote_ready_dossier: no fa_max_quote_ready_results row for result_id=%s", result_id)
        return None

    settings = get_settings()
    token = _resolve_bot_token(settings)
    channel = _resolve_channel(settings)
    if not token or not channel:
        logger.info(
            "[QuoteReady] Slack not configured (fa_max_slack_bot_token/fa_max_slack_channel_money) "
            "— result_id=%s stays unreviewed without a posted card", result_id,
        )
        return None

    text_body = build_dossier_text(row)
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value(), timeout=15, retry_handlers=[])
        kwargs = {"client_msg_id": delivery_id} if delivery_id else {}
        response = client.chat_postMessage(channel=channel, text=text_body, blocks=_build_dossier_blocks(row, text_body), **kwargs)
        return response["ts"]
    except Exception:
        logger.error("[QuoteReady] Slack post failed for result_id=%s", result_id, exc_info=True)
        return None


def decide_quote_ready(session: Session, *, result_id: str, decision: str, decided_by: str) -> bool:
    """Record a reviewer's Approve/Reject decision.

    decision must be one of persistence.ResultStatus's terminal reviewer
    values: 'approved', 'rejected'. Modify does NOT go through this
    function — see open_modify_modal()/handle_modify_submission() below;
    it opens a real edit flow rather than merely flagging the row, so a
    "3-minute Approve, Modify, or Reject review" (SOT.md §17) actually
    produces a revised scenario to re-review, not a dead-end status flip.

    Terminal at the data layer: the WHERE guard requires review_status IS
    NULL, so only the FIRST decision on a given result_id ever succeeds.
    A repeat click — whether the SAME decision (double-click/retry) or a
    CONFLICTING one (Approve then Reject on the same result_id) — is a
    no-op that returns False. This was previously guarded by
    `review_status IS DISTINCT FROM :review_status`, which let a second,
    conflicting decision silently overwrite the first (Approve then Reject
    both matched, since each new value was "distinct" from the prior one).
    That made the decision reversible with no error, contradicting the
    "never allowed at the data layer" claim below. Fixed by requiring
    review_status IS NULL: once ANY decision is recorded, every later
    decision attempt on that result_id fails the WHERE clause.

    DB-level safety net (belt-and-suspenders alongside the old card's
    buttons being removed by handle_modify_submission()'s neutralization
    step below): only applies to a row still in status='computed'. A row a
    Modify already superseded returns False here even if its Slack card
    somehow still shows live buttons (e.g. a client-side render race) —
    approving/rejecting stale, superseded figures is never allowed at the
    data layer, not just prevented by removing the button.
    """
    if decision not in ("approved", "rejected"):
        raise ValueError(f"decide_quote_ready: invalid decision {decision!r}")
    result = session.execute(
        _DECIDE_SQL, {"result_id": result_id, "review_status": decision, "reviewed_by": decided_by},
    ).fetchone()
    return result is not None


# ---------------------------------------------------------------------------
# Modify — a real edit-and-recompute loop (Banks' revisions.py pattern,
# SOT.md Part 3 reuse map: "button-driven threaded NL revision" ported as a
# Slack modal + view_submission, same mechanism already proven for Relay's
# own Revise button — see src.services.relay.slack_post._build_revise_modal).
# ---------------------------------------------------------------------------

MODIFY_CALLBACK_ID = "quote_ready_modify_submit"


def _build_modify_modal(row: dict[str, Any], *, origin_channel: str, origin_message_ts: str) -> dict:
    """Modal pre-filled with this scenario's current effective inputs.
    Submitting recomputes and produces a NEW scenario to re-review — the
    original row is marked 'superseded', never overwritten (same
    never-destroy-history pattern as everywhere else in this codebase).

    origin_channel/origin_message_ts (the card the Modify button was
    clicked on) travel in private_metadata because a view_submission
    payload carries no channel/message info of its own — only the modal's
    own metadata — so handle_modify_submission() has no other way to know
    which card to neutralize afterward."""
    inputs = row["inputs"] if isinstance(row["inputs"], dict) else json.loads(row["inputs"])

    def _field(key: str) -> str:
        value = inputs.get(key)
        return "" if value is None else str(value)

    return {
        "type": "modal",
        "callback_id": MODIFY_CALLBACK_ID,
        "private_metadata": json.dumps({
            "result_id": str(row["result_id"]),
            "origin_channel": origin_channel,
            "origin_message_ts": origin_message_ts,
        }),
        "title": {"type": "plain_text", "text": "Modify Scenario"[:24]},
        "submit": {"type": "plain_text", "text": "Recompute"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input", "block_id": "purchase_price_block", "optional": True,
                "label": {"type": "plain_text", "text": "Purchase price"},
                "element": {"type": "plain_text_input", "action_id": "purchase_price",
                            "initial_value": _field("purchase_price")},
            },
            {
                "type": "input", "block_id": "rehab_estimate_block", "optional": True,
                "label": {"type": "plain_text", "text": "Rehab estimate"},
                "element": {"type": "plain_text_input", "action_id": "rehab_estimate",
                            "initial_value": _field("rehab_estimate")},
            },
            {
                "type": "input", "block_id": "arv_block", "optional": True,
                "label": {"type": "plain_text", "text": "ARV (leave blank to keep WP-8B's computed range)"},
                "element": {"type": "plain_text_input", "action_id": "arv",
                            "initial_value": _field("arv")},
            },
            {
                "type": "input", "block_id": "max_ltc_block",
                "label": {"type": "plain_text", "text": "Max LTC (e.g. 0.85)"},
                "element": {"type": "plain_text_input", "action_id": "max_ltc",
                            "initial_value": _field("max_ltc") or "0.85"},
            },
            {
                "type": "input", "block_id": "max_ltv_block",
                "label": {"type": "plain_text", "text": "Max LTV (e.g. 0.75)"},
                "element": {"type": "plain_text_input", "action_id": "max_ltv",
                            "initial_value": _field("max_ltv") or "0.75"},
            },
        ],
    }


def open_modify_modal(
    session: Session, *, trigger_id: str, result_id: str, origin_channel: str = "", origin_message_ts: str = "",
) -> bool:
    """Open the real Modify modal for a scenario. Returns True on success,
    False if Slack isn't configured, the row doesn't exist, or the open
    call fails (never raises — a failed views.open must not 500 the Slack
    interactivity endpoint).

    origin_channel/origin_message_ts identify the card being modified, so
    it can be neutralized (buttons removed) once the submission lands —
    see handle_modify_submission(). Optional only for callers that don't
    care about neutralizing an origin card (e.g. a future non-Slack caller);
    every real Slack Modify click supplies both."""
    row = session.execute(_SELECT_RESULT_SQL, {"result_id": result_id}).mappings().first()
    if row is None:
        logger.warning("open_modify_modal: no row for result_id=%s", result_id)
        return False

    settings = get_settings()
    token = _resolve_bot_token(settings)
    if not token or not trigger_id:
        logger.info("[QuoteReady] cannot open modify modal for result_id=%s — no token or trigger_id", result_id)
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).views_open(
            trigger_id=trigger_id,
            view=_build_modify_modal(dict(row), origin_channel=origin_channel, origin_message_ts=origin_message_ts),
        )
        return True
    except Exception:
        logger.error("[QuoteReady] views.open failed for result_id=%s", result_id, exc_info=True)
        return False


OVERRIDE_ARV_CALLBACK_ID = "quote_ready_override_arv_submit"


def _build_override_arv_modal(arv, *, result_id: str) -> dict:
    """Modal pre-filled with the current published ARV range. Submitting
    calls override_arv_result() directly — unlike Modify, this does NOT
    recompute a new Quote Ready scenario; it only corrects the ARV figure
    itself, with the mandatory reason/audit trail WP-8B requires."""
    return {
        "type": "modal",
        "callback_id": OVERRIDE_ARV_CALLBACK_ID,
        "private_metadata": json.dumps({
            "result_id": result_id,
            "arv_result_id": arv.arv_result_id,
        }),
        "title": {"type": "plain_text", "text": "Override ARV"[:24]},
        "submit": {"type": "plain_text", "text": "Override"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Current: {arv.low:,.0f} – {arv.high:,.0f} (point {arv.point:,.0f}, "
                        f"{arv.comp_count} comps, {'WEAK' if arv.weak_comp else 'strong'} evidence)"
                    ),
                },
            },
            {
                "type": "input", "block_id": "override_low_block",
                "label": {"type": "plain_text", "text": "Override low"},
                "element": {"type": "plain_text_input", "action_id": "override_low",
                            "initial_value": str(arv.low)},
            },
            {
                "type": "input", "block_id": "override_point_block",
                "label": {"type": "plain_text", "text": "Override point"},
                "element": {"type": "plain_text_input", "action_id": "override_point",
                            "initial_value": str(arv.point)},
            },
            {
                "type": "input", "block_id": "override_high_block",
                "label": {"type": "plain_text", "text": "Override high"},
                "element": {"type": "plain_text_input", "action_id": "override_high",
                            "initial_value": str(arv.high)},
            },
            {
                "type": "input", "block_id": "reason_block",
                "label": {"type": "plain_text", "text": "Reason (required)"},
                "element": {"type": "plain_text_input", "action_id": "reason", "multiline": True},
            },
        ],
    }


def open_override_arv_modal(session: Session, *, trigger_id: str, result_id: str) -> bool:
    """Open the Override ARV modal for a scenario's published ARV. Returns
    True on success, False if Slack isn't configured, the scenario/ARV
    doesn't exist, or the open call fails (never raises — same contract as
    open_modify_modal above)."""
    row = session.execute(_SELECT_RESULT_SQL, {"result_id": result_id}).mappings().first()
    if row is None or not row.get("property_id"):
        logger.warning("open_override_arv_modal: no row/property for result_id=%s", result_id)
        return False
    arv = get_published_arv(session, row["property_id"])
    if arv is None:
        logger.warning("open_override_arv_modal: no published ARV for result_id=%s", result_id)
        return False

    settings = get_settings()
    token = _resolve_bot_token(settings)
    if not token or not trigger_id:
        logger.info("[QuoteReady] cannot open override-ARV modal for result_id=%s — no token or trigger_id", result_id)
        return False
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).views_open(
            trigger_id=trigger_id,
            view=_build_override_arv_modal(arv, result_id=result_id),
        )
        return True
    except Exception:
        logger.error("[QuoteReady] views.open (override ARV) failed for result_id=%s", result_id, exc_info=True)
        return False


def handle_override_arv_submission(session: Session, *, values: dict, metadata: dict, submitted_by: str) -> dict[str, Any]:
    """Apply a reviewer's ARV override via override_arv_result(). Returns
    {"ok": True} on success, or {"ok": False, "error": <field-level Slack
    modal error dict>} on bad input or a failed apply (e.g. the ARV row was
    already overridden/superseded by the time this submission lands —
    override_arv_result()'s own WHERE guard is the data-layer backstop,
    same "first decision wins" pattern as decide_quote_ready())."""
    from src.services.quote_ready.arv_persistence import override_arv_result

    def _field(block_id: str, action_id: str) -> str:
        return values.get(block_id, {}).get(action_id, {}).get("value") or ""

    try:
        override_low = _parse_decimal(_field("override_low_block", "override_low"))
        override_point = _parse_decimal(_field("override_point_block", "override_point"))
        override_high = _parse_decimal(_field("override_high_block", "override_high"))
    except Exception as exc:
        return {"ok": False, "error": {"override_low_block": f"Could not parse a number: {exc}"}}

    if override_low is None or override_point is None or override_high is None:
        return {"ok": False, "error": {"override_low_block": "Low, point, and high are all required."}}

    reason = _field("reason_block", "reason").strip()
    if not reason:
        return {"ok": False, "error": {"reason_block": "A reason is required."}}

    if not (override_low <= override_point <= override_high):
        return {
            "ok": False,
            "error": {"override_low_block": "Range must be non-decreasing: low <= point <= high."},
        }

    arv_result_id = metadata.get("arv_result_id")
    if not arv_result_id:
        return {"ok": False, "error": {"override_low_block": "Invalid view metadata — no ARV result to override."}}

    try:
        applied = override_arv_result(
            session,
            arv_result_id=arv_result_id,
            override_low=override_low,
            override_point=override_point,
            override_high=override_high,
            reason=reason,
            overridden_by=submitted_by,
        )
        session.commit()
    except ValueError as exc:
        session.rollback()
        return {"ok": False, "error": {"override_low_block": str(exc)}}

    if not applied:
        return {
            "ok": False,
            "error": {"override_low_block": "This ARV was already overridden or superseded — refresh and retry."},
        }
    logger.info("[QuoteReady] arv_result_id=%s overridden by=%s", arv_result_id, submitted_by)
    return {"ok": True}


def _parse_decimal(raw: str) -> Optional[Decimal]:
    raw = (raw or "").strip().replace(",", "").replace("$", "")
    if not raw:
        return None
    return Decimal(raw)


# Generous envelope around Backflip's real active loan range ($150K-$3M
# across every program in lender_box_programs) — wide enough to never
# reject a genuine deal, narrow enough to catch a typo'd extra digit.
_DOLLAR_FIELD_MAX = Decimal("10000000")  # $10M
_RATIO_FIELD_MAX = Decimal("1.5")        # 150% — LTC/LTV are always < 1.0 in
# practice; anything above 150% is certainly a typo (e.g. "85" instead of
# "0.85"), not a real ratio.


def _check_modal_bounds(
    *, purchase_price: Optional[Decimal], rehab_estimate: Optional[Decimal],
    arv: Optional[Decimal], max_ltc: Decimal, max_ltv: Decimal,
) -> Optional[dict]:
    """Reject obviously-mistyped modal inputs before they ever reach
    compute_quote_ready(). Returns a Slack modal field-error dict, or None
    if everything is in bounds."""
    checks = [
        ("purchase_price_block", purchase_price, _DOLLAR_FIELD_MAX),
        ("rehab_estimate_block", rehab_estimate, _DOLLAR_FIELD_MAX),
        ("arv_block", arv, _DOLLAR_FIELD_MAX),
    ]
    for block_id, value, ceiling in checks:
        if value is not None and (value < 0 or value > ceiling):
            return {block_id: f"That doesn't look right — got {value:,.0f}, expected under {ceiling:,.0f}."}
    for block_id, value in (("max_ltc_block", max_ltc), ("max_ltv_block", max_ltv)):
        if value < 0 or value > _RATIO_FIELD_MAX:
            return {block_id: f"That doesn't look right — got {value}, expected a ratio like 0.85."}
    return None


def handle_modify_submission(
    session: Session, *, result_id: str, values: dict, submitted_by: str,
    origin_channel: str = "", origin_message_ts: str = "",
) -> dict[str, Any]:
    """Recompute with the modal's edited inputs, persist a NEW scenario
    (supersedes the original via persist_quote_ready_result's
    idempotent-or-supersede path), post a fresh dossier card for it, and
    neutralize the OLD card (origin_channel/origin_message_ts) so it can no
    longer be Approved/Rejected — a live card with working buttons still
    sitting on now-superseded figures would let a reviewer approve a stale
    scenario by mistake. decide_quote_ready()'s status='computed' guard is
    the data-layer backstop for the same risk; this is the UI-layer half —
    both exist because either one alone leaves a window (a stale button
    click that races the neutralization, or a client that never
    re-renders) the other one closes.

    Returns {"ok": True, "new_result_id": ...} on success, or
    {"ok": False, "error": <field-level Slack modal error dict>} on bad
    input — the caller (admin_router) translates the latter into Slack's
    view_submission error response shape.
    """
    from src.services.quote_ready.compute import compute_quote_ready
    from src.services.quote_ready.models import QuoteReadyInput
    from src.services.quote_ready.persistence import persist_quote_ready_result

    original = session.execute(_SELECT_RESULT_SQL, {"result_id": result_id}).mappings().first()
    if original is None:
        return {"ok": False, "error": {"purchase_price_block": "Original scenario no longer exists."}}

    def _field(block_id: str, action_id: str) -> str:
        return values.get(block_id, {}).get(action_id, {}).get("value") or ""

    try:
        purchase_price = _parse_decimal(_field("purchase_price_block", "purchase_price"))
        rehab_estimate = _parse_decimal(_field("rehab_estimate_block", "rehab_estimate"))
        arv = _parse_decimal(_field("arv_block", "arv"))
        max_ltc = _parse_decimal(_field("max_ltc_block", "max_ltc"))
        max_ltv = _parse_decimal(_field("max_ltv_block", "max_ltv"))
    except Exception as exc:
        return {"ok": False, "error": {"purchase_price_block": f"Could not parse a number: {exc}"}}

    if max_ltc is None or max_ltv is None:
        return {"ok": False, "error": {"max_ltc_block": "Max LTC and Max LTV are required."}}

    # Bounds sanity check (confirmed real gap 2026-09-22: a manual-entry
    # typo — e.g. an extra "000" — produced a $31.5 BILLION purchase price
    # with nothing to catch it; _parse_decimal has no scaling bug, the
    # number was exactly what got typed). Backflip's real loan range is
    # $150K-$3M across every active program (lender_box_programs); a
    # dollar figure outside a generous envelope around that is essentially
    # certain to be a typo, not a real deal, so it's rejected here rather
    # than silently producing a nonsense scenario for review.
    bounds_error = _check_modal_bounds(
        purchase_price=purchase_price, rehab_estimate=rehab_estimate, arv=arv,
        max_ltc=max_ltc, max_ltv=max_ltv,
    )
    if bounds_error:
        return {"ok": False, "error": bounds_error}

    # ARV precedence unchanged from the original scenario when left blank —
    # prefer WP-8B's real published ARV over the manual field, same
    # fallback order compute_quote_ready's own docstring documents.
    arv_source, arv_confidence = "manual_modify", "medium"
    if arv is None:
        published = get_published_arv(session, original["property_id"]) if original["property_id"] else None
        if published is not None:
            arv = published.point
            arv_source = "wp8b_comp_range" + (":override" if published.overridden else "")
            arv_confidence = "high" if not published.weak_comp else "medium"

    inp = QuoteReadyInput(
        opportunity_id=original["opportunity_id"], property_id=original["property_id"],
        max_ltc=max_ltc, max_ltv=max_ltv,
        purchase_price=purchase_price, rehab_estimate=rehab_estimate,
        arv=arv, arv_source=arv_source, arv_confidence=arv_confidence,
    )
    result = compute_quote_ready(inp)
    new_result_id = persist_quote_ready_result(session, inp=inp, result=result, computed_by=f"modified_by:{submitted_by}")
    session.commit()

    logger.info("[QuoteReady] result_id=%s modified by=%s -> new_result_id=%s", result_id, submitted_by, new_result_id)
    # Posting the new dossier card and neutralizing the old one are each a
    # synchronous Slack API call (chat.postMessage / chat.update). They used
    # to run here, inline, before the Socket Mode ack this function's result
    # feeds into — the exact dispatch_failed risk fa_max_new_file_submit was
    # already reworked to avoid elsewhere in this same PR (see
    # socket_listener.py's callback_id == "fa_max_new_file_submit" branch):
    # under real Slack-API/network latency this handler's DB work plus two
    # sequential external calls can blow the ~3s ack window, so the reviewer
    # sees an error/stuck modal even though the new scenario is already
    # persisted server-side. Everything up to and including session.commit()
    # above must stay before the ack (Slack requires the SAME ack to carry
    # {"response_action": "errors", ...} for inline validation failures) --
    # only the two Slack calls below are deferred. See
    # finalize_modify_submission(), called from admin_router AFTER the ack.
    return {
        "ok": True,
        "new_result_id": new_result_id,
        "submitted_by": submitted_by,
        "origin_channel": origin_channel,
        "origin_message_ts": origin_message_ts,
    }


def finalize_modify_submission(
    *, new_result_id: str, submitted_by: str, origin_channel: str = "", origin_message_ts: str = "",
) -> None:
    """Post the new dossier card and neutralize the old one — the two Slack
    API calls deliberately deferred out of handle_modify_submission() (see
    that function's tail comment) so they run AFTER the Socket Mode ack
    instead of blocking it. Opens its own DB session since this runs after
    the caller's request-scoped session has already returned control to
    Slack, matching the deferred-work pattern already used for
    fa_max_new_file_submit in socket_listener.py. Best-effort: a failure
    here is logged, not raised -- the modal has already closed and the new
    scenario is already durably persisted regardless of whether the Slack
    post succeeds.
    """
    from src.core.database import get_db_context

    try:
        with get_db_context() as session:
            post_quote_ready_dossier(session, new_result_id)
            _neutralize_dossier_card(
                origin_channel, origin_message_ts,
                f":pencil2: Modified by <@{submitted_by}> — see the new scenario posted below.",
            )
    except Exception:
        logger.exception(
            "[QuoteReady] finalize_modify_submission failed for new_result_id=%s (scenario already persisted)",
            new_result_id,
        )


def _neutralize_dossier_card(channel: str, message_ts: str, reply_text: str) -> None:
    """Replace a card's Approve/Modify/Reject buttons with a plain status
    line, in place — same chat.update pattern as
    admin_router._update_quote_ready_slack_message, duplicated here rather
    than imported to avoid a dossier.py -> admin_router.py -> dossier.py
    import cycle (admin_router already imports this module)."""
    if not channel or not message_ts:
        return
    settings = get_settings()
    token = _resolve_bot_token(settings)
    if not token:
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_update(
            channel=channel, ts=message_ts, text=reply_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": reply_text}}],
        )
    except Exception:
        logger.error("[QuoteReady] chat.update (neutralize) failed for channel=%s ts=%s", channel, message_ts, exc_info=True)


# ---------------------------------------------------------------------------
# Automatic trigger — SOT.md §17: "When an opportunity looks real, [assemble]
# the whole deal picture before Josh reviews it... reducing 30-60 minutes of
# manual deal structuring into a 3-minute review." Confirmed trigger point
# (2026-09-22 review): an opportunity entering 'scoping' (fa_max_opportunity_
# stage_config) — the stage name IS "deal being structured", matching SOT's
# language most directly among the real stages
# (new -> qualifying -> scoping -> ready_to_submit -> submitted -> ...).
#
# Wired from state_engine._do_transition alongside the existing WP-5B
# profile-recompute hook (_maybe_enqueue_profile_recompute), same isolation
# pattern: its own savepoint, so a dossier-posting failure never poisons the
# state transition that already committed.
# ---------------------------------------------------------------------------

_SUBJECT_PROPERTY_SQL = text(
    """
    SELECT property_id FROM fa_max_opportunity_properties
    WHERE opportunity_id = :opportunity_id ::uuid AND role = 'subject'
    ORDER BY linked_at DESC LIMIT 1
    """
)

_FINANCIALS_SQL = text(
    """
    SELECT assessed_value_mkt, last_sale_price, est_repair_cost, arv AS legacy_arv
    FROM financials WHERE property_id = :property_id
    """
)

_LATEST_COMPUTED_FOR_TRIGGER_SQL = text(
    "SELECT result_id::text FROM fa_max_quote_ready_results "
    "WHERE opportunity_id = :opportunity_id ::uuid AND status = 'computed' "
    "ORDER BY computed_at DESC LIMIT 1"
)

# Defaults match builder_sizing.py's own established convention for this
# codebase (GRILL-DECISIONS.md Q8 amendment: 85% LTC) — the only other real
# caller of compute_quote_ready() in production uses the same constants.
_DEFAULT_MAX_LTC = Decimal("0.85")
_DEFAULT_MAX_LTV = Decimal("0.75")


def compute_and_persist_quote_ready(session: Session, *, opportunity_id: str, return_existing: bool = False) -> Optional[str]:
    """Compute a Quote Ready scenario from an opportunity's current facts
    and durably persist it — NO Slack delivery here. Returns the result_id
    of a genuinely NEW or changed scenario, or None when there is nothing
    new to deliver (no linked subject property, or the resulting scenario
    is identical to the last one already computed).

    Split out from maybe_trigger_quote_ready_review (code-review finding,
    ninth round, 2026-09) so a caller can commit this persist and THEN
    attempt Slack delivery in a separate step/transaction — posting to
    Slack from inside a savepoint of a larger, still-open transaction risks
    a "phantom card": if something LATER in that same outer transaction
    fails and rolls back, the Slack message was already sent (an
    irreversible external side effect) but the DB row backing its
    Approve/Modify/Reject buttons never committed. See
    maybe_trigger_quote_ready_review's docstring for the still-inline
    caller (the state_engine.transition() hook) and its accepted residual
    risk, and qualification_worker.py's own caller for the fully-split
    persist-then-deliver-after-commit pattern.

    Facts precedence (code-review finding, eighth round, 2026-09 — the T3-7
    Qualification Agent's client-confirmed facts and this financials-derived
    read were two disconnected sources of "current" deal facts, so a client
    correction gathered by T3-7 was never reflected in the dossier Josh
    actually reviews): T3-7's fa_max_opportunity_facts values, where present,
    now override the raw financials read below via
    fa_max_qualification.resolve_quote_ready_facts() — the SAME
    client-always-wins-when-set precedence set_facts() already enforces on
    the write side. financials/published-ARV remain the fallback for any
    field the client hasn't confirmed (most commonly on an opportunity T3-7
    never touched at all, or a pure enrichment-sourced field, or a property
    that simply has no financials row yet — code-review finding, ninth
    round, 2026-09: financials is a fallback SOURCE, not a REQUIREMENT; a
    property that hasn't been through enrichment yet must not block a
    scenario T3-7's own facts are otherwise complete enough to compute).
    """
    from src.services.fa_max_qualification import resolve_quote_ready_facts
    from src.services.quote_ready.compute import compute_quote_ready
    from src.services.quote_ready.models import QuoteReadyInput
    from src.services.quote_ready.persistence import persist_quote_ready_result

    property_id = session.execute(_SUBJECT_PROPERTY_SQL, {"opportunity_id": opportunity_id}).scalar()
    if property_id is None:
        logger.info("[QuoteReady] auto-trigger skipped for opportunity_id=%s — no linked subject property", opportunity_id)
        return None

    # financials is a FALLBACK source, not a requirement — T3-7's own
    # confirmed facts (resolved below) are the primary source and can be
    # completely sufficient on their own (code-review finding, ninth round,
    # 2026-09: this early return fired before T3-7's facts were ever read,
    # so a rehab opportunity with a client-confirmed purchase price, rehab
    # estimate, and ARV still produced NO scenario at all if its property
    # simply hadn't been through the enrichment pipeline yet — a newly
    # discovered or manually entered property has no financials row by
    # construction, not by error). A missing row degrades gracefully to an
    # all-None fallback; resolve_quote_ready_facts() below still lets T3-7's
    # confirmed values through per field.
    fin = session.execute(_FINANCIALS_SQL, {"property_id": property_id}).mappings().first()
    if fin is None:
        logger.info(
            "[QuoteReady] opportunity_id=%s property_id=%s has no financials row —"
            " proceeding on T3-7 facts alone where present",
            opportunity_id, property_id,
        )
        fin = {"assessed_value_mkt": None, "last_sale_price": None,
               "est_repair_cost": None, "legacy_arv": None}

    published_arv = get_published_arv(session, property_id)
    if published_arv is not None:
        arv, arv_source, arv_confidence = (
            published_arv.point, "wp8b_comp_range" + (":override" if published_arv.overridden else ""),
            "high" if not published_arv.weak_comp else "medium",
        )
    elif fin["legacy_arv"] is not None:
        arv, arv_source, arv_confidence = fin["legacy_arv"], "legacy_financial.arv", "low"
    else:
        arv, arv_source, arv_confidence = None, "legacy_financial.arv", "low"

    # Resolve T3-7 facts over this financials/ARV fallback — FOR UPDATE
    # locks the facts row for the rest of this function, so a concurrent
    # set_facts() can't land between this read and persist_quote_ready_result()
    # committing below without either being reflected in the OTHER's outcome.
    resolved = resolve_quote_ready_facts(
        session=session,
        opportunity_id=opportunity_id,
        fallback={
            "estimated_value": fin["assessed_value_mkt"],
            "last_sale_price": fin["last_sale_price"],
            "rehab_estimate": fin["est_repair_cost"],
            "rehab_source": "job_estimator",
            "arv": arv,
            "arv_source": arv_source,
            "arv_confidence": arv_confidence,
        },
    )
    facts_revision = resolved.pop("facts_revision")

    inp = QuoteReadyInput(
        opportunity_id=opportunity_id, property_id=property_id,
        max_ltc=_DEFAULT_MAX_LTC, max_ltv=_DEFAULT_MAX_LTV,
        purchase_price=resolved.get("purchase_price"),
        estimated_value=resolved["estimated_value"],
        assessed_value_mkt=resolved.get("assessed_value_mkt"),
        last_sale_price=resolved["last_sale_price"],
        rehab_estimate=resolved["rehab_estimate"], rehab_source=resolved["rehab_source"],
        rehab_confidence=resolved.get("rehab_confidence"),
        arv=resolved["arv"], arv_source=resolved["arv_source"], arv_confidence=resolved["arv_confidence"],
    )
    result = compute_quote_ready(inp)

    previous_id = session.execute(_LATEST_COMPUTED_FOR_TRIGGER_SQL, {"opportunity_id": opportunity_id}).scalar()
    new_result_id = persist_quote_ready_result(
        session, inp=inp, result=result,
        computed_by=f"auto_trigger:scoping:facts_rev={facts_revision}",
    )

    if new_result_id == previous_id:
        logger.info("[QuoteReady] opportunity_id=%s: scenario unchanged, nothing new to deliver",
                     opportunity_id)
        return new_result_id if return_existing else None

    logger.info("[QuoteReady] opportunity_id=%s computed+persisted result_id=%s"
                " facts_revision=%d (missing=%s) — pending delivery",
                opportunity_id, new_result_id, facts_revision, result.missing)
    return new_result_id


def maybe_trigger_quote_ready_review(session: Session, *, opportunity_id: str) -> None:
    """Legacy synchronous helper for explicit/manual callers only.

    Production transitions and qualification enqueue through quote_ready.workflow.
    This helper does not own a transaction and is not an automatic hook.
    """
    new_result_id = compute_and_persist_quote_ready(session, opportunity_id=opportunity_id)
    if new_result_id is not None:
        post_quote_ready_dossier(session, new_result_id)
