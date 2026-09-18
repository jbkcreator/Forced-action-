"""Tracked links — WP-7 self-serve pre-fill path, WI-1.

Every partner, campaign, or source gets its own opaque slug. A click is
recorded and attributed before the borrower types anything.

Attribution is durable server-side, not cookie-based: the click immediately
creates a selfserve_sessions row (see src/api/selfserve_router.py) and the
session's own UUID token is the URL the borrower continues on. This survives
cookie clearing and works across devices, at the cost of the borrower needing
the URL — the same tradeoff as any bookmarked/emailed link, and a stronger
durability guarantee than a 60-day cookie would have given.
"""
from __future__ import annotations

import logging
import re
import secrets
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.core.models import TrackedLink, TrackedLinkClick

logger = logging.getLogger(__name__)

_SLUG_BYTES = 8              # secrets.token_urlsafe(8) -> ~11-char opaque slug
_MINT_RETRIES = 3


def mint_link(
    db: Session,
    kind: str,
    label: str,
    created_by: str,
    partner_ref: Optional[str] = None,
    campaign_ref: Optional[str] = None,
    property_id: Optional[int] = None,
    buyer_entity_id: Optional[int] = None,
    destination: Optional[str] = None,
) -> TrackedLink:
    """Create a TrackedLink with an opaque, non-enumerable slug.

    Retries on the vanishingly rare slug collision, same pattern as
    affiliate_engine.mint_affiliate.
    """
    last_err: Optional[IntegrityError] = None
    for _ in range(_MINT_RETRIES):
        link = TrackedLink(
            slug=secrets.token_urlsafe(_SLUG_BYTES),
            kind=kind,
            label=label,
            partner_ref=partner_ref,
            campaign_ref=campaign_ref,
            property_id=property_id,
            buyer_entity_id=buyer_entity_id,
            destination=destination,
            created_by=created_by,
        )
        db.add(link)
        try:
            db.flush()
            logger.info("Minted tracked_link id=%s kind=%s", link.id, kind)
            return link
        except IntegrityError as exc:
            last_err = exc
            db.rollback()
    raise RuntimeError("Failed to mint tracked link after retries") from last_err


def resolve_slug(db: Session, slug: str) -> Optional[TrackedLink]:
    """Look up an active tracked link by slug. Returns None for an unknown or
    inactive slug — callers must degrade to the generic flow, never 404 a
    borrower (a dead link in a printed mailer must not be a dead end)."""
    row = db.execute(
        text(
            "SELECT id, slug, kind, label, partner_ref, campaign_ref, "
            "property_id, buyer_entity_id, destination, is_active, created_by, created_at "
            "FROM tracked_links WHERE slug = :slug AND is_active = true"
        ),
        {"slug": slug},
    ).mappings().first()
    if row is None:
        return None
    return TrackedLink(**dict(row))


def record_click(
    db: Session,
    tracked_link_id: int,
    session_token: str,
    ip_hash: Optional[str] = None,
    user_agent: Optional[str] = None,
    referer: Optional[str] = None,
) -> TrackedLinkClick:
    click = TrackedLinkClick(
        tracked_link_id=tracked_link_id,
        session_token=session_token,
        ip_hash=ip_hash,
        user_agent=user_agent,
        referer=referer,
    )
    db.add(click)
    db.flush()
    return click


# ---------------------------------------------------------------------------
# Slack slash command: /tracked-link — link generation without an engineer.
#
# Lives here, not in src/services/relay/, deliberately: WP-7 owns this logic,
# the shared Socket Mode connection (src/services/relay/socket_listener.py,
# PR #276) just imports and registers it. Contract matches that module's
# handle_socket_request exactly (client, request) -> bool, so it drops into
# the same socket_mode_request_listeners list without any changes there
# beyond one registration line — see the Phase B follow-up.
# ---------------------------------------------------------------------------

VALID_TRACKED_LINK_KINDS = ("partner", "campaign", "source")

_SLASH_COMMAND = "/tracked-link"
_SLASH_USAGE = (
    "Usage: `/tracked-link <partner|campaign|source> <label> [| <borrower name>] [| <mailing address>]`\n"
    "Examples:\n"
    "  `/tracked-link partner Acme Title Co` — generic link\n"
    "  `/tracked-link partner Acme Title Co | John Smith` — bound to a known repeat borrower\n"
    "  `/tracked-link partner | John Smith | 611 S Ft Harrison Ave` — label defaults to the name;\n"
    "  the mailing address only disambiguates if the name matches more than one borrower"
)

# Borrower-identity matching (WI-1 follow-up, 2026-09-18). Exact name match
# only — never fuzzy/similarity-scored, matching the precedent in
# borrower_profile_service._resolve_entity_id ("never fuzzy here to avoid
# silent misattribution"). primary_mailing_address is used only to break a
# tie among rows that already share the identical canonical_name — it is
# never an independent search key (checked empirically 2026-09-18: mailing
# address collides MORE often than name in every entity_type — LLCs route
# mail through shared registered-agent/tax-servicer addresses, so
# addr-collision 46.1% vs name-collision 32.7% for LLC alone — using it as a
# primary key would make ambiguity worse, not better). It is also a
# different field entirely from a target property's address (that's
# find_property_by_address in prefill_assembly.py — a separate, unrelated
# axis).
_NAME_MATCH_QUERY = text(
    "SELECT id, canonical_name, entity_type, total_purchase_count, primary_mailing_address "
    "FROM buyer_entities WHERE lower(canonical_name) = ANY(:names)"
)


def _name_variants(name: str) -> list[str]:
    """Deterministic word-order alternatives for an exact match — still not
    fuzzy, just tries more than one exact candidate string. County records
    store an individual as 'LAST, FIRST [MIDDLE]' (confirmed directly:
    buyer_entity_resolution.canonical_name() keeps the raw grantee/owner
    string as-is, no reordering). A human naturally types 'First [Middle]
    Last'. Without this, an exact match on canonical_name alone silently
    finds nothing for the overwhelmingly common case of someone typing a
    name the normal way — verified empirically against real samples
    (PALMER, ROBERT J / QUASIUS, JAMES R / BARNES, JOHN)."""
    stripped = name.strip()
    variants = [stripped]
    if "," in stripped:
        last_part, _, first_part = stripped.partition(",")
        variants.append(f"{first_part.strip()} {last_part.strip()}".strip())
    else:
        tokens = stripped.split()
        if len(tokens) >= 2:
            last, rest = tokens[-1], " ".join(tokens[:-1])
            variants.append(f"{last}, {rest}")
    return [v.lower() for v in variants]


_ADDRESS_DIRECTIONALS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}


def _address_tokens(value: str) -> set[str]:
    """Meaningful, order-independent tokens from an address — uppercased,
    punctuation stripped, directionals dropped. 'directionals dropped'
    matters because Josh typing '123 Main St' must still match a stored
    '123 N MAIN ST' — a missing/extra directional is the single most common
    reason a plain substring check misses an address a human would
    recognize as identical."""
    tokens = re.findall(r"[A-Z0-9]+", value.upper())
    return {t for t in tokens if t not in _ADDRESS_DIRECTIONALS}


def _mailing_address_matches(typed: str, stored: str) -> bool:
    """Every meaningful token Josh typed must appear somewhere in the stored
    address — order-independent, not a contiguous substring, so an extra
    directional or a unit number in between no longer breaks the match. Each
    token must still literally match (no similarity scoring), only ever
    used to break a tie among rows that already share one exact
    canonical_name."""
    typed_tokens = _address_tokens(typed)
    return bool(typed_tokens) and typed_tokens.issubset(_address_tokens(stored))


# 23.1% of buyer_entities rows join two people with "AND" (checked
# empirically 2026-09-18), e.g. 'KIMBERLY G AND RICHARD A KUHNAST' or 'YIBO
# FEN AND LANJU KANG' — common enough that Josh must be able to match on
# either person's own name, not only the full joined string.
_JOINT_NAME_SPLIT_RE = re.compile(r"\s+AND\s+", re.IGNORECASE)
_JOINT_NAME_CANDIDATE_QUERY = text(
    r"SELECT id, canonical_name, entity_type, total_purchase_count, primary_mailing_address "
    r"FROM buyer_entities WHERE canonical_name ~* '\mand\M' AND canonical_name ILIKE :pattern"
)


def _split_joint_names(canonical_name: str) -> list[str]:
    """Split a joint-owner canonical_name into each person's own name.

    Known, documented limitation: when the stored format shares one surname
    at the end for both people ('KIMBERLY G AND RICHARD A KUHNAST'), only
    the second half carries that surname as stored — the first half alone
    ('KIMBERLY G') will not match a typed 'Kimberly Kuhnast', since the
    surname genuinely is not present in that half's text. Reconstructing it
    would require guessing whether the format is 'one shared surname' or
    'two fully independent names' (e.g. 'YIBO FEN AND LANJU KANG', where FEN
    and KANG are two different surnames — attaching KANG to YIBO FEN would
    be wrong) — that guess is exactly the kind of ambiguity the anti-fuzzy
    precedent (borrower_profile_service._resolve_entity_id) exists to
    avoid, so it is left as a known gap rather than risking a wrong
    attachment."""
    return [p.strip() for p in _JOINT_NAME_SPLIT_RE.split(canonical_name) if p.strip()]


def _names_match(a: str, b: str) -> bool:
    return bool(set(_name_variants(a)) & set(_name_variants(b)))


def find_buyer_entity_by_name(
    db: Session, name: str, mailing_address: Optional[str] = None
) -> tuple[Optional[dict], str]:
    """Exact match on canonical_name (tried in both word orders, and against
    either half of a joint 'X AND Y' owner name — see _name_variants /
    _split_joint_names); ambiguous matches are narrowed by mailing_address
    when given, never guessed.

    Returns (row_or_None, status) — status is 'matched', 'not_found', or
    'ambiguous'.
    """
    rows = db.execute(_NAME_MATCH_QUERY, {"names": _name_variants(name)}).mappings().all()

    if not rows:
        # Joint-owner fallback. Narrow the scan with a plain ILIKE on the
        # typed name's last token (usually a surname) before doing the
        # exact per-half comparison in Python — cheap enough for an
        # occasional Slack command against ~800K rows, and the equality
        # check itself stays exact, never a similarity score.
        last_token = name.strip().split()[-1] if name.strip() else ""
        if last_token:
            candidates = db.execute(
                _JOINT_NAME_CANDIDATE_QUERY, {"pattern": f"%{last_token}%"}
            ).mappings().all()
            rows = [
                r for r in candidates
                if any(_names_match(name, half) for half in _split_joint_names(r["canonical_name"]))
            ]

    if not rows:
        return None, "not_found"
    if len(rows) == 1:
        return dict(rows[0]), "matched"

    if mailing_address:
        narrowed = [
            r for r in rows
            if r["primary_mailing_address"] and _mailing_address_matches(mailing_address, r["primary_mailing_address"])
        ]
        if len(narrowed) == 1:
            return dict(narrowed[0]), "matched"

    return None, "ambiguous"


def _slack_ephemeral(msg: str) -> dict:
    return {"response_type": "ephemeral", "text": msg}


def build_tracked_link_reply(db: Session, text_arg: str, user: str) -> dict:
    """Parse the slash command's text, mint the link, return the Slack
    response payload. Split out from the Socket Mode envelope handling below
    so it's testable with a plain db session — no fake Slack objects needed.

    Grammar: `<kind> <label>[| <name>][| <mailing address>]`. label may be
    left blank only when a name is given, in which case label defaults to
    the name (typing the same string twice is pure friction). At least one
    of label/name must be present — this is the one case that still needs
    an explicit human input, since a bare kind with nothing else is
    unidentifiable later in the admin list.
    """
    parts = (text_arg or "").strip().split(maxsplit=1)
    if len(parts) < 2:
        return _slack_ephemeral(_SLASH_USAGE)

    kind, rest = parts[0].lower(), parts[1].strip()
    if kind not in VALID_TRACKED_LINK_KINDS:
        return _slack_ephemeral(
            f"Unknown kind {kind!r}. Use one of: {', '.join(VALID_TRACKED_LINK_KINDS)}\n\n{_SLASH_USAGE}"
        )

    segments = [s.strip() for s in rest.split("|")]
    label = segments[0] if segments[0] else None
    name = segments[1] if len(segments) > 1 and segments[1] else None
    mailing_address = segments[2] if len(segments) > 2 and segments[2] else None

    if not label and not name:
        return _slack_ephemeral(f"Need a label or a borrower name.\n\n{_SLASH_USAGE}")
    label = label or name
    assert label is not None

    buyer_entity_id = None
    notes: list[str] = []
    if name:
        match, status = find_buyer_entity_by_name(db, name, mailing_address)
        if status == "matched" and match is not None:
            buyer_entity_id = match["id"]
            notes.append(
                f"Borrower recognized: {match['canonical_name']} "
                f"({match['entity_type']}, {match['total_purchase_count']} prior purchases)"
            )
            if mailing_address:
                notes.append(f"Disambiguated using mailing address: {match['primary_mailing_address']}")
        elif status == "ambiguous":
            notes.append(
                f"Found more than one borrower named {name!r} — link created without a borrower match. "
                "Add a mailing address (`| name | mailing address`) to narrow it down."
            )
        else:
            notes.append(f"No borrower found matching {name!r} — link created as generic.")

    link = mint_link(db, kind=kind, label=label, created_by=f"slack:{user}", buyer_entity_id=buyer_entity_id)

    from config.settings import settings
    base = settings.app_base_url.rstrip("/") if settings.app_base_url else ""
    lines = [f"Created: {base}/go/{link.slug}"] + notes
    return _slack_ephemeral("\n".join(lines))


def handle_tracked_link_socket_request(client, request) -> bool:
    """Socket Mode envelope handler for the /tracked-link slash command.

    Returns True only when this listener handled the request — every other
    app action is left untouched so a shared Socket Mode connection does not
    accidentally mutate another workflow (same contract as
    src/services/relay/socket_listener.py's handle_socket_request).
    """
    if request.type != "slash_commands":
        return False
    payload = request.payload or {}
    if payload.get("command") != _SLASH_COMMAND:
        return False

    from slack_sdk.socket_mode.response import SocketModeResponse
    from config.settings import settings
    from src.core.database import get_db_context

    # Slack requires this ack within 3 seconds — sent bare, before any DB
    # work, same discipline as socket_listener.py's _on_request. The actual
    # reply for a slash command can't ride this ack once work has happened
    # after it, so it goes to response_url instead (Slack keeps that URL
    # live for 30 minutes, way more headroom than the mint/match work needs).
    client.send_socket_mode_response(SocketModeResponse(envelope_id=request.envelope_id))

    allowed_channel = settings.fa_max_slack_channel_relationships
    response_url = payload.get("response_url")
    if allowed_channel and payload.get("channel_id") != allowed_channel:
        _post_slash_reply(
            response_url,
            _slack_ephemeral(f"{_SLASH_COMMAND} only works in the fa-max-relationships channel."),
        )
        return True

    text_arg = payload.get("text") or ""
    user = payload.get("user_name") or "someone"

    with get_db_context() as db:
        reply = build_tracked_link_reply(db, text_arg, user)

    _post_slash_reply(response_url, reply)
    return True


def _post_slash_reply(response_url: Optional[str], reply: dict) -> None:
    """Deliver a slash command's reply after the 3-second ack window has
    already been used. Best-effort: Slack has nothing to retry against if
    this fails, so log and move on rather than raising into the listener."""
    if not response_url:
        logger.warning("[tracked_links] no response_url on /tracked-link envelope, reply dropped")
        return
    from src.utils.http_helpers import requests_post_with_retry

    try:
        resp = requests_post_with_retry(response_url, json=reply, timeout=5)
        # Diagnostic (2026-09-18): requests_post_with_retry only raises on
        # HTTP error status codes. Slack's response_url endpoint can return
        # 200 with a body that indicates it rejected the payload (e.g. an
        # expired or already-used response_url), which raise_for_status()
        # would never catch — log the actual body so a silent Slack-side
        # rejection is visible instead of looking like a successful send.
        logger.info(
            "[tracked_links] response_url POST -> status=%s body=%s",
            resp.status_code, resp.text[:500],
        )
    except Exception:
        logger.exception("[tracked_links] failed to deliver /tracked-link reply via response_url")
