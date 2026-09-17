"""
Buyer entity resolution (HUNTER-01, H2).

Collapses `owners` rows (one per property) and `deeds.grantee` mentions,
cross-referenced with Sunbiz LLC-piercing data already denormalized onto
`owners` (managing_members, registered_agent_name, principal_address), into
canonical buyer identities. Same shape as src/services/contact_triangulation.py:
a pure classifier with no DB access, plus a thin DB-sweep layer around it.
See docs/plans/agent_lane_phase1_week1_dev_split.md for the full design.

Module layout (built incrementally, task by task):
  CandidateRecord / extract_candidates   — read-side, pure transform (H2.1)
"""
from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

from rapidfuzz import fuzz
from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from src.agents.hunter.gating import verification_status
from src.core.models import BuyerEntity, BuyerEntityLink
from src.loaders.base import BaseLoader
from src.services import phone_utils
from src.services.buyer_entity_exceptions import (
    DEFAULT_MAX_NEW_EXCEPTIONS_PER_RUN,
    record_exception,
    record_exceptions_batch,
)

logger = logging.getLogger(__name__)

_STREAM_BATCH = 1000  # yield_per size — owners/deeds are tens-of-thousands+ tables, never fetchall

# Structural (Sunbiz) linking thresholds — see find_structural_edges.
SUNBIZ_STRUCTURAL_MIN = 90        # token_set_ratio floor for two "controller mentions" to count as the same real person
SUNBIZ_STRUCTURAL_CONFIDENCE = 97 # fixed — this is a sourced fact (Sunbiz registry), not a computed score

_MIN_BLOCK_TOKEN_LEN = 3  # skip short tokens (initials, "JR", "LLC" residue) as blocking keys — too many false co-occurrences

# A token used by more than this many records is too generic to safely block
# on — comparing every pair within that group costs O(group_size^2). Confirmed
# via a real full-scale run: legitimate institutional investors (a REIT's
# corporate officers, a large FL developer) appear as a managing member on
# 500-700+ separate LLCs each, and generic words like "FLORIDA" are shared by
# many more unrelated companies on top of that — comparison groups built from
# these tokens blew up to 2M+ edges and 17+ minutes on one step alone.
MAX_BLOCK_FREQUENCY = 500


def _token_frequencies(token_sets: Iterable[set[str]]) -> dict[str, int]:
    """How many records each token appears in, across one full pass — used
    to identify tokens too generic to block on before any blocks are built."""
    freq: dict[str, int] = defaultdict(int)
    for tokens in token_sets:
        for t in tokens:
            freq[t] += 1
    return freq

# Deterministic pairwise-scoring thresholds — see score_candidate_pair.
NAME_AUTO_MATCH_MIN = 90   # + address agreement (>=ADDRESS_AGREE_MIN) -> exact_name_address
NAME_FUZZY_MIN = 85        # name alone (no address to corroborate, or address didn't agree) -> fuzzy_name
NAME_AMBIGUOUS_MIN = 65    # below fuzzy floor but not clearly different -> ambiguous, routed to the LLM tie-break (H2.4)
ADDRESS_AGREE_MIN = 90     # street-portion token_set_ratio floor to count as "same address"
ADDRESS_DISAGREE_MAX = 50  # street-portion token_set_ratio ceiling to count as "clearly different address"

_ZIP_RE = re.compile(r"(\d{5})(?:-\d{4})?\s*$")


@dataclass(frozen=True)
class CandidateRecord:
    """
    One raw record — an `owners` row or a `deeds.grantee` mention —
    considered for buyer-entity matching.
    """
    source_table: str                    # 'owners' | 'deeds'
    source_id: int
    raw_name: str
    normalized_name: str                 # via BaseLoader.normalize_owner_name
    mailing_address: Optional[str]
    entity_type_hint: Optional[str]      # Individual | LLC | Trust | Estate | Corporate (owners.owner_type); None for deed-sourced candidates
    managing_members: Optional[list]     # only populated for LLC-type owners rows
    county_id: Optional[str]
    # All contact slots, not just the primary one -- email_1/email_2,
    # phone_1/phone_2/phone_3 from owners. Empty for deed-sourced candidates
    # (deeds carries no contact columns). frozenset so score_candidate_pair
    # can intersect two candidates' contacts directly rather than comparing
    # a single scalar per side, which would miss a match where the shared
    # number sits in a different slot on each record (e.g. current phone_1
    # on one owner row, an older phone_2 still current on another). Phones
    # always via src/services/phone_utils.normalize (E.164); emails lowercased
    # and stripped.
    emails: frozenset[str] = frozenset()
    phones: frozenset[str] = frozenset()


def _owners_query(only_unresolved: bool) -> str:
    where_unresolved = "AND bel.id IS NULL" if only_unresolved else ""
    return f"""
        SELECT o.id, o.owner_name, o.mailing_address, o.owner_type,
               o.managing_members, o.county_id,
               o.email_1, o.email_2, o.phone_1, o.phone_2, o.phone_3
        FROM owners o
        LEFT JOIN buyer_entity_links bel
            ON bel.source_table = 'owners' AND bel.source_id = o.id
        WHERE o.owner_name IS NOT NULL AND o.owner_name != ''
        {where_unresolved}
        ORDER BY o.id
    """


def _deeds_query(only_unresolved: bool) -> str:
    where_unresolved = "AND bel.id IS NULL" if only_unresolved else ""
    # Deeds carry no mailing-address column of their own. The current owner's
    # address on the same property is the only proxy available — a WEAK
    # signal (the buyer may have moved, or the property may have changed
    # hands again since), used only for corroboration, never as a sole match
    # signal. owners.property_id is unique (1:1), so this join never fans out.
    return f"""
        SELECT d.id, d.grantee, own.mailing_address, d.county_id
        FROM deeds d
        LEFT JOIN owners own ON own.property_id = d.property_id
        LEFT JOIN buyer_entity_links bel
            ON bel.source_table = 'deeds' AND bel.source_id = d.id
        WHERE d.grantee IS NOT NULL AND d.grantee != ''
        {where_unresolved}
        ORDER BY d.id
    """


def extract_owner_candidates(
    session: Session, only_unresolved: bool = False,
) -> Iterator[CandidateRecord]:
    """
    Stream `owners` rows as CandidateRecords.

    only_unresolved=True restricts to rows with no existing
    buyer_entity_links row yet — the incremental-sweep mode (H2.7).
    False (default) is the full backfill mode (H2.6).
    """
    result = session.execute(text(_owners_query(only_unresolved))).yield_per(_STREAM_BATCH)
    for row in result:
        raw_name = row.owner_name or ""
        emails = frozenset(
            e.lower().strip() for e in (row.email_1, row.email_2) if e and e.strip()
        )
        phones = frozenset(
            p for p in (phone_utils.normalize(row.phone_1),
                        phone_utils.normalize(row.phone_2),
                        phone_utils.normalize(row.phone_3)) if p
        )
        yield CandidateRecord(
            source_table="owners",
            source_id=row.id,
            raw_name=raw_name,
            normalized_name=BaseLoader.normalize_owner_name(raw_name),
            mailing_address=row.mailing_address,
            entity_type_hint=row.owner_type,
            managing_members=row.managing_members,
            county_id=row.county_id,
            emails=emails,
            phones=phones,
        )


def extract_deed_candidates(
    session: Session, only_unresolved: bool = False,
) -> Iterator[CandidateRecord]:
    """
    Stream `deeds.grantee` mentions as CandidateRecords. See the
    module-level caveat on _deeds_query: mailing_address here is a weak,
    current-owner proxy, not the buyer's actual address at time of purchase.
    """
    result = session.execute(text(_deeds_query(only_unresolved))).yield_per(_STREAM_BATCH)
    for row in result:
        raw_name = row.grantee or ""
        yield CandidateRecord(
            source_table="deeds",
            source_id=row.id,
            raw_name=raw_name,
            normalized_name=BaseLoader.normalize_owner_name(raw_name),
            mailing_address=row.mailing_address,
            entity_type_hint=None,
            managing_members=None,
            county_id=row.county_id,
        )


def extract_candidates(
    session: Session, only_unresolved: bool = False,
) -> Iterator[CandidateRecord]:
    """Combined stream over both source tables — the entry point H2.3's
    blocking step consumes."""
    yield from extract_owner_candidates(session, only_unresolved=only_unresolved)
    yield from extract_deed_candidates(session, only_unresolved=only_unresolved)


# ─────────────────────────────────────────────────────────────────────────────
# H2.2 — Structural (Sunbiz) linking
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MatchVerdict:
    """The outcome of comparing two CandidateRecords."""
    is_match: bool
    confidence: int    # 0-100
    method: str        # sunbiz_llc_piercing | exact_name_address | fuzzy_name | llm_adjudicated
    explanation: str = ""
    # The controlling PERSON's name, when this edge is Sunbiz LLC-piercing --
    # e.g. "FLAIG SUSAN" for an edge linking "BARNZ WEST LLC" and "BARNZ LLC"
    # via a shared managing member. None for every other method (name/address/
    # contact edges are between two records of a KNOWN type already, not a
    # piercing fact). Carried through build_evidence_index into
    # _new_entity_from_cluster so an LLC-only cluster can be named after its
    # actual principal instead of after whichever LLC name happens to be
    # longest -- see canonical_name().
    principal_name: Optional[str] = None


@dataclass(frozen=True)
class _ControllerMention:
    """
    One 'this real person controls this owner record' fact — either the
    owner record IS that person (Individual/Trust), or that person is a
    managing_member of an LLC-type owner record (Sunbiz LLC-piercing).

    from_llc_piercing distinguishes the two: only the managing_members case
    is actually sourced from the Sunbiz registry. A direct Individual/Trust
    owner_name is just a property-appraiser string — matching it against
    ANOTHER Individual/Trust owner_name by name alone is not a Sunbiz fact,
    it's an unverified name coincidence (a government entity or a common
    name like "JOHN SMITH" owning many parcels would otherwise get
    auto-linked at structural-tier confidence with zero corroboration).
    """
    person_name_normalized: str
    source: CandidateRecord
    from_llc_piercing: bool


def _controller_mentions(owner_candidates: Iterable[CandidateRecord]) -> list[_ControllerMention]:
    mentions: list[_ControllerMention] = []
    for cand in owner_candidates:
        if cand.source_table != "owners":
            continue
        if cand.entity_type_hint == "LLC" and cand.managing_members:
            for member in cand.managing_members:
                name = member.get("name") if isinstance(member, dict) else None
                if not name:
                    continue
                normalized = BaseLoader.normalize_owner_name(name)
                if normalized:
                    mentions.append(_ControllerMention(
                        person_name_normalized=normalized, source=cand, from_llc_piercing=True,
                    ))
        elif cand.entity_type_hint in ("Individual", "Trust") and cand.normalized_name:
            mentions.append(_ControllerMention(
                person_name_normalized=cand.normalized_name, source=cand, from_llc_piercing=False,
            ))
    return mentions


def _record_key(cand: CandidateRecord) -> tuple[str, int]:
    return (cand.source_table, cand.source_id)


def find_structural_edges(
    owner_candidates: Iterable[CandidateRecord],
) -> list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]]:
    """
    Sunbiz structural linking — the highest-confidence tier, computed before
    any fuzzy scoring. Two owner records are linked here when the SAME real
    person controls both: directly (Individual/Trust owner_name) or via
    Sunbiz LLC-piercing (a managing_member entry). Sourced fact, not an
    inference — hence the fixed confidence rather than a computed score.

    The common real-world case this catches: two DIFFERENT LLCs sharing the
    same managing member(s) get linked to each other directly, even if that
    person never appears as a direct owner anywhere (e.g. "BARNZ WEST LLC"
    and "BARNZ LLC" both list FLAIG, SUSAN / FLAIG, GUNTHER as managers).

    Blocked by every significant token in the normalized name (not just the
    first), so a name matches regardless of which token order the source
    happened to use — owner_name and Sunbiz managing_members are both
    Last-first in the data observed so far, but blocking on every token is a
    cheap hedge against a source that isn't. Known limitation: this still
    assumes the *tokens themselves* are spelled consistently; it will not
    bridge across a name spelled differently enough that no token matches.

    Requires at least one side of every pair to be an LLC-piercing mention
    (from_llc_piercing=True) — confirmed against real data: without this
    gate, two plain Individual-type owner_name matches (e.g. a government
    entity like "HILLSBOROUGH COUNTY" owning many parcels, or two unrelated
    people who happen to share a common name) get auto-linked at
    structural-tier confidence with zero corroboration. That comparison
    belongs to H2.3's fuzzy+address scoring, not here.

    Tokens appearing in more than MAX_BLOCK_FREQUENCY mentions are excluded
    as blocking keys entirely (see that constant) — confirmed necessary via
    a real full-scale run where this step alone took 17 minutes and produced
    2M+ edges before this cap existed.
    """
    mentions = _controller_mentions(owner_candidates)
    mention_token_sets = [
        {t for t in m.person_name_normalized.split(" ") if len(t) >= _MIN_BLOCK_TOKEN_LEN}
        for m in mentions
    ]
    token_freq = _token_frequencies(mention_token_sets)

    blocks: dict[str, list[_ControllerMention]] = defaultdict(list)
    for m, tokens in zip(mentions, mention_token_sets):
        for token in tokens:
            if token_freq[token] > MAX_BLOCK_FREQUENCY:
                continue  # too generic -- would create a runaway O(n^2) comparison group
            blocks[token].append(m)

    edges: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]] = []
    seen_pairs: set[tuple[tuple[str, int], tuple[str, int]]] = set()

    for group in blocks.values():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if not (a.from_llc_piercing or b.from_llc_piercing):
                    continue  # neither side is a Sunbiz-sourced fact — not this tier's job (H2.3 handles it)
                key_a, key_b = _record_key(a.source), _record_key(b.source)
                if key_a == key_b:
                    continue  # same owner record — not a link between two records
                pair_key = (key_a, key_b) if key_a < key_b else (key_b, key_a)
                if pair_key in seen_pairs:
                    continue
                score = fuzz.token_set_ratio(a.person_name_normalized, b.person_name_normalized)
                if score < SUNBIZ_STRUCTURAL_MIN:
                    continue
                seen_pairs.add(pair_key)
                explanation = (
                    f"Sunbiz managing_member '{a.person_name_normalized}' on "
                    f"{a.source.source_table}#{a.source.source_id} matches "
                    f"'{b.person_name_normalized}' on "
                    f"{b.source.source_table}#{b.source.source_id} "
                    f"(token_set_ratio={score})"
                )
                # The longer of the two spellings, on the same "prefer the
                # more complete rendering" logic as canonical_name() below --
                # e.g. "FLAIG GUNTHER SUSAN" over "FLAIG SUSAN".
                principal_name = max(
                    (a.person_name_normalized, b.person_name_normalized), key=len,
                )
                edges.append((
                    a.source, b.source,
                    MatchVerdict(
                        is_match=True,
                        confidence=SUNBIZ_STRUCTURAL_CONFIDENCE,
                        method="sunbiz_llc_piercing",
                        explanation=explanation,
                        principal_name=principal_name,
                    ),
                ))
    return edges


# ─────────────────────────────────────────────────────────────────────────────
# H2.3 — Blocking + deterministic pairwise scoring
# ─────────────────────────────────────────────────────────────────────────────

def _extract_zip(address: Optional[str]) -> Optional[str]:
    """First 5 digits of a US ZIP from a free-text mailing address, or None."""
    if not address:
        return None
    match = _ZIP_RE.search(address.strip())
    return match.group(1) if match else None


def _block_key_tokens(normalized_name: str) -> set[str]:
    return {t for t in normalized_name.split(" ") if len(t) >= _MIN_BLOCK_TOKEN_LEN}


_CONTACT_KEY_PREFIXES = ("email:", "phone:")


def is_contact_block_key(key: str) -> bool:
    """True for a block key minted from a shared email/phone rather than a
    name token — used by score_blocked_pairs to bypass the co-occurrence
    gate for contact-corroborated pairs."""
    return key.startswith(_CONTACT_KEY_PREFIXES)


def _contact_key_tokens(cand: CandidateRecord) -> set[str]:
    """Blocking keys minted from a candidate's own contacts (never blended
    with name tokens — email:/phone: prefixes keep the two namespaces
    disjoint so a contact value can never collide with a name token)."""
    return {f"email:{e}" for e in cand.emails} | {f"phone:{p}" for p in cand.phones}


def block_candidates(
    candidates: Iterable[CandidateRecord],
) -> dict[tuple[str, Optional[str]], list[CandidateRecord]]:
    """
    Group candidates by (name-token, zip) so pairwise scoring only ever
    compares plausibly-related records, never the full O(n^2) cross product
    across the whole owners+deeds dataset. A candidate contributes to one
    block per significant token in its normalized name, each combined with
    its ZIP (or None if no address is available — typical for deed-sourced
    candidates whose property has no matching current owner).

    A SECOND blocking dimension is added here: a candidate also contributes
    to one block per shared email/phone it holds, keyed by the contact value
    itself (zip_code component is always None for these — the contact IS
    the corroborating signal, not a zip pairing). This is the only way two
    records with the same person at two different mailing addresses (and
    therefore two different ZIPs) can ever be COMPARED at all — the
    name-token+ZIP blocking above would never put them in the same group.
    Confirmed necessary against real data: 202 of 430 owner groups sharing a
    normalized phone span more than one ZIP.

    Tokens (name tokens AND contact values) appearing in more than
    MAX_BLOCK_FREQUENCY candidates are excluded as blocking keys entirely —
    the same institutional mega-names that force this cap in
    find_structural_edges (a REIT's officers, common words like "FLORIDA")
    apply here too, since this blocks on the same candidate pool. A shared
    office phone line or an `info@` mailbox is exactly this case for
    contacts — the cap keeps either from creating a runaway comparison group.
    """
    candidates = list(candidates)  # consumed twice below -- must not be a one-shot generator
    name_token_sets = [_block_key_tokens(c.normalized_name) for c in candidates]
    contact_token_sets = [_contact_key_tokens(c) for c in candidates]
    all_token_sets = [n | c for n, c in zip(name_token_sets, contact_token_sets)]
    token_freq = _token_frequencies(all_token_sets)

    blocks: dict[tuple[str, Optional[str]], list[CandidateRecord]] = defaultdict(list)
    for cand, name_tokens, contact_tokens in zip(candidates, name_token_sets, contact_token_sets):
        zip_code = _extract_zip(cand.mailing_address)
        for token in name_tokens:
            if token_freq[token] > MAX_BLOCK_FREQUENCY:
                continue
            blocks[(token, zip_code)].append(cand)
        for token in contact_tokens:
            if token_freq[token] > MAX_BLOCK_FREQUENCY:
                continue
            blocks[(token, None)].append(cand)
    return blocks


def _street_portion(address: str) -> str:
    """
    The first comma-separated segment of a US mailing address — house
    number + street name, the part that actually distinguishes two
    addresses. Comparing FULL addresses (including city/state/ZIP) dilutes
    this: confirmed empirically that two genuinely different houses in the
    same Florida town score 63-70 on a full-string token_set_ratio (city,
    state, and ZIP tokens alone supply most of that), which reads as weak
    corroboration when it's actually clear disagreement. Street-portion-only
    comparison separates these cleanly (32-39 for different addresses vs.
    100 for identical ones, on the same real data).
    """
    return address.split(",")[0].strip()


NAME_NEAR_IDENTICAL = 98  # see the disagreeing-address carve-out below


def score_candidate_pair(a: CandidateRecord, b: CandidateRecord) -> MatchVerdict:
    """
    Deterministic scoring only — never calls the LLM. Pairs in the ambiguous
    band come back with method='ambiguous' as a sentinel; H2.4 routes those
    to the LLM tie-break.

    The address-disagreement check runs BEFORE any name-based auto-match
    tier, not after — this ordering is load-bearing, found via a real bug:
    an earlier version checked NAME_FUZZY_MIN first, so a disagreeing
    address was never even consulted once name_score alone cleared 85.
    token_set_ratio doesn't distinguish tokens that actually identify a
    person from generic ones (shared first names, "AND", corporate
    boilerplate like "A FLORIDA LIMITED LIABILITY COMPANY") — confirmed
    against real data: "CHRISTOPHER G AND KIMBERLY A WEBER" auto-matched
    against "CHRISTOPHER B AND KIMBERLY A RIVELY" (different couples,
    different surnames), and "DIAZ 23 INVESTMENTS LLC" auto-matched against
    "FYLOS LLC" (unrelated companies sharing only the boilerplate suffix) —
    both at different addresses, both wrongly auto-confirmed as the same
    entity with zero review. A disagreeing address is real evidence against
    a match and now overrides a high name score, with one carve-out:
    NAME_NEAR_IDENTICAL (98+, functionally the same string) downgrades to
    'ambiguous' rather than an instant reject, since that's the one
    legitimate case this would otherwise wrongly kill — e.g. the same
    government entity filed under two valid mailing addresses.

    Email and phone are corroboration signals only — an exact match on
    either boosts a borderline name score (NAME_FUZZY_MIN ≤ score <
    NAME_AUTO_MATCH_MIN) up to the exact_name_address tier, and confirms
    a high name score (≥ NAME_AUTO_MATCH_MIN) even when no address is
    available. Neither signal alone overrides a clearly disagreeing address
    or a name score below the ambiguous floor.
    """
    name_score = fuzz.token_set_ratio(a.normalized_name, b.normalized_name)

    if name_score < NAME_AMBIGUOUS_MIN:
        return MatchVerdict(is_match=False, confidence=0, method="no_match")

    address_score: Optional[int] = None
    if a.mailing_address and b.mailing_address:
        address_score = fuzz.token_set_ratio(
            _street_portion(a.mailing_address).casefold(),
            _street_portion(b.mailing_address).casefold(),
        )

    if address_score is not None and address_score < ADDRESS_DISAGREE_MAX:
        if name_score >= NAME_NEAR_IDENTICAL:
            return MatchVerdict(
                is_match=False, confidence=0, method="ambiguous",
                explanation=f"name={name_score} (near-identical) but address disagrees (street={address_score})",
            )
        return MatchVerdict(
            is_match=False, confidence=0, method="no_match",
            explanation=f"name={name_score} but address clearly disagrees (street={address_score})",
        )

    matching_emails = a.emails & b.emails
    matching_phones = a.phones & b.phones
    contact_corroborated = bool(matching_emails or matching_phones)
    contact_detail = (
        f"email={next(iter(matching_emails))!r} matches" if matching_emails
        else (f"phone={next(iter(matching_phones))!r} matches" if matching_phones else "")
    )

    if name_score >= NAME_AUTO_MATCH_MIN and address_score is not None and address_score >= ADDRESS_AGREE_MIN:
        explanation = f"name={name_score}, street={address_score}"
        if contact_detail:
            explanation += f", {contact_detail}"
        return MatchVerdict(
            is_match=True,
            confidence=round((name_score + address_score) / 2),
            method="exact_name_address",
            explanation=explanation,
        )

    if name_score >= NAME_AUTO_MATCH_MIN and contact_corroborated:
        return MatchVerdict(
            is_match=True, confidence=name_score, method="exact_name_address",
            explanation=f"name={name_score}, {contact_detail} (no address)",
        )

    if name_score >= NAME_FUZZY_MIN and contact_corroborated:
        return MatchVerdict(
            is_match=True, confidence=name_score, method="exact_name_address",
            explanation=f"name={name_score} boosted by {contact_detail}",
        )

    if name_score >= NAME_FUZZY_MIN:
        addr_note = (
            f", street={address_score}" if address_score is not None
            else ", no address to corroborate"
        )
        return MatchVerdict(
            is_match=True, confidence=name_score, method="fuzzy_name",
            explanation=f"name={name_score}{addr_note}",
        )

    addr_note = f", street={address_score}" if address_score is not None else ""
    return MatchVerdict(
        is_match=False, confidence=0, method="ambiguous",
        explanation=f"name={name_score}{addr_note}, below auto-match floors",
    )


def score_blocked_pairs(
    blocks: dict[tuple[str, Optional[str]], list[CandidateRecord]],
) -> list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]]:
    """
    Score candidate pairs generated by blocking — but not every co-occurring
    pair earns a score. A pair is only scored if it has real corroborating
    evidence: either it co-occurred in a block with a genuine (non-None)
    ZIP, it shares at least two distinct name tokens, or it co-occurred in a
    CONTACT block (shared email/phone) — a contact match is stronger
    evidence than either of the other two gates on its own, so it always
    bypasses this gate rather than needing a second co-occurrence to clear it.

    Confirmed against real data: without the name/ZIP half of this gate, two
    unrelated people sharing only one common, possibly-generic first name
    (e.g. two different "PATRICIA ..." owners, neither with an address to
    corroborate — typical for deed-sourced candidates) flood the ambiguous
    band with pairs that are almost certainly not the same person, which
    would make H2.4's LLM tie-break prohibitively expensive at real volume.
    A shared surname-length or rarer token pair, or any ZIP-corroborated
    pair, still gets through — this only cuts the "one common first name,
    nothing else" case.
    """
    pair_records: dict[tuple, tuple[CandidateRecord, CandidateRecord]] = {}
    pair_token_count: dict[tuple, int] = defaultdict(int)
    pair_has_zip: dict[tuple, bool] = defaultdict(bool)
    pair_from_contact: dict[tuple, bool] = defaultdict(bool)

    for (block_key, zip_code), group in blocks.items():
        from_contact = is_contact_block_key(block_key)
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                key_a, key_b = _record_key(a), _record_key(b)
                if key_a == key_b:
                    continue
                pair_key = (key_a, key_b) if key_a < key_b else (key_b, key_a)
                pair_records[pair_key] = (a, b)
                pair_token_count[pair_key] += 1
                if zip_code is not None:
                    pair_has_zip[pair_key] = True
                if from_contact:
                    pair_from_contact[pair_key] = True

    results: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]] = []
    for pair_key, (a, b) in pair_records.items():
        if not (pair_has_zip[pair_key] or pair_token_count[pair_key] >= 2
                or pair_from_contact[pair_key]):
            continue
        verdict = score_candidate_pair(a, b)
        if verdict.method != "no_match":
            results.append((a, b, verdict))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# H2.4 — LLM tie-break for ambiguous pairs
# ─────────────────────────────────────────────────────────────────────────────

_LLM_TIE_BREAK_SYSTEM_PROMPT = """You are verifying whether two property-ownership records refer to the \
same real-world buyer (a person or company), for a real-estate data platform. \
Given two records, decide if they are the SAME buyer or a DIFFERENT buyer. \
Consider name similarity, address proximity, and whether one could plausibly \
be a variant spelling, maiden name, joint-ownership listing, or trustee \
designation of the other. Do not guess — if genuinely uncertain, say DIFFERENT \
and give a low confidence rather than inventing certainty.

Respond with two or three lines and nothing else:
SAME or DIFFERENT
<a confidence number from 0 to 100>
<optional: one short sentence explaining your reasoning, for a human audit trail>"""

_CONFIDENCE_RE = re.compile(r"\d+")


def _format_candidate_for_prompt(c: CandidateRecord) -> str:
    return (
        f"name: {c.raw_name!r}\n"
        f"mailing address: {c.mailing_address or 'unknown'}\n"
        f"type: {c.entity_type_hint or 'unknown'}"
    )


def _parse_llm_verdict(response: str) -> Optional[MatchVerdict]:
    """Parses the 2-line contract (verdict, confidence) exactly as before --
    a 3rd reasoning line, when present, is appended to explanation for
    human audit only and never affects is_match/confidence. Absence of the
    3rd line (or any response the model returns that omits it) is fully
    backward-compatible: this fails safe exactly as it did before that line
    was requested."""
    lines = [ln.strip() for ln in response.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    verdict_word = lines[0].upper()
    if verdict_word not in ("SAME", "DIFFERENT"):
        return None
    match = _CONFIDENCE_RE.search(lines[1])
    if match is None:
        return None
    confidence = max(0, min(100, int(match.group())))
    explanation = f"LLM returned {verdict_word} confidence={confidence}"
    if len(lines) >= 3 and lines[2]:
        explanation += f": {lines[2]!r}"
    return MatchVerdict(
        is_match=(verdict_word == "SAME"),
        confidence=confidence if verdict_word == "SAME" else 0,
        method="llm_adjudicated",
        explanation=explanation,
    )


def llm_adjudicate_pair(
    a: CandidateRecord, b: CandidateRecord, db: Optional[Session] = None,
) -> MatchVerdict:
    """
    The one place in this module that calls an LLM — a single, stateless
    classification call, cheap tier (claude_router._TASK_ROUTING
    ["buyer_entity_match"] = "haiku"). Only ever called for pairs H2.3
    flagged as genuinely ambiguous; never for anything score_candidate_pair
    already resolved deterministically.

    Fails safe: any API error or unparseable response returns a non-match
    rather than raising or guessing — per Hunter's constitution, "a polished
    guess is a constitution violation." A dropped match here just means the
    pair stays unlinked, not a wrong link.
    """
    from src.services.claude_router import call_claude

    prompt = (
        f"Record A:\n{_format_candidate_for_prompt(a)}\n\n"
        f"Record B:\n{_format_candidate_for_prompt(b)}"
    )

    try:
        response = call_claude(
            task_type="buyer_entity_match",
            messages=[{"role": "user", "content": prompt}],
            system=_LLM_TIE_BREAK_SYSTEM_PROMPT,
            max_tokens=80,  # was 20 -- the optional 3rd reasoning line needs room
            db=db,
        )
    except Exception as exc:
        logger.warning(
            "llm_adjudicate_pair: call_claude failed (%s vs %s): %s", a.raw_name, b.raw_name, exc,
        )
        return MatchVerdict(is_match=False, confidence=0, method="llm_adjudicated")

    verdict = _parse_llm_verdict(response)
    if verdict is None:
        logger.warning(
            "llm_adjudicate_pair: unparseable response for %s vs %s: %r", a.raw_name, b.raw_name, response,
        )
        return MatchVerdict(is_match=False, confidence=0, method="llm_adjudicated")
    return verdict


def adjudicate_ambiguous_pairs(
    scored_pairs: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]],
    session: Optional[Session] = None,
) -> list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]]:
    """
    Route every 'ambiguous' verdict from score_blocked_pairs through the LLM
    tie-break, replacing it with the LLM's verdict. Every other verdict
    (structural, exact_name_address, fuzzy_name) passes through unchanged —
    this function never re-scores something already resolved.
    """
    resolved: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]] = []
    for a, b, verdict in scored_pairs:
        if verdict.method == "ambiguous":
            resolved.append((a, b, llm_adjudicate_pair(a, b, db=session)))
        else:
            resolved.append((a, b, verdict))
    return resolved


# ─────────────────────────────────────────────────────────────────────────────
# H2.5 — Union-find cluster assembly
# ─────────────────────────────────────────────────────────────────────────────

class _UnionFind:
    """Standard disjoint-set-union, path compression + union by size, keyed
    by (source_table, source_id)."""

    def __init__(self) -> None:
        self._parent: dict[tuple[str, int], tuple[str, int]] = {}
        self._size: dict[tuple[str, int], int] = {}

    def _ensure(self, key: tuple[str, int]) -> None:
        if key not in self._parent:
            self._parent[key] = key
            self._size[key] = 1

    def find(self, key: tuple[str, int]) -> tuple[str, int]:
        self._ensure(key)
        root = key
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[key] != root:
            self._parent[key], key = root, self._parent[key]
        return root

    def union(self, a: tuple[str, int], b: tuple[str, int]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._size[ra] < self._size[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        self._size[ra] += self._size[rb]

    def components(self) -> dict[tuple[str, int], list[tuple[str, int]]]:
        """Root key -> every member key unioned into its component."""
        groups: dict[tuple[str, int], list[tuple[str, int]]] = defaultdict(list)
        for key in list(self._parent):
            groups[self.find(key)].append(key)
        return groups


def build_clusters(
    edges: Iterable[tuple[CandidateRecord, CandidateRecord, MatchVerdict]],
) -> list[list[CandidateRecord]]:
    """
    Assemble confirmed edges (from find_structural_edges, score_blocked_pairs,
    and adjudicate_ambiguous_pairs — is_match=True verdicts only) into
    connected components: one list of CandidateRecords per real-world buyer.

    Scope boundary: only candidates that appear in at least one is_match=True
    edge show up here. A candidate with zero matching edges is still a valid
    buyer entity of its own (a singleton) — finding and materializing those is
    the materialization step's job (H2.6/H2.7), not this function's; keeping
    this pure-clustering step from needing the full candidate list (only the
    edges) keeps it independently unit-testable.
    """
    uf = _UnionFind()
    record_by_key: dict[tuple[str, int], CandidateRecord] = {}

    for a, b, verdict in edges:
        if not verdict.is_match:
            continue  # defensive — callers should already filter to is_match=True
        key_a, key_b = _record_key(a), _record_key(b)
        record_by_key[key_a] = a
        record_by_key[key_b] = b
        uf.union(key_a, key_b)

    clusters: list[list[CandidateRecord]] = []
    for member_keys in uf.components().values():
        clusters.append([record_by_key[k] for k in member_keys])
    return clusters


# ─────────────────────────────────────────────────────────────────────────────
# H2.6 / H2.7 — Materialization (shared by the one-time backfill script and
# the nightly incremental sweep)
# ─────────────────────────────────────────────────────────────────────────────

def find_singletons(
    candidates: Iterable[CandidateRecord], clusters: list[list[CandidateRecord]],
) -> list[list[CandidateRecord]]:
    """Candidates that appeared in extraction but matched nothing — still
    valid buyer entities, just clusters of size 1."""
    clustered_keys = {_record_key(r) for cluster in clusters for r in cluster}
    return [[c] for c in candidates if _record_key(c) not in clustered_keys]


def canonical_name(cluster: list[CandidateRecord], principal_name: Optional[str] = None) -> str:
    """Prefer an Individual/Trust name (more recognizable to a human
    reviewer) over an LLC name; among same-type candidates, prefer the
    longest (most complete) raw name.

    An LLC-only cluster (no Individual/Trust candidate) with a known
    Sunbiz-pierced principal_name uses that instead of the longest LLC
    name -- otherwise a cluster of e.g. nine LLCs sharing one managing
    member is named after whichever LLC name is longest, when the actual
    person's name was already known from the piercing edge. An LLC-only
    cluster with NO pierced principal (principal_name is None) keeps the
    prior behavior exactly."""
    individual_like = [c for c in cluster if c.entity_type_hint in ("Individual", "Trust")]
    if individual_like:
        return max(individual_like, key=lambda c: len(c.raw_name)).raw_name
    if principal_name:
        return principal_name
    return max(cluster, key=lambda c: len(c.raw_name)).raw_name


def entity_type_for_cluster(cluster: list[CandidateRecord]) -> str:
    type_hints = {c.entity_type_hint for c in cluster if c.entity_type_hint}
    for preferred in ("Individual", "Trust", "LLC", "Corporate", "Estate"):
        if preferred in type_hints:
            return preferred
    return "Individual"  # deed-only clusters carry no entity_type_hint at all


def primary_mailing_address(cluster: list[CandidateRecord]) -> Optional[str]:
    addresses = [c.mailing_address for c in cluster if c.mailing_address]
    if not addresses:
        return None
    return Counter(addresses).most_common(1)[0][0]


def primary_email(cluster: list[CandidateRecord]) -> Optional[str]:
    """Modal email across the cluster's member records -- denormalized onto
    buyer_entities.primary_email so run_incremental's existing-entity
    anchors can be contact-matched against a new owners/deeds row."""
    emails = [e for c in cluster for e in c.emails]
    if not emails:
        return None
    return Counter(emails).most_common(1)[0][0]


def primary_phone(cluster: list[CandidateRecord]) -> Optional[str]:
    """Modal phone across the cluster's member records -- same rationale as
    primary_email. Already E.164-normalized on the way in (phone_utils)."""
    phones = [p for c in cluster for p in c.phones]
    if not phones:
        return None
    return Counter(phones).most_common(1)[0][0]


def _cluster_index_by_key(clusters: list[list[CandidateRecord]]) -> dict[tuple[str, int], int]:
    mapping: dict[tuple[str, int], int] = {}
    for i, cluster in enumerate(clusters):
        for rec in cluster:
            mapping[_record_key(rec)] = i
    return mapping


def compute_cluster_confidences(
    clusters: list[list[CandidateRecord]],
    edges: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]],
) -> list[int]:
    """
    Per-cluster confidence = the minimum non-structural link confidence
    among edges connecting its members — a chain is only as strong as its
    weakest inferred link. Structural (sunbiz_llc_piercing) edges are
    excluded from the rollup: near-certain by construction (fixed 97), they
    shouldn't inflate a cluster actually held together by a much weaker
    fuzzy/LLM link elsewhere. A cluster held together entirely by structural
    edges gets that structural confidence, since that IS all the evidence.
    Singleton clusters (no edges at all) get 100 — nothing was inferred, so
    there's no matching uncertainty to score.
    """
    index_by_key = _cluster_index_by_key(clusters)
    non_structural_mins: dict[int, int] = {}
    structural_max: dict[int, int] = {}

    for a, b, verdict in edges:
        if not verdict.is_match:
            continue
        idx = index_by_key.get(_record_key(a))
        if idx is None:
            continue
        if verdict.method == "sunbiz_llc_piercing":
            structural_max[idx] = max(structural_max.get(idx, 0), verdict.confidence)
        else:
            current = non_structural_mins.get(idx)
            non_structural_mins[idx] = verdict.confidence if current is None else min(current, verdict.confidence)

    confidences = []
    for i in range(len(clusters)):
        if i in non_structural_mins:
            confidences.append(non_structural_mins[i])
        elif i in structural_max:
            confidences.append(structural_max[i])
        else:
            confidences.append(100)
    return confidences


def build_evidence_index(
    edges: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]],
) -> dict[tuple[str, int], tuple[str, str, int, Optional[str]]]:
    """
    record_key -> its single best (method, explanation, confidence,
    principal_name) across every edge it appears in. Built in ONE pass over
    all edges so per-record lookup is O(1) — looping the full edge list per
    record would be O(records x edges), infeasible at real dataset scale.
    """
    best_by_key: dict[tuple[str, int], tuple[str, str, int, Optional[str]]] = {}
    for a, b, verdict in edges:
        if not verdict.is_match:
            continue
        for rec in (a, b):
            key = _record_key(rec)
            current = best_by_key.get(key)
            if current is None or verdict.confidence > current[2]:
                best_by_key[key] = (
                    verdict.method, verdict.explanation, verdict.confidence, verdict.principal_name,
                )
    return best_by_key


def _cluster_principal_name(
    records: list[CandidateRecord],
    evidence_index: dict[tuple[str, int], tuple[str, str, int, Optional[str]]],
) -> Optional[str]:
    """Modal principal_name across a cluster's records' best evidence, or
    None if no record in the cluster was linked via Sunbiz piercing. Used
    to name an LLC-only cluster after its actual controlling person instead
    of after an LLC's name -- see canonical_name()."""
    names = [
        evidence_index[_record_key(rec)][3]
        for rec in records
        if _record_key(rec) in evidence_index and evidence_index[_record_key(rec)][3]
    ]
    if not names:
        return None
    return Counter(names).most_common(1)[0][0]


def _new_entity_from_cluster(
    cluster: list[CandidateRecord], confidence: int, principal_name: Optional[str] = None,
) -> BuyerEntity:
    return BuyerEntity(
        canonical_name=canonical_name(cluster, principal_name=principal_name),
        entity_type=entity_type_for_cluster(cluster),
        primary_mailing_address=primary_mailing_address(cluster),
        primary_email=primary_email(cluster),
        primary_phone=primary_phone(cluster),
        principal_name=principal_name,
        confidence_score=confidence,
        verification_status=verification_status(confidence),
        county_id=cluster[0].county_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# H2.7 — Incremental matching (nightly-safe)
# ─────────────────────────────────────────────────────────────────────────────

_ENTITY_ANCHOR_TABLE = "buyer_entities"


def load_existing_entity_candidates(
    session: Session, county_id: Optional[str] = None,
) -> list[CandidateRecord]:
    """
    Represent each existing buyer_entities row as a CandidateRecord (using
    its denormalized canonical_name/primary_mailing_address/primary_email/
    primary_phone), so the SAME blocking/scoring/clustering pipeline used
    for raw records can also match new candidates against already-resolved
    entities. source_table='buyer_entities' marks these as entity anchors,
    not raw source rows — find_structural_edges already skips non-'owners'
    candidates, so anchors never contribute spurious new structural facts
    of their own.

    Without primary_email/primary_phone here, a new owners/deeds row could
    never contact-match an EXISTING entity in incremental (nightly) mode —
    only two records seen together in the same run could ever contact-
    corroborate, since block_candidates() only sees what's passed to it.
    """
    where_county = "WHERE county_id = :county_id" if county_id else ""
    rows = session.execute(
        text(f"SELECT id, canonical_name, primary_mailing_address, entity_type, "
             f"county_id, primary_email, primary_phone "
             f"FROM buyer_entities {where_county}"),
        {"county_id": county_id} if county_id else {},
    )
    result = []
    for row in rows:
        result.append(CandidateRecord(
            source_table=_ENTITY_ANCHOR_TABLE,
            source_id=row.id,
            raw_name=row.canonical_name,
            normalized_name=BaseLoader.normalize_owner_name(row.canonical_name),
            mailing_address=row.primary_mailing_address,
            entity_type_hint=row.entity_type,
            managing_members=None,
            county_id=row.county_id,
            emails=frozenset({row.primary_email}) if row.primary_email else frozenset(),
            phones=frozenset({row.primary_phone}) if row.primary_phone else frozenset(),
        ))
    return result


def cluster_against_anchors(
    combined: list[CandidateRecord],
) -> tuple[list[list[CandidateRecord]], list[int], dict, list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]]]:
    """
    Shared structural+blocking+scoring+clustering pass over a combined set
    of new/unresolved CandidateRecords plus existing buyer_entities anchors
    (see load_existing_entity_candidates). Used by both run_incremental
    (H2.7) and run_backfill (H2.6) so "does this new record match something
    that already exists" is answered identically in both places rather than
    two divergent implementations. Returns (relevant_clusters, confidences,
    evidence_index, ambiguous_pairs) -- relevant_clusters excludes
    pure-anchor singletons (existing entities matching nothing new need no
    action). ambiguous_pairs are the verdicts this pass correctly declined
    to link — callers write these to buyer_entity_match_exception (WI-4) so
    the client's "possible-match flag routes to EXCEPTIONS" is durable, not
    forgotten the moment this function returns.

    Stays a pure function -- no DB access, no side effects. Recording
    exceptions is the DB-sweep layer's job (run_incremental / run_backfill),
    same separation the module's own docstring already establishes.
    """
    structural_edges = find_structural_edges(combined)
    blocks = block_candidates(combined)
    scored = score_blocked_pairs(blocks)
    # No LLM tie-break (deliberate — Option A: never merge on uncertain
    # evidence). Ambiguous verdicts already carry is_match=False, so this
    # filter naturally excludes them without calling adjudicate_ambiguous_pairs.
    all_edges = structural_edges + [e for e in scored if e[2].is_match]
    ambiguous_pairs = [e for e in scored if e[2].method == "ambiguous"]

    clusters = build_clusters(all_edges)
    all_singletons = find_singletons(combined, clusters)
    # a singleton that's a pure existing-entity anchor with no new candidate
    # attached needs no action at all.
    new_singletons = [c for c in all_singletons if any(r.source_table != _ENTITY_ANCHOR_TABLE for r in c)]

    relevant_clusters = clusters + new_singletons
    confidences = compute_cluster_confidences(relevant_clusters, all_edges)
    evidence_index = build_evidence_index(all_edges)
    return relevant_clusters, confidences, evidence_index, ambiguous_pairs


def attach_or_create_entities(
    session: Session,
    relevant_clusters: list[list[CandidateRecord]],
    confidences: list[int],
    evidence_index: dict,
) -> dict:
    """
    Shared per-cluster materialization for H2.6/H2.7: a cluster touching
    exactly one existing buyer_entities anchor attaches its new records to
    that entity via a new BuyerEntityLink (existing entity IDs never
    change); a cluster touching zero anchors becomes a brand new entity.

    A cluster touching 2+ existing entities is a potential MERGE case (e.g.
    a new Sunbiz filing reveals two previously-separate entities share an
    owner) — deliberately NOT auto-merged, since collapsing established
    entity IDs risks breaking whatever already references them (whale
    flags, Cell #1's list). Flagged and left unresolved for manual review
    instead of guessing.

    Does not commit -- callers control the commit/batch boundary (H2.7
    commits once; H2.6's backfill commits every _ENTITY_COMMIT_BATCH
    clusters against the shared production DB).
    """
    new_entities_created = 0
    new_links_created = 0
    conflicts = 0
    # HUNTER-04/05 (H3/H4's nightly incremental wiring) need to know exactly
    # which entities this run touched, so downstream profiling/classification
    # steps can scope to entity_ids instead of re-scanning the whole table
    # every night -- see hunter_nightly_sweep.py. A set, not a list: the same
    # entity can be touched by more than one cluster in one run (e.g. two
    # separate new deeds both attaching to the same existing entity).
    changed_entity_ids: set[int] = set()

    for cluster, confidence in zip(relevant_clusters, confidences):
        entity_anchors = [r for r in cluster if r.source_table == _ENTITY_ANCHOR_TABLE]
        new_records = [r for r in cluster if r.source_table != _ENTITY_ANCHOR_TABLE]
        if not new_records:
            continue  # existing entities matching each other -- not this run's concern

        if len(entity_anchors) > 1:
            anchor_ids = sorted(a.source_id for a in entity_anchors)
            logger.warning(
                "attach_or_create_entities: cluster touches %d existing entities (ids=%s) -- "
                "potential merge, not auto-resolving; new records left unresolved: %s",
                len(entity_anchors), anchor_ids,
                [(r.source_table, r.source_id) for r in new_records],
            )
            # Representative refs for the unique key -- an N-anchor conflict
            # doesn't reduce to a natural pair, so left_ref is the sorted
            # anchor set (the actual conflict) and right_ref is the first new
            # record that bridged them (entity_ids on the row carries the
            # full anchor list for an admin UI; this is just for uniqueness).
            left_ref = f"buyer_entities#{','.join(str(i) for i in anchor_ids)}"
            first_new = new_records[0]
            right_ref = f"{first_new.source_table}#{first_new.source_id}"
            record_exception(
                session, kind="multi_anchor_conflict", left_ref=left_ref, right_ref=right_ref,
                entity_ids=anchor_ids,
                explanation=(
                    f"cluster touches {len(entity_anchors)} existing entities "
                    f"(ids={anchor_ids}) via {len(new_records)} new record(s) "
                    f"starting with {right_ref} -- potential merge, not auto-resolved"
                ),
            )
            conflicts += 1
            continue

        if len(entity_anchors) == 1:
            entity_id = entity_anchors[0].source_id
        else:
            principal = _cluster_principal_name(new_records, evidence_index)
            entity = _new_entity_from_cluster(cluster, confidence, principal_name=principal)
            session.add(entity)
            session.flush()
            entity_id = entity.id
            new_entities_created += 1

        changed_entity_ids.add(entity_id)
        for rec in new_records:
            # 'singleton_no_edge', not 'manual' -- this fallback fires for a
            # record with no corroborating edge in evidence_index (a
            # single-record cluster), not a human decision. 'manual' means
            # exactly what it says and is reserved for an actual human-set
            # link (e.g. an admin merge/reject action) -- see WI-5.
            method, explanation, link_confidence, _principal_name = evidence_index.get(
                _record_key(rec),
                ("singleton_no_edge", "single-record cluster; no corroborating edge", 100, None),
            )
            session.execute(insert(BuyerEntityLink).values(
                buyer_entity_id=entity_id,
                source_table=rec.source_table,
                source_id=rec.source_id,
                match_confidence=link_confidence,
                match_method=method,
                match_explanation=explanation or None,
            ))
            new_links_created += 1

    return {
        "new_entities": new_entities_created,
        "new_links": new_links_created,
        "conflicts": conflicts,
        "changed_entity_ids": sorted(changed_entity_ids),
    }


def record_ambiguous_pair_exceptions(
    session: Session,
    ambiguous_pairs: list[tuple[CandidateRecord, CandidateRecord, MatchVerdict]],
    max_new: int = DEFAULT_MAX_NEW_EXCEPTIONS_PER_RUN,
) -> int:
    """
    Write every ambiguous verdict cluster_against_anchors declined to link
    into buyer_entity_match_exception (kind='ambiguous_pair') -- the
    client's "possible-match flag routes to EXCEPTIONS" requirement.
    Written as ONE batched upsert (record_exceptions_batch), not one INSERT
    per pair -- run_incremental scores the entire anchor table against every
    new candidate (pre-existing design), so a single sweep routinely
    produces hundreds of these; a per-row round trip would make every
    incremental sweep call meaningfully slower for no benefit. Idempotent:
    a pair re-seen on a later sweep just bumps last_seen_at rather than
    creating a duplicate row.

    max_new caps how many pairs from THIS CALL get processed -- a first
    full backfill over ~810k entities must not attempt a million writes in
    one sweep. Pairs already recorded in a prior sweep are deprioritized
    below the cap (they're already in EXCEPTIONS -- re-upserting them just
    bumps last_seen_at) so a persistent ambiguity set can never crowd out
    a not-yet-seen pair forever; every pair reaches the queue within
    ceil(unseen_count / max_new) sweeps instead of never. Does not commit —
    caller controls the transaction boundary. Returns the count actually
    recorded (for logging).
    """
    rows = []
    refs = []
    for a, b, verdict in ambiguous_pairs:
        name_score = int(fuzz.token_set_ratio(a.normalized_name, b.normalized_name))
        address_score: Optional[int] = None
        if a.mailing_address and b.mailing_address:
            address_score = int(fuzz.token_set_ratio(
                _street_portion(a.mailing_address).casefold(),
                _street_portion(b.mailing_address).casefold(),
            ))
        left_key, right_key = _record_key(a), _record_key(b)
        left_ref, right_ref = f"{left_key[0]}#{left_key[1]}", f"{right_key[0]}#{right_key[1]}"
        if right_ref < left_ref:  # canonical order -- keeps the unique constraint from
            left_ref, right_ref = right_ref, left_ref  # treating (X,Y) and (Y,X) as distinct
        refs.append((left_ref, right_ref))
        rows.append({
            "kind": "ambiguous_pair", "left_ref": left_ref, "right_ref": right_ref,
            "explanation": verdict.explanation or f"name={name_score}, no further detail",
            "name_score": name_score, "address_score": address_score,
        })

    if len(rows) > max_new:
        existing = set()
        if refs:
            # ANY(:left_refs) narrows to candidate rows in one round trip; the
            # exact (left_ref, right_ref) match happens below in Python since
            # left_ref alone can't distinguish which right_ref paired with it.
            candidate_rows = session.execute(
                text("""
                    SELECT left_ref, right_ref FROM buyer_entity_match_exception
                     WHERE kind = 'ambiguous_pair'
                       AND left_ref = ANY(:left_refs)
                """),
                {"left_refs": [lr for lr, _ in refs]},
            ).mappings()
            existing = {(r["left_ref"], r["right_ref"]) for r in candidate_rows}
        # unseen pairs sort first so they win the cap; already-recorded pairs
        # (stable sort keeps their original relative order) fill any remainder.
        order = sorted(
            range(len(rows)), key=lambda i: refs[i] in existing,
        )
        rows = [rows[i] for i in order[:max_new]]
        logger.warning(
            "record_ambiguous_pair_exceptions: %d ambiguous pairs this run, "
            "capped to %d (unseen pairs prioritized) -- remaining %d will be "
            "re-evaluated on the next sweep",
            len(refs), max_new, len(refs) - max_new,
        )

    record_exceptions_batch(session, rows)
    return len(rows)


def run_incremental(session: Session, county_id: Optional[str] = None) -> dict:
    """
    Nightly-safe incremental resolution: match new/changed owners/deeds rows
    (only_unresolved=True) against EXISTING buyer_entities first — via their
    denormalized canonical_name/primary_mailing_address, run through the
    identical blocking/scoring/clustering pipeline the backfill uses — rather
    than re-clustering the world. Existing entity IDs never change: a new
    record either attaches to one via a new BuyerEntityLink, or (matching
    nothing existing) forms a brand new entity, exactly as the backfill does
    for first-time records. Commits once at the end.

    Returned stats include changed_entity_ids — every buyer_entity_id this
    run created or attached a new link to — so callers (hunter_nightly_sweep.py)
    can scope downstream profiling/classification to just what changed instead
    of re-scanning the whole buyer_entities table every run.
    """
    new_candidates = list(extract_candidates(session, only_unresolved=True))
    if county_id:
        new_candidates = [c for c in new_candidates if c.county_id == county_id]

    if not new_candidates:
        return {"new_entities": 0, "new_links": 0, "conflicts": 0, "processed": 0, "changed_entity_ids": []}

    # Anchors are loaded across ALL counties, never scoped to county_id --
    # a buyer entity isn't bound to one county. Scoping this to the cron's
    # county would make a cross-county repeat buyer (already resolved in
    # Hillsborough, say) invisible when their next purchase shows up in
    # Pinellas, producing a duplicate entity instead of a link onto the
    # existing one -- confirmed as a real production bug via a
    # same-owner-two-counties fixture (see tests/scenarios/
    # test_hunter_resolution_fixes.py).
    existing_entities = load_existing_entity_candidates(session, county_id=None)
    combined = new_candidates + existing_entities

    relevant_clusters, confidences, evidence_index, ambiguous_pairs = cluster_against_anchors(combined)
    stats = attach_or_create_entities(session, relevant_clusters, confidences, evidence_index)
    exceptions_recorded = record_ambiguous_pair_exceptions(session, ambiguous_pairs)

    session.commit()
    return {**stats, "processed": len(new_candidates), "exceptions_recorded": exceptions_recorded}


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio aggregation — shared by H3 (portfolio/cadence reporting) and
# HUNTER-02's W1 (whale threshold check). Neither H2.1-H2.7 nor the backfill/
# incremental scripts populate BuyerEntity.total_purchase_count/
# total_cash_volume, even though the columns exist on the schema from H1 --
# this is that missing piece.
# ─────────────────────────────────────────────────────────────────────────────

def refresh_portfolio_aggregates(session: Session, entity_ids: Optional[list[int]] = None) -> int:
    """
    Recompute BuyerEntity.total_purchase_count/total_cash_volume from linked
    deeds, in one SQL statement — never looping per-entity (an entity-by-
    entity Python loop would be exactly the "query inside a loop" pattern
    this repo's conventions forbid). Entities with no deed-sourced links are
    left at their existing value (0 by default) — the aggregation only
    touches entities that actually have deed history to sum.

    This is an ALL-TIME count/sum, a general portfolio-size summary for
    H3's reporting. It is NOT the same question as HUNTER-02's whale rule
    ("3+ purchases in the trailing 18 months") — that's a rolling window,
    and an all-time count can't answer it (3 purchases spread over 5 years
    doesn't qualify; W1 runs its own dedicated windowed query for that half
    of the rule, and reuses total_cash_volume from here for its other half,
    the >$500K-all-time check).

    entity_ids=None recomputes every entity (used once after the backfill);
    passing specific IDs scopes the recompute to just those (used by the
    nightly sweep, which only needs to refresh entities that got a new link
    this run, not the whole table).

    purchase_count is DISTINCT property_id, not a raw row count, and
    cash_volume excludes sale_price < $1,000 -- found via the HUNTER-02
    founder spot-check: multiple deed documents recorded for the same
    property/date (corrective re-recordings) and $1/$10 nominal-consideration
    transfers (family, trust) were inflating purchase counts without any
    real purchase happening.

    Both figures are computed from ONE canonical deed per
    (buyer_entity_id, property_id) -- the most recently recorded one, ties
    broken by id -- picked via DISTINCT ON before the count/sum. Without
    this, a corrective re-recording (a second deeds row for the same
    property, same sale) would still be counted once by
    COUNT(DISTINCT property_id) but its sale_price would be summed AGAIN
    on top of the original row's, inflating total_cash_volume and risking a
    false whale flag on a duplicated consideration amount rather than a
    real second purchase.
    """
    where_clause = "WHERE bel.buyer_entity_id = ANY(:entity_ids)" if entity_ids else ""
    result = session.execute(
        text(f"""
            UPDATE buyer_entities be
            SET total_purchase_count = agg.purchase_count,
                total_cash_volume = agg.cash_volume,
                last_updated_at = now()
            FROM (
                SELECT canonical.buyer_entity_id,
                       COUNT(*) AS purchase_count,
                       COALESCE(SUM(canonical.sale_price) FILTER (WHERE canonical.sale_price >= 1000), 0) AS cash_volume
                FROM (
                    SELECT DISTINCT ON (bel.buyer_entity_id, d.property_id)
                           bel.buyer_entity_id,
                           d.property_id,
                           d.sale_price
                    FROM buyer_entity_links bel
                    JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
                    {where_clause}
                    ORDER BY bel.buyer_entity_id, d.property_id, d.record_date DESC NULLS LAST, d.id DESC
                ) canonical
                GROUP BY canonical.buyer_entity_id
            ) agg
            WHERE be.id = agg.buyer_entity_id
        """),
        {"entity_ids": entity_ids} if entity_ids else {},
    )
    session.commit()
    return result.rowcount
