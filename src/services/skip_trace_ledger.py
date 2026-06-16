"""
Provider-generic skip-trace dedup ledger + per-run spend guard.

Skip-trace vendors (Tracerfy, BatchData, PDL) bill per lookup with **no
server-side dedup** — the only thing stopping us re-paying to trace an address
we have already traced is our own code. This module is that guardrail, factored
out so every skip-trace fallback reuses one well-tested implementation.

Two design rules, learned the hard way (≈$146 of duplicate Tracerfy charges):

1. **Dedup on SUBMISSION history, not on the recorded hit/miss flag.** A result
   we mislabel as a "miss" may have been a paid hit; the only trustworthy signal
   that we already spent money on an address is that we *submitted* it. Keying on
   `match_success` is what let already-hit addresses leak back into the pipeline
   and get re-charged on every run.

2. **Billing model selects the retry policy.** `PER_HIT` vendors (Tracerfy, PDL)
   charge 0 on a miss, so retrying a genuine miss is free and a single bounded
   retry is safe. `ALWAYS` vendors (BatchData) charge on every call, so any prior
   submission must block all re-submission.

Key granularity: the trace key is **building-level** — `normalize_street_address`
(which drops occupancy/unit) + 5-digit ZIP. This matches `Property.normalized_address`
(the historical ledger source, which also drops units) and biases toward NOT
re-submitting, the safe direction for billing. The full unit-bearing address is
still what gets sent to the vendor; the key is used only for dedup and matching.
"""

from __future__ import annotations

import enum
from typing import Iterable, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.utils.address_normalize import normalize_street_address

# Trace modes. Tracerfy "normal" = name+address (1 credit/hit); "advanced" =
# address-only (2 credits/hit). Other vendors map their tiers onto these.
NORMAL = "normal"
ADVANCED = "advanced"


class BillingModel(enum.Enum):
    """How a vendor bills, which decides whether a retry can re-charge us."""

    PER_HIT = "per_hit"   # 0 credits on a miss (Tracerfy, PDL) — a miss is free to retry
    ALWAYS = "always"     # charged on every call (BatchData) — never re-submit


def trace_key(address: Optional[str], zip_code: Optional[str]) -> str:
    """
    Canonical building-level dedup/match key: ``"<normalized street>|<zip5>"``.

    Used for BOTH the submission guardrail and result matching so the two sides
    always agree. Returns ``""`` for an address that cannot be normalized (e.g.
    intersections, blanks) — callers must treat an empty key as unmatchable and
    never submit it blind.
    """
    core = normalize_street_address(address)
    if not core:
        return ""
    return f"{core}|{(zip_code or '')[:5]}"


def already_traced(session: Session, source: str, keys: Iterable[str]) -> dict[str, set[str]]:
    """
    Return ``{trace_key: {modes_already_submitted}}`` for the given keys.

    Unions two sources, both parameterized by ``source`` so any vendor can use it:

      1. ``enriched_contacts`` joined to ``properties`` — mode-aware via
         ``trace_type`` and seeds traces that predate ``target_address`` logging.
         Legacy rows with a NULL ``trace_type`` are treated as ``advanced`` (the
         stricter floor) so they can never trigger another paid advanced retry.
      2. ``enrichment_usage_logs.target_address`` — the go-forward submission
         ledger every consumer of this module writes. Mode-blind, so a hit there
         contributes at least a NORMAL submission (existence floor).

    One set-based query per source — no per-row queries.
    """
    unique_keys = {k for k in keys if k}
    if not unique_keys:
        return {}

    ledger: dict[str, set[str]] = {}

    # Map normalized-core -> the full keys requesting it, so we can rebuild the
    # "core|zip5" key from the (normalized_address, zip) the DB returns.
    cores = sorted({k.rsplit("|", 1)[0] for k in unique_keys})

    ec_rows = session.execute(sa_text("""
        SELECT p.normalized_address                           AS core,
               left(coalesce(p.zip, ''), 5)                   AS z5,
               bool_or(ec.trace_type = 'advanced'
                       OR ec.trace_type IS NULL)              AS did_advanced,
               bool_or(ec.trace_type = 'normal')              AS did_normal
        FROM enriched_contacts ec
        JOIN properties p ON p.id = ec.property_id
        WHERE ec.source = :source
          AND p.normalized_address = ANY(:cores)
        GROUP BY p.normalized_address, left(coalesce(p.zip, ''), 5)
    """), {"source": source, "cores": cores}).mappings().all()

    for r in ec_rows:
        key = f"{r['core']}|{r['z5']}"
        if key not in unique_keys:
            continue
        modes = ledger.setdefault(key, set())
        if r["did_normal"]:
            modes.add(NORMAL)
        if r["did_advanced"]:
            modes.add(ADVANCED)

    usage_rows = session.execute(sa_text("""
        SELECT DISTINCT target_address
        FROM enrichment_usage_logs
        WHERE vendor = :source
          AND target_address = ANY(:keys)
    """), {"source": source, "keys": sorted(unique_keys)}).mappings().all()

    for r in usage_rows:
        ledger.setdefault(r["target_address"], set()).add(NORMAL)

    return ledger


def should_submit(key: str, mode: str, ledger: dict[str, set[str]], billing: BillingModel) -> bool:
    """
    The submission gate. ``ledger`` is the dict returned by :func:`already_traced`.

    - Empty key (unmatchable address): never submit blind.
    - ``ALWAYS`` billing: block if the address was submitted in ANY mode.
    - ``PER_HIT`` billing:
        * ``normal``  — submit only if no prior normal submission.
        * ``advanced``— submit only as the single retry of a prior normal
          submission (requires a prior normal, blocks a second advanced).
    """
    if not key:
        return False

    modes = ledger.get(key, set())

    if billing is BillingModel.ALWAYS:
        return not modes

    if mode == NORMAL:
        return NORMAL not in modes
    if mode == ADVANCED:
        return (NORMAL in modes) and (ADVANCED not in modes)
    return False


class RunSpendCap:
    """
    Hard per-run spend ceiling — a backstop independent of the dedup ledger.

    Track worst-case projected cost (assume every submitted record hits) and ask
    :meth:`would_exceed` before submitting each batch. A ``None`` or non-positive
    ceiling disables the cap.
    """

    def __init__(self, ceiling_cents: Optional[int]):
        self.ceiling_cents = ceiling_cents if (ceiling_cents and ceiling_cents > 0) else None
        self.projected_cents = 0

    def would_exceed(self, extra_cents: int) -> bool:
        if self.ceiling_cents is None:
            return False
        return (self.projected_cents + extra_cents) > self.ceiling_cents

    def add(self, cents: int) -> None:
        self.projected_cents += cents

    @property
    def remaining_cents(self) -> Optional[int]:
        if self.ceiling_cents is None:
            return None
        return max(0, self.ceiling_cents - self.projected_cents)
