"""
Cross-source contact triangulation thresholds (ADR 0015).

Source of truth for corroboration rules — mirrors the config/matching.py /
config/scoring.py pattern. The classifier in
src/services/contact_triangulation.py takes these as injectable defaults so
unit tests can override without monkeypatching.
"""

# ── name agreement ───────────────────────────────────────────────────────────
# rapidfuzz token_set_ratio on BaseLoader.normalize_owner_name() output.
# Voters match to the *property* (household), not the owner — a phone match
# without name agreement may mean the trace found a tenant/co-resident.
NAME_AGREEMENT_MIN = 80

# ── corroboration strength rules ─────────────────────────────────────────────
# Weak corroboration caps the freshness label at this level.
WEAK_CAP_LABEL = "medium"
# A match against a historical voter phone (voters.phones JSONB) is weak.
HISTORICAL_VOTER_PHONE_IS_WEAK = True
# A match against an inactive (registration_status='INA') voter is weak.
INA_VOTER_IS_WEAK = True

# ── email corroboration (full witness — see ADR 0015 email probe) ────────────
# An EC<->voter email match with name agreement is identity-anchor-grade and
# can lift a single-source mobile to strong/high.
EMAIL_MATCH_IS_IDENTITY_ANCHOR = True
# Role inboxes corroborate identity weakly (shared, not personal).
EMAIL_WEAK_LOCALPARTS = frozenset({"info", "admin", "office", "contact", "sales", "support"})
# Disposable domains never corroborate strongly. Extend as found in data.
EMAIL_DISPOSABLE_DOMAINS = frozenset({"mailinator.com", "guerrillamail.com", "10minutemail.com"})

# ── freshness interaction ────────────────────────────────────────────────────
# Strong corroboration: base-score boost and relaxed age decay (a number two
# independent sources keep confirming decays slower).
STRONG_BASE_BOOST = 0.20
STRONG_AGE_DECAY_FACTOR = 0.5
# Weak corroboration: small boost, label capped at WEAK_CAP_LABEL.
WEAK_BASE_BOOST = 0.05

# ── pack validation ──────────────────────────────────────────────────────────
PACK_CONTACTABLE_LABELS = frozenset({"high", "medium"})
PACK_MIN_CONTACTABLE_PCT = 0.80
