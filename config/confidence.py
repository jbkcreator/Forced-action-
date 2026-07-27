"""A2 — Lead Confidence gating constants.

Tuning knobs for the Lead Confidence metric (see tasks/A2-implementation-plan.md
and CONTEXT.md "Lead Confidence Gating (A2)"). Structure is fixed; these values
are provisional and meant to be retuned against real data without a code change.

Match-quality bands (auto_match / review_min) are NOT defined here — they are
reused from config/matching.py so A2 never drifts from the loaders' definition
of a good record match.
"""

# A lead with Lead Confidence below this is flagged is_guess_lead and withheld
# from paid surfaces (Lead Feed / Lead Packs / Lifecycle recs).
MIN_CONFIDENCE_THRESHOLD = 0.40

# Weighted blend of the two factors. Must sum to 1.0.
W_MATCH = 0.6  # how well the underlying records matched to the parcel
W_CORR = 0.4   # how many independent signals corroborate (thin vs solid file)

# corroboration_component as a function of N = distinct corroborating signals
# within STACKING_WINDOW_DAYS. N >= 4 saturates at 1.0.
CORROBORATION_CURVE = {1: 0.0, 2: 0.5, 3: 0.8}
CORROBORATION_MAX = 1.0
