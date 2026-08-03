# Agent Constitutions

Versioned, checked-in source of truth for each fleet agent's operating rules. These files supersede *FA-Agent-Lane-Build-Spec-TEAM.pdf* Parts 2–4 for build purposes — the PDF is the historical origin, not the thing to keep reading.

| File | Agent | Seat |
|---|---|---|
| [vera.md](vera.md) | Vera | Truth & Verification |
| [cora.md](cora.md) | Cora | Revenue Generation |
| [hunter.md](hunter.md) | Hunter | Data & Targets |

Each file is self-contained (no cross-file references) even where the source spec noted a section as "identical to Vera's" or "identical to fleet sections" — those are expanded in full in each file so a single agent's constitution can be read on its own.

## Structure

Every constitution follows the same section order, mirroring the spec's own format:

1. **Identity** — who the agent is, how it signs output, who the operator is.
2. **Your One Job** — the single-sentence mandate.
3. **How You Work / Standing Runs** — day-to-day operating method.
4. **Hard Rules — Immutable Core** — hashed, structurally unamendable; only Josh edits this section directly.
5. **Revenue Coupling / Learning & Fleet Memory / Coordination / Self-Healing** — the fleet-wide mechanisms (Part 1 of the spec), spelled out per agent.
6. **Standing Jobs** — the agent's recurring deliverables.
7. **Weekly Scorecard** — numeric measurables, red/green, per the EOS layer.
8. **Acceptance (Build Sign-off)** — the bar a build must clear before going live.
9. **Memory / Spend** — memory file layout and compute budget.

## Versioning

Each file carries a version line at the bottom (`v2.1 — July 2026` today). A future amendment:

- To an **AMENDABLE** section (Standing Jobs, Weekly Scorecard, Learning & Fleet Memory, etc.): proposed by the agent itself as a one-line diff, approved or rejected by Josh, landed as a normal PR to this file, version bumped.
- To the **Hard Rules — Immutable Core** section: edited by Josh directly, never by an agent or an automated amendment process — this is the one section the nightly amendment loop is structurally forbidden from touching.

New trigger-hired agents (Atlas, Nova, Forge, Clara, Ledger, Vector, Dean — see Part 0 of the spec) get their own file here, following the same section order, at the point they're actually hired (not speculatively ahead of time).

## Not yet built here

Hashing/drift-detection for the Immutable Core sections (spec §1.1 "constitutions split IMMUTABLE (hashed, structurally unamendable...) vs AMENDABLE") is not implemented by this file set — these are plain checked-in Markdown today. That enforcement mechanism is separate follow-on work, not part of this task's definition of done.
