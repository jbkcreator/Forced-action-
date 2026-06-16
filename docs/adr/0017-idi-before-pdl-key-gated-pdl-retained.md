# IDI inserted before PDL in the cascade, key-gated; PDL retained

The v4 cascade tail is `… → BatchData → IDI → PDL`. IDI is inserted ahead of
PDL but is **API-key-gated**: with no `IDI_API_KEY` set it skips gracefully via
the existing guard, so today the cascade effectively ends at BatchData → PDL.
PDL is **retained** as the terminal fallback rather than removed.

This deviates from the v4 Phase-1 task diagram, which lists IDI as the terminal
stage and omits PDL.

## Considered Options

- **Replace PDL with IDI (literal task diagram)** — rejected for now. The IDI
  key does not yet exist, so replacing PDL would leave the cascade with no deep
  tier at all and depress match rates until the key is provisioned.
- **Insert IDI before PDL, keep PDL (chosen)** — additive, no regression today;
  converges to the diagram's intent once IDI is live (IDI runs first, PDL
  becomes last-resort).

## Consequences

- The 80¢ per-lead cost ceiling bounds escalation and decides IDI-vs-PDL once
  both are available, rather than always running both deep tiers.
- When IDI is provisioned and proven out, PDL may be retired in a follow-up;
  until then PDL must not be "cleaned up" as dead code — its retention is
  deliberate.
