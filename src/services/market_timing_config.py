"""Sprint 4.7 Phase 2: Market timing config loader and validator.

Loads config/market_timing_rules.json and exposes a typed helper for
fetching CDS multipliers based on pressure labels returned by
market_pressure_service.get_county_market_pressure_context().

Not yet wired into CDS scoring — Phase 3 will call get_multipliers_for_pressure()
from MultiVerticalScorer.__init__ after composing a unified multiplier dict.

Design rules (mirrors macro_signal_multiplier_service.py):
  - Never raises — any config or input error returns {} (neutral).
  - Multipliers are clamped to [MULTIPLIER_MIN, MULTIPLIER_MAX].
  - Unknown pressure labels return {} (neutral).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "market_timing_rules.json"

MULTIPLIER_MIN: float = 1.0
MULTIPLIER_MAX: float = 2.0

_VALID_PRESSURE_LABELS = frozenset({
    "elevated", "high", "rising", "declining", "neutral", "unknown",
})

_DEFAULT_CONFIG: dict = {"version": "unknown", "signals": {}}


def load_market_timing_config(config_path: Path = _CONFIG_PATH) -> dict:
    """Load market_timing_rules.json. Returns a safe empty config on any error."""
    try:
        raw = config_path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("market_timing_rules.json must be a JSON object")
        return parsed
    except FileNotFoundError:
        logger.warning(
            "[MarketTimingConfig] Config not found at %s — multipliers disabled",
            config_path,
        )
        return _DEFAULT_CONFIG.copy()
    except (json.JSONDecodeError, ValueError, OSError) as exc:
        logger.warning(
            "[MarketTimingConfig] Failed to load config: %s — multipliers disabled", exc
        )
        return _DEFAULT_CONFIG.copy()


def _clamp(value: Any, key_path: str) -> float:
    """Parse and clamp a multiplier value to [MULTIPLIER_MIN, MULTIPLIER_MAX]."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        logger.warning(
            "[MarketTimingConfig] Non-numeric multiplier at %s — using 1.0", key_path
        )
        return 1.0
    if not (MULTIPLIER_MIN <= v <= MULTIPLIER_MAX):
        logger.warning(
            "[MarketTimingConfig] Multiplier %.3f at %s out of [%.1f, %.1f] — clamped",
            v, key_path, MULTIPLIER_MIN, MULTIPLIER_MAX,
        )
        return max(MULTIPLIER_MIN, min(MULTIPLIER_MAX, v))
    return v


def get_multipliers_for_pressure(
    signal_key: str,
    pressure_label: str,
    config: dict | None = None,
) -> dict[str, float]:
    """Return {cds_signal: multiplier} for a macro signal at a given pressure level.

    Args:
        signal_key: One of the keys under config["signals"]
                    (e.g. "mortgage_rate_30yr", "house_price_index").
        pressure_label: Pressure string from market_pressure_service
                        (e.g. "elevated", "high", "rising").
        config: Pre-loaded config dict. If None, loads from disk.

    Returns:
        Dict mapping CDS signal type → float multiplier.
        Returns {} (neutral, no-op) when:
        - signal_key not in config
        - pressure_label has no rule for that signal
        - config is missing or malformed
    """
    if config is None:
        config = load_market_timing_config()

    signals = config.get("signals", {})
    signal_cfg = signals.get(signal_key)
    if not signal_cfg:
        return {}

    pressure_rules: dict = signal_cfg.get("pressure_multipliers", {})
    rule: dict | None = pressure_rules.get(pressure_label)
    if not rule:
        return {}

    max_mult = _clamp(
        signal_cfg.get("max_multiplier", MULTIPLIER_MAX),
        f"signals.{signal_key}.max_multiplier",
    )

    result: dict[str, float] = {}
    for cds_signal, raw_mult in rule.items():
        clamped = _clamp(
            raw_mult,
            f"signals.{signal_key}.pressure_multipliers.{pressure_label}.{cds_signal}",
        )
        result[cds_signal] = min(clamped, max_mult)

    return result


# Maps market_pressure_service context keys → market_timing_rules signal keys.
_CONTEXT_TO_SIGNAL_KEY: dict[str, str] = {
    "mortgage_rate":       "mortgage_rate_30yr",
    "county_hpi":          "house_price_index",
    "county_unemployment": "county_unemployment_rate",
}

# These pressure labels never trigger a boost.
_NON_BOOSTING_PRESSURE = frozenset({"neutral", "unknown", "declining"})


def compose_market_timing_multipliers(
    market_pressure_context: dict,
    config: dict | None = None,
) -> dict[str, float]:
    """Compose a unified {cds_signal: multiplier} from a market pressure context dict.

    Args:
        market_pressure_context: Output of market_pressure_service.get_county_market_pressure_context().
        config: Pre-loaded config dict. If None, loads from disk.

    Returns:
        Dict mapping CDS signal type → float multiplier.
        For CDS signals present in multiple macro sources, takes the maximum (no compounding).
        Returns {} (neutral) when context or config is empty or all pressures are non-boosting.
        Never raises.
    """
    if not market_pressure_context:
        return {}
    if config is None:
        config = load_market_timing_config()

    signals_cfg = config.get("signals", {})
    if not signals_cfg:
        return {}

    unified: dict[str, float] = {}

    for ctx_key, signal_key in _CONTEXT_TO_SIGNAL_KEY.items():
        signal_cfg = signals_cfg.get(signal_key)
        if not signal_cfg:
            continue

        ctx_section = market_pressure_context.get(ctx_key, {})
        pressure = ctx_section.get("pressure", "neutral")

        if pressure in _NON_BOOSTING_PRESSURE:
            continue

        pressure_rules = signal_cfg.get("pressure_multipliers", {})
        rule = pressure_rules.get(pressure)
        if not rule:
            continue

        try:
            max_mult = float(signal_cfg.get("max_multiplier", MULTIPLIER_MAX))
            max_mult = min(max_mult, MULTIPLIER_MAX)
        except (TypeError, ValueError):
            max_mult = MULTIPLIER_MAX

        for cds_signal, raw_mult in rule.items():
            try:
                v = min(float(raw_mult), max_mult)
                v = max(MULTIPLIER_MIN, v)
            except (TypeError, ValueError):
                continue
            # Take maximum across all macro signals — no compounding / double-boost
            if cds_signal not in unified or v > unified[cds_signal]:
                unified[cds_signal] = v

    return unified


def validate_market_timing_config(config: dict) -> list[str]:
    """Validate config structure. Returns a list of error strings (empty list = valid).

    Useful in tests and CLI health checks. Does not raise.
    """
    errors: list[str] = []

    signals = config.get("signals")
    if not isinstance(signals, dict):
        errors.append("'signals' must be a dict")
        return errors

    for sig_key, sig_cfg in signals.items():
        if not isinstance(sig_cfg, dict):
            errors.append(f"signals.{sig_key}: must be a dict")
            continue

        pressure_rules = sig_cfg.get("pressure_multipliers", {})
        if not isinstance(pressure_rules, dict):
            errors.append(f"signals.{sig_key}.pressure_multipliers must be a dict")
            continue

        for label, rule in pressure_rules.items():
            if label not in _VALID_PRESSURE_LABELS:
                errors.append(
                    f"signals.{sig_key}: unknown pressure label '{label}'"
                )
            if not isinstance(rule, dict):
                errors.append(
                    f"signals.{sig_key}.pressure_multipliers.{label}: must be a dict"
                )
                continue
            for cds_sig, mult in rule.items():
                try:
                    v = float(mult)
                    if not (MULTIPLIER_MIN <= v <= MULTIPLIER_MAX):
                        errors.append(
                            f"signals.{sig_key}.pressure_multipliers.{label}.{cds_sig}: "
                            f"multiplier {v} out of [{MULTIPLIER_MIN}, {MULTIPLIER_MAX}]"
                        )
                except (TypeError, ValueError):
                    errors.append(
                        f"signals.{sig_key}.pressure_multipliers.{label}.{cds_sig}: "
                        f"non-numeric multiplier '{mult}'"
                    )

    return errors
