"""
Prompt template loader for Cora graphs.

Reads YAML files under src/agents/prompts/<graph>/. Caches parsed YAML so
repeated calls inside the same process do not re-hit the filesystem.

Rendering uses simple str.format_map with an empty-default dict so missing
variables become literal {placeholders} rather than raising — graphs can
then notice empty context and skip the compose step if necessary.

A/B variant support
-------------------
A graph can define `variant_a.yaml` and `variant_b.yaml` alongside its
`system.yaml`. Each variant file overrides the `user` (and optionally
`system`) block of the base template. Traffic splits are configured in
`config/cora_ab_tests.yaml` and routed through `src/services/ab_engine.py`
so the same subscriber always sees the same variant (deterministic MD5
hash assignment, capped by `cora_guardrails.ab_test_traffic_cap`).

The high-level helper `render_for_subscriber(graph, subscriber_id, context, db)`
encapsulates the full flow: read config → assign/lookup variant → render
the right system + user prompts → return them alongside the variant id
so the caller can attach attribution to MessageOutcome.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml


_PROMPTS_ROOT = Path(__file__).resolve().parent
_AB_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "cora_ab_tests.yaml"

logger = logging.getLogger(__name__)


class _SafeDict(dict):
	"""dict subclass that leaves missing keys as-is in str.format_map output."""
	def __missing__(self, key: str) -> str:
		return "{" + key + "}"


@lru_cache(maxsize=64)
def _load_raw(graph: str, name: str) -> Dict[str, Any]:
	path = _PROMPTS_ROOT / graph / f"{name}.yaml"
	if not path.exists():
		raise FileNotFoundError(f"Prompt not found: {path}")
	return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache(maxsize=1)
def _load_ab_config() -> Dict[str, Any]:
	"""Parse config/cora_ab_tests.yaml once and cache for the process lifetime."""
	if not _AB_CONFIG_PATH.exists():
		return {}
	try:
		return yaml.safe_load(_AB_CONFIG_PATH.read_text(encoding="utf-8")) or {}
	except Exception as exc:
		logger.warning("Could not parse cora_ab_tests.yaml: %s", exc)
		return {}


def load_prompt(graph: str, name: str) -> Dict[str, Any]:
	"""Return the parsed YAML for a graph's prompt file (system/fallback/variant_*/...)."""
	return dict(_load_raw(graph, name))


def render(template: str, context: Dict[str, Any]) -> str:
	"""Render a str.format-style template with missing keys left literal."""
	return template.format_map(_SafeDict(context))


def render_system_and_user(
	graph: str,
	context: Dict[str, Any],
) -> Tuple[str, str]:
	"""Load graph/system.yaml and return (system_rendered, user_rendered)."""
	data = load_prompt(graph, "system")
	return (
		render(data.get("system", ""), context),
		render(data.get("user", ""), context),
	)


def render_fallback_body(graph: str, context: Dict[str, Any]) -> str:
	"""Load graph/fallback.yaml and return the rendered body string."""
	data = load_prompt(graph, "fallback")
	return render(data.get("body", ""), context).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Variant + traffic-split support
# ─────────────────────────────────────────────────────────────────────────────

def get_traffic_config(graph: str) -> Optional[Dict[str, Any]]:
	"""
	Return the enabled A/B test config for the given graph, or None if no
	enabled test is registered.

	Shape returned (when present):
	    {
	      "test_name":   "fomo_sms_v1",
	      "graph":       "fomo",
	      "enabled":     True,
	      "traffic_pct": 50,
	      "segment":     "all",
	      "variant_a":   {...},
	      "variant_b":   {...},
	    }
	"""
	cfg = _load_ab_config()
	for test_name, entry in cfg.items():
		if not isinstance(entry, dict):
			continue
		if entry.get("graph") != graph or not entry.get("enabled"):
			continue
		return {"test_name": test_name, **entry}
	return None


def render_variant(
	graph: str,
	variant: str,
	context: Dict[str, Any],
) -> Tuple[str, str]:
	"""
	Render system+user for a specific variant. The variant file may override
	either or both of `system` and `user`; whatever it does NOT override falls
	back to the base `system.yaml`.
	"""
	base = load_prompt(graph, "system")
	try:
		var = load_prompt(graph, f"variant_{variant}")
	except FileNotFoundError:
		logger.warning(
			"Variant %r not found for graph %r — falling back to base prompt",
			variant, graph,
		)
		var = {}
	sys_tpl = var.get("system") or base.get("system", "")
	usr_tpl = var.get("user")   or base.get("user", "")
	return render(sys_tpl, context), render(usr_tpl, context)


def render_for_subscriber(
	graph: str,
	subscriber_id: int,
	context: Dict[str, Any],
	db: Any,
) -> Tuple[str, str, Optional[str], Optional[str]]:
	"""
	One-call entrypoint for graph nodes that compose a message for a subscriber.

	Reads the A/B test config for this graph; if a test is enabled, ensures
	the test row exists, looks up (or makes) this subscriber's variant assignment,
	and renders the corresponding variant prompt. Falls back cleanly to the base
	system prompt when:
	  - No test is configured for the graph
	  - The test is configured but disabled
	  - The subscriber falls outside the traffic_pct sample
	  - The variant file is missing (logs a warning)

	Returns: (system_rendered, user_rendered, variant, test_name)
	         `variant`   is "a" / "b" / None.
	         `test_name` is the test name when a test is active for this graph,
	                      regardless of whether THIS subscriber landed in it
	                      (useful for attribution / dashboards). None otherwise.
	"""
	cfg = get_traffic_config(graph)
	if not cfg:
		sys_txt, usr_txt = render_system_and_user(graph, context)
		return sys_txt, usr_txt, None, None

	test_name = cfg["test_name"]
	try:
		# Lazy import to avoid a circular dep at module load time.
		from src.services.ab_engine import assign_variant, get_or_create_test

		get_or_create_test(
			test_name=test_name,
			segment=cfg.get("segment", "all"),
			variant_a=cfg.get("variant_a") or {},
			variant_b=cfg.get("variant_b") or {},
			traffic_pct=int(cfg.get("traffic_pct", 0)),
			db=db,
		)
		variant = assign_variant(subscriber_id, test_name, db)
	except Exception as exc:
		# If the ab_engine path errors (DB unavailable in a unit test etc.),
		# don't block the message — fall through to the base prompt.
		logger.warning(
			"ab_engine variant assignment failed for graph=%s sub=%s: %s",
			graph, subscriber_id, exc,
		)
		variant = None

	if variant in ("a", "b"):
		sys_txt, usr_txt = render_variant(graph, variant, context)
	else:
		sys_txt, usr_txt = render_system_and_user(graph, context)

	return sys_txt, usr_txt, variant, test_name


def reset_ab_config_cache() -> None:
	"""Drop the cached A/B config so the next call re-reads from disk.

	Useful in tests, and in the admin UI when editing cora_ab_tests.yaml
	live without restarting the process.
	"""
	_load_ab_config.cache_clear()
