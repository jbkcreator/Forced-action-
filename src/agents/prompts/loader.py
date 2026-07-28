"""
Prompt template loader for Lifecycle graphs.

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
`config/lifecycle_ab_tests.yaml` and routed through `src/services/ab_engine.py`
so the same subscriber always sees the same variant (deterministic MD5
hash assignment, capped by `lifecycle_guardrails.ab_test_traffic_cap`).

The high-level helper `render_for_subscriber(graph, subscriber_id, context, db)`
encapsulates the full flow: read config → assign/lookup variant → render
the right system + user prompts → return them alongside the variant id
so the caller can attach attribution to MessageOutcome.
"""

from __future__ import annotations

import hashlib
import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml


_PROMPTS_ROOT = Path(__file__).resolve().parent
_AB_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "lifecycle_ab_tests.yaml"
_HOLDOUT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "lifecycle_holdout_tests.yaml"

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
	"""Parse config/lifecycle_ab_tests.yaml once and cache for the process lifetime."""
	if not _AB_CONFIG_PATH.exists():
		return {}
	try:
		return yaml.safe_load(_AB_CONFIG_PATH.read_text(encoding="utf-8")) or {}
	except Exception as exc:
		logger.warning("Could not parse lifecycle_ab_tests.yaml: %s", exc)
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


def base_prompt_fingerprint(graph: str) -> str:
	"""Stable content hash of a graph's base system.yaml (system + user
	templates, pre-render). Task 4.1: the holdout control arm always renders
	this base prompt, so a holdout's verdict is only valid while the baseline
	is unchanged. Recorded at holdout-test creation and re-checked by
	lifecycle_holdout_check — a mismatch means the baseline drifted mid-experiment
	and the verdict must not promote on mixed control copy. Returns "" if the
	prompt can't be loaded (treated as "unknown", never a false match)."""
	try:
		data = load_prompt(graph, "system")
	except Exception:
		return ""
	payload = json.dumps(
		{"system": data.get("system", ""), "user": data.get("user", "")},
		sort_keys=True,
	)
	return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def render_fallback_body(graph: str, context: Dict[str, Any]) -> str:
	"""
	Load and render the fallback SMS body for the given graph.

	Resolution order (first match wins):
	  1. graph/fallback_{vertical}.yaml  — vertical-specific copy
	  2. graph/fallback.yaml             — monolithic default

	The vertical-specific templates are optional; if missing the default is
	used transparently. Both templates receive the full context dict so county,
	segment, and score band can be substituted in fallback copy too.
	"""
	vertical = (context.get("vertical") or "").lower().replace(" ", "_")
	if vertical:
		try:
			data = load_prompt(graph, f"fallback_{vertical}")
			return render(data.get("body", ""), context).strip()
		except FileNotFoundError:
			pass
	data = load_prompt(graph, "fallback")
	return render(data.get("body", ""), context).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Variant + traffic-split support
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_holdout_config() -> Dict[str, Any]:
	"""Parse config/lifecycle_holdout_tests.yaml once and cache for the process lifetime."""
	if not _HOLDOUT_CONFIG_PATH.exists():
		return {}
	try:
		return yaml.safe_load(_HOLDOUT_CONFIG_PATH.read_text(encoding="utf-8")) or {}
	except Exception as exc:
		logger.warning("Could not parse lifecycle_holdout_tests.yaml: %s", exc)
		return {}


def get_holdout_config(graph: str) -> Optional[Dict[str, Any]]:
	"""
	Return the enabled control-holdout config for the given graph, or None.

	Shape returned (when present):
	    {
	      "test_name":  "wallet_push_holdout",
	      "graph":      "accelerated_wallet_push",
	      "enabled":    True,
	      "control_pct": 10,
	      "segment":    "all",
	    }
	"""
	cfg = _load_holdout_config()
	for test_name, entry in cfg.items():
		if not isinstance(entry, dict):
			continue
		if entry.get("graph") != graph or not entry.get("enabled"):
			continue
		return {"test_name": test_name, **entry}
	return None


def get_holdout_config_by_test_name(test_name: str) -> Optional[Dict[str, Any]]:
	"""Look up a holdout config by its test_name (the YAML key) rather than
	by graph — used by the scheduled surfacing job (lifecycle_holdout_check),
	which iterates AbTest rows and needs each one's conversion_window_days
	without knowing which graph it belongs to ahead of time."""
	cfg = _load_holdout_config()
	entry = cfg.get(test_name)
	if not isinstance(entry, dict) or not entry.get("enabled"):
		return None
	return {"test_name": test_name, **entry}


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

	Frozen control-holdout gate (Task 4.1 — 10% control group): if a holdout
	test is configured+enabled for this graph (config/lifecycle_holdout_tests.yaml),
	the subscriber is first assigned a rollout arm via
	ab_engine.assign_rollout_arm (records BOTH arms, so control's conversion
	rate becomes measurable). The 'control' arm always gets this frozen base
	prompt and returns immediately — it never reaches a/b assignment below,
	which is the isolation the holdout exists to guarantee. Every other case
	(no holdout configured, or the subscriber landed in 'variant') falls
	through unchanged to the a/b logic. The holdout's own test_name is a
	well-known constant that conversion sites pass to
	ab_engine.record_outcome directly — it never needs threading through
	message state, so this gate doesn't change the return shape.

	Returns: (system_rendered, user_rendered, variant, test_name)
	         `variant`   is "a" / "b" / None.
	         `test_name` is the test name when a test is active for this graph,
	                      regardless of whether THIS subscriber landed in it
	                      (useful for attribution / dashboards). None otherwise.
	"""
	holdout_cfg = get_holdout_config(graph)
	cfg = get_traffic_config(graph)

	# Control-holdout gate (Task 4.1). A holdout only measures something when
	# the graph ALSO has an active a/b treatment (cfg): without treatment the
	# 90% "variant" arm renders the same base prompt as the control arm, so
	# the verdict's z-test compares baseline to baseline and sampling noise
	# can produce a false "promote". So refuse holdout assignment until a
	# treatment exists — the holdout activates automatically once the graph's
	# a/b test is enabled. (PR #133 review, finding 1.)
	if holdout_cfg and cfg:
		try:
			from src.services.ab_engine import assign_rollout_arm, get_or_create_holdout_test

			holdout_test_name = holdout_cfg["test_name"]
			control_pct = int(holdout_cfg.get("control_pct", 10))
			# Uncapped on purpose — traffic_pct here is the treatment majority
			# (100 - control_pct); ab_engine.get_or_create_test's 10% cap would
			# invert the split. See get_or_create_holdout_test.
			get_or_create_holdout_test(
				test_name=holdout_test_name,
				segment=holdout_cfg.get("segment", "all"),
				traffic_pct=100 - control_pct,
				db=db,
				baseline_fingerprint=base_prompt_fingerprint(graph),
			)
			holdout_arm = assign_rollout_arm(subscriber_id, holdout_test_name, db)
		except Exception as exc:
			logger.warning(
				"holdout arm assignment failed for graph=%s sub=%s: %s",
				graph, subscriber_id, exc,
			)
			holdout_arm = None

		if holdout_arm == "control":
			sys_txt, usr_txt = render_system_and_user(graph, context)
			return sys_txt, usr_txt, None, None

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


def render_for_subscriber_auto(
	graph: str,
	subscriber_id: int,
	context: Dict[str, Any],
) -> Tuple[str, str, Optional[str], Optional[str]]:
	"""
	Same as render_for_subscriber but opens (and closes) its own DB session.

	Graph nodes are pure context-builders today — they do not carry a session
	through state. This wrapper lets them pick up A/B variant assignment with
	a single call. Errors degrade to the base prompt rather than blocking the
	graph (same fail-open posture as render_for_subscriber).
	"""
	try:
		from src.core.database import Database
		with Database().session_scope() as session:
			return render_for_subscriber(graph, subscriber_id, context, session)
	except Exception as exc:
		logger.warning(
			"render_for_subscriber_auto: DB session unavailable for graph=%s sub=%s — using base prompt (%s)",
			graph, subscriber_id, exc,
		)
		sys_txt, usr_txt = render_system_and_user(graph, context)
		return sys_txt, usr_txt, None, None


def reset_ab_config_cache() -> None:
	"""Drop the cached A/B + holdout configs so the next call re-reads from disk.

	Useful in tests, and in the admin UI when editing lifecycle_ab_tests.yaml or
	lifecycle_holdout_tests.yaml live without restarting the process.
	"""
	_load_ab_config.cache_clear()
	_load_holdout_config.cache_clear()


def validate_ab_config_vs_db(db: Any = None) -> list:
	"""
	Compare YAML traffic_pct vs DB traffic_pct for every enabled A/B test.
	Returns a list of warning strings (empty list = all in sync).

	Intended for startup checks and admin diagnostics. The ab_engine
	auto-syncs on next get_or_create_test call, so mismatches here mean
	a test exists in DB but hasn't been touched since the YAML changed.

	Pass a SQLAlchemy session as `db`, or omit to open one automatically.
	"""
	cfg = _load_ab_config()
	if not cfg:
		return []

	def _check(session: Any) -> list:
		mismatches = []
		try:
			from sqlalchemy import select as _select
			from src.core.models import AbTest
			for test_name, entry in cfg.items():
				if not isinstance(entry, dict) or not entry.get("enabled"):
					continue
				yaml_pct = int(entry.get("traffic_pct", 0))
				row = session.execute(
					_select(AbTest).where(AbTest.test_name == test_name)
				).scalar_one_or_none()
				if row and row.traffic_pct != yaml_pct:
					msg = (
						f"A/B config mismatch — {test_name}: "
						f"YAML traffic_pct={yaml_pct}, DB traffic_pct={row.traffic_pct}"
					)
					logger.warning(msg)
					mismatches.append(msg)
		except Exception as exc:
			logger.warning("validate_ab_config_vs_db: check failed: %s", exc)
		return mismatches

	if db is not None:
		return _check(db)

	try:
		from src.core.database import Database
		with Database().session_scope() as session:
			return _check(session)
	except Exception as exc:
		logger.warning("validate_ab_config_vs_db: DB unavailable: %s", exc)
		return []
