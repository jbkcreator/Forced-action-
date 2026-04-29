"""
Prompt template loader for Cora graphs.

Reads YAML files under src/agents/prompts/<graph>/. Caches parsed YAML so
repeated calls inside the same process do not re-hit the filesystem.

Rendering uses simple str.format_map with an empty-default dict so missing
variables become literal {placeholders} rather than raising — graphs can
then notice empty context and skip the compose step if necessary.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml


_PROMPTS_ROOT = Path(__file__).resolve().parent


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


def load_prompt(graph: str, name: str) -> Dict[str, Any]:
	"""Return the parsed YAML for a graph's prompt file (system/fallback/...)."""
	return dict(_load_raw(graph, name))


def render(template: str, context: Dict[str, Any]) -> str:
	"""Render a str.format-style template with missing keys left literal."""
	return template.format_map(_SafeDict(context))


def render_system_and_user(
	graph: str,
	context: Dict[str, Any],
) -> tuple[str, str]:
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
