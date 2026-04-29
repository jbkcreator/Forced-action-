"""
Tool registry for Cora graphs.

Every callable a LangGraph node can invoke on behalf of Cora is a "tool".
Tools are declared with the @tool decorator, which:

  1. Validates category + idempotency declarations at registration time
  2. Records the tool in a module-level registry keyed by name
  3. Attaches a ToolSpec to the function for observability / testing
  4. Makes the tool discoverable via tools_for_graph(graph_name)

Rules enforced at registration:
  - category must be one of: read, write, gating
  - write tools MUST declare idempotent=True or False explicitly (no default)
  - A tool name cannot be registered twice

Per-graph tool scoping: allowed_graphs=None means every graph can use the tool.
Declaring a list restricts access — attempts to call an out-of-scope tool
from a graph raise ToolScopeError.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Literal, Optional, TypeVar


ToolCategory = Literal["read", "write", "gating"]


class ToolRegistrationError(Exception):
	"""Raised when a tool is registered with invalid configuration."""


class ToolScopeError(Exception):
	"""Raised when a graph tries to call a tool outside its allowed scope."""


@dataclass(frozen=True)
class ToolSpec:
	"""Metadata attached to every registered tool."""
	name: str
	category: ToolCategory
	idempotent: bool
	requires_compliance: bool
	allowed_graphs: Optional[List[str]]  # None = any graph
	description: str
	func: Callable


# Module-level registry. Populated by @tool at import time.
TOOL_REGISTRY: Dict[str, ToolSpec] = {}


F = TypeVar("F", bound=Callable)


def tool(
	*,
	category: ToolCategory,
	idempotent: Optional[bool] = None,
	requires_compliance: bool = False,
	allowed_graphs: Optional[List[str]] = None,
	name: Optional[str] = None,
) -> Callable[[F], F]:
	"""
	Register a function as a Cora tool.

	Args:
		category: "read", "write", or "gating"
		idempotent: Required for write tools. Read/gating default to True.
		requires_compliance: True if the tool can emit to users (SMS, VM).
		allowed_graphs: Restrict which graphs can call this tool. None = all.
		name: Override the registered name. Defaults to the function's __name__.

	Raises:
		ToolRegistrationError: on invalid configuration or duplicate name.
	"""

	if category not in ("read", "write", "gating"):
		raise ToolRegistrationError(
			f"Invalid tool category {category!r} — must be 'read', 'write', or 'gating'"
		)

	def decorator(fn: F) -> F:
		tool_name = name or fn.__name__

		# Write tools must be explicit about idempotency — no silent default.
		if category == "write" and idempotent is None:
			raise ToolRegistrationError(
				f"Tool {tool_name!r}: category='write' requires explicit idempotent=True/False"
			)

		# Read and gating tools default to idempotent=True.
		_idempotent = True if idempotent is None else idempotent

		# Compliance flag only makes sense for write tools. Warn otherwise.
		if requires_compliance and category != "write":
			raise ToolRegistrationError(
				f"Tool {tool_name!r}: requires_compliance=True only valid for write tools"
			)

		if tool_name in TOOL_REGISTRY:
			raise ToolRegistrationError(
				f"Tool {tool_name!r} is already registered — names must be unique"
			)

		spec = ToolSpec(
			name=tool_name,
			category=category,
			idempotent=_idempotent,
			requires_compliance=requires_compliance,
			allowed_graphs=allowed_graphs,
			description=(fn.__doc__ or "").strip().split("\n")[0],
			func=fn,
		)
		TOOL_REGISTRY[tool_name] = spec

		# Attach spec to the function for introspection.
		fn.__cora_tool_spec__ = spec  # type: ignore[attr-defined]
		return fn

	return decorator


def get_tool(name: str) -> ToolSpec:
	"""Look up a tool by name. Raises KeyError if unknown."""
	if name not in TOOL_REGISTRY:
		raise KeyError(f"Unknown tool: {name!r}")
	return TOOL_REGISTRY[name]


def tools_for_graph(graph_name: str) -> List[ToolSpec]:
	"""
	Return all tools callable from the named graph.

	A tool is in scope if its allowed_graphs is None (unrestricted) or
	includes graph_name.
	"""
	return [
		spec for spec in TOOL_REGISTRY.values()
		if spec.allowed_graphs is None or graph_name in spec.allowed_graphs
	]


def assert_tool_in_scope(tool_name: str, graph_name: str) -> None:
	"""
	Runtime check: raise ToolScopeError if the named graph cannot call this tool.
	Called inside tool wrappers when scoping needs to be enforced.
	"""
	spec = get_tool(tool_name)
	if spec.allowed_graphs is not None and graph_name not in spec.allowed_graphs:
		raise ToolScopeError(
			f"Graph {graph_name!r} is not permitted to call tool {tool_name!r}"
		)
