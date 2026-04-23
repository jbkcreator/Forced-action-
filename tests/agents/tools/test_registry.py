"""
Tests for the Cora tool registry.

Covers:
  - @tool decorator valid registration (read / write / gating)
  - Write tools must declare idempotency explicitly
  - Duplicate names are rejected
  - requires_compliance is only valid for write tools
  - Per-graph scoping via allowed_graphs works for lookups + enforcement
"""

import pytest

from src.agents.tools.registry import (
	TOOL_REGISTRY,
	ToolRegistrationError,
	ToolScopeError,
	assert_tool_in_scope,
	get_tool,
	tool,
	tools_for_graph,
)


@pytest.fixture
def isolated_registry():
	"""Snapshot and restore the module-level registry so tests don't leak."""
	snapshot = dict(TOOL_REGISTRY)
	yield TOOL_REGISTRY
	TOOL_REGISTRY.clear()
	TOOL_REGISTRY.update(snapshot)


def test_read_tool_registers_with_defaults(isolated_registry):
	@tool(category="read")
	def _reader():
		"""Read something."""
		return 1

	spec = get_tool("_reader")
	assert spec.category == "read"
	assert spec.idempotent is True           # default for read tools
	assert spec.requires_compliance is False
	assert spec.allowed_graphs is None


def test_gating_tool_registers_with_defaults(isolated_registry):
	@tool(category="gating")
	def _gate():
		"""Gating check."""
		return True

	spec = get_tool("_gate")
	assert spec.category == "gating"
	assert spec.idempotent is True


def test_write_tool_requires_explicit_idempotency(isolated_registry):
	with pytest.raises(ToolRegistrationError, match="requires explicit idempotent"):
		@tool(category="write")
		def _bad_writer():
			"""Missing idempotency declaration."""
			pass


def test_write_tool_registers_when_idempotency_declared(isolated_registry):
	@tool(category="write", idempotent=True, requires_compliance=True)
	def _good_writer():
		"""Declared idempotent + compliance-gated."""
		pass

	spec = get_tool("_good_writer")
	assert spec.category == "write"
	assert spec.idempotent is True
	assert spec.requires_compliance is True


def test_non_write_tool_cannot_require_compliance(isolated_registry):
	with pytest.raises(ToolRegistrationError, match="requires_compliance=True only valid"):
		@tool(category="read", requires_compliance=True)
		def _reader():
			"""Compliance on a read tool is a bug."""
			pass


def test_invalid_category_rejected(isolated_registry):
	with pytest.raises(ToolRegistrationError, match="Invalid tool category"):
		@tool(category="bogus")  # type: ignore[arg-type]
		def _nope():
			pass


def test_duplicate_name_rejected(isolated_registry):
	@tool(category="read")
	def _dup():
		"""First."""
		pass

	with pytest.raises(ToolRegistrationError, match="already registered"):
		@tool(category="read", name="_dup")
		def _dup_two():
			"""Same name."""
			pass


def test_tools_for_graph_returns_unscoped_and_matching(isolated_registry):
	@tool(category="read")
	def _everyone():
		"""Available anywhere."""
		pass

	@tool(category="read", allowed_graphs=["fomo"])
	def _fomo_only():
		"""FOMO only."""
		pass

	@tool(category="read", allowed_graphs=["abandonment"])
	def _abandonment_only():
		"""Abandonment only."""
		pass

	fomo_names = {t.name for t in tools_for_graph("fomo")}
	abandon_names = {t.name for t in tools_for_graph("abandonment")}
	hello_names = {t.name for t in tools_for_graph("hello_world")}

	assert "_everyone" in fomo_names
	assert "_everyone" in abandon_names
	assert "_everyone" in hello_names
	assert "_fomo_only" in fomo_names and "_fomo_only" not in abandon_names
	assert "_abandonment_only" in abandon_names and "_abandonment_only" not in fomo_names
	assert "_fomo_only" not in hello_names and "_abandonment_only" not in hello_names


def test_assert_tool_in_scope_raises_for_wrong_graph(isolated_registry):
	@tool(category="read", allowed_graphs=["fomo"])
	def _fomo_only():
		"""FOMO only."""
		pass

	# In-scope call is fine
	assert_tool_in_scope("_fomo_only", "fomo")

	# Out-of-scope call raises
	with pytest.raises(ToolScopeError):
		assert_tool_in_scope("_fomo_only", "abandonment")


def test_get_tool_raises_for_unknown_name(isolated_registry):
	with pytest.raises(KeyError):
		get_tool("this_tool_does_not_exist")


def test_tool_spec_attached_to_function(isolated_registry):
	@tool(category="read")
	def _inspect():
		"""A tool."""
		pass

	assert hasattr(_inspect, "__cora_tool_spec__")
	assert _inspect.__cora_tool_spec__.name == "_inspect"  # type: ignore[attr-defined]
