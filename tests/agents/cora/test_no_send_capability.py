"""
Acceptance item 11: no direct send capability — a static, structural
guarantee, not a runtime check. Cora is permanently drafts-only; this test
fails the build if that ever silently regresses.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

CORA_SRC = Path(__file__).resolve().parents[3] / "src" / "agents" / "cora"

FORBIDDEN_MODULES = ("src.agents.tools.write_tools",)
FORBIDDEN_CALL_NAMES = ("send_email", "send_sms", "send_message")
ALLOWED_EXCEPTION_SUBSTRINGS = ("suppress_contact",)  # a compliance write, not a send


def _python_files():
    return sorted(CORA_SRC.rglob("*.py"))


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: str(p.relative_to(CORA_SRC)))
def test_file_never_imports_write_tools(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert node.module not in FORBIDDEN_MODULES, f"{path}: imports forbidden module {node.module!r}"
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name not in FORBIDDEN_MODULES, f"{path}: imports forbidden module {alias.name!r}"


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: str(p.relative_to(CORA_SRC)))
def test_file_never_calls_a_send_function(path: Path):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name in FORBIDDEN_CALL_NAMES:
                pytest.fail(f"{path}: calls forbidden send function {name!r}")


def test_no_send_capability_files_were_actually_scanned():
    # Guards against the parametrized tests above silently collecting zero files
    # (e.g. a path typo) and passing vacuously.
    assert len(_python_files()) >= 15
