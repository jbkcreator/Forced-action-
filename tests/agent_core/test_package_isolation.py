"""agent_core must stay a standalone library with no Banks job-search code."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE_DIR = Path(__file__).resolve().parents[2] / "packages" / "agent_core"
SOURCE_FILES = sorted(PACKAGE_DIR.rglob("*.py"))

JOB_SEARCH_MARKERS = ("TOOL-SOURCING", "TOOL-INBOUND", "TOOL-LINKEDIN", "linkedin", "attack_queue",
                      "opportunit", "recruiter", "clay_")
HOST_PACKAGES = ("src", "config", "banks", "migrations")


def test_package_has_source_files() -> None:
    assert len(SOURCE_FILES) >= 10


@pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
def test_no_job_search_references(path: Path) -> None:
    source = path.read_text(encoding="utf-8").lower()
    found = [marker for marker in JOB_SEARCH_MARKERS if marker.lower() in source]
    assert not found, f"{path.name} still references job-search code: {found}"


@pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
def test_imports_nothing_from_the_host_app(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & set(HOST_PACKAGES), f"{path.name} imports from the host app: {imported & set(HOST_PACKAGES)}"


def test_never_reads_the_environment() -> None:
    offenders = [path.name for path in SOURCE_FILES if "os.environ" in path.read_text(encoding="utf-8")
                 or "getenv" in path.read_text(encoding="utf-8")]
    assert offenders == []
