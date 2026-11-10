"""P0.2 review: the deploy installs exactly the commit the Tests job passed, never a newer commit merged into dev
while those tests ran. Runs scripts/deploy/sync_dev.sh against throwaway git repos (no network, no server)."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
SYNC = ROOT / "scripts" / "deploy" / "sync_dev.sh"

pytestmark = pytest.mark.skipif(sys.platform == "win32" or not (shutil.which("bash") and shutil.which("git")),
                                reason="needs bash and git (runs in CI on Linux)")


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _commit(repo: Path, name: str) -> str:
    (repo / f"{name}.txt").write_text(name)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", name)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repos(tmp_path):
    """origin (bare, plays GitHub), dev (a developer pushing to dev) and server (the prod checkout)."""
    origin, dev, server = tmp_path / "origin.git", tmp_path / "dev", tmp_path / "server"
    _git(tmp_path, "init", "-q", "--bare", "-b", "dev", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(dev))
    for repo in (dev,):
        _git(repo, "config", "user.email", "ci@example.com")
        _git(repo, "config", "user.name", "ci")
        _git(repo, "checkout", "-q", "-b", "dev")
    base = _commit(dev, "base")
    _git(dev, "push", "-q", "origin", "dev")
    _git(tmp_path, "clone", "-q", "-b", "dev", str(origin), str(server))
    return {"origin": origin, "dev": dev, "server": server, "base": base}


def _sync(server: Path, *sha: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", str(SYNC), str(server), *sha], capture_output=True, text=True)


def _head(server: Path) -> str:
    return _git(server, "rev-parse", "HEAD")


def test_dev_advancing_during_tests_does_not_get_the_untested_commit_deployed(repos):
    tested = _commit(repos["dev"], "A")          # CI starts testing A
    _git(repos["dev"], "push", "-q", "origin", "dev")
    untested = _commit(repos["dev"], "B")        # B merges while A's tests run
    _git(repos["dev"], "push", "-q", "origin", "dev")

    result = _sync(repos["server"], tested)      # A passed: deploy A

    assert result.returncode == 0, result.stderr
    assert _head(repos["server"]) == tested != untested
    assert _git(repos["server"], "rev-parse", "--abbrev-ref", "HEAD") == "dev"


def test_the_newer_commit_is_deployed_by_its_own_run(repos):
    a = _commit(repos["dev"], "A")
    b = _commit(repos["dev"], "B")
    _git(repos["dev"], "push", "-q", "origin", "dev")
    assert _sync(repos["server"], a).returncode == 0
    assert _sync(repos["server"], b).returncode == 0
    assert _head(repos["server"]) == b


def test_an_older_run_finishing_late_never_moves_dev_backwards(repos):
    a = _commit(repos["dev"], "A")
    b = _commit(repos["dev"], "B")
    _git(repos["dev"], "push", "-q", "origin", "dev")
    assert _sync(repos["server"], b).returncode == 0   # B's run deployed first
    result = _sync(repos["server"], a)                 # A's run arrives late
    assert result.returncode == 0 and "not moved" in result.stdout
    assert _head(repos["server"]) == b


def test_a_commit_not_on_origin_dev_is_refused(repos):
    stray = _commit(repos["dev"], "never-pushed")
    result = _sync(repos["server"], stray)
    assert result.returncode == 3
    assert _head(repos["server"]) == repos["base"]


@pytest.mark.parametrize("bad", ["abc123", "HEAD", "origin/dev", "$(touch /tmp/x)"])
def test_anything_but_a_full_sha_is_refused(repos, bad):
    result = _sync(repos["server"], bad)
    assert result.returncode == 2
    assert _head(repos["server"]) == repos["base"]


def test_without_a_sha_a_manual_run_pulls_dev_as_before(repos):
    newest = _commit(repos["dev"], "A")
    _git(repos["dev"], "push", "-q", "origin", "dev")
    assert _sync(repos["server"]).returncode == 0
    assert _head(repos["server"]) == newest


def test_the_workflow_deploys_the_tested_sha_with_that_commits_own_scripts():
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "deploy.yml").read_text(encoding="utf-8"))
    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == "test"
    script = deploy["steps"][0]["with"]["script"]
    assert 'sha="${{ github.sha }}"' in script
    assert 'git archive "$sha" deploy.sh scripts/deploy/sync_dev.sh' in script
    assert 'bash "$tmp/deploy.sh" "$sha"' in script
    assert "bash /root/Forced-action-/deploy.sh" not in script  # never the server's own, older copy


def test_deploy_sh_syncs_through_the_helper_instead_of_pulling_the_branch_tip():
    deploy_sh = (ROOT / "deploy.sh").read_text(encoding="utf-8")
    assert 'sync_dev.sh" "$PROJECT_DIR" $TARGET_SHA' in deploy_sh
    assert "git pull origin dev" not in deploy_sh
