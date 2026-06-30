"""Behavior of ops/hermes-code-sync against tmp git mirrors."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ops" / "hermes-code-sync"


def _run(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(args),
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
    )


def _git(repo: Path, *args: str, **kw) -> subprocess.CompletedProcess:
    return _run(repo, "git", "-C", str(repo), *args, **kw)


def test_sync_normalizes_stale_branch_name_to_configured_base(tmp_path: Path):
    upstream = tmp_path / "upstream"
    origin = tmp_path / "origin.git"
    code_dir = tmp_path / "code"
    mirror = code_dir / "mirror"
    values = tmp_path / "deploy.values.yaml"
    helper = tmp_path / "values_helper.py"

    subprocess.run(["git", "init", "--quiet", "-b", "main", str(upstream)], check=True)
    _git(upstream, "config", "user.email", "u@h.l")
    _git(upstream, "config", "user.name", "u")
    (upstream / "README.md").write_text("main\n", encoding="utf-8")
    _git(upstream, "add", "-A")
    _git(upstream, "commit", "-q", "-m", "main seed")
    _git(upstream, "checkout", "-q", "-b", "develop")
    (upstream / "README.md").write_text("develop\n", encoding="utf-8")
    _git(upstream, "commit", "-am", "develop seed", "-q")

    subprocess.run(
        ["git", "clone", "--quiet", "--bare", str(upstream), str(origin)],
        check=True,
    )
    subprocess.run(
        ["git", "--git-dir", str(origin), "symbolic-ref", "HEAD", "refs/heads/main"],
        check=True,
    )
    code_dir.mkdir()
    subprocess.run(
        ["git", "clone", "--quiet", "--branch", "main", str(origin), str(mirror)],
        check=True,
    )
    _git(mirror, "fetch", "--quiet", "origin", "develop")
    _git(mirror, "checkout", "-q", "-b", "stale-pr-branch", "origin/main")
    _git(mirror, "reset", "--hard", "origin/develop", "--quiet")
    (mirror / "README.md").write_text("local divergence\n", encoding="utf-8")

    values.write_text("repos: []\n", encoding="utf-8")
    helper.write_text(
        "#!/usr/bin/env python3\n"
        "print('mirror\\tfile:///unused\\tdevelop\\t\\t')\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "HERMES_CODE_DIR": str(code_dir),
            "HERMES_VALUES_FILE": str(values),
            "HERMES_VALUES_HELPER": str(helper),
        }
    )
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "synced 1 ok, 0 failed, 0 skipped" in result.stdout
    assert _git(mirror, "branch", "--show-current").stdout.strip() == "develop"
    assert _git(mirror, "rev-parse", "HEAD").stdout == _git(
        mirror, "rev-parse", "origin/develop"
    ).stdout
    assert (mirror / "README.md").read_text(encoding="utf-8") == "develop\n"
