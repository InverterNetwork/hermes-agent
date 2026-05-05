"""End-to-end behavior of ops/hermes-upstream-sync against tmp git repos.

The script spends most of its time orchestrating git + gh, so the
cheapest meaningful coverage is a tmp `upstream` + `origin` + fork
triplet plus a stub `gh` that records its arguments. That exercises the
exact code path operators run, including the conflict-detection branch
that's hardest to reason about by inspection.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ops" / "hermes-upstream-sync"


def _run(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(args),
        cwd=cwd, check=check, capture_output=True, text=True,
    )


def _git(repo: Path, *args: str, **kw) -> subprocess.CompletedProcess:
    return _run(repo, "git", "-C", str(repo), *args, **kw)


@pytest.fixture
def triplet(tmp_path: Path) -> dict:
    """Set up upstream + origin (bare) + fork (working copy with both remotes)."""
    upstream = tmp_path / "upstream"
    upstream_bare = tmp_path / "upstream.git"
    origin_bare = tmp_path / "origin.git"
    fork = tmp_path / "fork"

    subprocess.run(["git", "init", "--quiet", "-b", "main", str(upstream)], check=True)
    _git(upstream, "config", "user.email", "u@h.l")
    _git(upstream, "config", "user.name", "u")
    (upstream / "README.md").write_text("seed\n")
    _git(upstream, "add", "-A")
    _git(upstream, "commit", "-q", "-m", "seed")

    subprocess.run(
        ["git", "clone", "--quiet", "--bare", str(upstream), str(upstream_bare)],
        check=True,
    )
    subprocess.run(
        ["git", "clone", "--quiet", "--bare", str(upstream), str(origin_bare)],
        check=True,
    )
    subprocess.run(
        ["git", "clone", "--quiet", str(origin_bare), str(fork)],
        check=True,
    )
    _git(fork, "config", "user.email", "f@h.l")
    _git(fork, "config", "user.name", "f")
    _git(fork, "remote", "add", "upstream", str(upstream_bare))
    _git(fork, "fetch", "--quiet", "upstream")

    # Stubbed gh records its argv + env to the bin dir for assertions.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh_log = tmp_path / "gh.log"
    gh_env_log = tmp_path / "gh.env.log"
    gh_stub = bin_dir / "gh"
    gh_stub.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" >> {gh_log}\n'
        f'printf "GH_TOKEN=%s\\n" "${{GH_TOKEN:-}}" >> {gh_env_log}\n'
        "exit 0\n"
    )
    gh_stub.chmod(0o755)

    return {
        "tmp": tmp_path,
        "upstream": upstream,
        "upstream_bare": upstream_bare,
        "origin_bare": origin_bare,
        "fork": fork,
        "bin": bin_dir,
        "gh_log": gh_log,
        "gh_env_log": gh_env_log,
    }


def _run_script(triplet: dict, **env_overrides) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["FORK_DIR"] = str(triplet["fork"])
    env["PATH"] = f"{triplet['bin']}{os.pathsep}{env['PATH']}"
    env.update(env_overrides)
    return subprocess.run(
        ["bash", str(SCRIPT)],
        env=env, check=False, capture_output=True, text=True,
    )


def _advance_upstream(triplet: dict, file: str = "FEATURE.md", content: str = "x\n") -> str:
    """Add a commit to upstream, push to its bare remote. Returns the new SHA."""
    upstream = triplet["upstream"]
    (upstream / file).write_text(content)
    _git(upstream, "add", "-A")
    _git(upstream, "commit", "-q", "-m", f"upstream: add {file}")
    _git(upstream, "push", "-q", str(triplet["upstream_bare"]), "main")
    return _git(upstream, "rev-parse", "--short", "HEAD").stdout.strip()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHermesUpstreamSync:
    def test_no_divergence_exits_quietly(self, triplet):
        result = _run_script(triplet)
        assert result.returncode == 0, result.stderr
        assert "fully merged" in result.stdout
        assert not triplet["gh_log"].exists()

    def test_dry_run_with_divergence_does_not_create_branch(self, triplet):
        upstream_short = _advance_upstream(triplet)
        result = _run_script(triplet, PR_DRY_RUN="1")
        assert result.returncode == 0, result.stderr
        assert "DRY RUN" in result.stdout
        assert upstream_short in result.stdout
        # No branch created on origin.
        branches = _git(triplet["origin_bare"], "branch").stdout
        assert "upstream-sync" not in branches
        assert not triplet["gh_log"].exists()

    def test_clean_merge_pushes_branch_and_calls_gh(self, triplet):
        upstream_short = _advance_upstream(triplet)
        result = _run_script(triplet)
        assert result.returncode == 0, result.stderr

        branches = _git(triplet["origin_bare"], "branch").stdout
        assert f"upstream-sync/" in branches
        assert upstream_short in branches

        assert triplet["gh_log"].exists()
        gh_args = triplet["gh_log"].read_text()
        assert "pr create" in gh_args
        assert "--base main" in gh_args
        assert "--draft" not in gh_args
        assert "Sync upstream hermes-agent @" in gh_args
        assert "(CONFLICTS)" not in gh_args

    def test_already_open_branch_is_skipped(self, triplet):
        _advance_upstream(triplet)
        first = _run_script(triplet)
        assert first.returncode == 0, first.stderr

        # Second tick: same upstream SHA, same date → same branch name.
        # Wipe the gh log so we can confirm no second PR call happens.
        triplet["gh_log"].unlink()
        second = _run_script(triplet)
        assert second.returncode == 0, second.stderr
        assert "already exists on origin" in second.stderr or \
               "already exists on origin" in second.stdout
        assert not triplet["gh_log"].exists()

    def test_conflict_path_opens_draft_pr(self, triplet):
        """A divergent edit on the same file in fork + upstream → draft PR."""
        # Fork advances first.
        fork = triplet["fork"]
        (fork / "SHARED.md").write_text("fork-edit\n")
        _git(fork, "add", "-A")
        _git(fork, "commit", "-q", "-m", "fork: edit SHARED")
        _git(fork, "push", "-q", "origin", "main")

        # Upstream advances with a conflicting edit.
        upstream = triplet["upstream"]
        (upstream / "SHARED.md").write_text("upstream-edit\n")
        _git(upstream, "add", "-A")
        _git(upstream, "commit", "-q", "-m", "upstream: edit SHARED")
        _git(upstream, "push", "-q", str(triplet["upstream_bare"]), "main")

        result = _run_script(triplet)
        assert result.returncode == 0, result.stderr

        gh_args = triplet["gh_log"].read_text()
        assert "--draft" in gh_args
        assert "(CONFLICTS)" in gh_args
        assert "SHARED.md" in gh_args  # listed in body's conflict-files block

        # The branch's HEAD is a "WIP: upstream sync with conflicts" commit.
        branches = _git(triplet["origin_bare"], "branch").stdout.split()
        sync_branch = next(b for b in branches if "upstream-sync/" in b)
        msg = _git(
            triplet["origin_bare"], "log", "-1", "--pretty=%s", sync_branch,
        ).stdout.strip()
        assert msg == "WIP: upstream sync with conflicts (resolve before merging)"

    def test_dirty_working_tree_is_refused(self, triplet):
        _advance_upstream(triplet)
        # Dirty the fork without committing.
        (triplet["fork"] / "DIRTY.md").write_text("uncommitted\n")
        result = _run_script(triplet)
        assert result.returncode != 0
        assert "working tree dirty" in result.stderr
        assert not triplet["gh_log"].exists()

    def test_missing_upstream_remote_fails_loudly(self, triplet):
        # Remove the upstream remote.
        _git(triplet["fork"], "remote", "remove", "upstream")
        result = _run_script(triplet)
        assert result.returncode != 0
        assert "no 'upstream' remote" in result.stderr
        assert not triplet["gh_log"].exists()

    def test_token_helper_is_minted_when_gh_token_unset(self, triplet, tmp_path):
        """When GH_TOKEN is not in the environment, the script mints one
        via $HERMES_TOKEN_HELPER and exports it before invoking gh.

        Verified by stubbing the helper to print a marker token and
        checking the recorded GH_TOKEN seen by the gh stub matches.
        """
        _advance_upstream(triplet)
        helper = tmp_path / "fake_token_helper.py"
        helper.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "if sys.argv[1] == 'mint':\n"
            "    print('marker-installation-token-xyz')\n"
        )
        helper.chmod(0o755)
        result = _run_script(
            triplet,
            HERMES_TOKEN_HELPER=str(helper),
            HERMES_TOKEN_PYTHON=sys.executable,
            GH_TOKEN="",
            GITHUB_TOKEN="",
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        env_seen = triplet["gh_env_log"].read_text().strip()
        assert env_seen == "GH_TOKEN=marker-installation-token-xyz"

    def test_existing_gh_token_is_reused(self, triplet):
        """When the caller supplies $GH_TOKEN, the helper is skipped and
        the existing token flows through to gh."""
        _advance_upstream(triplet)
        result = _run_script(triplet, GH_TOKEN="caller-supplied-token")
        assert result.returncode == 0, result.stderr
        env_seen = triplet["gh_env_log"].read_text().strip()
        assert env_seen == "GH_TOKEN=caller-supplied-token"

    def test_stale_local_branch_is_replaced(self, triplet):
        """A previous failed tick can leave a local sync branch with the
        same name as the new one. The remote check above passes (push
        never landed last time), so the script must delete the stale
        local branch before recreating it — otherwise ``checkout -b``
        fails on 'branch already exists'."""
        upstream_short = _advance_upstream(triplet)
        date = subprocess.run(
            ["date", "-u", "+%Y-%m-%d"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        stale = f"upstream-sync/{date}-{upstream_short}"
        _git(triplet["fork"], "branch", stale, "main")
        # Confirm the branch exists locally but not on origin.
        local = _git(triplet["fork"], "show-ref", "--verify",
                     f"refs/heads/{stale}")
        assert local.returncode == 0
        remote_branches = _git(triplet["origin_bare"], "branch").stdout
        assert stale not in remote_branches

        result = _run_script(triplet)
        assert result.returncode == 0, result.stderr
        assert "stale local" in result.stderr
        # Remote branch was created cleanly.
        remote_branches = _git(triplet["origin_bare"], "branch").stdout
        assert stale in remote_branches
