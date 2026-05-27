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
    env["HERMES_HOME"] = str(triplet["tmp"] / "hermes-home")
    env["HERMES_UPSTREAM_SYNC_WORKTREE_ROOT"] = str(triplet["tmp"] / "worktrees")
    env["GITHUB_REPOSITORY"] = "InverterNetwork/hermes-agent"
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

    def test_no_divergence_does_not_need_github_repository(self, triplet):
        result = _run_script(triplet, GITHUB_REPOSITORY="")
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
        assert "api repos/InverterNetwork/hermes-agent/pulls" in gh_args
        assert "-X POST" in gh_args
        assert "base=main" in gh_args
        assert "head=upstream-sync/" in gh_args
        assert "draft=true" not in gh_args
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
        assert "api repos/InverterNetwork/hermes-agent/pulls" in gh_args
        assert "draft=true" in gh_args
        assert "(CONFLICTS)" in gh_args
        assert "SHARED.md" in gh_args  # listed in body's conflict-files block

        # The branch's HEAD is a "WIP: upstream sync with conflicts" commit.
        branches = _git(triplet["origin_bare"], "branch").stdout.split()
        sync_branch = next(b for b in branches if "upstream-sync/" in b)
        msg = _git(
            triplet["origin_bare"], "log", "-1", "--pretty=%s", sync_branch,
        ).stdout.strip()
        assert msg == "WIP: upstream sync with conflicts (resolve before merging)"

    def test_dirty_primary_working_tree_uses_isolated_worktree(self, triplet):
        upstream_short = _advance_upstream(triplet)
        # Dirty the primary fork checkout without committing. The service
        # should still run because merge work happens in a temp worktree.
        dirty = triplet["fork"] / "DIRTY.md"
        dirty.write_text("uncommitted\n")
        result = _run_script(triplet)
        assert result.returncode == 0, result.stderr
        assert "primary checkout is dirty" in result.stderr
        assert dirty.read_text() == "uncommitted\n"

        branches = _git(triplet["origin_bare"], "branch").stdout
        assert "upstream-sync/" in branches
        assert upstream_short in branches
        assert triplet["gh_log"].exists()

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

    def test_status_file_records_success(self, triplet, tmp_path):
        _advance_upstream(triplet)
        state_dir = tmp_path / "state"
        result = _run_script(
            triplet,
            HERMES_UPSTREAM_SYNC_STATE_DIR=str(state_dir),
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        status = (state_dir / "status.env").read_text()
        assert "state=ok" in status
        assert "branch=upstream-sync/" in status
        assert "upstream_sha=" in status

    def test_active_lock_skips_without_opening_pr(self, triplet, tmp_path):
        _advance_upstream(triplet)
        state_dir = tmp_path / "state"
        lock_dir = state_dir / "lock"
        lock_dir.mkdir(parents=True)
        (lock_dir / "pid").write_text(str(os.getpid()))

        result = _run_script(
            triplet,
            HERMES_UPSTREAM_SYNC_STATE_DIR=str(state_dir),
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "another hermes-upstream-sync run is active" in result.stderr
        assert not triplet["gh_log"].exists()
        status = (state_dir / "status.env").read_text()
        assert "state=skipped" in status

    @staticmethod
    def _install_git_push_spy(triplet: dict, log_path: Path) -> None:
        """Place a `git` shim on PATH that records argv whenever an
        invocation contains `push`, then execs real git so the rest of
        the script still drives the bare-repo fixture normally. The real
        git path is resolved at install time so the spy's own PATH
        override can't re-enter itself.
        """
        real_git = subprocess.run(
            ["which", "git"], check=True, capture_output=True, text=True,
        ).stdout.strip()
        git_stub = triplet["bin"] / "git"
        git_stub.write_text(
            "#!/usr/bin/env bash\n"
            "for arg in \"$@\"; do\n"
            "  if [ \"$arg\" = \"push\" ]; then\n"
            f"    printf '%s\\n' \"$*\" >> {log_path}\n"
            "    break\n"
            "  fi\n"
            "done\n"
            f"exec {real_git} \"$@\"\n"
        )
        git_stub.chmod(0o755)

    def _assert_push_carries_inline_token_helper(
        self, push_invocation: str, token_value: str,
    ) -> None:
        """git push must override credential.https://github.com.helper with
        a tiny inline shell helper that emits x-access-token + $GH_TOKEN.
        Critically: the literal token must NOT be in argv (which would
        leak via `ps`) — the helper references $GH_TOKEN by name and the
        spawned sh expands it from inherited env.
        """
        assert "-c credential.https://github.com.helper=" in push_invocation, (
            f"push lacks scoped credential helper: {push_invocation!r}"
        )
        assert "x-access-token" in push_invocation
        assert "GH_TOKEN" in push_invocation, (
            "inline helper must reference $GH_TOKEN by name so the token "
            f"value never lands in argv: {push_invocation!r}"
        )
        assert token_value not in push_invocation, (
            f"token value {token_value!r} leaked into push argv: {push_invocation!r}"
        )

    def test_git_push_wires_inline_credential_helper_for_minted_token(
        self, triplet, tmp_path,
    ):
        """When the script mints $GH_TOKEN from the App helper, the push
        command must wire an inline credential.https://github.com.helper
        that emits that token. Otherwise the unattended timer wedges on
        git's username prompt — git does not read $GH_TOKEN on its own.
        """
        _advance_upstream(triplet)

        # Helper stub: print a marker token on `mint`. Only the mint path
        # runs in this fixture; the inline credential helper takes over
        # from there.
        helper = tmp_path / "fake_token_helper.py"
        helper.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "if sys.argv[1] == 'mint':\n"
            "    print('marker-installation-token-xyz')\n"
        )
        helper.chmod(0o755)

        push_log = tmp_path / "git-push.log"
        self._install_git_push_spy(triplet, push_log)

        result = _run_script(
            triplet,
            HERMES_TOKEN_HELPER=str(helper),
            HERMES_TOKEN_PYTHON=sys.executable,
            GH_TOKEN="",
            GITHUB_TOKEN="",
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert push_log.exists(), "spy never observed a `git push` call"
        self._assert_push_carries_inline_token_helper(
            push_log.read_text().strip(), "marker-installation-token-xyz",
        )

    def test_git_push_wires_inline_credential_helper_for_caller_supplied_token(
        self, triplet, tmp_path,
    ):
        """A token supplied via $GH_TOKEN (e.g. from EnvironmentFile, or
        an operator bypassing the helper) must reach `git push` too, not
        just the GitHub REST API call. Without this the override is silently broken
        and the push falls back to whatever git auth is — typically none,
        so the timer dies on the username prompt.
        """
        _advance_upstream(triplet)

        push_log = tmp_path / "git-push.log"
        self._install_git_push_spy(triplet, push_log)

        result = _run_script(triplet, GH_TOKEN="caller-supplied-token")
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert push_log.exists(), "spy never observed a `git push` call"
        self._assert_push_carries_inline_token_helper(
            push_log.read_text().strip(), "caller-supplied-token",
        )

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
