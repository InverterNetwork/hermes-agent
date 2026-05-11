"""Tests for agent/state_repo.py — ITRY-1283 inline commit plumbing."""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from agent import state_repo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def state_repo_dir(tmp_path, monkeypatch):
    """A tmp git repo configured as the state repo, with skills/ + memories/."""
    repo = tmp_path / "state"
    repo.mkdir()
    subprocess.run(["git", "init", "--quiet", "-b", "main", str(repo)], check=True)
    _git(repo, "config", "user.email", "test@hermes.local")
    _git(repo, "config", "user.name", "test")
    (repo / "skills").mkdir()
    (repo / "memories").mkdir()
    (repo / "README.md").write_text("seed\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "seed", "--quiet")
    monkeypatch.setenv("HERMES_STATE_DIR", str(repo))
    yield repo


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class TestStateRepoDir:
    def test_returns_none_when_unset_and_default_missing(self, monkeypatch, tmp_path):
        monkeypatch.delenv("HERMES_STATE_DIR", raising=False)
        # Force home() to a tmp path with no state repo to make sure we don't
        # accidentally pick up the developer's real install.
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
        # Re-import to pick up the patched _DEFAULT_STATE_DIR via call.
        assert state_repo.state_repo_dir() is None

    def test_resolves_override(self, state_repo_dir):
        assert state_repo.state_repo_dir() == state_repo_dir


# ---------------------------------------------------------------------------
# Session context
# ---------------------------------------------------------------------------


class TestSessionContext:
    def test_default_is_none_pair(self):
        # The contextvar default is (None, None) — but since other tests may
        # have set it, exercise inside a fresh contextvars.Context.
        import contextvars
        ctx = contextvars.Context()
        assert ctx.run(state_repo.get_session_context) == (None, None)

    def test_set_and_get(self):
        state_repo.set_session_context("sess-1", 7)
        assert state_repo.get_session_context() == ("sess-1", 7)

    def test_empty_string_session_id_normalized_to_none(self):
        state_repo.set_session_context("", 0)
        sid, inv = state_repo.get_session_context()
        assert sid is None
        assert inv == 0


# ---------------------------------------------------------------------------
# commit_skill_change
# ---------------------------------------------------------------------------


class TestCommitSkillChange:
    def test_no_state_repo_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HERMES_STATE_DIR", raising=False)
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
        sha = state_repo.commit_skill_change("create", "noop")
        assert sha is None

    def test_commits_new_skill(self, state_repo_dir):
        state_repo.set_session_context("sess-A", 3)
        skill_dir = state_repo_dir / "skills" / "test-skill"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("---\nname: test-skill\n---\nbody\n")

        sha = state_repo.commit_skill_change("create", "test-skill")

        assert sha is not None
        msg = _git(state_repo_dir, "log", "-1", "--pretty=%B")
        assert msg.startswith("skill: create test-skill (session sess-A, invocation 3)")
        # File made it into the commit
        ls = _git(state_repo_dir, "ls-tree", "-r", "HEAD", "--name-only")
        assert "skills/test-skill/SKILL.md" in ls.splitlines()

    def test_no_op_when_nothing_staged(self, state_repo_dir):
        """Idempotent re-write (same bytes) must not produce an empty commit."""
        state_repo.set_session_context("sess-B", None)
        skill = state_repo_dir / "skills" / "noop"
        skill.mkdir()
        (skill / "SKILL.md").write_text("---\nname: noop\n---\nbody\n")
        first = state_repo.commit_skill_change("create", "noop")
        assert first is not None
        baseline = len(_git(state_repo_dir, "log", "--oneline").splitlines())

        # Same content, second call → nothing staged → None.
        sha = state_repo.commit_skill_change("patch", "noop")
        assert sha is None
        assert len(_git(state_repo_dir, "log", "--oneline").splitlines()) == baseline

    def test_message_omits_invocation_when_unset(self, state_repo_dir):
        state_repo.set_session_context("sess-C", None)
        skill_dir = state_repo_dir / "skills" / "x"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("---\nname: x\n---\n")
        state_repo.commit_skill_change("create", "x")
        msg = _git(state_repo_dir, "log", "-1", "--pretty=%B")
        assert msg.startswith("skill: create x (session sess-C)")
        assert "invocation" not in msg

    def test_propagates_git_failure(self, state_repo_dir, monkeypatch):
        state_repo.set_session_context("sess-D", 1)
        skill_dir = state_repo_dir / "skills" / "y"
        skill_dir.mkdir()
        (skill_dir / "SKILL.md").write_text("---\nname: y\n---\n")

        # Patch the commit step specifically so add/checkout/clean still work
        # for the rollback path. The wrapped git call raises only when called
        # with "commit" in its args.
        real_git = state_repo._git

        def _flaky_git(state_path, *args, **kwargs):
            if args and args[0] == "commit":
                raise subprocess.CalledProcessError(
                    1, ("git", "commit"), stderr="cannot lock ref",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _flaky_git)
        with pytest.raises(state_repo.StateRepoError, match="cannot lock ref"):
            state_repo.commit_skill_change("create", "y")

    def test_isolates_pathspec_from_pre_staged_cron(self, state_repo_dir):
        """A pre-staged change in cron/ must NOT land in the skill commit.

        Without pathspec isolation a leftover stage from an earlier tick
        gets swept into the next inline commit, breaking audit semantics.
        """
        state_repo.set_session_context("sess-iso", 4)
        # Pre-stage a cron/ change to simulate a leftover from another writer.
        cron = state_repo_dir / "cron"
        cron.mkdir(exist_ok=True)
        (cron / "tick.cron").write_text("* * * * * /bin/true\n")
        _git(state_repo_dir, "add", "cron/tick.cron")

        # Now do a skill write + commit.
        skill = state_repo_dir / "skills" / "iso-skill"
        skill.mkdir()
        (skill / "SKILL.md").write_text("---\nname: iso-skill\n---\n")
        sha = state_repo.commit_skill_change("create", "iso-skill")
        assert sha is not None

        # The skill commit must NOT contain the cron change.
        files = _git(state_repo_dir, "show", "--name-only", "--pretty=", sha)
        names = files.splitlines()
        assert "skills/iso-skill/SKILL.md" in names
        assert "cron/tick.cron" not in names

        # And the cron change must still be staged (intact for a future
        # commit by whoever owns it).
        status = _git(state_repo_dir, "status", "--porcelain")
        assert "A  cron/tick.cron" in status

    def test_rollback_removes_new_skill_on_commit_failure(self, state_repo_dir, monkeypatch):
        """create + commit failure must restore skills/<name>/ to HEAD.

        Otherwise the LLM's retry hits the existing-skill collision and
        the synchronous-commit AC can never be satisfied for that action.
        """
        state_repo.set_session_context("sess-rb", 1)
        skill = state_repo_dir / "skills" / "rb-create"
        skill.mkdir()
        (skill / "SKILL.md").write_text("---\nname: rb-create\n---\n")
        assert skill.exists()

        real_git = state_repo._git

        def _commit_fails(state_path, *args, **kwargs):
            if args and args[0] == "commit":
                raise subprocess.CalledProcessError(
                    1, ("git", "commit"), stderr="bad commit",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _commit_fails)
        with pytest.raises(state_repo.StateRepoError):
            state_repo.commit_skill_change("create", "rb-create")
        # The brand-new skill (never in HEAD) must be gone after rollback.
        assert not skill.exists(), "rollback should have removed the new skill"

    def test_commits_categorized_skill_via_rel_path(self, state_repo_dir):
        """Categorized layout (skills/<category>/<name>) is the failure mode
        from BRIX-1370: hardcoding ``skills/<name>`` produces a pathspec
        error and a silent rollback miss. Passing the real rel path through
        must stage and commit the categorized subtree just like the flat
        case."""
        state_repo.set_session_context("sess-cat", 9)
        skill_dir = state_repo_dir / "skills" / "quay" / "quay-run"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("---\nname: quay-run\n---\nbody\n")

        sha = state_repo.commit_skill_change(
            "create", "quay-run", rel_path="quay/quay-run",
        )

        assert sha is not None
        msg = _git(state_repo_dir, "log", "-1", "--pretty=%B")
        assert msg.startswith("skill: create quay-run (session sess-cat, invocation 9)")
        ls = _git(state_repo_dir, "ls-tree", "-r", "HEAD", "--name-only")
        assert "skills/quay/quay-run/SKILL.md" in ls.splitlines()

    def test_discovers_categorized_skill_without_rel_path(self, state_repo_dir):
        """When rel_path is omitted, the helper must locate the categorized
        directory on disk rather than falling back to ``skills/<name>``."""
        state_repo.set_session_context("sess-disc", 2)
        skill_dir = state_repo_dir / "skills" / "quay" / "quay-disc"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("---\nname: quay-disc\n---\nbody\n")

        sha = state_repo.commit_skill_change("create", "quay-disc")
        assert sha is not None
        ls = _git(state_repo_dir, "ls-tree", "-r", "HEAD", "--name-only")
        assert "skills/quay/quay-disc/SKILL.md" in ls.splitlines()

    def test_rejects_rel_path_outside_skills(self, state_repo_dir):
        """Pathspec must stay inside skills/ — anything else would let a
        misconfigured caller commit cron/ or memories/ under a skill
        message."""
        with pytest.raises(state_repo.StateRepoError, match="invalid skill rel_path"):
            state_repo.commit_skill_change(
                "patch", "evil", rel_path="../memories/some-skill",
            )
        with pytest.raises(state_repo.StateRepoError, match="invalid skill rel_path"):
            state_repo.commit_skill_change(
                "patch", "evil", rel_path="/absolute/path",
            )
        with pytest.raises(state_repo.StateRepoError, match="invalid skill rel_path"):
            state_repo.commit_skill_change("patch", "evil", rel_path="")

    def test_rollback_removes_categorized_skill_on_commit_failure(
        self, state_repo_dir, monkeypatch,
    ):
        """The pre-fix rollback dialed ``skills/<name>`` and silently
        missed the real path under a category. With the rel-path plumbing,
        a failed create under ``skills/<cat>/<name>`` must leave the disk
        clean."""
        state_repo.set_session_context("sess-rb-cat", 1)
        skill_dir = state_repo_dir / "skills" / "quay" / "rb-cat"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("---\nname: rb-cat\n---\n")
        assert skill_dir.exists()

        real_git = state_repo._git

        def _commit_fails(state_path, *args, **kwargs):
            if args and args[0] == "commit":
                raise subprocess.CalledProcessError(
                    1, ("git", "commit"), stderr="bad commit",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _commit_fails)
        with pytest.raises(state_repo.StateRepoError):
            state_repo.commit_skill_change(
                "create", "rb-cat", rel_path="quay/rb-cat",
            )
        assert not skill_dir.exists(), "rollback should have removed the new categorized skill"

    def test_rollback_restores_modified_skill_on_commit_failure(
        self, state_repo_dir, monkeypatch,
    ):
        """edit + commit failure must restore the original SKILL.md."""
        # Seed a skill so we have a HEAD version to roll back to.
        skill = state_repo_dir / "skills" / "rb-edit"
        skill.mkdir()
        (skill / "SKILL.md").write_text("original\n")
        _git(state_repo_dir, "add", "-A")
        _git(state_repo_dir, "commit", "-m", "seed rb-edit", "--quiet")

        # Mutate on disk.
        (skill / "SKILL.md").write_text("modified-but-uncommitted\n")
        state_repo.set_session_context("sess-rb2", 2)

        real_git = state_repo._git

        def _commit_fails(state_path, *args, **kwargs):
            if args and args[0] == "commit":
                raise subprocess.CalledProcessError(
                    1, ("git", "commit"), stderr="bad commit",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _commit_fails)
        with pytest.raises(state_repo.StateRepoError):
            state_repo.commit_skill_change("edit", "rb-edit")
        # File is back to the HEAD version.
        assert (skill / "SKILL.md").read_text() == "original\n"


# ---------------------------------------------------------------------------
# commit_session_start
# ---------------------------------------------------------------------------


class TestCommitSessionStart:
    def test_returns_none_when_no_state_repo(self, tmp_path, monkeypatch):
        monkeypatch.delenv("HERMES_STATE_DIR", raising=False)
        monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
        assert state_repo.commit_session_start("sess-1") is None

    def test_emits_allow_empty_commit(self, state_repo_dir):
        sha = state_repo.commit_session_start("sess-XYZ")
        assert sha is not None
        msg = _git(state_repo_dir, "log", "-1", "--pretty=%B")
        assert msg.startswith("memory: session-start snapshot (session sess-XYZ)")

    def test_picks_up_pending_memory_writes(self, state_repo_dir):
        (state_repo_dir / "memories" / "MEMORY.md").write_text("- stale\n")
        sha = state_repo.commit_session_start("sess-mem")
        assert sha is not None
        files = _git(state_repo_dir, "show", "--name-only", "--pretty=", sha)
        assert "memories/MEMORY.md" in files.splitlines()

    def test_propagates_git_failure(self, state_repo_dir, monkeypatch):
        real_git = state_repo._git

        def _commit_fails(state_path, *args, **kwargs):
            if args and args[0] == "commit":
                raise subprocess.CalledProcessError(
                    1, ("git", "commit"), stderr="bad commit",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _commit_fails)
        with pytest.raises(state_repo.StateRepoError, match="bad commit"):
            state_repo.commit_session_start("sess-fail")

    def test_isolates_pathspec_from_pre_staged_cron(self, state_repo_dir):
        """Session-start snapshot must NOT include pre-staged cron/ work."""
        cron = state_repo_dir / "cron"
        cron.mkdir(exist_ok=True)
        (cron / "tick.cron").write_text("* * * * *\n")
        _git(state_repo_dir, "add", "cron/tick.cron")

        sha = state_repo.commit_session_start("sess-iso")
        assert sha is not None
        files = _git(state_repo_dir, "show", "--name-only", "--pretty=", sha)
        assert "cron/tick.cron" not in files.splitlines()
        # Cron stays staged for whoever owns it.
        assert "A  cron/tick.cron" in _git(state_repo_dir, "status", "--porcelain")

    def test_propagates_add_failure(self, state_repo_dir, monkeypatch):
        """An `add` step that fails (permissions, index corruption) must
        propagate as StateRepoError — otherwise allow-empty commit would
        land a misleading SHA on top of broken state."""
        real_git = state_repo._git

        def _add_fails(state_path, *args, **kwargs):
            if args and args[0] == "add":
                raise subprocess.CalledProcessError(
                    1, ("git", "add"), stderr="permission denied: .git/index",
                )
            return real_git(state_path, *args, **kwargs)

        monkeypatch.setattr(state_repo, "_git", _add_fails)
        with pytest.raises(state_repo.StateRepoError, match="permission denied"):
            state_repo.commit_session_start("sess-add-fail")

    def test_seeds_anchor_on_brand_new_install(self, tmp_path, monkeypatch):
        """Empty memories/ on a fresh install must still record a SHA.

        Without an anchor file, ``git commit --allow-empty -- memories/``
        fails because pathspec doesn't match anything. The helper drops a
        ``.gitkeep`` only when memories/ is empty so the pathspec resolves.
        """
        repo = tmp_path / "fresh"
        repo.mkdir()
        subprocess.run(["git", "init", "--quiet", "-b", "main", str(repo)], check=True)
        _git(repo, "config", "user.email", "t@h.l")
        _git(repo, "config", "user.name", "t")
        (repo / "README.md").write_text("seed\n")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "seed", "--quiet")
        # Note: no memories/ dir at all yet.
        monkeypatch.setenv("HERMES_STATE_DIR", str(repo))

        sha = state_repo.commit_session_start("sess-fresh")
        assert sha is not None
        # .gitkeep should now be in HEAD's tree.
        ls = _git(repo, "ls-tree", "-r", "HEAD", "--name-only")
        assert "memories/.gitkeep" in ls.splitlines()
