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
        state_repo.set_session_context("sess-B", None)
        # No skill on disk → nothing to stage.
        sha = state_repo.commit_skill_change("patch", "ghost")
        assert sha is None
        # And no new commit landed.
        log = _git(state_repo_dir, "log", "--oneline")
        assert len(log.splitlines()) == 1  # just the seed

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

        def _broken_git(*args, **kwargs):
            # Mimic git failing on commit (e.g. .git revoked write).
            raise subprocess.CalledProcessError(1, args, stderr="cannot lock ref")

        monkeypatch.setattr(state_repo, "_git", _broken_git)
        with pytest.raises(state_repo.StateRepoError, match="cannot lock ref"):
            state_repo.commit_skill_change("create", "y")


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
        def _broken_git(*args, **kwargs):
            raise subprocess.CalledProcessError(1, args, stderr="bad commit")
        monkeypatch.setattr(state_repo, "_git", _broken_git)
        with pytest.raises(state_repo.StateRepoError, match="bad commit"):
            state_repo.commit_session_start("sess-fail")
