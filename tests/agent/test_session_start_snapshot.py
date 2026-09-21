"""Failure semantics for AIAgent._record_session_start_memory_snapshot.

Reviewer flagged that a partial failure (commit succeeded, DB persist
failed) was being log+swallowed, which would let the session boot with
a missing SHA. These tests pin the new behavior: both the git commit and
the DB persist propagate, and import-time unavailability of state_repo
is the only tolerated degradation.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import run_agent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _agent_stub() -> SimpleNamespace:
    """Build a minimal stand-in for AIAgent.

    Bypasses the real constructor (which pulls in OpenAI + the full toolset)
    by binding just the bound method we want to exercise. ``_session_db``
    and ``session_id`` are the only inputs the helper reads.
    """
    obj = SimpleNamespace(
        session_id="sess-test",
        _session_db=MagicMock(),
    )
    obj._record_session_start_memory_snapshot = (
        run_agent.AIAgent._record_session_start_memory_snapshot.__get__(obj)
    )
    return obj


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRecordSessionStartMemorySnapshot:
    def test_commit_failure_propagates(self, monkeypatch):
        """A failing git commit must raise — the caller leaves
        _cached_system_prompt unset so the next attempt retries."""
        from agent.state_repo import StateRepoError

        agent = _agent_stub()

        with patch("agent.state_repo.commit_session_start",
                   side_effect=StateRepoError("git commit blew up")):
            with pytest.raises(StateRepoError, match="git commit blew up"):
                agent._record_session_start_memory_snapshot()

        agent._session_db.set_memory_snapshot_sha.assert_not_called()

    def test_persist_failure_propagates(self):
        """A successful commit followed by a failing SessionDB persist must
        propagate — leaving sessions.id unable to address the SHA is the
        same 'missing SHA' failure mode as a failed commit."""
        agent = _agent_stub()
        agent._session_db.set_memory_snapshot_sha.side_effect = RuntimeError(
            "sqlite locked"
        )

        with patch("agent.state_repo.commit_session_start",
                   return_value="abcdef0123456789"):
            with pytest.raises(RuntimeError, match="sqlite locked"):
                agent._record_session_start_memory_snapshot()

    def test_no_state_repo_returns_silently(self):
        """When the install has no state repo, commit_session_start returns
        None and the persist step is skipped. No exception, no SHA written."""
        agent = _agent_stub()

        with patch("agent.state_repo.commit_session_start", return_value=None):
            agent._record_session_start_memory_snapshot()

        agent._session_db.set_memory_snapshot_sha.assert_not_called()

    def test_no_session_db_skips_persist(self):
        """Sessions without a DB (gateway path can run without one) get the
        commit but no persist call — that's by design, not a failure."""
        agent = _agent_stub()
        agent._session_db = None

        with patch("agent.state_repo.commit_session_start",
                   return_value="cafebabe"):
            # Must not raise.
            agent._record_session_start_memory_snapshot()

    def test_import_failure_is_tolerated(self, monkeypatch):
        """If the state_repo module itself can't import (legacy install),
        the session continues without a snapshot — matches dev workstations
        that don't ship the plumbing."""
        agent = _agent_stub()

        # Force the import to fail by removing it from sys.modules and
        # blocking re-import.
        monkeypatch.setitem(sys.modules, "agent.state_repo", None)

        # Must not raise; persist is never reached.
        agent._record_session_start_memory_snapshot()
        agent._session_db.set_memory_snapshot_sha.assert_not_called()

    def test_happy_path_persists_sha(self):
        agent = _agent_stub()

        with patch("agent.state_repo.commit_session_start",
                   return_value="0123456789abcdef"):
            agent._record_session_start_memory_snapshot()

        agent._session_db.set_memory_snapshot_sha.assert_called_once_with(
            "sess-test", "0123456789abcdef",
        )
