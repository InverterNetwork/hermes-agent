"""Synchronous git-commit plumbing for the agent's state repo.

The state repo at ``~/.hermes/state/`` versions everything the agent writes
freely (skills, memories, cron). This module provides the inline commit
helpers used by ``skill_manage`` (per-action) and the agent's session-start
hook (memory snapshot). Mid-session memory writes and cron edits are picked
up by the periodic ``hermes-sync`` timer instead.

ITRY-1283 invariants:
* I1 — local commit failure propagates; never swallow.
* I2 — single-writer for commits (the in-process lock here covers two
  threads inside one agent; the sync timer runs in a separate process and
  relies on git's own ``.git/index.lock`` for cross-process exclusion).
* I3 — append-only history on ``main`` (no rebase/squash/force-push from
  here; the sync timer's only rewrite is ``rebase --autostash`` on pull,
  which it aborts on conflict).
"""

from __future__ import annotations

import contextvars
import logging
import os
import subprocess
import threading
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ── State-repo discovery ────────────────────────────────────────────────────


def state_repo_dir() -> Optional[Path]:
    """Resolve the state repo root.

    Reads ``HERMES_STATE_DIR`` first (tests + non-default installs), then
    falls back to ``$HERMES_HOME/state``. Returns ``None`` when the install
    has no state repo (dev workstations without ``setup-hermes.sh`` applied);
    callers treat that as "auto-commit disabled" rather than as an error.
    """
    override = os.environ.get("HERMES_STATE_DIR")
    if override:
        candidate = Path(override).expanduser()
    else:
        try:
            from hermes_constants import get_hermes_home
            candidate = get_hermes_home() / "state"
        except Exception:
            candidate = Path.home() / ".hermes" / "state"
    if (candidate / ".git").exists():
        return candidate
    return None


# ── Per-agent session context (read by the inline commit hooks) ─────────────

_SessionCtx = Tuple[Optional[str], Optional[int]]
_session_ctx: "contextvars.ContextVar[_SessionCtx]" = contextvars.ContextVar(
    "hermes_state_session_ctx", default=(None, None)
)


def set_session_context(session_id: Optional[str], invocation_n: Optional[int]) -> None:
    """Publish the active session id + invocation counter for downstream commits.

    Called from the agent loop at session start and on each user turn. Inline
    commits read these values to format the commit message.
    """
    _session_ctx.set((session_id or None, invocation_n))


def get_session_context() -> _SessionCtx:
    return _session_ctx.get()


# ── Single-writer lock (in-process) ─────────────────────────────────────────

_GIT_LOCK = threading.RLock()


# ── Git plumbing ────────────────────────────────────────────────────────────

class StateRepoError(RuntimeError):
    """Raised when a required commit cannot be recorded.

    Per ITRY-1283 I1 this propagates out of ``skill_manage`` and the
    session-start hook so the caller knows the write isn't recoverable.
    """


def _git(state: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(state), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _format_skill_message(action: str, name: str, session_id: Optional[str],
                          invocation_n: Optional[int]) -> str:
    sid = session_id or "unknown"
    if invocation_n is not None:
        return f"skill: {action} {name} (session {sid}, invocation {invocation_n})"
    return f"skill: {action} {name} (session {sid})"


def commit_skill_change(action: str, name: str, *,
                        state_root: Optional[Path] = None) -> Optional[str]:
    """Stage ``skills/`` and commit a skill_manage write.

    Returns the resulting commit SHA, or ``None`` when there's nothing staged
    (idempotent re-write) or when the install has no state repo. Raises
    :class:`StateRepoError` when git fails — the caller must surface this so
    the LLM knows the write didn't record a recoverable state.
    """
    state = state_root or state_repo_dir()
    if state is None:
        return None

    sid, inv = get_session_context()
    msg = _format_skill_message(action, name, sid, inv)

    with _GIT_LOCK:
        try:
            _git(state, "add", "-A", "skills/")
            diff = _git(state, "diff", "--cached", "--quiet", check=False)
            if diff.returncode == 0:
                # Nothing actually changed (e.g. patch produced identical
                # bytes). No commit, no SHA — still a success.
                return None
            _git(state, "commit", "-m", msg)
            return _git(state, "rev-parse", "HEAD").stdout.strip()
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise StateRepoError(
                f"state-repo commit failed for skill_manage({action} {name}): {stderr}"
            ) from exc


def commit_session_start(session_id: str, *,
                         state_root: Optional[Path] = None) -> Optional[str]:
    """Snapshot ``memories/`` at the moment the LLM freezes its memory view.

    Always emits an allow-empty commit so every session has exactly one
    snapshot SHA, even when memories haven't changed. Returns the resulting
    SHA, or ``None`` when the install has no state repo. Raises
    :class:`StateRepoError` on git failure (per ITRY-1283 I1, session start
    fails loudly rather than proceeding with a missing SHA).
    """
    state = state_root or state_repo_dir()
    if state is None:
        return None

    sid = session_id or "unknown"
    msg = f"memory: session-start snapshot (session {sid})"

    with _GIT_LOCK:
        try:
            # memories/ may legitimately be absent on a fresh install. ``add``
            # tolerates a missing path with check=False; if there's nothing to
            # stage the allow-empty commit still records the SHA.
            _git(state, "add", "memories/", check=False)
            _git(state, "commit", "--allow-empty", "-m", msg)
            return _git(state, "rev-parse", "HEAD").stdout.strip()
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise StateRepoError(
                f"state-repo session-start commit failed (session {sid}): {stderr}"
            ) from exc
