"""Synchronous git-commit plumbing for the agent's state repo.

The state repo at ``~/.hermes/state/`` versions everything the agent writes
freely (skills, memories, cron). This module provides the inline commit
helpers used by ``skill_manage`` (per-action) and the agent's session-start
hook (memory snapshot). Mid-session memory writes and cron edits are picked
up by the periodic ``hermes-sync`` timer instead.

Invariants:

* Local commit failure propagates; never swallow. The snapshot/replay
  pipeline cannot detect orphan SHAs after the fact, so the only safe
  option is to surface failure immediately and let the caller retry.
* Single-writer for commits. The in-process lock here covers two threads
  inside one agent; the sync timer runs in a separate process and relies
  on git's own ``.git/index.lock`` for cross-process exclusion.
* Append-only history on ``main`` — no rebase/squash/force-push from here.
  The sync timer's only rewrite is ``rebase --autostash`` on pull, which
  it aborts on conflict.
* Path-isolated commits. Every commit uses an explicit ``-- <pathspec>``
  so a pre-staged change in another subtree (left behind by the sync
  timer or a crashed previous attempt) cannot leak into the wrong commit.
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

    Propagates out of ``skill_manage`` and the session-start hook so the
    caller knows the write isn't recoverable. The snapshot/replay pipeline
    cannot detect orphan SHAs after the fact, so swallowing here would
    silently break replay fidelity.
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


def _rollback_skill_subtree(state: Path, name: str) -> None:
    """Restore ``skills/<name>/`` to HEAD after a failed commit.

    Without this, a partial write (file on disk but not committed) would
    trip the LLM's retry: ``create`` would hit the existing-skill collision,
    ``delete``/``remove_file`` would hit "not found", etc. We undo the disk
    mutation in three steps so both new-create and modify-existing cases
    are covered:

    1. ``git reset HEAD -- skills/<name>`` — unstage anything we just added.
    2. ``git checkout HEAD -- skills/<name>`` — restore tracked content
       (no-op when the path has no HEAD entry, e.g. brand-new create).
    3. ``git clean -fd skills/<name>`` — remove now-untracked leftovers
       (the new files from a failed create).

    Best-effort — rollback failures are logged but never re-raised, because
    the original commit-failure error is what the caller needs to see.
    """
    rel = f"skills/{name}"
    try:
        _git(state, "reset", "-q", "HEAD", "--", rel, check=False)
        _git(state, "checkout", "HEAD", "--", rel, check=False)
        _git(state, "clean", "-fd", "--", rel, check=False)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("state-repo rollback for %s failed: %s", rel, exc)


def commit_skill_change(action: str, name: str, *,
                        state_root: Optional[Path] = None) -> Optional[str]:
    """Stage ``skills/<name>/`` and commit a skill_manage write.

    Returns the resulting commit SHA, or ``None`` when there's nothing staged
    (idempotent re-write) or when the install has no state repo. Raises
    :class:`StateRepoError` when git fails. On failure, also rolls the
    on-disk skill subtree back to HEAD so the caller's retry sees pre-write
    state — otherwise ``create`` retries collide on the half-applied file
    and ``delete``/``remove_file`` retries trip on "not found".
    """
    state = state_root or state_repo_dir()
    if state is None:
        return None

    sid, inv = get_session_context()
    msg = _format_skill_message(action, name, sid, inv)
    rel = f"skills/{name}"

    with _GIT_LOCK:
        try:
            # Stage only this skill's subtree (covers new files, modifications,
            # deletes inside skills/<name>/). Path-isolated so a pre-staged
            # cron/ or memories/ change can't leak into the commit.
            _git(state, "add", "-A", "--", rel)
            diff = _git(state, "diff", "--cached", "--quiet", "--", rel, check=False)
            if diff.returncode == 0:
                # Nothing actually changed (e.g. patch produced identical
                # bytes). No commit, no SHA — still a success.
                return None
            # Pathspec on commit limits the new tree to this skill's subtree;
            # any other staged paths stay in the index for a future commit.
            _git(state, "commit", "-m", msg, "--", rel)
            return _git(state, "rev-parse", "HEAD").stdout.strip()
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            _rollback_skill_subtree(state, name)
            raise StateRepoError(
                f"state-repo commit failed for skill_manage({action} {name}): {stderr}"
            ) from exc


def _ensure_memories_anchor(state: Path) -> None:
    """Make sure ``memories/`` has at least one tracked file so pathspec works.

    On a fresh install ``memories/`` may not exist or may be empty. Without
    a tracked file in there, ``git commit --allow-empty -- memories/`` fails
    with "pathspec did not match any file(s)". We drop a ``.gitkeep`` only
    when the directory is genuinely empty; existing memory writes are left
    untouched.
    """
    mem = state / "memories"
    mem.mkdir(exist_ok=True)
    ls = _git(state, "ls-files", "--", "memories/", check=False)
    if not ls.stdout.strip() and not any(mem.iterdir()):
        (mem / ".gitkeep").touch()
        _git(state, "add", "--", "memories/.gitkeep", check=False)


def commit_session_start(session_id: str, *,
                         state_root: Optional[Path] = None) -> Optional[str]:
    """Snapshot ``memories/`` at the moment the LLM freezes its memory view.

    Always emits an allow-empty commit so every session has exactly one
    snapshot SHA, even when memories haven't changed. Returns the resulting
    SHA, or ``None`` when the install has no state repo. Raises
    :class:`StateRepoError` on git failure — session start fails loudly
    rather than proceeding with a missing SHA.
    """
    state = state_root or state_repo_dir()
    if state is None:
        return None

    sid = session_id or "unknown"
    msg = f"memory: session-start snapshot (session {sid})"

    with _GIT_LOCK:
        try:
            _ensure_memories_anchor(state)
            # Stage current memories/ state. ``add`` tolerates an empty
            # tree; the anchor above guarantees pathspec match either way.
            _git(state, "add", "--", "memories/", check=False)
            # Pathspec limits the commit to memories/ even if cron/ or other
            # subtrees happen to be staged from a prior tick.
            _git(state, "commit", "--allow-empty", "-m", msg, "--", "memories/")
            return _git(state, "rev-parse", "HEAD").stdout.strip()
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise StateRepoError(
                f"state-repo session-start commit failed (session {sid}): {stderr}"
            ) from exc
