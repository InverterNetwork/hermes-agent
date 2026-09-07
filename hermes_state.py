#!/usr/bin/env python3
"""SQLite state store for Hermes Agent: session metadata, message history, model
config, FTS5 search. WAL mode (concurrent readers + one writer); compression
splits sessions via parent_session_id chains; sessions are source-tagged
('cli', 'telegram', ...). Batch-runner / RL trajectories live elsewhere.
"""

import asyncio
import atexit
import hashlib
import json
import logging
import os
import queue
import random
import re
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from pathlib import Path

from agent.message_sanitization import _sanitize_surrogates
from hermes_constants import get_hermes_home
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, cast

from hermes_state_common import escape_like as _escape_like, stat_db_file_identity as _stat_db_file_identity
from hermes_state_errors import (
    _DELETED_WAL_GENERATION_MSG, _DISK_IO_ERROR_MARKER, _STATE_DB_CORRUPT_MSG, _STATE_DB_GENERATION_KEY,
    _STATE_DB_REPLACED_MSG, DeletedWalGenerationError, SessionCompressionInProgressError, StateDbCorruptError,
    StateDbReplacedError, _is_no_more_rows, classify_persistence_error, is_malformed_db_error,
    is_malformed_schema_error,
)
from hermes_state_guard import (
    _STATE_DB_GUARD_BYPASS_ENV, _in_test_context, _is_production_state_db, _real_platform_state_root,
    _set_last_init_error, get_last_init_error,
)
from hermes_state_readpool import _READ_POOL_MAX, _proc_fd_targets, _read_budget_for
from hermes_state_sessions import SessionSessionsMixin
from hermes_state_fts import SessionFtsSetupMixin, load_fts5_cjk_extension
from hermes_state_portability import SessionPortabilityMixin
from hermes_state_telegram import SessionTelegramTopicsMixin
from hermes_state_schema import SessionSchemaMixin
import hermes_state_holders as _state_holders
from hermes_state_dbfile import (
    _canonical_sqlite_path, _connect_tracked_db, _read_sqlite_application_id, _stat_sqlite_sidecar_identity,
    _watched_sqlite_sidecar_paths, is_zeroed_state_db, quarantine_cross_process_lock, quarantine_zeroed_state_db,
    refuse_deleted_wal_generation,
)
from hermes_state_messages import SessionMessagesMixin
from hermes_state_wal import _WAL_INCOMPAT_MARKERS, apply_database_pragmas, apply_wal_with_fallback
from hermes_state_repair import _claim_repair_attempt, preflight_db_writability, repair_state_db_schema
from hermes_state_titles import SessionTitlesMixin
from hermes_state_usage import SessionUsageMixin
from hermes_state_maintenance import SessionMaintenanceMixin
from hermes_state_gateway import SessionGatewayMixin
from hermes_state_compression import SessionCompressionMixin
from hermes_state_search import SessionSearchMixin

try:  # Hard dependency, but tolerate scaffold-phase imports before pip install.
    import psutil
except ImportError:  # pragma: no cover - stripped/scaffold installs only
    psutil = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_MAX_SAFE_MESSAGES = 20_000  # resume/export guard default


def _configured_transcript_limit(key: str, fallback: int = _MAX_SAFE_MESSAGES) -> int:
    """``sessions.<key>`` from config.yaml (lazy import: circular at load), else *fallback*; 0 disables."""
    try:
        from hermes_cli.config import load_config_readonly
        value = (load_config_readonly().get("sessions") or {}).get(key)
        if value is None:
            return fallback
        limit = int(value)
        return limit if limit >= 0 else fallback
    except Exception:
        return fallback


def resolved_max_resume_messages() -> int:
    return _configured_transcript_limit("max_resume_messages")


def resolved_max_export_messages() -> int:
    return _configured_transcript_limit("max_export_messages")


class SessionResumeTooLargeError(ValueError):
    def __init__(
        self, message_count: int, limit: int = _MAX_SAFE_MESSAGES, scope: str = "across its lineage",
    ):
        self.message_count, self.limit = message_count, limit
        super().__init__(
            f"session has at least {message_count} active messages {scope}; "
            f"safe resume limit is {limit}. Export the session instead, or set "
            "sessions.max_resume_messages: 0 in config.yaml to disable the guard."
        )


class SessionExportTooLargeError(ValueError):
    def __init__(self, session_id: str, message_count: int, limit: int = _MAX_SAFE_MESSAGES):
        self.session_id, self.message_count, self.limit = session_id, message_count, limit
        super().__init__(
            f"session '{session_id}' has at least {message_count} active messages; "
            f"safe in-memory export limit is {limit}"
        )


def _compression_lock_holder_process_is_dead(holder: str) -> bool:
    """True only when a ``pid=<n>`` lock holder's local PID is provably gone.
    Reclaim on kernel proof only: unstructured/same-process holders (another
    thread's live lease) and any probe doubt keep the lease until TTL expiry
    (PID reuse must never steal a live lease; a wrongly-kept one self-heals)."""
    match = re.search(r"(?:^|:)pid=(\d+)(?::|$)", holder or "")
    pid = int(match.group(1)) if match else 0
    if pid <= 0 or pid == os.getpid():
        return False
    if psutil is not None:
        try:
            return not psutil.pid_exists(pid)  # recycled PIDs read as alive (conservative)
        except Exception:
            return False
    # psutil-less fallback is POSIX-only: on Windows os.kill(pid, 0) maps sig=0 to
    # CTRL_C_EVENT and can kill the target's console group.
    if os.name == "nt":
        return False
    try:
        os.kill(pid, 0)  # windows-footgun: ok — nt early-returns just above
    except ProcessLookupError:
        return True
    except (OSError, OverflowError):  # PermissionError is an OSError: alive but foreign
        return False
    return False


def _scrub_surrogates(value: Any) -> Any:
    """Replace lone surrogates in text (sqlite3 raises UnicodeEncodeError, aborting the whole write)."""
    return _sanitize_surrogates(value) if isinstance(value, str) else value


# Billing buckets that aren't a routable provider identity: a session that persisted only
# one of these (never ran /model) falls back to the config default. Shared by
# session_gateway_runtime and tui_gateway.server so they cannot drift.
_BARE_BILLING_PROVIDERS = frozenset({"auto", "custom"})

T = TypeVar("T")

# Import-time snapshot lets _default_db_path() detect a re-pointed DEFAULT_DB_PATH
# (tests monkeypatch the constant directly).
DEFAULT_DB_PATH = _IMPORT_DEFAULT_DB_PATH = get_hermes_home() / "state.db"

# Back off from read-only opens after one fails: not per query, but short enough that
# transient fd pressure doesn't strand the read pool.
_READ_OPEN_RETRY_SECONDS = 60.0
# Transient SQLITE_IOERR retry budget for READ-ONLY opens (#100436): a WAL writer's checkpoint/
# reset/frame flush surfaces "disk I/O error" to a concurrent mode=ro reader for a millisecond-
# wide window — the ro connection cannot perform WAL recovery because recovery writes the -shm
# index, which mode=ro refuses. The writer closes the window on its own, so a few short retries
# make the open succeed instead of 500-ing the whole /api/sessions poll (or any other ro opener).
# Deliberately NOT for writable opens: a writer owns the transition, so an IOERR there is a real
# storage/fd problem. A persistent IOERR still exhausts the budget and propagates.
_READ_ONLY_IOERR_RETRY_ATTEMPTS, _READ_ONLY_IOERR_RETRY_BACKOFF_S = 3, 0.05


def _default_db_path() -> Path:
    """Default state DB path at CALL time: a re-pointed ``DEFAULT_DB_PATH`` wins, else
    ``get_hermes_home()`` is resolved fresh (a runtime HERMES_HOME redirect works regardless of import)."""
    return DEFAULT_DB_PATH if DEFAULT_DB_PATH != _IMPORT_DEFAULT_DB_PATH else get_hermes_home() / "state.db"


# Live-DB guard knobs live HERE (not in hermes_state_guard): the hermetic conftest monkeypatches
# ``hermes_state._STATE_DB_GUARD_BYPASS`` (``@pytest.mark.live_system_guard_bypass`` escape hatch)
# and ``_EXTRA_DENY_ROOTS`` (the pre-sandbox root, so custom-HERMES_HOME deployments are covered).
_STATE_DB_GUARD_BYPASS = False
_STATE_DB_GUARD_EXTRA_DENY_ROOTS: Tuple[Path, ...] = ()


def _ensure_test_isolation(db_path: Path) -> None:
    """Raise before any connection/mkdir/pragma/byte probe when a pytest-context process
    (env OR ancestry) resolves a production DB.

    Env alone is not enough: a child spawned with a rebuilt environment loses ``PYTEST_*`` and
    ``HERMES_HOME`` together, which is precisely the state in which it writes to production (#82770).
    """
    if _STATE_DB_GUARD_BYPASS or os.environ.get(_STATE_DB_GUARD_BYPASS_ENV) or not _in_test_context():
        return
    try:
        resolved = Path(db_path).expanduser().resolve()
    except Exception:
        return
    roots = [r for r in (_real_platform_state_root(),) if r is not None]
    for extra in _STATE_DB_GUARD_EXTRA_DENY_ROOTS:
        try:
            roots.append(Path(extra).expanduser().resolve())
        except Exception:
            continue
    for root in roots:
        if _is_production_state_db(resolved, root):
            raise RuntimeError(
                "live-system guard: test attempted to open production "
                f"state.db at {resolved} (under real Hermes root {root}). "
                "Tests must run against a temporary HERMES_HOME — pass an "
                "explicit tmp db_path or let the hermetic conftest redirect "
                "HERMES_HOME. If this test genuinely needs the live database, mark it with "
                "@pytest.mark.live_system_guard_bypass — or, for a spawned "
                f"child process, export {_STATE_DB_GUARD_BYPASS_ENV}=1 in "
                "its environment."
            )


# Openings of the background-review harness prompts (agent/background_review.py).
_REVIEW_HARNESS_PREFIXES = (
    "Review the conversation above and update the skill library",
    "Review the conversation above and consider saving to memory",
)


def _is_background_review_harness_message(msg: Dict[str, Any]) -> bool:
    """Persisted harness prompt (older builds wrote the forked curator's turns
    into real sessions; replaying them hijacks the session)."""
    if not isinstance(msg, dict) or msg.get("role") not in {"user", "system"}:
        return False
    content = msg.get("content")
    return isinstance(content, str) and content.lstrip().startswith(_REVIEW_HARNESS_PREFIXES)


def _strip_background_review_harness(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop harness messages and the curator-mode assistant reply that immediately followed each."""
    if not messages:
        return messages
    out: List[Dict[str, Any]] = []
    skip_next_assistant = False
    for msg in messages:
        if _is_background_review_harness_message(msg):
            skip_next_assistant = True
            continue
        if skip_next_assistant:
            skip_next_assistant = False
            if isinstance(msg, dict) and msg.get("role") == "assistant":
                continue  # the curator-mode reply to the harness prompt
        out.append(msg)
    return out


# Matches a bare protocol/tool-name marker such as "[memory]" or "[skill_manage]".
_STALE_TOOL_CALL_MARKER_RE = re.compile(r"^\[[A-Za-z_][A-Za-z0-9_.-]*\]$")


def _is_stale_tool_call_marker_message(msg: Dict[str, Any]) -> bool:
    """Assistant tool-call turn whose content is a bare ``[marker]`` (an older
    conversation_loop persisted a local template's marker as the final response)."""
    if not isinstance(msg, dict) or msg.get("role") != "assistant" or not msg.get("tool_calls"):
        return False
    content = msg.get("content")
    return isinstance(content, str) and bool(_STALE_TOOL_CALL_MARKER_RE.fullmatch(content.strip()))


def _strip_stale_tool_call_markers(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Blank stale ``[marker]`` assistant content (replaying it teaches the model
    to keep emitting it); tool_call/result pairing stays intact."""
    repaired = 0
    for msg in filter(_is_stale_tool_call_marker_message, messages):
        msg["content"] = ""
        repaired += 1
    if repaired:
        logger.info(
            "Cleared %d stale tool-call marker message(s) while restoring session (#78148)", repaired,
        )
    return messages


def format_session_db_unavailable(prefix: str = "Session database not available") -> str:
    """User-facing message with the captured init cause (+ WAL-docs hint for NFS/SMB locking failures)."""
    cause = get_last_init_error()
    if not cause:
        return f"{prefix}."
    hint = " (state.db may be on NFS/SMB/FUSE/ZFS — see https://www.sqlite.org/wal.html)"
    return f"{prefix}: {cause}{hint if any(m in cause.lower() for m in _WAL_INCOMPAT_MARKERS) else ''}."


# Auto-repair at most once per DB path per process (no repair loops; serialises concurrent
# web_server / gateway opens on the same malformed file).
_repair_attempted_paths: set[str] = set()
_repair_attempt_lock = threading.Lock()
# Cross-process schema-surgery lock timeout (``_repair_attempt_lock`` covers one interpreter
# only); sized for the slowest legitimate holder (VACUUM, multi-GB DB).
_REPAIR_LOCK_TIMEOUT_SECONDS = 120.0
_IS_WINDOWS = sys.platform == "win32"


def divert_session_transcript_jsonl(session_id: str, messages) -> "Optional[Path]":
    """Append pending messages to HERMES_HOME/sessions/<id>.jsonl (state.db was replaced under a
    live process). Returns the path, or None if nothing to write."""
    sid = str(session_id or "").strip()
    if not sid or not messages:
        return None
    sessions_dir = get_hermes_home() / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)
    path = sessions_dir / f"{sid}.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        for msg in messages:
            if msg is not None:
                record = msg if isinstance(msg, dict) else {"content": str(msg)}
                handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    return path


# Process-wide shared SessionDB registry: long-lived in-process callers share ONE writer
# connection per resolved path via hermes_state_registry.acquire(); one-shots use SessionDB() + close().
def _foreign_state_db_holders(db_path: Path) -> List[Tuple[int, str]]:
    """Compatibility delegate to the state-holder authority."""
    return _state_holders.foreign_state_db_holders(db_path)


# ── Process-wide shared SessionDB registry (#90837) ── lives in hermes_state_registry.py (acquire /
# release / close_all / release_or_close). Long-lived in-process callers (gateway, tui_gateway, cron,
# in-process tools) share ONE writer connection per resolved path via hermes_state_registry.acquire(); CLI
# one-shots, recovery flows, and read-only cross-profile opens use SessionDB() directly with their own close().


class SessionDB(
    SessionSessionsMixin, SessionFtsSetupMixin, SessionSearchMixin, SessionSchemaMixin,
    SessionPortabilityMixin, SessionTelegramTopicsMixin, SessionCompressionMixin,
    SessionGatewayMixin, SessionMaintenanceMixin, SessionUsageMixin, SessionTitlesMixin,
    SessionMessagesMixin,
):
    """SQLite-backed session storage with FTS5 search; many reader threads, one writer (WAL)."""

    # Only these state-owned producers join automatic stale-open reconciliation; messaging/UI
    # sources have their own lifecycle owners; unknown sources fail closed.
    # See #60609.
    _AUTO_PRUNE_STALE_OPEN_SOURCES: Tuple[str, ...] = (
        "cli", "cron", "kanban", "acp", "api_server", "subagent", "tool",
    )

    # ── Write-contention tuning ──
    # SQLite's deterministic busy handler convoys under many hermes processes: keep its
    # timeout short (1s) and retry with random jitter. Patience is TIME-based (a sibling
    # legitimately holds the lock for seconds: checkpoint at close, VACUUM, recovery, FTS
    # optimize); attempt-counted budgets destroyed turns on a healthy store. Transcript
    # writes (failure aborts the turn) get the long budget; observation-only activity
    # writes sit on the response-critical path and get a sub-second one.
    _WRITE_PATIENCE_S, _TRANSCRIPT_WRITE_PATIENCE_S, _ACTIVITY_WRITE_PATIENCE_S = 20.0, 60.0, 0.5
    # A live compression lock gets a short wait (compression publishes in seconds), but the lease
    # is a correctness boundary: a writer still locked out afterwards is refused.
    # Observation-only activity heartbeat/label writes (#76354 review S1): these run on (or adjacent to) the
    # response-critical path and must never wait out the full routine patience under contention. Sub-second
    # budget; a skipped write is retried naturally at the next heartbeat window.
    # A live compression lock gets its own, much shorter budget than the write lock. Compression publishes
    # in a couple of seconds, so a brief wait saves the overwhelming majority of concurrent turns (#75083).
    # It deliberately stays short: the lease is a correctness boundary, not just a busy signal (see
    # test_compression_lease_blocks_non_owner_but_allows_owner_flush), so a writer that is still locked out
    # after this budget must still be refused rather than allowed to land a stale turn in a session whose
    # compression is genuinely long-running or wedged.
    _COMPRESSION_BUSY_WAIT_S = 5.0
    _WRITE_RETRY_MIN_S, _WRITE_RETRY_MAX_S = 0.020, 0.150  # fast jitter for the first _SLOW_AFTER_S
    _WRITE_RETRY_SLOW_AFTER_S = 2.0
    _WRITE_RETRY_SLOW_MIN_S, _WRITE_RETRY_SLOW_MAX_S = 0.250, 1.000
    # PASSIVE WAL checkpoint every N successful writes.
    _CHECKPOINT_EVERY_N_WRITES = 50
    # Bounded FTS ``'merge'`` (ms of lock each) instead of ``'optimize'`` (9-18s per index on a 10GB
    # DB, longer than a writer's patience); up to _COMMANDS_PER_PASS per index, stopping on no-progress.
    _FTS_MERGE_EVERY_N_WRITES, _FTS_MERGE_MAX_PAGES_PER_INDEX, _FTS_MERGE_COMMANDS_PER_PASS = 1000, 500, 4
    # Imports cap lower than exports: an import holds one BEGIN IMMEDIATE.
    _IMPORT_MAX_SESSIONS, _IMPORT_MAX_MESSAGES_PER_SESSION, _IMPORT_MAX_TOTAL_MESSAGES = 500, 10_000, 50_000
    _IMPORT_MAX_SESSION_BYTES, _IMPORT_MAX_TOTAL_BYTES = 5 * 1024 * 1024, 25 * 1024 * 1024
    # Accounting workers retire when idle so a bound-method target can't keep an abandoned SessionDB alive.
    _TOKEN_WRITER_IDLE_SECONDS = 30.0

    @staticmethod
    def _store_system_prompt(conn, system_prompt: Optional[str]) -> Optional[str]:
        if system_prompt is None:
            return None
        prompt_hash = hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()
        conn.execute(
            "INSERT OR IGNORE INTO system_prompts (hash, prompt) VALUES (?, ?)",
            (prompt_hash, system_prompt),
        )
        return prompt_hash

    @staticmethod
    def _delete_unreferenced_system_prompts(conn) -> None:
        conn.execute(
            "DELETE FROM system_prompts WHERE NOT EXISTS ("
            "SELECT 1 FROM sessions WHERE sessions.system_prompt_hash = system_prompts.hash)"
        )

    @staticmethod
    def _session_row_dict(row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        if "_system_prompt_resolved" in data:
            resolved = data.pop("_system_prompt_resolved")
            if "system_prompt" in data:
                data["system_prompt"] = resolved
        return data

    @staticmethod
    def _close_connection_quietly(conn: Optional[sqlite3.Connection]) -> None:
        """Close a partially initialized connection without masking its error."""
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            logger.debug("Could not close a SessionDB connection", exc_info=True)

    def _close_conn_logged(self, conn, label: str) -> None:
        """Close *conn*; a failing close leaks a tracked fd: logged at WARNING, never swallowed."""
        try:
            conn.close()
        except Exception as exc:
            logger.warning("%s close failed for %s: %s", label, self.db_path, exc)

    def __init__(self, db_path: Path = None, read_only: bool = False):
        self.db_path = db_path or _default_db_path()
        _ensure_test_isolation(self.db_path)  # before any connection/pragma/mkdir
        self.read_only = read_only
        self._lock = threading.Lock()
        # Read-path split (WAL only): reads borrow from a BOUNDED read-only pool so they
        # never queue behind writer flushes on self._lock (see _read_ctx); unbounded
        # per-thread connections pinned fds for the process lifetime and hit EMFILE.
        self._read_pool: "queue.LifoQueue[sqlite3.Connection]" = queue.LifoQueue(maxsize=_READ_POOL_MAX)
        # Permits bound PEAK descriptors (the pool bounds only the idle set), shared per
        # DATABASE PATH; acquired non-blocking so a permitless reader degrades to the writer lock.
        # One permit per live read connection, held from before the open in _get_read_conn() until after the
        # close in _close_read_conn(). See _READ_POOL_MAX. Acquired non-blocking on purpose: a reader that
        # cannot get a permit must degrade to the writer lock, not queue here — blocking would convert fd
        # exhaustion into a stall, which is the same outage with a different stack trace. Permits are shared
        # per DATABASE PATH, not per instance: the descriptors they ration belong to the file, and one
        # process holds several SessionDB objects on the same state.db (#98573). See _PathReadBudget.
        self._read_budget = _read_budget_for(self.db_path)
        self._read_budget.register(self)
        self._read_permits = self._read_budget.permits
        self._read_conns_lock = threading.Lock()
        # Set when close() begins; an in-flight reader then closes its own connection
        # instead of re-populating a pool nobody will drain again.
        self._read_conns_closed = False
        # Read-open failure backoff is a TIMESTAMP, not a sticky bool: the likeliest trigger
        # is transient EMFILE, and a permanent flag would demote every reader forever.
        self._read_open_failed_at = 0.0
        self._wal_active, self._write_count = False, 0
        # File identity of the opened state.db, compared on every write so an out-of-band
        # replace cannot limp through in-place surgery (inode: mv/new-file; application_id: cp).
        self._db_file_identity: Optional[tuple] = None
        self._db_file_application_id: int = 0
        self._db_sidecar_identity: Dict[str, tuple] = {}
        self._db_replaced = self._db_wal_generation_lost = False
        self._db_corrupt, self._db_corrupt_reason = False, ""  # sticky quarantine (StateDbCorruptError)
        self._fts_usermerge_floor_applied = False  # one-shot usermerge-floor write guard
        self._fts_enabled = self._fts_stale = self._trigram_available = False
        # _fts_cjk_loaded: tokenizer on the writer connection; _fts_cjk_available: messages_fts_cjk
        # is queryable AND not marked stale.
        self._fts_cjk_loaded = self._fts_cjk_available = self._fts_unavailable_warned = False
        self._conn = None
        # Async token accounting; distinct from self._lock so enqueue/flush never contends with writes.
        self._token_queue: deque = deque()
        self._token_queue_cond = threading.Condition(threading.Lock())
        self._token_writer_thread: Optional[threading.Thread] = None
        self._token_writer_stop = self._token_writer_busy = False
        self._token_atexit_hook: Optional[Callable[[], None]] = None
        # Opened via hermes_state_registry.acquire(): close() releases a refcount instead.
        # Set True when this instance is opened via hermes_state_registry.acquire(). Makes close() a no-op so the
        # registry (not individual callers) controls the connection lifecycle (#90837).
        self._shared_registry_owned = False
        initialization_complete = False
        try:
            if read_only:
                self._open_read_only()
            else:
                self._open_writer()
            self._record_db_file_identity()
            initialization_complete = True
        except Exception as exc:
            # Surface WHY via /resume and friends; callers keep their ``_session_db = None`` path.
            _set_last_init_error(f"{type(exc).__name__}: {exc}")
            raise
        finally:
            if not initialization_complete:
                conn, self._conn = self._conn, None
                self._close_connection_quietly(conn)

    def _open_writer(self) -> None:
        """Writable open: preflight, zero-byte quarantine, connect + schema (one in-place repair of a
        malformed sqlite_master), generation stamp."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Read-only file/sidecar preflight BEFORE the first connection: an actionable message
        # instead of an opaque "attempt to write a readonly database" from inside _init_schema.
        preflight_db_writability(self.db_path, db_label="state.db")
        try:
            # Serialize zero-byte check, quarantine, connect and schema commit so concurrent
            # openers don't race the absent-path -> schema-commit window.
            if not self.db_path.exists() or is_zeroed_state_db(self.db_path):
                with quarantine_cross_process_lock(self.db_path) as lock_acquired:
                    if not lock_acquired:
                        logger.warning(
                            "startup quarantine lock for %s not acquired within 5s; proceeding",
                            self.db_path,
                        )
                    self._handle_quarantine_if_zeroed(already_locked=lock_acquired)
                    self._connect_and_init_with_lock_patience()
            else:
                self._handle_quarantine_if_zeroed(already_locked=False)
                self._connect_and_init_with_lock_patience()
        except sqlite3.DatabaseError as exc:
            # A malformed schema fails on the very first statement (before _init_schema), so the
            # FTS-rebuild layer never sees it: repair sqlite_master in place (backup first), reopen once.
            if not is_malformed_schema_error(exc) or not _claim_repair_attempt(self.db_path):
                raise
            logger.error(
                "state.db schema is malformed (%s) — attempting automatic "
                "repair (a backup copy is made first).", exc,
            )
            self._close_connection_quietly(self._conn)
            if not repair_state_db_schema(self.db_path).get("repaired"):
                raise
            self._connect_and_init_with_lock_patience()
        # FTS optimization is OPT-IN (`hermes db optimize`); no background worker races session lifecycle.
        self._ensure_db_file_generation()

    def _open_read_only(self) -> None:
        """Read-only attach for cross-profile aggregation: no schema init, NO write
        lock (sidebar polling never contends with that profile's backend); the DB
        must exist. FTS flags are probed with SELECTs only, and the connection is
        closed on ANY probe failure (malformed schema raises DatabaseError) so a
        leaked tracked connection cannot block the forensic backup the writable heal takes next."""
        for attempt in range(_READ_ONLY_IOERR_RETRY_ATTEMPTS + 1):
            try:
                self._conn = conn = self._connect_read_only(timeout=1.0)
                try:
                    apply_database_pragmas(conn, db_label="state.db")
                    cursor = conn.cursor()
                    self._fts_enabled = self._fts_table_probe(cursor, "messages_fts") is True
                    if self._fts_enabled:
                        self._trigram_available = (
                            self._fts_table_probe(cursor, "messages_fts_trigram") is True
                        )
                except BaseException:
                    self._conn = None
                    self._close_connection_quietly(conn)
                    raise
                return
            except sqlite3.OperationalError as ioerr:
                # In-flight WAL checkpoint/reset/frame-flush on the writer side can surface
                # SQLITE_IOERR to a mode=ro reader (it can't do the -shm recovery the read
                # needs). Closes in milliseconds: retry a bounded number of times before
                # classifying the store as failed (#100436; see _READ_ONLY_IOERR_RETRY_ATTEMPTS).
                transient = _DISK_IO_ERROR_MARKER in str(ioerr).lower()
                if attempt >= _READ_ONLY_IOERR_RETRY_ATTEMPTS or not transient:
                    raise
                time.sleep(_READ_ONLY_IOERR_RETRY_BACKOFF_S)

    def _connect_read_only(self, timeout: float) -> sqlite3.Connection:
        """``mode=ro`` tracked connection with Row factory. check_same_thread=False: pooled connections
        are borrowed by whichever thread reads next; exclusive ownership is enforced by pool checkout."""
        conn = _connect_tracked_db(
            f"file:{self.db_path}?mode=ro", tracking_path=self.db_path, uri=True,
            check_same_thread=False, timeout=timeout, isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _handle_quarantine_if_zeroed(self, already_locked: bool = False) -> None:
        """Quarantine a zero-byte/headerless state.db so a fresh one can open; if quarantine failed,
        raise the clear message instead of opening the zeroed file."""
        if not (self.db_path.exists() and is_zeroed_state_db(self.db_path)):
            return
        try:
            zsize = self.db_path.stat().st_size
        except OSError:
            zsize = -1
        qpath = quarantine_zeroed_state_db(self.db_path, already_locked=already_locked)
        msg = (
            f"state.db looks ZEROED ({zsize} bytes, no SQLite header). "
            f"Preserved at {qpath or '(quarantine failed — file left in place)'}. "
            f"Restore from {self.db_path.parent / 'state-snapshots'} via `hermes snapshot list` / "
            f"`hermes snapshot restore <id>` if available. "
            "Opening a fresh empty database so the agent can start."
        )
        logger.error(msg)
        _set_last_init_error(msg)
        if qpath is None and self.db_path.exists() and is_zeroed_state_db(self.db_path):
            raise sqlite3.DatabaseError(msg)

    def _open_writer_conn(self) -> sqlite3.Connection:
        """Connect + WAL/pragma/tokenizer setup for a writer connection (no schema init). Short timeout:
        jittered application-level retry handles contention, not SQLite's busy handler;
        isolation_level=None: explicit BEGIN IMMEDIATE."""
        conn = _connect_tracked_db(
            str(self.db_path), check_same_thread=False, timeout=1.0, isolation_level=None,
        )
        try:
            conn.row_factory = sqlite3.Row
            self._wal_active = apply_wal_with_fallback(conn, db_label="state.db") == "wal"
            apply_database_pragmas(conn, db_label="state.db")
            conn.execute("PRAGMA foreign_keys=ON")
            self._fts_cjk_loaded = load_fts5_cjk_extension(conn)
        except BaseException:
            self._close_connection_quietly(conn)
            raise
        return conn

    def _connect_and_init(self) -> None:
        # Refuse before sqlite3.connect (under the startup lock) so we cannot mint
        # a replacement WAL while a live writer still holds a deleted sidecar inode.
        refuse_deleted_wal_generation(self.db_path)
        self._conn = self._open_writer_conn()
        self._init_schema()

    def _connect_and_init_with_lock_patience(self) -> None:
        """Open + init, waiting out a sibling's write lock with jittered patience:
        _init_schema's DDL runs on a 1s-timeout connection, so a sibling's VACUUM
        or checkpoint used to fail the ENTIRE open and callers disabled
        persistence for the whole run. Non-lock errors propagate immediately."""
        # Lock contention during open: _init_schema's DDL/reconcile statements run on a 1s-timeout
        # connection with no retry, so a sibling process holding the write lock (VACUUM, TRUNCATE checkpoint
        # at close, a long FTS pass from an older still-running install) used to fail the ENTIRE open —
        # callers then disable persistence for the whole run ("Failed to initialize SessionDB ... database
        # is locked", #74478). The store is healthy; wait it out with the same jittered patience the write
        # path uses.
        deadline = time.monotonic() + self._WRITE_PATIENCE_S
        while True:
            try:
                self._connect_and_init()
                return
            except sqlite3.OperationalError as exc:
                err = str(exc).lower()
                if "locked" not in err and "busy" not in err:
                    raise
                self._close_connection_quietly(self._conn)
                now = time.monotonic()
                if now >= deadline:
                    raise
                jitter = random.uniform(self._WRITE_RETRY_SLOW_MIN_S, self._WRITE_RETRY_SLOW_MAX_S)
                time.sleep(min(jitter, max(deadline - now, 0.001)))

    # ── Read-path split ──

    def _get_read_conn(self) -> Optional[sqlite3.Connection]:
        """Open a fresh read-only connection, or None when unavailable (callers
        return it to self._read_pool). WAL only: WAL readers never block on the
        writer, so reads skip self._lock; under DELETE journal mode (NFS fallback)
        readers hit SQLITE_BUSY storms, so the legacy locked path stays. Autocommit
        reads see everything committed so far (read-your-writes for flush-then-search)."""
        if not self._wal_active or self.read_only:
            return None
        with self._read_conns_lock:
            failed_at = self._read_open_failed_at
            backing_off = failed_at and time.monotonic() - failed_at < _READ_OPEN_RETRY_SECONDS
            if self._read_conns_closed or backing_off:
                return None
        # Permit BEFORE the open: openers race for permits, not descriptors.
        if not self._read_budget.acquire(self):
            logger.debug(
                "read pool at capacity (%d) for %s; serving this read from the "
                "locked writer connection", _READ_POOL_MAX, self.db_path,
            )
            return None
        conn = None  # bound before the try so the handlers can close a half-open one
        try:
            conn = self._connect_read_only(timeout=5.0)
            apply_database_pragmas(conn, db_label="state.db")
            if self._fts_cjk_loaded:  # registers in the connection, not the file: ro is fine
                load_fts5_cjk_extension(conn)
        except BaseException as exc:
            # A half-open connection (open ok, extension load failed) is a live tracked descriptor,
            # the leak shape this pool exists to fix; a stranded permit would shrink the read
            # path by one slot forever. (Not _close_read_conn: callers release their own permit.)
            if conn is not None:
                self._close_conn_logged(conn, "partially-opened read conn")
            self._read_budget.release()
            if not isinstance(exc, sqlite3.Error):
                raise
            with self._read_conns_lock:
                self._read_open_failed_at = time.monotonic()
            logger.debug("read-only connection open failed for %s", self.db_path, exc_info=True)
            return None
        return conn

    def _evict_one_idle_read_conn(self) -> bool:
        """Close one idle pooled connection (a peer on the same file wants its permit); never a live one."""
        try:
            conn = self._read_pool.get_nowait()
        except queue.Empty:
            return False
        self._close_read_conn(conn)
        return True

    def _close_read_conn(self, conn) -> None:
        """Close a pooled read connection and release its permit even when the close fails (withholding
        it would narrow the read path forever). Over-releasing the BoundedSemaphore raises ValueError."""
        try:
            self._close_conn_logged(conn, "read-conn")
        finally:
            self._read_budget.release()

    def _checkout_read_conn(self) -> Optional[sqlite3.Connection]:
        """Borrow a read connection, opening on a miss; None when the read path is unavailable.
        A pool hit costs no permit (the connection already holds one)."""
        if not self._wal_active or self.read_only:
            return None
        try:
            return self._read_pool.get_nowait()
        except queue.Empty:
            return self._get_read_conn()

    @contextmanager
    def _read_ctx(self) -> Iterator[sqlite3.Connection]:
        """Yield a connection for read-only statements: a pooled read-only
        connection with NO lock under WAL; otherwise (non-WAL, open failure,
        ceiling reached) the writer connection under self._lock — deliberate
        degradation: slower beats EMFILE, which the supervisor cannot see."""
        conn = self._checkout_read_conn()
        if conn is not None:
            try:
                yield conn
            finally:
                returned = False
                with self._read_conns_lock:
                    if not self._read_conns_closed:
                        try:
                            self._read_pool.put_nowait(conn)
                            returned = True
                        except queue.Full:
                            pass
                if not returned:
                    # close() drained the pool (or queue.Full: unreachable while
                    # permits == maxsize, load-bearing if they drift): surplus.
                    self._close_read_conn(conn)
            return
        with self._lock:
            if self._conn is None:  # close() raced a still-unwinding reader
                self._reopen_after_close_locked(context="read")
            yield cast(sqlite3.Connection, self._conn)

    def _reopen_after_close_locked(self, context: str = "write") -> None:
        """Reopen the writer after ``close()`` raced a live caller (a teardown owner
        set ``_conn = None`` while a worker still had a transcript flush to land).
        Loud (WARNING) and bounded (only after an explicit close()). Caller holds
        ``self._lock``. No _init_schema: no DDL races with siblings during teardown."""
        if self.read_only:
            raise sqlite3.ProgrammingError(
                f"SessionDB for {self.db_path} was closed (read-only handle); "
                f"cannot serve a {context} after close()"
            )
        # A reopen resolves the PATH again: a replaced file would be written through stale WAL/shm
        # assumptions; a quarantined handle must never hand a fresh connection to a damaged file.
        if self._db_corrupt and not (self._db_replaced or self._db_file_was_replaced()):
            raise self._corrupt_error(
                f"state.db connection for {self.db_path} is quarantined after "
                f"structural corruption; refusing to reopen for a {context} "
                "after close(). "
            )
        self._halt_if_db_generation_changed()
        logger.warning(
            "state.db connection for %s was closed while a %s was still in "
            "flight — reopening (teardown/worker race, #94736)", self.db_path, context,
        )
        try:
            self._conn = self._open_writer_conn()
        except Exception as exc:
            raise sqlite3.OperationalError(
                f"state.db connection was closed while a {context} was still "
                f"in flight (a session-teardown path called close() before "
                f"this worker finished — #94736) and the automatic reopen failed: {exc}"
            ) from exc

    def _execute_write(
        self, fn: Callable[[sqlite3.Connection], T], patience_s: Optional[float] = None,
    ) -> T:
        """Run *fn(conn)* inside BEGIN IMMEDIATE with jittered lock retry; commit
        is handled here (callers must not commit). Returns *fn*'s result.
        BEGIN IMMEDIATE takes the WAL write lock up front so contention surfaces
        immediately; on locked/busy the Python lock is released, a jitter slept,
        and the WHOLE callback retried — *fn* must stay idempotent under retry."""
        if patience_s is None:
            patience_s = self._WRITE_PATIENCE_S
        deadline = time.monotonic() + patience_s
        compression_deadline: Optional[float] = None  # set on the first compression-busy collision
        # One retry for SQLITE_IOERR raised by BEGIN IMMEDIATE itself (callback not run: nothing
        # replayed). Once fn has started, an IOERR leaves settlement unknown and must propagate.
        # The callback has not run at that point, so there is no durable effect to replay and the retry is
        # exactly-once safe (#99502's contract). Once the callback starts, an IOERR leaves the write's
        # settlement unknown and must propagate — this helper owns non-idempotent transcript/counter
        # mutations, not just idempotent UPSERTs.
        ioerr_begin_retried = False
        while True:
            self._raise_if_db_corrupt()
            self._raise_if_db_replaced()
            fn_started = False
            try:
                with self._lock:
                    if self._conn is None:  # close() raced this writer
                        self._reopen_after_close_locked(context="write")
                    self._conn.execute("BEGIN IMMEDIATE")
                    try:
                        fn_started = True
                        result = fn(self._conn)
                        self._conn.commit()
                    except BaseException:
                        try:
                            self._conn.rollback()
                        except Exception:
                            pass
                        raise
                # Success — periodic best-effort checkpoint + FTS merge.
                self._write_count += 1
                if self._write_count % self._CHECKPOINT_EVERY_N_WRITES == 0:
                    self._try_wal_checkpoint()
                if self._write_count % self._FTS_MERGE_EVERY_N_WRITES == 0:
                    self._try_incremental_merge_fts()
                return result
            except SessionCompressionInProgressError:
                # Transient (see _COMPRESSION_BUSY_WAIT_S): a steer landing mid-compression must not abort.
                # A live foreign compression lock is transient: the compressor publishes in a couple of
                # seconds. Without any wait, a steer that lands mid-compression aborts the user's turn as
                # session_persistence_failed and sends the operator hunting disk space that was never the
                # problem (#75083). The budget is _COMPRESSION_BUSY_WAIT_S, not the write-lock patience: the
                # lease is a correctness boundary, so a writer still locked out after a short wait must be
                # refused rather than left to land a stale turn once a long-running or wedged compression
                # finally lets go.
                if compression_deadline is None:
                    compression_deadline = min(time.monotonic() + self._COMPRESSION_BUSY_WAIT_S, deadline)
                if self._sleep_before_write_retry(
                    compression_deadline, self._COMPRESSION_BUSY_WAIT_S
                ):
                    continue
                raise
            except sqlite3.Error as exc:
                # 'no more rows' is a transient engine error on contended WAL appends (some builds
                # raise it as InterfaceError, a sibling of DatabaseError): retry like locked/busy.
                if _is_no_more_rows(exc) and self._sleep_before_write_retry(deadline, patience_s):
                    continue
                err_msg = str(exc).lower()
                if isinstance(exc, sqlite3.OperationalError):
                    if "locked" in err_msg or "busy" in err_msg:
                        if self._sleep_before_write_retry(deadline, patience_s):
                            continue
                        # Say what actually happened, not disk/permission damage.
                        raise sqlite3.OperationalError(
                            f"database is locked (another Hermes process held the "
                            f"state.db write lock for over {patience_s:.0f}s — "
                            "likely a long maintenance operation such as VACUUM, "
                            "a large WAL checkpoint, or an older pre-update "
                            "process; the database itself is healthy)"
                        ) from exc
                    if (
                        _DISK_IO_ERROR_MARKER in err_msg and not fn_started and not ioerr_begin_retried
                        and self._sleep_before_write_retry(deadline, patience_s)
                    ):
                        # Retry on the SAME connection: close()+reopen would cancel this process's
                        # POSIX locks for every sibling (howtocorrupt §2.2).
                        ioerr_begin_retried = True
                        continue
                    raise  # non-lock error, callback already ran, or patience exhausted
                if isinstance(exc, sqlite3.DatabaseError):
                    # An out-of-band replace surfaces as this same corruption class; in-file repair
                    # on a NEW generation amplifies the damage.
                    if (
                        "not a database" in err_msg or is_malformed_db_error(exc)
                        or self._is_fts_write_corruption_error(exc)
                    ):
                        self._raise_if_db_replaced()
                    # Corrupt FTS shadow tables fail every write via the sync triggers while canonical
                    # rows are intact: detach the derived indexes atomically and retry (never rebuild here).
                    if self._enter_fts_fail_open(exc):
                        continue
                    # What survives both checks is structural damage: quarantine.
                    if self._is_structural_corruption_error(exc):
                        self._halt_db_corrupt(exc)
                raise

    def _write_sql(
        self, sql: str, params: Any = (), *, many: bool = False, patience_s: Optional[float] = None,
    ) -> None:
        """Run one INSERT/UPDATE/DELETE through ``_execute_write``."""
        def _do(conn):
            (conn.executemany if many else conn.execute)(sql, params)
        self._execute_write(_do, patience_s=patience_s)

    def _write_rowcount(self, sql: str, params: Any = (), *, patience_s: Optional[float] = None) -> int:
        """Run one UPDATE/DELETE through ``_execute_write``; return rows changed
        (``SELECT changes()`` when the driver reports None / negative)."""
        def _do(conn):
            rowcount = conn.execute(sql, params).rowcount
            if rowcount is None or rowcount < 0:
                rowcount = conn.execute("SELECT changes()").fetchone()[0]
            return rowcount
        return self._execute_write(_do, patience_s=patience_s)

    def _read_one(self, sql: str, params: Any = ()) -> Optional[sqlite3.Row]:
        """``fetchone()`` of one read-only statement via ``_read_ctx``."""
        with self._read_ctx() as conn:
            return conn.execute(sql, params).fetchone()

    def _read_all(self, sql: str, params: Any = ()) -> List[sqlite3.Row]:
        """``fetchall()`` of one read-only statement via ``_read_ctx``."""
        with self._read_ctx() as conn:
            return conn.execute(sql, params).fetchall()

    def _ensure_db_file_generation(self) -> None:
        """Mint a once-per-file generation stamp (state_meta + application_id). First opener wins (INSERT
        OR IGNORE); application_id is written only while 0 so racers converge. PASSIVE checkpoint only.

        See #45383.
        """
        if self.read_only or self._conn is None:
            return
        token = uuid.uuid4().hex
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT OR IGNORE INTO state_meta (key, value) VALUES (?, ?)",
                    (_STATE_DB_GENERATION_KEY, token),
                )
                row = self._conn.execute(
                    "SELECT value FROM state_meta WHERE key = ?", (_STATE_DB_GENERATION_KEY,),
                ).fetchone()
                if row and row[0]:
                    token = str(row[0])
                pragma_row = self._conn.execute("PRAGMA application_id").fetchone()
                current = int(pragma_row[0] or 0) if pragma_row else 0
                if current == 0:
                    current = (int(token[:8], 16) & 0x7FFFFFFF) or 1
                    self._conn.execute(f"PRAGMA application_id={current}")
                self._db_file_application_id = current
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except sqlite3.Error:
                    pass
        except sqlite3.Error as exc:
            logger.debug("state.db generation stamp skipped: %s", exc)

    def _record_db_file_identity(self) -> None:
        """Snapshot inode plus the on-disk generation header when present."""
        self._db_file_identity = _stat_db_file_identity(self.db_path)
        self._db_sidecar_identity = _stat_sqlite_sidecar_identity(self.db_path)
        disk_id = _read_sqlite_application_id(self.db_path)
        if disk_id:
            self._db_file_application_id = disk_id
        elif self._conn is not None and not self._db_file_application_id:
            try:
                pragma_row = self._read_one("PRAGMA application_id")
            except sqlite3.Error:
                pragma_row = None
            if pragma_row and pragma_row[0]:
                self._db_file_application_id = int(pragma_row[0])

    def _db_file_was_replaced(self) -> bool:
        """True when the path no longer names the file this instance opened."""
        recorded = self._db_file_identity
        if recorded is not None and _stat_db_file_identity(self.db_path) != recorded:
            return True
        recorded_app = int(self._db_file_application_id or 0)
        if not recorded_app:
            return False
        # Header 0 = WAL not yet checkpointed, not a replace; a real replacement is nonzero.
        disk_app = _read_sqlite_application_id(self.db_path)
        return bool(disk_app and disk_app != recorded_app)

    def _wal_generation_was_lost(self) -> bool:
        """True when the WAL/SHM generation this handle opened is gone. Recorded
        generation: pure stat (no /proc walk on healthy writes). Empty identity
        (WAL appeared after open, or cleared by a clean close()): probe
        /proc/self/fd for deleted sidecars and adopt the current ones once clean."""
        recorded = self._db_sidecar_identity or {}
        base = os.fspath(self.db_path)
        if recorded:
            return any(
                _stat_db_file_identity(Path(base + suffix)) != ident for suffix, ident in recorded.items()
            )
        if not self._wal_active:  # no sidecar generation to lose; keep /proc off the hot path
            return False
        if sys.platform.startswith("linux"):
            watched = _watched_sqlite_sidecar_paths(self.db_path)
            try:
                for target in _proc_fd_targets(os.getpid()):
                    if " (deleted)" in target and _canonical_sqlite_path(target) in watched:
                        return True
            except OSError:
                return False
        # Probe clean (or unavailable): adopt the current sidecar generation.
        current_identity = _stat_sqlite_sidecar_identity(self.db_path)
        if current_identity:
            self._db_sidecar_identity = current_identity
        return False

    def _halt_if_db_generation_changed(self) -> None:
        """Stop writes (logging once) when the file was replaced or its WAL/SHM generation
        is gone: never run in-file repair on a new generation, never keep committing on a
        split WAL. Both flags are sticky."""
        # A reopen resolves the PATH again — if the file at that path is no longer the one this instance
        # originally opened (out-of-band restore/cp/mv), reconnecting would write into the new generation
        # through stale WAL/shm assumptions (#89332). Refuse instead.
        if self._db_replaced or self._db_file_was_replaced():
            self._db_replaced = True
            logger.error(_STATE_DB_REPLACED_MSG)
            raise StateDbReplacedError(_STATE_DB_REPLACED_MSG)
        if self._db_wal_generation_lost or self._wal_generation_was_lost():
            self._db_wal_generation_lost = True
            logger.error(_DELETED_WAL_GENERATION_MSG)
            raise DeletedWalGenerationError(_DELETED_WAL_GENERATION_MSG)

    def _raise_if_db_replaced(self) -> None:
        """Sticky-flag fast path (no log spam on every write), then the live probe."""
        if self._db_replaced:
            raise StateDbReplacedError(_STATE_DB_REPLACED_MSG)
        if self._db_wal_generation_lost:
            raise DeletedWalGenerationError(_DELETED_WAL_GENERATION_MSG)
        self._halt_if_db_generation_changed()

    @classmethod
    def _is_structural_corruption_error(cls, exc: BaseException) -> bool:
        """Bare SQLITE_CORRUPT/NOTADB with no FTS provenance: canonical B-tree/schema/freelist damage,
        never repairable from the live write path."""
        return (
            isinstance(exc, sqlite3.DatabaseError)
            and not isinstance(exc, StateDbCorruptError)
            and not cls._is_fts_write_corruption_error(exc)
            and classify_persistence_error(exc) == "corrupt"
        )

    def _corrupt_error(self, prefix: str = "") -> "StateDbCorruptError":
        """Build the quarantine error for this handle (message assembled once)."""
        return StateDbCorruptError(f"{prefix}{_STATE_DB_CORRUPT_MSG} (cause: {self._db_corrupt_reason})")

    def _halt_db_corrupt(self, exc: BaseException) -> None:
        """Quarantine this handle and raise; never run in-file repair here."""
        self._db_corrupt = True
        self._db_corrupt_reason = str(exc)
        self._disable_close_time_checkpoint()
        logger.error(
            "state.db %s reported structural corruption outside the FTS "
            "indexes (%s); quarantining this handle: no further writes, no "
            "automatic reopen, no explicit WAL checkpoint at close. Stop the "
            "gateway and run `hermes sessions recover --source %s --inspect-only`.", self.db_path, exc,
            self.db_path,
        )
        err = self._corrupt_error()
        for attr in ("sqlite_errorcode", "sqlite_errorname"):
            if getattr(exc, attr, None) is not None:
                setattr(err, attr, getattr(exc, attr))
        raise err from exc

    def _disable_close_time_checkpoint(self) -> None:
        """Best-effort SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE (Python 3.12+): sqlite3's
        close() otherwise runs the internal last-connection checkpoint that wrote
        the incident's pages under wrong page numbers (see StateDbCorruptError).
        <3.12 has no setconfig; the residual checkpoint only carries
        pre-quarantine committed frames, which is tolerable."""
        flag = getattr(sqlite3, "SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE", None)
        conn = self._conn
        setconfig = getattr(conn, "setconfig", None)
        if flag is None or setconfig is None:
            return
        try:
            setconfig(flag, True)
        except Exception:
            logger.debug(
                "Could not disable SQLite's close-time checkpoint on the quarantined handle for %s",
                self.db_path, exc_info=True,
            )

    def _raise_if_db_corrupt(self) -> None:
        if self._db_corrupt:
            raise self._corrupt_error()

    def _sleep_before_write_retry(self, deadline: float, patience_s: float) -> bool:
        """Sleep one jitter interval if the budget allows; True = retry, False = deadline passed. Small
        jitter for the first _WRITE_RETRY_SLOW_AFTER_S, then slow; never overshoots the deadline."""
        now = time.monotonic()
        if now >= deadline:
            return False
        slow = now - (deadline - patience_s) >= self._WRITE_RETRY_SLOW_AFTER_S
        jitter = random.uniform(*(
            (self._WRITE_RETRY_SLOW_MIN_S, self._WRITE_RETRY_SLOW_MAX_S) if slow
            else (self._WRITE_RETRY_MIN_S, self._WRITE_RETRY_MAX_S)
        ))
        time.sleep(min(jitter, max(deadline - now, 0.001)))
        return True

    def _foreign_state_db_holders(self) -> List[Tuple[int, str]]:
        """Foreign processes holding this DB or its WAL sidecars (see hermes_state_holders)."""
        return _foreign_state_db_holders(self.db_path)

    def _try_wal_checkpoint(self) -> None:
        """Best-effort PASSIVE WAL checkpoint; never raises. PASSIVE never blocks writers;
        TRUNCATE corrupted B-trees on 65K+ page databases under exclusive-lock I/O pressure.

        Previous TRUNCATE strategy caused B-tree corruption on large databases (65K+ pages) due to the
        exclusive-lock I/O pressure from checkpointing thousands of frames at once (issue #45383).
        """
        if self._db_corrupt:
            return  # quarantined: never checkpoint over a damaged image
        try:
            with self._lock:
                result = self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                if result and result[1] > 0:
                    logger.debug("WAL checkpoint: %d/%d pages checkpointed", result[2], result[1])
        except Exception as exc:
            logger.warning("WAL checkpoint (PASSIVE) failed: %s", exc)

    def __enter__(self) -> "SessionDB":
        """``with SessionDB(path) as db:`` closes on exit; owners must release deterministically.

        Ownership of a SessionDB should be released explicitly. Historically an instance with a started
        token writer pinned ITSELF (bound-method writer target plus a strong ``atexit`` drain hook), so
        ``__del__`` never ran for exactly the instances that leaked descriptors (#88033). The writer now
        retires after an idle window and the atexit hook holds only a weak reference, so abandoned handles
        are eventually collectible — but "eventually, after the idle window and a GC cycle" is not a release
        policy. Call sites owning a handle are still expected to close it deterministically (see the
        ownership comments in ``run_agent.py`` and ``tui_gateway/methods_session.py``).
        """
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False  # never suppress the caller's exception

    def close(self):
        """Drain queued token deltas, then a PASSIVE checkpoint on writable handles
        (NOT TRUNCATE: a full WAL reset races the gateway's live writer, tearing
        B-tree pages). A registry-shared instance RELEASES one refcount instead.

        Drains queued token deltas first (the background writer needs the connection). Read-only connections
        never request a checkpoint. See #45383.
        When this instance is shared (opened via ``hermes_state_registry.acquire``), ``close()`` RELEASES one
        refcount instead of tearing down the connection: the registry owns the lifecycle and only closes on
        the final release (#90837). This prevents one caller's close from tearing down the writer connection
        that other callers in the same process are still using — while still letting legacy ``close()`` call
        sites return their reference instead of leaking it.
        """
        if self._shared_registry_owned:
            from hermes_state_registry import release
            release(self)
            return
        self._stop_token_writer()
        hook, self._token_atexit_hook = self._token_atexit_hook, None
        if hook is not None:
            atexit.unregister(hook)
        # Closed flag first: an in-flight reader then closes its own connection.
        with self._read_conns_lock:
            self._read_conns_closed = True
        while self._evict_one_idle_read_conn():
            pass
        with self._lock:
            if self._conn:
                if self._db_corrupt:  # quarantined: no checkpoint over a damaged image
                    logger.warning(
                        "Skipping the close-time WAL checkpoint for %s: this "
                        "handle observed structural corruption (%s). Take a "
                        "snapshot of state.db, -wal and -shm before restarting, "
                        "then run `hermes sessions recover --source %s --inspect-only`.", self.db_path,
                        self._db_corrupt_reason, self.db_path,
                    )
                elif not self.read_only:  # PASSIVE, not TRUNCATE (see docstring)
                    try:
                        # Every cron run_agent opens+closes a transient SessionDB, so a TRUNCATE here fires
                        # a full WAL reset many times/hour, racing the gateway's long-lived writer on large
                        # WAL databases and tearing hot B-tree pages -- the #45383 corruption this class's
                        # own periodic checkpoint was already made PASSIVE to avoid. TRUNCATE belongs only
                        # on a sole-opener/quiescent connection.
                        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    except Exception as exc:
<<<<<<< HEAD
                        logger.debug(
                            "WAL checkpoint (TRUNCATE) at close failed: %s",
                            exc,
                        )
                self._conn.close()
                self._conn = None

    # ── Chunked FTS rebuild engine (v23 opt-in optimize) ──
    #
    # `optimize_fts_storage()` (the `hermes sessions optimize-storage`
    # command) drops the legacy inline FTS indexes and backfills the new
    # external-content ones. A single blocking rebuild measured ~16 minutes
    # of held write lock on a real 25 GB DB, so the backfill runs in small
    # chunks, each in its own short write transaction:
    #   - concurrent readers/writers are never starved (WAL stays small,
    #     each chunk checkpoints via the normal _execute_write cadence);
    #   - an interrupted run (Ctrl-C, crash) resumes from
    #     fts_rebuild_progress when the command is re-run;
    #   - multiple processes sharing the DB don't double-run it — each chunk
    #     claims work by compare-and-swap on fts_rebuild_progress, so even a
    #     concurrent second runner just interleaves chunks safely.
    #
    # THROTTLING (the part that keeps a live gateway sharing the DB
    # responsive): a greedy chunk loop re-acquires BEGIN IMMEDIATE nearly
    # back-to-back and can starve another process's writer into exhausting
    # its lock retries (an early 5000-row/50ms version owned the write lock
    # ~85% of the time and visibly froze concurrent CLI sessions on a large
    # install). Two layers prevent that:
    #   1. Small chunks (500 rows) — a foreground write queues behind a
    #      chunk for at most ~tens of ms.
    #   2. Inter-chunk pause — the loop sleeps max(_FTS_REBUILD_MIN_PAUSE,
    #      chunk cost x _FTS_REBUILD_DUTY_FACTOR) between chunks, capping
    #      this process's share of DB bandwidth so concurrent writers always
    #      find open windows. This works cross-process (unlike any
    #      same-process activity stamp) because it bounds our own duty
    #      cycle unconditionally.

    _FTS_REBUILD_CHUNK_ROWS = 500
    _FTS_REBUILD_DUTY_FACTOR = 4.0      # sleep >= 4x chunk cost (≤20% duty)
    _FTS_REBUILD_MIN_PAUSE = 0.2        # seconds — floor between chunks

    # Demoted v22 FTS shadow tables awaiting teardown (see the v23 migration:
    # DROP of a multi-GB FTS vtable blocks for minutes, so the migration
    # demotes the vtable definitions out of sqlite_master and renames the
    # orphaned shadow tables — now plain tables — to fts_v22_trash_*; the
    # worker empties them in bounded chunks, then drops them cheaply).
    _FTS_TRASH_PREFIX = "fts_v22_trash_"

    # ── CJK-bigram index backfill (dedicated marker pair) ──
    #
    # Same chunk engine as the main deferred rebuild, but on the
    # ``fts_cjk_rebuild_*`` markers so a cjk-only backfill (the common case:
    # an already-optimized v23 DB gaining the cjk index) never gates the
    # complete ``messages_fts`` / trigram triggers.

    # ── Opt-in v23 FTS storage optimization (`hermes sessions optimize-storage`) ──
    #
    # This is the ONLY path that migrates an existing legacy (v22 inline) DB
    # to the v23 external-content schema. It is deliberately foreground and
    # user-invoked, never automatic, because it is disk-heavy and long. It
    # runs the throttled/resumable chunk engine above to completion
    # synchronously — demote → new schema → chunked backfill → chunked
    # teardown — with progress callbacks, a disk preflight in the CLI
    # wrapper, a VACUUM at the end, and a defensive schema_version bump.

    def _has_fts_trash(self, conn) -> bool:
        """True when demoted v22 shadow tables are still awaiting teardown.
        Caller must hold ``self._lock`` (or pass a migration-time cursor)."""
        return bool(conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name LIKE ? ESCAPE '\\' LIMIT 1",
            (self._FTS_TRASH_PREFIX.replace("_", "\\_") + "%",),
        ).fetchone())

    # =========================================================================
    # Session lifecycle
    # =========================================================================

    def _insert_session_row(
        self,
        session_id: str,
        source: str,
        model: str = None,
        model_config: Dict[str, Any] = None,
        system_prompt: str = None,
        user_id: str = None,
        session_key: Optional[str] = None,
        chat_id: str = None,
        chat_type: str = None,
        thread_id: str = None,
        parent_session_id: str = None,
        cwd: str = None,
        profile_name: str = None,
        git_repo_root: str = None,
        origin_json: str = None,
        display_name: str = None,
    ) -> None:
        """Insert a session row, enriching NULL metadata on conflict.

        The gateway's ``get_or_create_session`` creates a bare row (source +
        user_id) *before* the agent exists; the agent's later
        ``create_session`` then carries the real ``model`` / ``model_config`` /
        ``system_prompt``. A plain ``INSERT OR IGNORE`` silently dropped that
        enrichment, leaving gateway sessions with NULL model/billing metadata.
        The ``ON CONFLICT`` upsert backfills those fields via ``COALESCE`` —
        only filling columns that are still NULL, never overwriting values an
        earlier writer already set (so a later bare call with source="unknown"
        can't clobber a real source/model).

        ``chat_id``/``thread_id`` record the messaging origin (the chat/room and
        thread the session was started in) so that gateway ``/resume`` can prove
        a persisted, now-inactive row belongs to the caller's chat/thread before
        switching to it (IDOR scoping — without them the ``sessions`` table has
        no chat/thread to compare).

        When ``parent_session_id`` is set (compression fork, delegate/subagent
        spawn, branch continuation) and this row's own ``cwd``/``git_repo_root``/
        ``git_branch``/``profile_name`` are still NULL after the insert, they are
        backfilled from the parent row. Callers of ``create_session`` for a child
        session historically didn't propagate these fields themselves (e.g. the
        compression-fork path), so a lineage could silently lose its working
        directory and drop out of the project sidebar every time it forked
        (#64709), or lose its owning profile and be aggregated as "default" every
        time it rotated or branched (the cross-profile session-jump bug). This
        only fills NULLs — an explicit value on the child is never overwritten.
        For compression forks specifically
        (parent ended with ``end_reason='compression'``), the gateway origin
        columns (``user_id``/``session_key``/``chat_id``/``chat_type``/
        ``thread_id``/``display_name``/``origin_json``) are inherited too, so a
        crash before the gateway re-records the peer can't strand the child
        without a recoverable routing mapping (#59527).
        """
        def _do(conn):
            system_prompt_hash = self._store_system_prompt(conn, system_prompt)
            conn.execute(
                """INSERT INTO sessions (
                   id, source, user_id, session_key, chat_id, chat_type, thread_id,
                   model, model_config, system_prompt, system_prompt_hash,
                   parent_session_id, cwd, profile_name, git_repo_root,
                   origin_json, display_name, started_at
                )
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       model = COALESCE(sessions.model, excluded.model),
                       model_config = COALESCE(sessions.model_config, excluded.model_config),
                       system_prompt_hash = COALESCE(
                           sessions.system_prompt_hash,
                           excluded.system_prompt_hash
                       ),
                       system_prompt = CASE
                           WHEN sessions.system_prompt_hash IS NULL
                                AND excluded.system_prompt_hash IS NOT NULL
                           THEN NULL
                           ELSE sessions.system_prompt
                       END,
                       session_key = COALESCE(sessions.session_key, excluded.session_key),
                       chat_id = COALESCE(sessions.chat_id, excluded.chat_id),
                       chat_type = COALESCE(sessions.chat_type, excluded.chat_type),
                       thread_id = COALESCE(sessions.thread_id, excluded.thread_id),
                       parent_session_id = COALESCE(sessions.parent_session_id, excluded.parent_session_id),
                       cwd = COALESCE(sessions.cwd, excluded.cwd),
                       profile_name = COALESCE(sessions.profile_name, excluded.profile_name),
                       git_repo_root = COALESCE(sessions.git_repo_root, excluded.git_repo_root),
                       origin_json = COALESCE(sessions.origin_json, excluded.origin_json),
                       display_name = COALESCE(sessions.display_name, excluded.display_name)""",
                (
                    session_id,
                    source,
                    user_id,
                    session_key,
                    chat_id,
                    chat_type,
                    thread_id,
                    model,
                    json.dumps(model_config) if model_config else None,
                    system_prompt_hash,
                    parent_session_id,
                    cwd,
                    profile_name,
                    git_repo_root,
                    origin_json,
                    display_name,
                    time.time(),
                ),
            )
            if system_prompt_hash is not None:
                self._delete_unreferenced_system_prompts(conn)
            if parent_session_id:
                conn.execute(
                    """UPDATE sessions
                       SET cwd = COALESCE(sessions.cwd,
                                 (SELECT p.cwd FROM sessions p
                                   WHERE p.id = sessions.parent_session_id)),
                           git_repo_root = COALESCE(sessions.git_repo_root,
                                           (SELECT p.git_repo_root FROM sessions p
                                             WHERE p.id = sessions.parent_session_id)),
                           git_branch = COALESCE(sessions.git_branch,
                                        (SELECT p.git_branch FROM sessions p
                                          WHERE p.id = sessions.parent_session_id)),
                           profile_name = COALESCE(sessions.profile_name,
                                          (SELECT p.profile_name FROM sessions p
                                            WHERE p.id = sessions.parent_session_id))
                     WHERE id = ? AND parent_session_id IS NOT NULL""",
                    (session_id,),
                )
                # Belt-and-suspenders for gateway routing metadata (#59527):
                # the gateway re-records the peer on the child after rotation
                # (d5b4879d4), but a hard crash between child creation and that
                # write leaves the child row without origin columns, so
                # ``find_latest_gateway_session_for_peer`` can't recover the
                # mapping on restart. Inherit them from the parent at creation
                # time — but ONLY for compression forks (parent already ended
                # with end_reason='compression'). Delegate/subagent children
                # are spawned while the parent is still live and must NOT
                # inherit routing keys, or peer recovery could repoint gateway
                # traffic into a subagent's session.
                conn.execute(
                    """UPDATE sessions
                       SET user_id = COALESCE(sessions.user_id,
                                     (SELECT p.user_id FROM sessions p
                                       WHERE p.id = sessions.parent_session_id)),
                           session_key = COALESCE(sessions.session_key,
                                         (SELECT p.session_key FROM sessions p
                                           WHERE p.id = sessions.parent_session_id)),
                           chat_id = COALESCE(sessions.chat_id,
                                     (SELECT p.chat_id FROM sessions p
                                       WHERE p.id = sessions.parent_session_id)),
                           chat_type = COALESCE(sessions.chat_type,
                                       (SELECT p.chat_type FROM sessions p
                                         WHERE p.id = sessions.parent_session_id)),
                           thread_id = COALESCE(sessions.thread_id,
                                       (SELECT p.thread_id FROM sessions p
                                         WHERE p.id = sessions.parent_session_id)),
                           display_name = COALESCE(sessions.display_name,
                                          (SELECT p.display_name FROM sessions p
                                            WHERE p.id = sessions.parent_session_id)),
                           origin_json = COALESCE(sessions.origin_json,
                                         (SELECT p.origin_json FROM sessions p
                                           WHERE p.id = sessions.parent_session_id))
                     WHERE id = ? AND parent_session_id IS NOT NULL
                       AND EXISTS (
                           SELECT 1 FROM sessions p
                           WHERE p.id = sessions.parent_session_id
                             AND p.end_reason = 'compression'
                       )""",
                    (session_id,),
                )
        # Session-row creation is transcript-critical: if it fails, the
        # first flush of a new session fails and the turn is aborted as
        # session_persistence_failed. Ride out long sibling holds.
        self._execute_write(_do, patience_s=self._TRANSCRIPT_WRITE_PATIENCE_S)

    def create_session(self, session_id: str, source: str, **kwargs) -> str:
        """Create a new session record. Returns the session_id."""
        self._insert_session_row(session_id, source, **kwargs)
        return session_id

    def record_gateway_session_peer(
        self,
        session_id: str,
        *,
        source: str,
        user_id: str = None,
        session_key: str = None,
        chat_id: str = None,
        chat_type: str = None,
        thread_id: str = None,
        display_name: str = None,
        origin_json: str = None,
        include_compression_ancestors: bool = False,
    ) -> None:
        """Persist the gateway routing peer for an existing session row.

        ``display_name`` / ``origin_json`` carry the gateway's presentation
        and full origin metadata (#9006) so consumers (mcp_serve, mirror,
        channel directory) can read routing data from state.db instead of
        sessions.json.  They are COALESCE'd only in the sense that ``None``
        leaves the existing value untouched.

        ``include_compression_ancestors`` keeps a logical compression lineage
        on one routing peer when an explicit gateway resume moves its tip to a
        different lane. Normal per-turn metadata refreshes update only the
        supplied row.

        Self-healing (#82616): when the target row does not exist yet — the
        gateway's ``create_session`` write failed and was deferred, or a
        crash landed between routing publication and row creation — this
        recorder INSERTs the row with the full identity instead of silently
        no-opping. Every per-turn peer refresh is therefore a repair
        opportunity: a gateway session row can no longer be first-created by
        an identity-less lazy writer (``update_token_counts`` /
        ``record_auxiliary_usage``) and stay unroutable forever.
        """
        if not session_id or not session_key:
            return

        def _do(conn):
            lineage_cte = ""
            target_clause = "WHERE id = ?"
            query_params = []
            if include_compression_ancestors:
                lineage_cte = """
                    WITH RECURSIVE compression_lineage(id) AS (
                        SELECT ?
                        UNION
                        SELECT parent.id
                        FROM compression_lineage lineage
                        JOIN sessions child ON child.id = lineage.id
                        JOIN sessions parent ON parent.id = child.parent_session_id
                        WHERE parent.end_reason = 'compression'
                          AND json_extract(
                              COALESCE(child.model_config, '{}'),
                              '$._branched_from'
                          ) IS NULL
                          AND json_extract(
                              COALESCE(child.model_config, '{}'),
                              '$._delegate_from'
                          ) IS NULL
                          AND COALESCE(child.source, '') != 'tool'
                    )
                """
                target_clause = "WHERE id IN (SELECT id FROM compression_lineage)"
                query_params.append(session_id)
            query_params.extend(
                (
                    session_key,
                    source,
                    user_id,
                    chat_id,
                    chat_type,
                    thread_id,
                    display_name,
                    origin_json,
                )
            )
            if not include_compression_ancestors:
                query_params.append(session_id)
            conn.execute(
                f"""{lineage_cte}
                   UPDATE sessions
                   SET session_key = ?, source = ?, user_id = ?, chat_id = ?,
                       chat_type = ?, thread_id = ?,
                       display_name = COALESCE(?, display_name),
                       origin_json = COALESCE(?, origin_json)
                   {target_clause}""",
                query_params,
            )
            # Self-heal (#82616): the UPDATE is a silent no-op when the row
            # is missing (create_session failed earlier, or a crash landed
            # between routing publication and row creation). Insert it with
            # the full identity so the session is durably routable — never
            # leave first-creation to an identity-less lazy writer.
            if not include_compression_ancestors:
                cur = conn.execute(
                    "SELECT 1 FROM sessions WHERE id = ? LIMIT 1", (session_id,)
                )
                if cur.fetchone() is None:
                    conn.execute(
                        """INSERT INTO sessions (
                               id, source, user_id, session_key, chat_id,
                               chat_type, thread_id, display_name, origin_json,
                               started_at
                           )
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(id) DO UPDATE SET
                               session_key = COALESCE(sessions.session_key, excluded.session_key),
                               chat_id = COALESCE(sessions.chat_id, excluded.chat_id),
                               chat_type = COALESCE(sessions.chat_type, excluded.chat_type),
                               thread_id = COALESCE(sessions.thread_id, excluded.thread_id),
                               display_name = COALESCE(sessions.display_name, excluded.display_name),
                               origin_json = COALESCE(sessions.origin_json, excluded.origin_json)""",
                        (
                            session_id,
                            source,
                            user_id,
                            session_key,
                            chat_id,
                            chat_type,
                            thread_id,
                            display_name,
                            origin_json,
                            time.time(),
                        ),
                    )

        self._execute_write(_do)

    def set_expiry_finalized(self, session_id: str, finalized: bool = True) -> None:
        """Mark a gateway session's expiry-finalization flag in state.db.

        Mirrors ``SessionEntry.expiry_finalized`` (sessions.json) so the flag
        survives even if the JSON index is pruned or lost (#9006).
        """
        if not session_id:
            return

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET expiry_finalized = ? WHERE id = ?",
                (1 if finalized else 0, session_id),
            )

        self._execute_write(_do)

    # ── Gateway routing index (replaces sessions.json, #9006 follow-up) ────

    def save_gateway_routing_entry(
        self, session_key: str, entry_json: str, *, scope: str = ""
    ) -> None:
        """Upsert one gateway routing entry (session_key -> SessionEntry JSON).

        The gateway_routing table is the durable replacement for
        sessions.json: one row per routing key, holding the full serialized
        ``SessionEntry`` so the gateway can rehydrate exactly what it wrote.

        ``scope`` namespaces the index the way separate sessions.json files
        did (one per sessions_dir) — callers pass their sessions_dir path so
        two stores with different directories never share routing state.
        """
        if not session_key or not entry_json:
            return

        def _do(conn):
            conn.execute(
                """INSERT INTO gateway_routing (scope, session_key, entry_json, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(scope, session_key) DO UPDATE SET
                       entry_json = excluded.entry_json,
                       updated_at = excluded.updated_at""",
                (scope, session_key, entry_json, time.time()),
            )

        self._execute_write(_do)

    def replace_gateway_routing_entries(
        self, entries: Dict[str, str], *, scope: str = ""
    ) -> None:
        """Atomically replace the routing index for *scope* with *entries*.

        Mirrors the sessions.json full-rewrite semantics: keys absent from
        *entries* are removed (pruned/reset sessions disappear from the
        index).  Runs as a single write transaction.  Other scopes are
        untouched.
        """
        now = time.time()

        def _do(conn):
            conn.execute("DELETE FROM gateway_routing WHERE scope = ?", (scope,))
            if entries:
                conn.executemany(
                    "INSERT INTO gateway_routing (scope, session_key, entry_json, updated_at) "
                    "VALUES (?, ?, ?, ?)",
                    [(scope, k, v, now) for k, v in entries.items() if k and v],
                )

        self._execute_write(_do)

    def load_gateway_routing_entries(self, *, scope: str = "") -> Dict[str, str]:
        """Load routing entries for *scope* as {session_key: entry_json}."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT session_key, entry_json FROM gateway_routing WHERE scope = ?",
                (scope,),
            ).fetchall()
        return {r["session_key"]: r["entry_json"] for r in rows}

    def delete_gateway_routing_entries(
        self, session_keys: List[str], *, scope: str = ""
    ) -> None:
        """Remove routing entries for the given session keys in *scope*."""
        if not session_keys:
            return

        def _do(conn):
            conn.executemany(
                "DELETE FROM gateway_routing WHERE scope = ? AND session_key = ?",
                [(scope, k) for k in session_keys],
            )

        self._execute_write(_do)

    def list_gateway_sessions(
        self,
        *,
        platform: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """List gateway sessions (rows with a session_key) from state.db.

        Returns the newest row per session_key — the same shape consumers got
        from sessions.json: one live mapping per routing key.  ``platform``
        filters on ``source``; ``active_only`` restricts to sessions that
        have not ended.
        """
        # Full rows carry token/cost totals (MCP listings, /status) — drain
        # queued async accounting deltas so consumers see exact counters.
        self.flush_token_counts()
        query = f"""
            SELECT sessions.*,
                   COALESCE(sp.prompt, sessions.system_prompt)
                       AS _system_prompt_resolved,
                   {_sql_session_last_active("sessions")} AS last_active
            FROM sessions
            LEFT JOIN system_prompts sp
              ON sp.hash = sessions.system_prompt_hash
            WHERE session_key IS NOT NULL
              AND started_at = (
                  SELECT MAX(s2.started_at) FROM sessions s2
                  WHERE s2.session_key = sessions.session_key
              )
        """
        params: list = []
        if platform:
            query += " AND LOWER(source) = LOWER(?)"
            params.append(platform)
        if active_only:
            query += " AND ended_at IS NULL"
        query += " ORDER BY last_active DESC"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._session_row_dict(r) for r in rows]

    def find_session_by_origin(
        self,
        *,
        platform: str,
        chat_id: str,
        thread_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Optional[str]:
        """Find the most recent live session_id for a platform + chat origin.

        Equivalent of gateway/mirror's sessions.json scan: matches on
        source + chat_id (+ thread_id when provided).  When ``user_id`` is
        provided, exact sender matches are preferred; if multiple distinct
        users share the chat and none matches, returns None rather than
        contaminating another participant's session.
        """
        if not platform or chat_id in (None, ""):
            return None
        query = """
            SELECT id, user_id, started_at FROM sessions
            WHERE LOWER(source) = LOWER(?)
              AND session_key IS NOT NULL
              AND chat_id = ?
              AND ended_at IS NULL
        """
        params: list = [platform, str(chat_id)]
        if thread_id is not None:
            query += " AND COALESCE(thread_id, '') = ?"
            params.append(str(thread_id))
        query += " ORDER BY started_at DESC"
        with self._lock:
            rows = [dict(r) for r in self._conn.execute(query, params).fetchall()]
        if not rows:
            return None
        if user_id:
            exact = [r for r in rows if str(r.get("user_id") or "") == str(user_id)]
            if exact:
                return str(exact[0]["id"])
            if len(rows) > 1:
                return None
        elif len(rows) > 1:
            distinct_users = {
                str(r.get("user_id") or "").strip()
                for r in rows
                if str(r.get("user_id") or "").strip()
            }
            if len(distinct_users) > 1:
                return None
        return str(rows[0]["id"])

    def find_latest_gateway_session_for_peer(
        self,
        *,
        source: str,
        user_id: Optional[str] = None,
        session_key: Optional[str] = None,
        chat_id: Optional[str] = None,
        chat_type: Optional[str] = None,
        thread_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Find the latest recoverable gateway session for a routing peer.

        ``sessions.json`` is the fast routing index, but it can be missing or
        pruned after process-level restart bugs.  New gateway sessions persist
        the deterministic ``session_key`` on the durable session row so the
        mapping can be rebuilt exactly.  Rows ended only by older gateway
        cleanup's ``agent_close`` bug or a mistaken TUI ``ws_orphan_reap``
        (dashboard viewer disconnect before #60609) are treated as recoverable;
        explicit conversation boundaries such as /new, /resume switches, and
        compression splits are not.

        Ordering and emptiness (#82616): candidates are ranked by actual
        conversation recency (``last_activity_at``, falling back to
        ``started_at``) — ``started_at`` alone resurrected days-old zombie
        rows over the live conversation. Rows with messages are preferred,
        but an empty keyed row is still returned rather than ``None``:
        returning ``None`` mints a brand-new session id, which is a worse
        outcome than resuming an empty-but-correctly-keyed row (and "empty"
        may just mean the transcript lives under a compression child).

        Reset boundaries fence recovery (#68539): an intentional boundary
        such as ``session_reset`` (or any explicit non-recoverable
        end_reason) must block fallback to an *older* row for the same
        peer. Without the fence, the has-messages ranking above could reach
        behind a /new reset and silently restore the exact context the user
        reset. Each candidate is therefore rejected when a boundary row for
        the peer ended *after* the candidate's last activity — if the
        conversation's most recent event is an intentional reset, recovery
        returns nothing rather than reaching behind it.
        """
        if not session_key:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT s.*,
                       COALESCE(sp.prompt, s.system_prompt)
                           AS _system_prompt_resolved,
                       (COALESCE(s.message_count, 0) > 0 OR EXISTS (
                           SELECT 1 FROM messages WHERE messages.session_id = s.id LIMIT 1
                       )) AS _has_messages
                FROM sessions s
                LEFT JOIN system_prompts sp ON sp.hash = s.system_prompt_hash
                WHERE s.session_key = ?
                  AND s.source = ?
                  AND (s.ended_at IS NULL OR s.end_reason IN ('agent_close', 'ws_orphan_reap'))
                  AND NOT EXISTS (
                      SELECT 1 FROM sessions b
                      WHERE b.session_key = s.session_key
                        AND b.source = s.source
                        AND b.ended_at IS NOT NULL
                        AND b.end_reason IN ('session_reset', 'session_switch',
                                             'idle', 'daily', 'suspended',
                                             'resume_pending_expired')
                        AND b.ended_at
                            > COALESCE(s.last_activity_at, s.started_at)
                  )
                ORDER BY _has_messages DESC,
                         COALESCE(s.last_activity_at, s.started_at) DESC
                LIMIT 1
                """,
                (session_key, source),
            ).fetchone()
            if row is not None:
                return self._session_row_dict(row)

            # Conservative fallback for rows created by current code but with a
            # temporarily-missing exact key: still require the complete peer
            # tuple so we never cross chats/threads/users.
            if chat_id is None or chat_type is None:
                return None
            row = self._conn.execute(
                """
                SELECT s.*,
                       COALESCE(sp.prompt, s.system_prompt)
                           AS _system_prompt_resolved,
                       (COALESCE(s.message_count, 0) > 0 OR EXISTS (
                           SELECT 1 FROM messages WHERE messages.session_id = s.id LIMIT 1
                       )) AS _has_messages
                FROM sessions s
                LEFT JOIN system_prompts sp ON sp.hash = s.system_prompt_hash
                WHERE s.source = ?
                  AND COALESCE(s.user_id, '') = COALESCE(?, '')
                  AND COALESCE(s.chat_id, '') = COALESCE(?, '')
                  AND COALESCE(s.chat_type, '') = COALESCE(?, '')
                  AND COALESCE(s.thread_id, '') = COALESCE(?, '')
                  AND (s.ended_at IS NULL OR s.end_reason IN ('agent_close', 'ws_orphan_reap'))
                  AND (COALESCE(s.message_count, 0) > 0 OR EXISTS (
                      SELECT 1 FROM messages WHERE messages.session_id = s.id LIMIT 1
                  ))
                  AND NOT EXISTS (
                      SELECT 1 FROM sessions b
                      WHERE b.source = s.source
                        AND COALESCE(b.user_id, '') = COALESCE(s.user_id, '')
                        AND COALESCE(b.chat_id, '') = COALESCE(s.chat_id, '')
                        AND COALESCE(b.chat_type, '') = COALESCE(s.chat_type, '')
                        AND COALESCE(b.thread_id, '') = COALESCE(s.thread_id, '')
                        AND b.ended_at IS NOT NULL
                        AND b.end_reason IN ('session_reset', 'session_switch',
                                             'idle', 'daily', 'suspended',
                                             'resume_pending_expired')
                        AND b.ended_at
                            > COALESCE(s.last_activity_at, s.started_at)
                  )
                ORDER BY COALESCE(s.last_activity_at, s.started_at) DESC
                LIMIT 1
                """,
                (source, user_id, chat_id, chat_type, thread_id),
            ).fetchone()
        return self._session_row_dict(row) if row else None

    # ── Orphaned gateway-session repair (#82616) ──────────────────────────
    # A write-path failure (corrupt FTS, crash between routing publication
    # and row creation) can leave the live conversation in a session row
    # that never received its identity columns. Both queries above require
    # those columns, so the row holding the real transcript is invisible to
    # recovery: the chat resolves to the last keyed row instead — days older
    # — and the conversation time-travels. Hardening the write side cannot
    # reach a row that is *already* damaged; these two methods are the
    # offline repair path behind ``hermes sessions repair-routing``.

    # Widest plausible gap between a keyed predecessor going quiet and its
    # unkeyed successor being minted. The reported incident gap was ~60s;
    # 15 minutes stays generous without spanning unrelated conversations.
    _ORPHAN_ADOPTION_MAX_GAP_S = 900.0

    def find_orphaned_gateway_sessions(
        self, *, max_gap_s: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Report message-bearing session rows that lost their routing identity.

        A row is a candidate orphan when it has messages but no
        ``session_key``. It is only *adoptable* when exactly one keyed
        predecessor can be named as the conversation it continues:

        * ``lineage`` — ``parent_session_id`` points at a keyed row of the
          same source. That is a recorded fact, so no time window applies.
        * ``contiguity`` — exactly one keyed row of the same source (and
          compatible ``user_id``) fell quiet within *max_gap_s* of the
          orphan's start, and is older than the orphan's own last activity.

        Anything ambiguous is reported with ``adoptable=False`` and a reason
        rather than guessed at: mis-adopting would splice one person's
        conversation into another person's chat. Branch/delegate/tool rows
        are excluded outright — they are unkeyed by design, not by damage.
        """
        gap = (
            self._ORPHAN_ADOPTION_MAX_GAP_S
            if max_gap_s is None
            else float(max_gap_s)
        )
        orphan_active = _sql_session_last_active("o")
        donor_active = _sql_session_last_active("d")
        donor_columns = (
            "d.id, d.session_key, d.chat_id, d.chat_type, d.thread_id, "
            "d.user_id, d.origin_json, d.display_name, d.end_reason"
        )
        records: List[Dict[str, Any]] = []

        with self._lock:
            orphans = self._conn.execute(
                f"""
                SELECT o.id, o.source, o.user_id, o.started_at,
                       o.parent_session_id,
                       {orphan_active} AS last_active,
                       (SELECT COUNT(*) FROM messages m
                         WHERE m.session_id = o.id) AS message_count
                FROM sessions o
                WHERE o.session_key IS NULL
                  AND EXISTS (SELECT 1 FROM messages m
                               WHERE m.session_id = o.id)
                  AND COALESCE(o.source, '') != 'tool'
                  AND json_extract(COALESCE(o.model_config, '{{}}'),
                                   '$._branched_from') IS NULL
                  AND json_extract(COALESCE(o.model_config, '{{}}'),
                                   '$._delegate_from') IS NULL
                ORDER BY o.started_at ASC
                """
            ).fetchall()

            for orphan in orphans:
                donor = None
                evidence = ""
                reason = ""

                if orphan["parent_session_id"]:
                    evidence = "lineage"
                    donor = self._conn.execute(
                        f"""
                        SELECT {donor_columns}
                        FROM sessions d
                        WHERE d.id = ?
                          AND d.session_key IS NOT NULL
                          AND COALESCE(d.source, '') = COALESCE(?, '')
                        """,
                        (orphan["parent_session_id"], orphan["source"]),
                    ).fetchone()
                    if donor is None:
                        reason = (
                            "parent session carries no gateway identity of "
                            "this source"
                        )
                else:
                    evidence = "contiguity"
                    candidates = self._conn.execute(
                        f"""
                        SELECT {donor_columns}, {donor_active} AS last_active
                        FROM sessions d
                        WHERE d.session_key IS NOT NULL
                          AND d.id != ?
                          AND COALESCE(d.source, '') = COALESCE(?, '')
                          AND (COALESCE(d.user_id, '') = ''
                               OR COALESCE(?, '') = ''
                               OR d.user_id = ?)
                          AND {donor_active} BETWEEN ? AND ?
                          AND {donor_active} < ?
                        ORDER BY last_active DESC
                        LIMIT 2
                        """,
                        (
                            orphan["id"],
                            orphan["source"],
                            orphan["user_id"],
                            orphan["user_id"],
                            (orphan["started_at"] or 0) - gap,
                            (orphan["started_at"] or 0) + gap,
                            orphan["last_active"],
                        ),
                    ).fetchall()
                    if not candidates:
                        reason = (
                            f"no keyed predecessor fell quiet within {gap:.0f}s "
                            "of this session's start"
                        )
                    elif len(candidates) > 1:
                        reason = (
                            "ambiguous: more than one keyed predecessor "
                            "matches this window"
                        )
                    else:
                        donor = candidates[0]

                records.append(
                    {
                        "orphan_id": orphan["id"],
                        "source": orphan["source"],
                        "message_count": orphan["message_count"],
                        "started_at": orphan["started_at"],
                        "last_active": orphan["last_active"],
                        "donor_id": donor["id"] if donor else None,
                        "session_key": donor["session_key"] if donor else None,
                        "evidence": evidence if donor else "",
                        "adoptable": donor is not None,
                        "reason": reason,
                    }
                )

        # Two unkeyed successors claiming the same predecessor means at most
        # one of them continues that chat, and nothing here says which.
        contested = {
            r["donor_id"]
            for r in records
            if r["adoptable"]
            and sum(1 for x in records if x["donor_id"] == r["donor_id"]) > 1
        }
        for record in records:
            if record["donor_id"] in contested:
                record["adoptable"] = False
                record["reason"] = (
                    "ambiguous: more than one unkeyed session claims this "
                    "predecessor"
                )
        return records

    def adopt_orphaned_gateway_session(
        self, orphan_id: str, donor_id: str
    ) -> bool:
        """Stamp *orphan_id* with *donor_id*'s routing identity, retire *donor_id*.

        Re-verifies the pair inside the write transaction, so a concurrent
        gateway that healed either row in the meantime turns this into a
        no-op instead of a conflicting write. Existing non-NULL columns on
        the orphan are preserved. Returns True when the adoption applied.
        """
        if not orphan_id or not donor_id or orphan_id == donor_id:
            return False

        def _do(conn):
            donor = conn.execute(
                "SELECT session_key, chat_id, chat_type, thread_id, user_id, "
                "origin_json, display_name, source FROM sessions WHERE id = ?",
                (donor_id,),
            ).fetchone()
            orphan = conn.execute(
                "SELECT session_key, source FROM sessions WHERE id = ?",
                (orphan_id,),
            ).fetchone()
            if donor is None or orphan is None:
                return False
            if not donor["session_key"] or orphan["session_key"]:
                return False
            if (donor["source"] or "") != (orphan["source"] or ""):
                return False

            conn.execute(
                """UPDATE sessions
                      SET session_key = ?,
                          chat_id = COALESCE(chat_id, ?),
                          chat_type = COALESCE(chat_type, ?),
                          thread_id = COALESCE(thread_id, ?),
                          user_id = COALESCE(user_id, ?),
                          origin_json = COALESCE(origin_json, ?),
                          display_name = COALESCE(display_name, ?),
                          parent_session_id = COALESCE(parent_session_id, ?)
                    WHERE id = ? AND session_key IS NULL""",
                (
                    donor["session_key"],
                    donor["chat_id"],
                    donor["chat_type"],
                    donor["thread_id"],
                    donor["user_id"],
                    donor["origin_json"],
                    donor["display_name"],
                    donor_id,
                    orphan_id,
                ),
            )
            # Retire the predecessor under a reason recovery does NOT treat
            # as resumable — 'agent_close'/'ws_orphan_reap' would keep it in
            # the running, and the newly keyed orphan could lose the chat
            # again on the next restart.
            conn.execute(
                "UPDATE sessions SET ended_at = COALESCE(ended_at, ?), "
                "end_reason = 'superseded_by_repair' WHERE id = ?",
                (time.time(), donor_id),
            )
            return True

        return self._execute_write(_do)

    # Children that carry a ``parent_session_id`` but are NOT compression
    # continuations: branches, delegate/subagent runs, and tool sessions.
    # A marker only disqualifies a child when it points at the parent being
    # queried — compression continuations inherit the rotated agent's
    # ``model_config`` verbatim (``publish_compression_child`` callers pass
    # ``agent._session_init_model_config``), so a delegate subagent's
    # continuation carries ``_delegate_from=<the delegate's own parent>``.
    # Matching markers by mere presence misclassified those real
    # continuations as delegate children (fail-open for orphan reopen,
    # fail-closed for adoption). Bind the parent id for both markers.
    _NON_CONTINUATION_CHILD_FILTER_SQL = (
        "  AND COALESCE(json_extract(COALESCE({alias}model_config, '{{}}'),"
        " '$._branched_from'), '') != ?\n"
        "  AND COALESCE(json_extract(COALESCE({alias}model_config, '{{}}'),"
        " '$._delegate_from'), '') != ?\n"
        "  AND COALESCE({alias}source, '') != 'tool'\n"
    )

    def find_live_compression_child(
        self, parent_session_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the unique live direct child of a compression-ended session.

        A stale agent may observe that another compression path already rotated
        its parent. Recovery is safe only when the durable lineage identifies
        exactly one live direct continuation. Multiple children are treated as
        ambiguous and fail closed rather than guessing which transcript owns
        subsequent messages.
        """
        if not parent_session_id:
            return None
        with self._lock:
            parent = self._conn.execute(
                "SELECT ended_at, end_reason FROM sessions WHERE id = ?",
                (parent_session_id,),
            ).fetchone()
            if (
                parent is None
                or parent["ended_at"] is None
                or parent["end_reason"] != "compression"
            ):
                return None
            rows = self._conn.execute(
                """
                SELECT s.*,
                       COALESCE(sp.prompt, s.system_prompt)
                           AS _system_prompt_resolved
                FROM sessions s
                LEFT JOIN system_prompts sp ON sp.hash = s.system_prompt_hash
                WHERE s.parent_session_id = ?
                  AND s.ended_at IS NULL
                """
                + self._NON_CONTINUATION_CHILD_FILTER_SQL.format(alias="s.")
                + """
                ORDER BY s.started_at ASC
                LIMIT 2
                """,
                (parent_session_id, parent_session_id, parent_session_id),
            ).fetchall()
        return self._session_row_dict(rows[0]) if len(rows) == 1 else None

    def reopen_orphaned_compression_session(self, session_id: str) -> bool:
        """Reopen a compression parent only when no continuation was published.

        Compression publication is atomic in current builds, but older builds
        could leave a closed parent behind after an interrupted handoff.  This
        recovery is deliberately conservative: an active compression lease or
        any canonical child means the lineage is still owned by another path,
        so the caller must fail closed instead of reopening the parent.
        """
        if not session_id:
            return False

        def _do(conn):
            parent = conn.execute(
                "SELECT ended_at, end_reason FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
            if (
                parent is None
                or parent["ended_at"] is None
                or parent["end_reason"] != "compression"
            ):
                return False

            # Treat any direct non-branch/non-delegate/non-tool child as a
            # continuation, regardless of its current ended state. Reopening
            # in that case could create a second live head for one lineage.
            child = conn.execute(
                """
                SELECT 1
                FROM sessions
                WHERE parent_session_id = ?
                """
                + self._NON_CONTINUATION_CHILD_FILTER_SQL.format(alias="")
                + """
                LIMIT 1
                """,
                (session_id, session_id, session_id),
            ).fetchone()
            if child is not None:
                return False

            # refresh_compression_lock() deliberately lets an owner revive its
            # own expired row. Reclaim that row inside this write transaction
            # before reopening: refresh-first makes the lease active and aborts
            # recovery; recovery-first deletes the holder identity so a later
            # refresh cannot resurrect it.
            now = time.time()
            lock_row = conn.execute(
                "SELECT holder, expires_at FROM compression_locks "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if lock_row is not None:
                expires_at = lock_row["expires_at"]
                if expires_at is None or float(expires_at) >= now:
                    return False
                deleted = conn.execute(
                    "DELETE FROM compression_locks "
                    "WHERE session_id = ? AND holder = ? AND expires_at = ?",
                    (session_id, lock_row["holder"], expires_at),
                )
                if deleted.rowcount != 1:
                    return False

            updated = conn.execute(
                "UPDATE sessions SET ended_at = NULL, end_reason = NULL "
                "WHERE id = ? AND ended_at IS NOT NULL "
                "AND end_reason = 'compression'",
                (session_id,),
            )
            # rowcount==1 is guaranteed by the parent SELECT at the top of
            # this same BEGIN IMMEDIATE transaction. If this is ever edited
            # to return False past this point, note that the lease DELETE
            # above will still COMMIT (_execute_write commits unless _do
            # raises) — raise instead of returning False to roll back.
            return updated.rowcount == 1

        return bool(self._execute_write(_do))

    def publish_compression_child(
        self,
        *,
        parent_session_id: str,
        child_session_id: str,
        source: str,
        messages: List[Dict[str, Any]],
        model: str = None,
        model_config: Dict[str, Any] = None,
        system_prompt: str = None,
        cwd: str = None,
        profile_name: str = None,
        compression_lock_holder: str = None,
        require_compression_lease: bool = True,
    ) -> None:
        """Atomically close a parent and publish its durable compression child.

        The parent closure, child row, and compacted handoff become visible in
        one transaction. Readers can therefore observe either the live parent or
        a complete child, never an ended parent with a missing/empty child.
        """
        def _do(conn):
            lock_row = conn.execute(
                "SELECT holder, expires_at FROM compression_locks WHERE session_id = ?",
                (parent_session_id,),
            ).fetchone()
            if require_compression_lease and (
                lock_row is None
                or not compression_lock_holder
                or lock_row["holder"] != compression_lock_holder
                or float(lock_row["expires_at"]) <= time.time()
            ):
                raise CompressionSessionBusyError(
                    f"Compression lease lost before publication: {parent_session_id}"
                )
            parent = conn.execute(
                """SELECT ended_at, cwd, git_branch, git_repo_root,
                          user_id, session_key, chat_id, chat_type,
                          thread_id, display_name, origin_json, profile_name
                   FROM sessions WHERE id = ?""",
                (parent_session_id,),
            ).fetchone()
            if parent is None:
                raise RuntimeError(f"Compression parent not found: {parent_session_id}")
            if parent["ended_at"] is not None:
                raise RuntimeError(f"Compression parent already ended: {parent_session_id}")
            if not messages:
                raise RuntimeError("Compression child handoff must not be empty")
            system_prompt_hash = self._store_system_prompt(conn, system_prompt)

            conn.execute(
                """INSERT INTO sessions (
                   id, source, model, model_config, system_prompt,
                   system_prompt_hash,
                   parent_session_id, cwd, git_branch, git_repo_root,
                   profile_name, user_id, session_key, chat_id, chat_type,
                   thread_id, display_name, origin_json, started_at
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    child_session_id,
                    source,
                    model,
                    json.dumps(model_config) if model_config else None,
                    system_prompt_hash,
                    parent_session_id,
                    cwd or parent["cwd"],
                    parent["git_branch"],
                    parent["git_repo_root"],
                    # Same inheritance contract as _insert_session_row's
                    # compression-fork backfill (#59527 / cross-profile jump
                    # fix): the child stays on the parent's profile and keeps
                    # the gateway routing/origin columns so peer recovery
                    # still works after a crash at the boundary.
                    profile_name or parent["profile_name"],
                    parent["user_id"],
                    parent["session_key"],
                    parent["chat_id"],
                    parent["chat_type"],
                    parent["thread_id"],
                    parent["display_name"],
                    parent["origin_json"],
                    time.time(),
                ),
            )
            total_messages, total_tool_calls = self._insert_message_rows(
                conn, child_session_id, messages
            )
            conn.execute(
                "UPDATE sessions SET message_count = ?, tool_call_count = ? WHERE id = ?",
                (total_messages, total_tool_calls, child_session_id),
            )
            updated = conn.execute(
                "UPDATE sessions SET ended_at = ?, end_reason = 'compression' "
                "WHERE id = ? AND ended_at IS NULL",
                (time.time(), parent_session_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError(
                    f"Compression parent changed during publication: {parent_session_id}"
                )

        self._execute_write(_do)

    def end_session(self, session_id: str, end_reason: str) -> None:
        """Mark a session as ended.

        No-ops when the session is already ended. The first end_reason wins:
        compression-split sessions must keep their ``end_reason = 'compression'``
        record even if a later stale ``end_session()`` call (e.g. from a
        desynced CLI session_id after ``/resume`` or ``/branch``) targets them
        with a different reason. Use ``reopen_session()`` first if you
        intentionally need to re-end a closed session with a new reason.
        """
        def _do(conn):
            conn.execute(
                "UPDATE sessions SET ended_at = ?, end_reason = ? "
                "WHERE id = ? AND ended_at IS NULL",
                (time.time(), end_reason, session_id),
            )
        self._execute_write(_do)

    def reopen_session(self, session_id: str) -> None:
        """Clear ended_at/end_reason so a session can be resumed."""
        def _do(conn):
            conn.execute(
                "UPDATE sessions SET ended_at = NULL, end_reason = NULL WHERE id = ?",
                (session_id,),
            )
        self._execute_write(_do)

    def promote_to_session_reset(
        self, session_id: str, reason: str = "session_reset"
    ) -> bool:
        """Durably mark a session as ended by an intentional reset boundary.

        Promotes *only* live rows (``ended_at IS NULL``) or rows carrying an
        accidental end_reason that the recovery query
        (``find_latest_gateway_session_for_peer``) treats as recoverable:
        ``agent_close`` (older gateway cleanup bug) and ``ws_orphan_reap``
        (mistaken TUI reaper).  Explicit conversation boundaries such as
        ``compression``, ``session_reset``, ``session_switch``, etc. are
        preserved — the first writer wins for those, and a later expiry
        finalization must not silently overwrite them.

        Plain ``end_session()`` is NOT sufficient for reset boundaries: it
        no-ops on an already-ended row, so a row that agent cleanup already
        closed as ``agent_close`` would stay recoverable and stale-route
        recovery would resurrect the reset session with its full history
        (#61220, #61993, #63539).

        Keep this promotion set in sync with the recoverable set in
        ``find_latest_gateway_session_for_peer`` — any reason recovery would
        reopen must be promotable here.

        ``reason`` lets reset paths keep their auditable specific reasons
        (``idle``, ``daily``, ``suspended``, ``resume_pending_expired``).

        Returns ``True`` when the row was promoted, ``False`` when skipped
        (already has a different explicit end_reason, or row not found).
        """
        if not session_id:
            return False
        now = time.time()

        def _do(conn):
            cursor = conn.execute(
                "UPDATE sessions SET ended_at = ?, end_reason = ? "
                "WHERE id = ? AND (ended_at IS NULL "
                "OR end_reason IN ('agent_close', 'ws_orphan_reap'))",
                (now, reason, session_id),
            )
            return cursor.rowcount

        try:
            rows = self._execute_write(_do)
            return bool(rows)
        except Exception:
            return False

    def update_session_cwd(
        self,
        session_id: str,
        cwd: str,
        git_branch: str = None,
        git_repo_root: str = None,
        replace_git_meta: bool = False,
    ) -> None:
        """Persist the session working directory when a frontend knows it.

        ``git_branch`` records the git branch checked out in ``cwd`` at the time
        the session started/resumed. The sidebar groups main-checkout sessions
        by this so feature-branch work doesn't pile under a single "main" row
        (the main checkout's *current* branch is transient and would
        misattribute past sessions).

        ``git_repo_root`` records the git repo this cwd belongs to — the
        authoritative project key. Resolving it here, at the lowest level, means
        every surface reads the same membership instead of re-probing git in the
        GUI over a partial page. Each field is only written when non-empty so a
        probe failure never clobbers a previously-captured value.

        ``replace_git_meta`` inverts that non-empty rule: a deliberate workspace
        MOVE (re-homing a session into another project) must overwrite the old
        repo identity even when the new cwd resolves to none — keeping the stale
        root would leave the session grouped under the project it just left.
        """
        if not session_id or not cwd:
            return

        branch = (git_branch or "").strip()
        repo_root = (git_repo_root or "").strip()

        sets = ["cwd = ?"]
        params: List[Any] = [cwd]
        if branch or replace_git_meta:
            sets.append("git_branch = ?")
            params.append(branch or None)
        if repo_root or replace_git_meta:
            sets.append("git_repo_root = ?")
            params.append(repo_root or None)
        params.append(session_id)

        def _do(conn):
            conn.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE id = ?", params)

        self._execute_write(_do)

    def backfill_repo_roots(self, cwd_to_root: Dict[str, str]) -> None:
        """Persist resolved git repo roots for cwds that don't have one yet.

        Backfills history so projects light up for sessions created before the
        column existed, without clobbering an already-recorded root. Only
        non-empty roots are written (a non-git cwd stays NULL).
        """
        pairs = [(root, cwd) for cwd, root in cwd_to_root.items() if root and cwd]
        if not pairs:
            return

        def _do(conn):
            for root, cwd in pairs:
                conn.execute(
                    "UPDATE sessions SET git_repo_root = ? "
                    "WHERE cwd = ? AND COALESCE(git_repo_root, '') = ''",
                    (root, cwd),
                )

        self._execute_write(_do)

    def record_compression_failure_cooldown(
        self,
        session_id: str,
        cooldown_until: float,
        error: Optional[str] = None,
    ) -> None:
        """Persist the active compression-failure cooldown for a session."""
        if not session_id:
            return

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET compression_failure_cooldown_until = ?, "
                "compression_failure_error = ? WHERE id = ?",
                (cooldown_until, error, session_id),
            )

        try:
            self._execute_write(_do)
        except sqlite3.Error as exc:
            logger.warning(
                "record_compression_failure_cooldown(%s) failed: %s",
                session_id, exc,
            )

    def get_compression_failure_cooldown(
        self,
        session_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return the active compression-failure cooldown for ``session_id``."""
        if not session_id:
            return None
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT compression_failure_cooldown_until, compression_failure_error "
                "FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        cooldown_until = (
            row["compression_failure_cooldown_until"]
            if isinstance(row, sqlite3.Row)
            else row[0]
        )
        if cooldown_until is None:
            return None
        cooldown_until = float(cooldown_until)
        if cooldown_until <= now:
            return None
        error = (
            row["compression_failure_error"]
            if isinstance(row, sqlite3.Row)
            else row[1]
        )
        return {
            "cooldown_until": cooldown_until,
            "remaining_seconds": cooldown_until - now,
            "error": error,
        }

    def get_compression_failure_cooldown_row(
        self,
        session_id: str,
    ) -> Dict[str, Any]:
        """Return the exact stored cooldown columns without expiry filtering.

        Compression cancellation uses this under its session lease so rollback
        can preserve an expired row, a partially-null row, or an absent session
        exactly instead of converting those states through the active-cooldown
        API.
        """
        if not session_id:
            return {"session_exists": False, "cooldown_until": None, "error": None}
        with self._lock:
            row = self._conn.execute(
                "SELECT compression_failure_cooldown_until, compression_failure_error "
                "FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return {"session_exists": False, "cooldown_until": None, "error": None}
        cooldown_until = (
            row["compression_failure_cooldown_until"]
            if isinstance(row, sqlite3.Row)
            else row[0]
        )
        error = (
            row["compression_failure_error"]
            if isinstance(row, sqlite3.Row)
            else row[1]
        )
        return {
            "session_exists": True,
            "cooldown_until": (
                float(cooldown_until) if cooldown_until is not None else None
            ),
            "error": error,
        }

    def restore_compression_failure_cooldown_row(
        self,
        session_id: str,
        snapshot: Dict[str, Any],
    ) -> None:
        """Restore and verify an exact cooldown-row snapshot.

        Unlike the ordinary record/clear helpers, this transactional rollback
        API deliberately propagates write and verification failures. A caller
        must not report cancellation as mutation-free when compensation failed.
        """
        expected_exists = bool(snapshot.get("session_exists", False))
        if not expected_exists:
            actual = self.get_compression_failure_cooldown_row(session_id)
            if actual.get("session_exists", False):
                raise RuntimeError(
                    "cannot restore absent compression cooldown row: session now exists"
                )
            return

        deadline = snapshot.get("cooldown_until")
        error = snapshot.get("error")

        def _do(conn):
            cursor = conn.execute(
                "UPDATE sessions SET compression_failure_cooldown_until = ?, "
                "compression_failure_error = ? WHERE id = ?",
                (deadline, error, session_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"compression cooldown rollback session missing: {session_id}"
                )

        self._execute_write(_do)
        actual = self.get_compression_failure_cooldown_row(session_id)
        expected = {
            "session_exists": True,
            "cooldown_until": float(deadline) if deadline is not None else None,
            "error": error,
        }
        if actual != expected:
            raise RuntimeError(
                f"compression cooldown rollback verification failed: "
                f"expected={expected!r}, actual={actual!r}"
            )

    def clear_compression_failure_cooldown(self, session_id: str) -> None:
        """Clear any persisted compression-failure cooldown for a session."""
        if not session_id:
            return

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET compression_failure_cooldown_until = NULL, "
                "compression_failure_error = NULL WHERE id = ?",
                (session_id,),
            )

        try:
            self._execute_write(_do)
        except sqlite3.Error as exc:
            logger.warning(
                "clear_compression_failure_cooldown(%s) failed: %s",
                session_id, exc,
            )

    def get_compression_fallback_streak(self, session_id: str) -> int:
        """Return the persisted deterministic-fallback streak."""
        if not session_id:
            return 0
        with self._lock:
            conn = self._conn
            if conn is None:
                return 0
            row = conn.execute(
                "SELECT compression_fallback_streak FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return 0
        value = (
            row["compression_fallback_streak"]
            if isinstance(row, sqlite3.Row)
            else row[0]
        )
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def set_compression_fallback_streak(self, session_id: str, streak: int) -> None:
        """Persist the deterministic-fallback streak for one session."""
        if not session_id:
            return
        normalized = max(0, int(streak))

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET compression_fallback_streak = ? WHERE id = ?",
                (normalized, session_id),
            )

        self._execute_write(_do)

    def get_compression_ineffective_count(self, session_id: str) -> int:
        """Return the persisted ineffective-compaction strike count.

        Mirrors ``get_compression_fallback_streak``: this is the durable half
        of the anti-thrash guard (``_ineffective_compression_count`` on the
        built-in compressor), persisted so that a fresh compressor bound to a
        resumed session inherits an armed/tripped guard instead of starting
        from zero across process restarts (#54923).
        """
        if not session_id:
            return 0
        with self._lock:
            conn = self._conn
            if conn is None:
                return 0
            row = conn.execute(
                "SELECT compression_ineffective_count FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return 0
        value = (
            row["compression_ineffective_count"]
            if isinstance(row, sqlite3.Row)
            else row[0]
        )
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def set_compression_ineffective_count(self, session_id: str, count: int) -> None:
        """Persist the ineffective-compaction strike count for one session."""
        if not session_id:
            return
        normalized = max(0, int(count))

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET compression_ineffective_count = ? WHERE id = ?",
                (normalized, session_id),
            )

        self._execute_write(_do)

    # ──────────────────────────────────────────────────────────────────────
    # Compression locks
    # ──────────────────────────────────────────────────────────────────────
    # Atomic per-session locks that prevent two compression paths from
    # racing on the same session_id and producing orphan child sessions.
    #
    # The race: ``conversation_compression.py`` rotates ``agent.session_id``
    # as a side effect of a successful compression (end old session, create
    # new). That mutation is local to the AIAgent instance — but ``state.db``
    # is shared across all instances. Two AIAgents that share the same
    # ``session_id`` at the moment they both decide to compress (most
    # commonly the parent turn's agent + a background-review fork started
    # right after the turn ended) each end the parent and create their own
    # NEW session, parented to the same old id. The gateway SessionEntry
    # only catches one rotation; the other child silently accumulates
    # writes — Damien's "parent → two orphan children" repro shape.
    #
    # The lock is keyed by ``session_id`` and is held for the duration of
    # the compress() call plus the rotation. ``holder`` identifies the
    # current owner (pid:tid:nonce) for diagnostics; the lock is recovered
    # via ``expires_at`` if the holder process crashed without releasing.
    def refresh_compression_lock(
        self,
        session_id: str,
        holder: str,
        ttl_seconds: float = 300.0,
    ) -> bool:
        """Extend the compression lock lease if ``holder`` still owns it.

        Ownership is decided by the ``holder`` column alone, deliberately NOT
        by ``expires_at``: a live owner whose refresher thread was starved
        (GC pause, loaded CI runner, a slow write escaping ``_execute_write``'s
        retry budget) past its own TTL must be able to revive its still-unclaimed
        row on the next tick. Requiring ``expires_at >= now`` here made such a
        stall permanent — every later refresh matched 0 rows, so the owner kept
        compressing and rotating with no lease at all, which is exactly the
        unprotected window a competing path can fork the session lineage in.

        This does not resurrect a lock somebody else already took: SQLite
        serialises writes, so a reclaim (DELETE-expired + INSERT-or-IGNORE in
        :meth:`try_acquire_compression_lock`) and this UPDATE never interleave.
        Reclaim-first replaces ``holder``, so this UPDATE matches nothing and
        returns False; refresh-first pushes ``expires_at`` into the future, so
        the reclaimer's DELETE-expired matches nothing and its acquire fails.
        """
        if not session_id or not holder:
            return False
        now = time.time()
        expires_at = now + ttl_seconds

        def _do(conn):
            cur = conn.execute(
                "UPDATE compression_locks SET expires_at = ? "
                "WHERE session_id = ? AND holder = ?",
                (expires_at, session_id, holder),
            )
            return cur.rowcount > 0

        try:
            return bool(self._execute_write(_do))
        except sqlite3.Error as exc:
            logger.warning(
                "refresh_compression_lock(%s) failed: %s",
                session_id, exc,
            )
            return False

    def try_acquire_compression_lock(
        self,
        session_id: str,
        holder: str,
        ttl_seconds: float = 300.0,
    ) -> bool:
        """Try to atomically acquire the compression lock for ``session_id``.

        Returns ``True`` on success (caller now owns the lock and must
        release via :meth:`release_compression_lock`).  Returns ``False``
        if another holder already owns a non-expired lock — the caller
        MUST NOT proceed with compression in that case (its rotation would
        race against the holder's, splitting the session lineage).

        Expired locks (``expires_at < now``) are reclaimed transparently.
        Structured holders whose local ``pid=`` no longer exists are reclaimed
        immediately, so a gateway killed during compression does not stall the
        replacement process for the full lease TTL.

        Implementation: single-transaction DELETE-expired + INSERT-or-IGNORE,
        followed by a SELECT to confirm we got the row. SQLite serialises
        writes, so the whole sequence is atomic against other writers.
        """
        if not session_id:
            return False
        now = time.time()
        expires_at = now + ttl_seconds

        def _do(conn):
            reclaimed_holder = None
            row = conn.execute(
                "SELECT holder, expires_at FROM compression_locks "
                "WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is not None:
                current_holder = (
                    row["holder"] if isinstance(row, sqlite3.Row) else row[0]
                )
                current_expires_at = (
                    row["expires_at"] if isinstance(row, sqlite3.Row) else row[1]
                )
                if (
                    current_expires_at < now
                    or _compression_lock_holder_process_is_dead(current_holder)
                ):
                    conn.execute(
                        "DELETE FROM compression_locks "
                        "WHERE session_id = ? AND holder = ?",
                        (session_id, current_holder),
                    )
                    reclaimed_holder = current_holder
            # Then: try to insert. INSERT OR IGNORE returns no rowcount
            # difference — verify ownership via SELECT.
            conn.execute(
                "INSERT OR IGNORE INTO compression_locks "
                "(session_id, holder, acquired_at, expires_at) "
                "VALUES (?, ?, ?, ?)",
                (session_id, holder, now, expires_at),
            )
            row = conn.execute(
                "SELECT holder FROM compression_locks WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            acquired = row is not None and (
                row["holder"] if isinstance(row, sqlite3.Row) else row[0]
            ) == holder
            return acquired, reclaimed_holder

        try:
            acquired, reclaimed_holder = self._execute_write(_do)
            if reclaimed_holder:
                logger.warning(
                    "Reclaimed stale compression lock for session=%s "
                    "(holder=%s)",
                    session_id,
                    reclaimed_holder,
                )
            return bool(acquired)
        except sqlite3.Error as exc:
            logger.warning(
                "try_acquire_compression_lock(%s) failed: %s",
                session_id, exc,
            )
            # Fail open: returning False makes the caller skip compression,
            # which is the safe behaviour when the lock subsystem is broken.
            return False

    def release_compression_lock(self, session_id: str, holder: str) -> None:
        """Release the compression lock for ``session_id`` iff we own it.

        Idempotent: no-op when the lock has already expired and been
        reclaimed by a different holder, or when no lock exists. The
        ``holder`` check prevents a late-returning compressor from
        clobbering a fresh lock held by someone else.
        """
        if not session_id:
            return

        def _do(conn):
            conn.execute(
                "DELETE FROM compression_locks "
                "WHERE session_id = ? AND holder = ?",
                (session_id, holder),
            )

        try:
            self._execute_write(_do)
        except sqlite3.Error as exc:
            logger.warning(
                "release_compression_lock(%s) failed: %s",
                session_id, exc,
            )

    def get_compression_lock_holder(self, session_id: str) -> Optional[str]:
        """Return the current (non-expired) holder for ``session_id``, or None.

        Diagnostic helper — not used by the locking protocol itself.
        """
        if not session_id:
            return None
        now = time.time()
        row = self._conn.execute(
            "SELECT holder FROM compression_locks "
            "WHERE session_id = ? AND expires_at >= ?",
            (session_id, now),
        ).fetchone()
        if row is None:
            return None
        return row["holder"] if isinstance(row, sqlite3.Row) else row[0]

    def touch_session_activity(
        self,
        session_id: str,
        ts: Optional[float] = None,
        *,
        description: Optional[str] = None,
        provenance: Optional[ActivityProvenance] = None,
    ) -> None:
        """Stamp durable mid-turn session activity (observation-only).

        Called (rate-limited) from ``AIAgent._touch_activity`` so gateway/CLI
        surfaces and stall consumers observe API/tool/compaction activity
        even when no new message row has been written yet (#72016 / #72039).

        Never moves ``last_activity_at`` backwards. When the timestamp
        advances, bounded ``last_activity_description`` /
        ``last_activity_provenance`` are written with it. No-ops when
        ``session_id`` is empty or the row does not exist.
        """
        if not session_id:
            return
        from agent.session_activity import (
            bound_activity_description,
            normalize_activity_provenance,
        )

        when = float(ts if ts is not None else time.time())
        desc = bound_activity_description(description)
        prov = normalize_activity_provenance(provenance).value

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET "
                "last_activity_at = ?, "
                "last_activity_description = ?, "
                "last_activity_provenance = ? "
                "WHERE id = ? AND (last_activity_at IS NULL OR last_activity_at < ?)",
                (when, desc, prov, session_id, when),
            )

        # Observation-only write: never let it ride the full routine
        # write-patience budget (#76354 review S1). Under contention a
        # heartbeat that waits ~20s would delay the response-critical path
        # it is merely observing; give up after a sub-second budget instead
        # (the next due window retries naturally).
        self._execute_write(_do, patience_s=self._ACTIVITY_WRITE_PATIENCE_S)

    def clear_session_activity_labels(self, session_id: str) -> None:
        """Clear mid-turn activity labels after a turn ends.

        Keeps ``last_activity_at`` intact so idle / watchdog clocks stay
        continuous. Description and provenance are observation labels for
        *what was happening at* that timestamp during an active turn; once
        the turn is idle they must not keep advertising "compressing" /
        "executing tool" (#72039).

        Response-critical-path contract (#76354 review S1): runs in the
        turn's ``finally``; a no-op clear (labels already empty) skips the
        write transaction entirely, and a real clear uses the same short
        sub-second busy budget as :meth:`touch_session_activity` instead of
        the full routine write patience.
        """
        if not session_id:
            return
        from agent.session_activity import ActivityProvenance

        # No-op fast path: skip the transaction when there is nothing to
        # clear. Read-only, no write lock.
        try:
            row = self._conn.execute(
                "SELECT last_activity_description, last_activity_provenance "
                "FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        except sqlite3.Error:
            row = None
        if row is not None:
            desc = row[0] if not isinstance(row, sqlite3.Row) else row["last_activity_description"]
            prov = row[1] if not isinstance(row, sqlite3.Row) else row["last_activity_provenance"]
            if not desc and (
                not prov or prov == ActivityProvenance.UNKNOWN.value
            ):
                return

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET "
                "last_activity_description = ?, "
                "last_activity_provenance = ? "
                "WHERE id = ?",
                ("", ActivityProvenance.UNKNOWN.value, session_id),
            )

        self._execute_write(_do, patience_s=self._ACTIVITY_WRITE_PATIENCE_S)

    def get_session_activity(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return the durable activity snapshot for *session_id*, or None."""
        if not session_id:
            return None
        row = self.get_session(session_id)
        if not row:
            return None
        from agent.session_activity import build_activity_snapshot

        return build_activity_snapshot(
            last_activity_at=row.get("last_activity_at"),
            last_activity_description=row.get("last_activity_description"),
            last_activity_provenance=row.get("last_activity_provenance"),
        )

    def update_session_meta(
        self,
        session_id: str,
        model_config_json: str,
        model: Optional[str] = None,
    ) -> None:
        """Update model_config and optionally model for an existing session.

        Uses COALESCE so that passing model=None leaves the stored model
        column unchanged.  Routes through _execute_write for the standard
        BEGIN IMMEDIATE + jitter-retry + lock guarantee.
        """
        # Barrier against queued token deltas — see update_session_model.
        self.flush_token_counts()

        def _do(conn):
            conn.execute(
                "UPDATE sessions SET model_config = ?, model = COALESCE(?, model) WHERE id = ?",
                (model_config_json, model, session_id),
            )
        self._execute_write(_do)

    def update_system_prompt(
        self, session_id: str, system_prompt: Optional[str]
    ) -> None:
        """Store the full assembled system prompt snapshot."""
        def _do(conn):
            system_prompt_hash = self._store_system_prompt(conn, system_prompt)
            conn.execute(
                "UPDATE sessions "
                "SET system_prompt_hash = ?, system_prompt = NULL WHERE id = ?",
                (system_prompt_hash, session_id),
            )
            self._delete_unreferenced_system_prompts(conn)
        self._execute_write(_do)

    def set_memory_snapshot_sha(self, session_id: str, sha: str) -> None:
        """Persist the state-repo SHA captured at session start.

        Used by the session-start hook so replay can pin the exact memory
        view the LLM froze for that session.
        """
        def _do(conn):
            conn.execute(
                "UPDATE sessions SET memory_snapshot_sha = ? WHERE id = ?",
                (sha, session_id),
            )
        self._execute_write(_do)

    def get_memory_snapshot_sha(self, session_id: str) -> Optional[str]:
        """Return the memory-snapshot SHA recorded at session start, if any."""
        cursor = self._conn.execute(
            "SELECT memory_snapshot_sha FROM sessions WHERE id = ?",
            (session_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return row["memory_snapshot_sha"] if isinstance(row, sqlite3.Row) else row[0]

    def update_session_model(self, session_id: str, model: str) -> None:
        """Update the model for a session after a mid-session switch.

        Unlike ``update_token_counts`` which uses ``COALESCE(model, ?)``
        (only filling in NULL), this unconditionally sets the model column
        so that the dashboard reflects the user's latest /model choice.
        Also nulls ``system_prompt`` so stale ``Model:`` / ``Provider:``
        footer metadata is rebuilt on the next turn. A successful /model
        switch explicitly replaces any confirmed Browser runtime lock while
        preserving unrelated lineage markers in ``model_config``.
        """
        # This write bypasses the token queue, so deltas enqueued before the
        # switch must land first: a still-queued first delta carries the
        # pre-switch route, and applying it after this UPDATE would trip the
        # first_accounted_route overwrite in update_token_counts (row sees
        # api_call_count == 0 + a route mismatch) and resurrect the old
        # model/provider. Flushing here restores the pre-queue ordering.
        self.flush_token_counts()

        def _do(conn):
            conn.execute(
                """UPDATE sessions SET
                   model = ?,
                   model_config = CASE
                       WHEN model_config IS NULL THEN NULL
                       WHEN json_valid(model_config)
                           THEN json_remove(model_config, '$.browser_model_lock')
                       ELSE model_config
                   END,
                   system_prompt = NULL,
                   system_prompt_hash = NULL
                   WHERE id = ?""",
                (model, session_id),
            )
            self._delete_unreferenced_system_prompts(conn)
        self._execute_write(_do)

    def _merge_model_config_json(
        self,
        conn,
        session_id: str,
        patch: Dict[str, Any],
        *,
        on_missing: str = "skip",
    ):
        """SELECT + tolerant-parse + merge ``patch`` into a session's model_config.

        Shared by every model_config writer (``update_session_runtime_lock``,
        ``set_session_yolo``, ``archive_and_compact``,
        ``patch_session_model_config``) so the merge discipline that keeps
        lineage markers like ``_branched_from`` / ``_delegate_from`` alive
        lives in exactly one place. A ``None`` patch value deletes that key.
        Must run inside an open write transaction (callers own the UPDATE).

        Returns the serialized merged JSON — ``None`` when the merged dict is
        empty (matching ``create_session``'s NULL convention) — or the
        ``_MODEL_CONFIG_ROW_MISSING`` sentinel when the row doesn't exist and
        ``on_missing == "skip"``; ``on_missing == "raise"`` raises ValueError.
        """
        row = conn.execute(
            "SELECT model_config FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            if on_missing == "raise":
                raise ValueError(f"Session not found: {session_id}")
            return _MODEL_CONFIG_ROW_MISSING
        raw = row["model_config"] if isinstance(row, sqlite3.Row) else row[0]
        config: Dict[str, Any] = {}
        if isinstance(raw, str) and raw.strip():
=======
                        logger.debug("WAL checkpoint (PASSIVE) at close failed: %s", exc)
                conn, self._conn = self._conn, None
                self._close_connection_quietly(conn)
                # A clean close lets SQLite unlink the sidecars (a legitimate end of the
                # generation, not a split): a teardown-race reopen must re-adopt.
                self._db_sidecar_identity = {}

    def __del__(self) -> None:
        """Safety net: close() if the caller forgot. Attribute access stays
        guarded: module teardown order is undefined."""
        if self.__dict__.get("_conn") is not None:
>>>>>>> upstream/main
            try:
                self.close()
            except Exception:
                pass

    # ── Async token accounting (SessionUsageMixin) ──
    # queue_token_counts() is a deque append; a single-writer thread applies deltas in
    # order, coalescing consecutive deltas whose route fields are EQUAL (so the merged
    # UPDATE equals applying them sequentially). Exact readers call flush_token_counts().
    _TOKEN_DELTA_SUM_FIELDS = (
        "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens",
        "api_call_count",
    )
    _TOKEN_DELTA_COST_FIELDS = ("estimated_cost_usd", "actual_cost_usd")
    _TOKEN_DELTA_ROUTE_FIELDS = (
        "model", "cost_status", "cost_source", "pricing_version", "billing_provider", "billing_base_url",
        "billing_mode",
    )

    MAX_TITLE_LENGTH = 100

    # Title provenance, lowest to highest authority: auto-titling may only replace a
    # strictly lower-authority title (``derived`` -> ``llm`` once; never a user-typed name).
    TITLE_SOURCE_DERIVED, TITLE_SOURCE_LLM, TITLE_SOURCE_USER = "derived", "llm", "user"
    _TITLE_SOURCE_RANK = {TITLE_SOURCE_DERIVED: 0, TITLE_SOURCE_LLM: 1, TITLE_SOURCE_USER: 2}

    # Bot Mode's canonical chat is resolved by exact-title lookup: the title IS the identity,
    # so _set_session_title refuses renames of a hidden row holding it.
    # Bot Mode's forever-chat registry: the session titled exactly this, on a bot's profile, IS the bot's
    # canonical chat — resolved by exact-title lookup on every open (no session-id pointer exists). See
    # #92473.
    CANONICAL_BOT_CHAT_TITLE = "Bot Chat"

    # ── Message storage constants (SessionMessagesMixin) ──
    # Prefix marking JSON-encoded structured content; NUL cannot collide with text.
    _CONTENT_JSON_PREFIX = "\x00json:"
    #: Reactions live inside ``display_metadata`` so they survive row rewrites.
    REACTIONS_METADATA_KEY = "reactions"
    # Columns every conversation projection decodes; ``active`` rides along so a display read
    # can split compaction-archived rows without a second query.
    _CONVERSATION_ROW_COLUMNS = (
        "id, role, content, tool_call_id, tool_calls, tool_name, effect_disposition, "
        "finish_reason, reasoning, reasoning_content, reasoning_details, "
        "codex_reasoning_items, codex_message_items, platform_message_id, observed, "
        "_compressed_summary, timestamp, active, api_content, display_kind, display_metadata"
    )

    # ── Meta key/value (scheduler bookkeeping) ──

    def get_meta(self, key: str) -> Optional[str]:
        """Read state_meta[key] on self._lock (not _read_ctx): fts_rebuild_step reads progress before its
        write transaction and a WAL reader would not see it."""
        with self._lock:
            row = self._conn.execute("SELECT value FROM state_meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else row[0]

    def set_meta(self, key: str, value: str, *, cursor: Optional[sqlite3.Cursor] = None) -> None:
        """Upsert state_meta[key]; with ``cursor`` the write is inline (the caller already holds a
        transaction — nesting BEGIN IMMEDIATE would deadlock)."""
        sql = (
            "INSERT INTO state_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        if cursor is not None:
            cursor.execute(sql, (key, value))
        else:
            self._write_sql(sql, (key, value))

    def retag_kanban_worker_sessions(self, workspaces_root: str) -> int:
        """Retag legacy kanban worker rows from ``cli`` to ``kanban`` by cwd under the board's workspaces
        root; gated once per root via state_meta. Returns rows retagged."""
        prefix = str(workspaces_root).rstrip("/\\")
        if not prefix:
            return 0
        gate = f"kanban_worker_source_retagged:{prefix}"
        if self.get_meta(gate) == "1":
            return 0
        def _do(conn):
            cursor = conn.execute(
                "UPDATE sessions SET source = 'kanban' "
                "WHERE source = 'cli' AND (cwd = ? OR cwd LIKE ? ESCAPE '\\')",
                (prefix, _escape_like(prefix) + "/%"),
            )
            # rowcount BEFORE set_meta reuses this cursor for its INSERT.
            retagged = cursor.rowcount or 0
            self.set_meta(gate, "1", cursor=cursor)
            return retagged
        return self._execute_write(_do)

    def list_meta_prefix(self, prefix: str) -> List[Tuple[str, str]]:
        """``[(key, value), ...]`` for state_meta keys starting with the literal
        ``prefix`` (LIKE wildcards escaped) — e.g. ``loop:<session_id>`` rows."""
        if not prefix:
            return []
        rows = self._read_all(
            "SELECT key, value FROM state_meta WHERE key LIKE ? ESCAPE '\\'", (_escape_like(prefix) + "%",),
        )
        return [(row[0], row[1]) for row in rows]


class AsyncSessionDB:
    """Async door onto SessionDB: every call runs via asyncio.to_thread so a blocking SQLite call
    never freezes the event loop (no method returns a live cursor)."""

    def __init__(self, db: "SessionDB") -> None:
        self._db = db

    def __getattr__(self, name: str):
        attr = getattr(self._db, name)
        if not callable(attr):
            return attr
        async def _offloaded(*args, **kwargs):
            return await asyncio.to_thread(attr, *args, **kwargs)
        return _offloaded


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
from typing import Set  # noqa: F401,E402
import contextlib  # noqa: F401,E402
import errno  # noqa: F401,E402
import struct  # noqa: F401,E402
import weakref  # noqa: F401,E402

MAX_SAFE_EXPORT_MESSAGES = 20_000

MAX_SAFE_RESUME_MESSAGES = 20_000


_PLUGIN_COMPAT_LAZY = {
    'AUTO_VACUUM_MIN_FREELIST_RATIO': ('hermes_state_common', 'AUTO_VACUUM_MIN_FREELIST_RATIO'),
    'ActivityProvenance': ('agent.session_activity', 'ActivityProvenance'),
    'CompressionSessionBusyError': ('hermes_state_errors', 'CompressionSessionBusyError'),
    'CompressionSessionClosedError': ('hermes_state_errors', 'CompressionSessionClosedError'),
    'DEFERRED_INDEX_SQL': ('hermes_state_common', 'DEFERRED_INDEX_SQL'),
    'FTS_CJK_STALE_KEY': ('hermes_state_common', 'FTS_CJK_STALE_KEY'),
    'FTS_CJK_TABLE_SQL': ('hermes_state_fts', 'FTS_CJK_TABLE_SQL'),
    'FTS_CJK_TRIGGER_SQL': ('hermes_state_fts', 'FTS_CJK_TRIGGER_SQL'),
    'FTS_REBUILD_DEFERRAL_KEY': ('hermes_state_common', 'FTS_REBUILD_DEFERRAL_KEY'),
    'FTS_SQL': ('hermes_state_common', 'FTS_SQL'),
    'FTS_STALE_KEY': ('hermes_state_common', 'FTS_STALE_KEY'),
    'FTS_STORAGE_VERSION': ('hermes_state_common', 'FTS_STORAGE_VERSION'),
    'FTS_TRIGRAM_SQL': ('hermes_state_common', 'FTS_TRIGRAM_SQL'),
    'LEGACY_FTS_SQL': ('hermes_state_common', 'LEGACY_FTS_SQL'),
    'LEGACY_FTS_TRIGRAM_SQL': ('hermes_state_common', 'LEGACY_FTS_TRIGRAM_SQL'),
    'MAX_FTS5_QUERY_CHARS': ('hermes_state_common', 'MAX_FTS5_QUERY_CHARS'),
    'PERSISTENCE_ERROR_CAUSES': ('hermes_state_errors', 'PERSISTENCE_ERROR_CAUSES'),
    'SCHEMA_SQL': ('hermes_state_common', 'SCHEMA_SQL'),
    'SCHEMA_VERSION': ('hermes_state_common', 'SCHEMA_VERSION'),
    'SESSION_STATUS_COMPLETE': ('hermes_state_sessions', 'SESSION_STATUS_COMPLETE'),
    'SESSION_STATUS_EMPTY': ('hermes_state_sessions', 'SESSION_STATUS_EMPTY'),
    'SESSION_STATUS_ERROR': ('hermes_state_sessions', 'SESSION_STATUS_ERROR'),
    'SESSION_STATUS_INTERRUPTED': ('hermes_state_sessions', 'SESSION_STATUS_INTERRUPTED'),
    'SKILL_EXCERPT_JOINT': ('agent.skill_commands', 'SKILL_EXCERPT_JOINT'),
    'SKILL_SCAFFOLD_SQL_LIKE': ('agent.skill_commands', 'SKILL_SCAFFOLD_SQL_LIKE'),
    'SessionTurnLeaseLostError': ('hermes_state_errors', 'SessionTurnLeaseLostError'),
    'WalUnsupportedError': ('hermes_state_wal', 'WalUnsupportedError'),
    'apply_durability_barriers': ('hermes_state_repair', 'apply_durability_barriers'),
    'classify_session_status': ('hermes_state_sessions', 'classify_session_status'),
    'collect_state_db_stats': ('hermes_state_dbfile', 'collect_state_db_stats'),
    'count_db_holders': ('hermes_state_dbfile', 'count_db_holders'),
    'describe_skill_invocation': ('agent.skill_commands', 'describe_skill_invocation'),
    'fts5_cjk_so_path': ('hermes_state_fts', 'fts5_cjk_so_path'),
    'is_advisory_lock_contention': ('hermes_state_common', 'is_advisory_lock_contention'),
    'is_automatic_end_reason': ('hermes_state_common', 'is_automatic_end_reason'),
    'is_disk_full_error': ('hermes_state_errors', 'is_disk_full_error'),
    'is_sqlite_wal_reset_vulnerable': ('hermes_state_wal', 'is_sqlite_wal_reset_vulnerable'),
    'is_transient_sqlite_error': ('hermes_state_errors', 'is_transient_sqlite_error'),
    'iter_deleted_sqlite_sidecar_holders': ('hermes_state_dbfile', 'iter_deleted_sqlite_sidecar_holders'),
    'release_or_close': ('hermes_state_registry', 'release_or_close'),
    'report_startup_progress': ('hermes_startup_watchdog', 'report_startup_progress'),
    'resolve_journal_mode': ('hermes_state_wal', 'resolve_journal_mode'),
    'resolve_synchronous_level': ('hermes_state_wal', 'resolve_synchronous_level'),
    'sanitize_context': ('agent.memory_manager', 'sanitize_context'),
    'sqlite_source_id': ('hermes_state_wal', 'sqlite_source_id'),
    'workspace_key': ('hermes_state_sessions', 'workspace_key'),
}


def __getattr__(name):  # PEP 562 — lazy so no import cycles
    target = _PLUGIN_COMPAT_LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    from hermes_cli.plugin_compat import warn_once
    warn_once(__name__, name, *target)
    return getattr(importlib.import_module(target[0]), target[1])
# ---- END PLUGIN-COMPAT ----
