"""Gateway slash-command handlers for GatewayRunner: lifted out of ``gateway/run.py`` into a mixin
so ``self._handle_*_command`` keeps resolving via the MRO.  Cohesive clusters live in the sibling
mixins (``slash_commands_model/_session/_status/_goals``); this module keeps the shared helpers plus
the one-off commands.  run.py helpers are imported lazily."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import logging
import os
import re
import shlex
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from agent.i18n import t
from gateway.config import HomeChannel, Platform, PlatformConfig, persist_home_channel
from gateway.platforms.base import EphemeralReply
from gateway.platforms.event import MessageEvent
from gateway.session import AsyncSessionStore
from gateway.session_transcript import TranscriptReadError
from gateway.slash_commands_goals import GatewayGoalCommandsMixin
from gateway.slash_commands_model import GatewayModelCommandsMixin
from gateway.slash_commands_session import GatewaySessionCommandsMixin
from gateway.slash_commands_login import GatewayLoginCommandsMixin
from gateway.slash_commands_status import HISTORY_UNREADABLE, GatewayStatusCommandsMixin
from hermes_cli.config import atomic_config_write, cfg_get
from utils import atomic_json_write, is_truthy_value

logger = logging.getLogger("gateway.run")


# /rollback result keys -> i18n line for files the safe restore left alone.
_ROLLBACK_SKIP_LINES = (("skipped_user_edits", "gateway.rollback.kept_user_edits"),
                        ("skipped_oversize", "gateway.rollback.kept_oversize"),
                        ("failed_deletes", "gateway.rollback.failed_deletes"))

# /busy input modes -> (status-card behavior, set-confirmation behavior).
_BUSY_MODE_BEHAVIOR = {
    "queue": ("queues for next turn", "Messages will be queued for the next turn while Hermes is busy."),
    "steer": ("steers into current run (after next tool call)",
              "Messages will be steered into the current run (after the next tool call)."),
    "interrupt": ("interrupts current run", "Messages will interrupt the current run while Hermes is busy."),
}

# /diff argument -> diff mode (unknown args leave the mode unchanged).
_DIFF_MODE_BY_ARG = {**dict.fromkeys(("staged", "--staged", "cached", "--cached"), "staged"),
                     **dict.fromkeys(("all", "--all", "head"), "all"), "session": "session"}

# /voice subcommand -> stored mode (None = auto-TTS disabled), confirmation i18n key.
_VOICE_MODE_BY_ARG = {
    **dict.fromkeys(("on", "enable"), ("voice_only", "gateway.voice.enabled_voice_only")),
    **dict.fromkeys(("off", "disable"), ("off", "gateway.voice.disabled_text")),
    "tts": ("all", "gateway.voice.tts_enabled")}

# /footer argument -> new enabled state ("" toggles; anything else is a usage error).
_FOOTER_STATE_BY_ARG = {**dict.fromkeys(("on", "enable", "true", "1"), True),
                        **dict.fromkeys(("off", "disable", "false", "0"), False)}

# /approve modifier tokens -> approval choice (default "once").
_APPROVE_CHOICE_BY_ARG = {**dict.fromkeys(("always", "permanent", "permanently"), "always"),
                          **dict.fromkeys(("session", "ses"), "session")}

_PLATFORM_USAGE = ("Usage: /platform <list|pause|resume> [name]\n"
                   "  /platform list — show platform status\n"
                   "  /platform pause <name> — stop retrying a failing platform\n"
                   "  /platform resume <name> — re-queue a paused platform")

_WINDOWS_UPDATE_HELPER = """
import os, subprocess, sys
output_path, exit_code_path, cmd = sys.argv[1], sys.argv[2], sys.argv[3:]
env = dict(os.environ, PYTHONUNBUFFERED="1")
with open(output_path, "wb") as f:
    rc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=env).wait(timeout=3600)
with open(exit_code_path, "w", encoding="utf-8") as f:
    f.write(str(rc))
""".strip()


def _nested_dict(root: dict, *keys: str) -> dict:
    """Walk/create ``root[k1][k2]...`` as dicts, replacing any non-dict value on the path."""
    for k in keys:
        if not isinstance(root.get(k), dict):
            root[k] = {}
        root = root[k]
    return root


def _preview(text: str, limit: int = 60) -> str:
    return text[:limit] + ("..." if len(text) > limit else "")


def _execute(command: str, **ctx_kwargs):
    """Run *command* through the shared slash executor on the gateway surface."""
    from hermes_cli.slash_exec import CommandContext, execute_command
    return execute_command(command, CommandContext(surface="gateway", **ctx_kwargs))


def _restart_notify_payload(event: MessageEvent) -> dict:
    """Requester routing info so the new gateway process can notify them once back online.
    ``profile`` is persisted so the notice leaves through the requester's own profile bot after the
    restart (a bare platform lookup would resolve the default profile's adapter)."""
    source = event.source
    data = {"platform": source.platform.value if source.platform else None,
            "chat_id": source.chat_id, "chat_type": source.chat_type}
    if source.delivered_via_upstream_relay is True:
        data["delivered_via_upstream_relay"] = True
        data.update({k: getattr(source, k) for k in ("user_id", "scope_id") if getattr(source, k)})
    optional = (("thread_id", source.thread_id), ("message_id", event.message_id),
                ("profile", getattr(source, "profile", None)))
    data.update({k: v for k, v in optional if v})
    return data


def _spawn_detached_update(hermes_cmd, output_path, exit_code_path) -> None:
    """Spawn ``hermes update --gateway`` detached so it survives the gateway restart it may trigger.
    setsid is portable (works where ``systemd-run --user`` lacks a D-Bus session); ``--gateway``
    enables file-based IPC so interactive prompts are forwarded; PYTHONUNBUFFERED lets the gateway
    stream output live.  Windows has no setsid: an inline helper runs the updater as a module under
    this interpreter (not venv\\Scripts\\hermes.exe — that shim holds its own file open, and the
    update must replace it), redirects both outputs to one file and writes the exit code."""
    import shutil
    import subprocess
    if sys.platform == "win32":
        from hermes_cli._subprocess_compat import windows_detach_popen_kwargs
        subprocess.Popen(
            [sys.executable, "-c", _WINDOWS_UPDATE_HELPER, str(output_path), str(exit_code_path),
             sys.executable, "-m", "hermes_cli.main", "update", "--gateway"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **windows_detach_popen_kwargs())
        return
    hermes_cmd_str = " ".join(shlex.quote(part) for part in hermes_cmd)
    update_cmd = (
        f"PYTHONUNBUFFERED=1 {hermes_cmd_str} update --gateway"
        f" > {shlex.quote(str(output_path))} 2>&1; "
        # Avoid `status=$?`: `status` is read-only in zsh and this template is reused in
        # macOS/zsh operator wrappers, so keep it zsh-safe even though bash runs it here.
        f"rc=$?; printf '%s' \"$rc\" > {shlex.quote(str(exit_code_path))}")
    # Preferred: setsid creates a new session, fully detached; fallback start_new_session=True
    # calls os.setsid() in the child.
    setsid_bin = shutil.which("setsid")
    argv = [setsid_bin, "bash", "-c", update_cmd] if setsid_bin else ["bash", "-c", update_cmd]
    subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def _home_thread_from_source(source) -> Optional[str]:
    """The thread id /sethome should persist on the home target, or None.  Slack thread-per-message
    keying stamps a top-level message's own id as ``source.thread_id`` (a session key, not a
    location); persisting it would pin HOME to that ephemeral thread.  A thread id equal to the
    message's own id is synthetic and dropped; a real thread (id = parent's) is kept."""
    thread_id = getattr(source, "thread_id", None)
    if not thread_id:
        return None
    synthetic = (getattr(source, "platform", None) == Platform.SLACK and getattr(source, "message_id", None)
                 and str(thread_id) == str(source.message_id))
    return None if synthetic else str(thread_id)


class GatewaySlashCommandsMixin(
    GatewayLoginCommandsMixin,
    GatewayModelCommandsMixin,
    GatewaySessionCommandsMixin,
    GatewayStatusCommandsMixin,
    GatewayGoalCommandsMixin):
    """In-session slash-command handlers for GatewayRunner (plus the helpers the sibling mixins share)."""

    async_session_store: AsyncSessionStore

    # ------------------------------------------------------------------ shared helpers
    def _cached_agent_for(self, session_key: str, *, lockless_fallback: bool = False):
        """Peek the cached AIAgent for *session_key* without evicting it, or None. Entries are
        ``(agent, signature, ...)`` tuples (bare agents from test doubles accepted). Historical callers
        read the cache ONLY under ``_agent_cache_lock`` and got None when a fixture that skipped
        ``__init__`` had no lock; the manual codex ``/compress`` path was the one exception that read
        lock-free (``lockless_fallback=True``)."""
        cache = getattr(self, "_agent_cache", None)
        lock = getattr(self, "_agent_cache_lock", None)
        if cache is None or (lock is None and not lockless_fallback):
            return None
        try:
            if lock:
                with lock:
                    entry = cache.get(session_key)
            else:
                entry = cache.get(session_key)
        except Exception:
            return None
        return (entry[0] if entry else None) if isinstance(entry, (tuple, list)) else entry or None

    def _resident_agent_for(self, session_key: str):
        """The live running agent for *session_key*, else the cached one, else None. The pending
        sentinel (a run that is starting) never counts as a usable agent."""
        from gateway.run import _AGENT_PENDING_SENTINEL
        agent = self._running_agents.get(session_key)
        if agent is not None and agent is not _AGENT_PENDING_SENTINEL:
            return agent
        return self._cached_agent_for(session_key)

    @staticmethod
    def _session_db_unavailable_reply() -> str:
        from hermes_state import format_session_db_unavailable
        return format_session_db_unavailable(prefix=t("gateway.shared.session_db_unavailable_prefix"))

    def _reply_metadata(self, event: MessageEvent):
        """Thread/reply metadata for an outbound send anchored on *event*."""
        return self._thread_metadata_for_source(event.source, self._reply_anchor_for_event(event))

    def _adapter_and_key_for(self, event: MessageEvent):
        """``(adapter, session_key)`` for the event's source, either None when no source. The source's
        OWN transport (profile-aware, fail-closed) — ``self.adapters`` is the default profile's map."""
        if not event.source:
            return None, None
        return self._adapter_for_source(event.source), self._session_key_for_source(event.source)

    def _telegramized_command_reply(self, event: MessageEvent, text: str) -> str:
        from gateway.run import _telegramize_command_mentions
        return _telegramize_command_mentions(text, getattr(getattr(event, "source", None), "platform", None))

    def _checkpoint_manager(self):
        """A CheckpointManager from gateway config, or None when checkpoints are disabled."""
        from gateway.run import _checkpoint_agent_kwargs, _load_gateway_config
        from tools.checkpoint_manager import CheckpointManager
        cp = _checkpoint_agent_kwargs(_load_gateway_config())
        if not cp["checkpoints_enabled"]:
            return None
        # AIAgent kwargs are ``checkpoint_<field>``; CheckpointManager takes the bare field names.
        fields = {k[len("checkpoint_"):]: v for k, v in cp.items() if k.startswith("checkpoint_")}
        return CheckpointManager(enabled=True, **fields)

    def _write_approval_setter(self, section: str, event: MessageEvent):
        """``set_mode_fn`` for /memory and /skills: persist ``<section>.write_approval``. Raw read is
        correct for the write-back round-trip (merged defaults must not be persisted back to the
        user's file); the cached agent is dropped so the setting takes effect next message."""
        from gateway.run import _gateway_config_home
        # Persist to config (default) unless --session opted out, mirroring the text /model command path
        # above so a picked model survives across sessions like a typed one (#49066).
        from hermes_cli.config import read_user_config_raw
        config_path = _gateway_config_home() / "config.yaml"
        session_key = self._session_key_for_source(event.source)

        def _set_approval(enabled: bool):
            user_config = read_user_config_raw(config_path)
            user_config.setdefault(section, {})["write_approval"] = bool(enabled)
            atomic_config_write(config_path, user_config)
            # Evict any cached agent for this session so the next message rebuilds with the correct
            # session_id end-to-end — mirrors /branch and /reset. Without this, the cached AIAgent (and its
            # memory provider, which cached `_session_id` during initialize()) keeps writing into the wrong
            # session's record. See #6672.
            self._evict_cached_agent(session_key)
        return _set_approval

    async def _deliver_approval_confirmation(self, event: MessageEvent, confirmation_text: str, verb: str):
        """Return *confirmation_text* for normal delivery, or push it on native-streaming adapters
        (WeCom msgtype:"stream"), which need it sent directly with control-lane metadata (reliable
        proactive send, not the finalized reply stream). ``is not True``: mocks auto-create attrs."""
        source = event.source
        adapter = self._adapter_for_source(source)  # the receiving bot, not the default profile's
        if adapter:
            adapter.resume_typing_for_chat(source.chat_id)  # agent is about to continue
        if getattr(adapter, "SUPPORTS_NATIVE_STREAMING", False) is not True:
            return confirmation_text
        if adapter:
            try:
                await adapter.send(
                    source.chat_id, confirmation_text, reply_to=event.message_id,
                    metadata={"is_approval_prompt": True, "force_proactive_send": True})
            except Exception as exc:
                logger.warning("Failed to send /%s confirmation to %s: %s", verb, source.chat_id,
                               exc, exc_info=True)
        return None

    def _typed_command_prefix_for(self, platform) -> str:
        """The prefix users can always type to reach Hermes commands (adapter ``typed_command_prefix``,
        default "/"). Slack and Matrix use "!" because typed "/" is blocked/reserved there; their
        adapters rewrite "!command" to "/command"."""
        adapter = self.adapters.get(platform) if getattr(self, "adapters", None) else None
        return getattr(adapter, "typed_command_prefix", "/") if adapter is not None else "/"

<<<<<<< HEAD
    async def _handle_reset_command(self, event: MessageEvent) -> Union[str, EphemeralReply]:
        """Handle /new or /reset command."""
        source = event.source

        # Get existing session key
        session_key = self._session_key_for_source(source)
        self._invalidate_session_run_generation(session_key, reason="session_reset")
        # Evict the running-agent slot now that the generation is bumped. The
        # in-flight run's own guarded release (run_generation=old) will return
        # False and leave its dead agent behind; clearing here keeps the slot
        # from becoming a zombie that silently drops all later messages (#28686).
        # Idempotent, so the run's finally calling it again is harmless.
        self._release_running_agent_state(session_key)
=======
    def _terminal_cwd(self) -> str:
        from tools.terminal_scope import terminal_env
        return terminal_env("TERMINAL_CWD", str(Path.home()))
>>>>>>> upstream/main

    @staticmethod
    def _display_config_target(event: MessageEvent):
        """``(config.yaml path, platform config key)`` for the per-platform display settings."""
        from gateway.run import _gateway_config_home, _platform_config_key
        return _gateway_config_home() / "config.yaml", _platform_config_key(event.source.platform)

    async def _handle_profile_command(self, event: MessageEvent) -> str:
        """Handle /profile — show the profile serving this source and its home.  On a multiplexed
        gateway the process-level profile is the multiplexer's own ("default" in every chat), so
        with ``multiplex_profiles`` on report ``source.profile`` and resolve home under that
        profile's runtime scope; when off the stamp is ignored, mirroring ``_run_agent``."""
        from hermes_constants import display_hermes_home
        source = getattr(event, "source", None)
        profile_name = display = ""
        if getattr(getattr(self, "config", None), "multiplex_profiles", False):
            profile_name = (getattr(source, "profile", "") or "").strip()
            try:
                from gateway.run import _profile_runtime_scope
                with _profile_runtime_scope(self._resolve_profile_home_for_source(source)):
                    display = display_hermes_home()
            except Exception:
                display = display_hermes_home()

        # Shared executor resolves process-level fallbacks; the multiplexed per-source overrides
        # (when any) ride in via options.
        reply = _execute("profile", options={"profile_name": profile_name, "home_display": display})
        return "\n".join([t("gateway.profile.header", profile=reply.data["profile"]),
                          t("gateway.profile.home", home=reply.data["home"])])

    async def _handle_whoami_command(self, event: MessageEvent) -> str:
        """Handle /whoami — platform, DM-vs-group scope, tier and runnable commands (always allowed)."""
        from gateway.slash_access import policy_for_source
        source = event.source
        policy = policy_for_source(self.config, source)
        platform = source.platform.value if source and source.platform else "?"
        chat_type = ((source.chat_type if source else "") or "dm").lower()
        scope = "DM" if chat_type in {"dm", "direct", "private", ""} else "group/channel"
        user_id = (source.user_id if source else None) or "?"
        head = f"**You** — {platform} ({scope})\nUser ID: `{user_id}`\n"
        if not policy.enabled:
            return head + "Tier: unrestricted (no admin list configured for this scope)\nSlash commands: all available"
        if policy.is_admin(user_id):
            return head + "Tier: **admin**\nSlash commands: all available"
        # Non-admin: floor first (mirrors slash_access._ALWAYS_ALLOWED_FOR_USERS), then operator
        # additions, deduped in order.
        runnable = list(dict.fromkeys(["help", "whoami"] + sorted(policy.user_allowed_commands)))
        runnable_str = ", ".join(f"/{c}" for c in runnable) if runnable else "(none)"
        return head + f"Tier: user\nSlash commands you can run: {runnable_str}"

    async def _handle_kanban_command(self, event: MessageEvent) -> str:
        """Handle /kanban — delegate to the shared kanban CLI (DB work in a thread pool). Allowed
        while an agent runs: the board is profile-agnostic and never touches agent state."""
        from hermes_cli.kanban import run_slash

        # Strip the leading "/kanban" (with or without slash), leaving args.
        text = (event.text or "").strip().lstrip("/")
        if text.startswith("kanban"):
            text = text[len("kanban"):].lstrip()
        requested_board = action = None
        tokens = iter(shlex.split(text) if text else [])
        for tok in tokens:  # leading --board/--board=<b> options, then the action verb
            if tok == "--board":
                requested_board = next(tokens, requested_board)
            elif tok.startswith("--board="):
                requested_board = tok.split("=", 1)[1]
            else:
                action = tok
                break
        try:
            output = await asyncio.to_thread(run_slash, text)
        except Exception as exc:  # pragma: no cover - defensive
            return t("gateway.kanban.error_prefix", error=exc)

        # Auto-subscribe on create, parsing the task id from the CLI's standard success line
        # ("Created t_abcd  (ready, ...)"). With --json there is no such line, so a scripting user
        # gets no subscription and can call /kanban notify-subscribe explicitly.
        m = re.search(r"Created\s+(t_[0-9a-f]+)\b", output) if action == "create" and output else None
        if m:
            task_id = m.group(1)
            try:
                if await self._kanban_auto_subscribe(event, task_id, requested_board):
                    output = output.rstrip() + "\n" + t("gateway.kanban.subscribed_suffix", task_id=task_id)
            except Exception as exc:
                logger.warning("kanban create auto-subscribe failed: %s", exc)

        # Gateway messages have practical length caps; truncate long listings.
        if len(output) > 3800:
            output = output[:3800] + "\n" + t("gateway.kanban.truncated_suffix")
        return output or t("gateway.kanban.no_output")

    async def _kanban_auto_subscribe(self, event: MessageEvent, task_id: str, requested_board) -> bool:
        """Subscribe the event's chat to *task_id* notifications (notify+wake). False when the
        source has no platform/chat to route back to."""
        source = event.source

        def _field(name: str) -> Optional[str]:
            return str(getattr(source, name, "") or "") or None
        platform = getattr(source, "platform", None)
        platform_str = (platform.value if hasattr(platform, "value") else str(platform or "")).lower()
        chat_id, chat_type = _field("chat_id"), _field("chat_type")
        delivery_metadata = self._reply_metadata(event) or None
        if isinstance(delivery_metadata, dict) and chat_type:
            delivery_metadata.setdefault("chat_type", chat_type)
        if not (platform_str and chat_id):
            return False

        def _sub():
            from hermes_cli import kanban_db as _kb
            from hermes_cli import kanban_db_connect as _kbc
            from hermes_cli import kanban_db_notify as _kbn
            conn = _kbc.connect(board=requested_board)
            try:
                _kbn.add_notify_sub(
                    conn, task_id=task_id, platform=platform_str, chat_id=chat_id, chat_type=chat_type,
                    thread_id=_field("thread_id"), user_id=_field("user_id"),
                    # Also persist the stable alt id (Signal UUID, Feishu union_id): build_session_key
                    # keys the participant on ``user_id_alt or user_id``, so a replayed wake rebuilds
                    # the same session key only when the alt id survives the round-trip.
                    user_id_alt=_field("user_id_alt"),
                    notifier_profile=_field("profile") or getattr(self, "_kanban_notifier_profile", None) or self._active_profile_name(),
                    # Subscribing from chat: deliver the passive message and wake the destination agent.
                    delivery_mode="notify+wake", delivery_metadata=delivery_metadata)
            finally:
                conn.close()
        await asyncio.to_thread(_sub)
        return True

    async def _handle_stop_command(self, event: MessageEvent) -> Union[str, EphemeralReply]:
        """Handle /stop command - interrupt a running agent.  A truly hung agent (blocked thread
        never checking _interrupt_requested) is caught by the early intercept in _handle_message();
        this handler runs via normal dispatch or as a fallback, and force-cleans the session lock in
        all cases.  The session is preserved so the user can continue."""
        from gateway.run import _AGENT_PENDING_SENTINEL, _INTERRUPT_REASON_STOP
        source = event.source
        session_entry = await self.async_session_store.get_or_create_session(source)
        session_key = session_entry.session_key

        async def _stop(key: str, invalidation_reason: str) -> None:
            await self._interrupt_and_clear_session(
                key, source, interrupt_reason=_INTERRUPT_REASON_STOP,
                invalidation_reason=invalidation_reason)
        agent = self._running_agents.get(session_key)
        if agent is _AGENT_PENDING_SENTINEL:  # force-clean the sentinel so the session is unlocked
            await _stop(session_key, "stop_command_pending")
            logger.info("STOP (pending) for session %s — sentinel cleared", session_key)
            return EphemeralReply(t("gateway.stop.stopped_pending"))
        if agent:  # force-clean the session lock so a truly hung agent doesn't keep it forever
            await _stop(session_key, "stop_command_handler")
            return EphemeralReply(t("gateway.stop.stopped"))

        # No run under the caller's own key. In a per-user thread (thread_sessions_per_user=True) a
        # run another user started lives under a different key, yet authorized users must still be
        # able to /stop it: fall back to sibling runs in this thread, gated on authorization.
        sibling_keys = self._sibling_thread_run_keys(source, session_key)
        if sibling_keys and self._is_user_authorized_for_source(source):
            for sibling_key in sibling_keys:
                await _stop(sibling_key, "stop_command_thread_sibling")
            logger.info("STOP (thread sibling) by %s — interrupted %d run(s) in thread: %s",
                        session_key, len(sibling_keys), ", ".join(sibling_keys))
            return EphemeralReply(t("gateway.stop.stopped"))

        # No running agent anywhere for this scope. A platform status indicator can still be stuck —
        # e.g. Slack's persistent assistant.threads.setStatus survives a gateway restart or a turn
        # that died without a final send.
        # Best-effort clear so /stop always dismisses a phantom "is thinking...". See #32295.
        adapter = getattr(self, "adapters", {}).get(source.platform)
        try:
            if adapter and hasattr(adapter, "_stop_typing_with_metadata"):
                await adapter._stop_typing_with_metadata(source.chat_id, self._reply_metadata(event))
        except Exception:
            logger.debug("Failed to clear typing on /stop with no active agent", exc_info=True)
        return t("gateway.stop.no_active")

    async def _handle_platform_command(self, event: MessageEvent) -> str:
        """Handle ``/platform list|pause|resume [name]`` — inspect and manually control failed/paused
        adapters (pause stops the reconnect watcher; resume re-queues for retry)."""
        # Strip the leading "/platform" (or "/PLATFORM") token if present
        parts = (getattr(event, "content", "") or "").strip().split(maxsplit=2)
        if parts and parts[0].lower().lstrip("/").startswith("platform"):
            parts = parts[1:]
        action = (parts[0] if parts else "list").lower()
        target = parts[1].lower() if len(parts) > 1 else ""
        failed = getattr(self, "_failed_platforms", {}) or {}
        if action == "list":
            connected = ", ".join(sorted(p.value for p in self.adapters)) or "(none)"
            lines = ["**Gateway platforms**", f"Connected: {connected}"]
            for p, info in failed.items():
                if info.get("paused"):
                    reason = info.get("pause_reason") or "paused"
                    lines.append(f"  · {p.value} — PAUSED ({reason}). Resume with `/platform resume {p.value}`.")
                else:
                    lines.append(f"  · {p.value} — retrying (attempt {info.get('attempts', 0)})")
            return "\n".join(lines + ([] if failed else ["Failed/paused: (none)"]))
        if action not in {"pause", "resume"}:
            return _PLATFORM_USAGE
        if not target:
            return f"Usage: /platform {action} <name>"
        # Resolve platform name (case-insensitive, value match)
        platform = next((p for p in Platform.__members__.values() if p.value.lower() == target), None)
        if platform is None:
            return f"Unknown platform: {target}"
        name = platform.value
        queued = platform in failed
        paused = queued and bool(failed[platform].get("paused"))
        if action == "pause":
            if not queued:
                return f"{name} is not in the retry queue (it's either connected or not enabled)."
            if paused:
                return f"{name} is already paused."
            self._pause_failed_platform(platform, reason="paused via /platform pause")
            return f"✓ {name} paused. Resume with `/platform resume {name}` or `hermes gateway restart` to reset."
        if not queued:
            return f"{name} is not in the retry queue — nothing to resume."
        if not paused:
            return f"{name} is already retrying — no resume needed."
        self._resume_paused_platform(platform)
        return f"✓ {name} resumed — retrying on next watcher tick."

    async def _handle_restart_command(self, event: MessageEvent) -> Union[str, EphemeralReply]:
        """Handle /restart command - drain active work, then restart the gateway."""
        from gateway.run import _hermes_home
        # Idempotency check: if the previous gateway process recorded this same /restart (platform +
        # update_id) and we see it *again*, it's a redelivery from PTB's graceful-shutdown get_updates
        # ACK failing on the way out. Ignoring it prevents a loop where every fresh gateway re-restarts.
        if self._is_stale_restart_redelivery(event):
            src = event.source
            logger.info("Ignoring redelivered /restart (platform=%s, update_id=%s) — "
                        "already processed by a previous gateway instance.",
                        src.platform.value if src and src.platform else "?",
                        event.platform_update_id)
            return ""
        if self._restart_requested or self._draining:
            count = self._running_agent_count()
            return t("gateway.draining", count=count) if count else EphemeralReply(t("gateway.restart.in_progress"))

        async def _write_marker(name: str, build, label: str) -> None:
            try:
                await asyncio.to_thread(atomic_json_write, _hermes_home / name, build(), indent=None)
            except Exception as e:
                logger.debug("Failed to write restart %s: %s", label, e)

        def _notify_payload() -> dict:
            data = _restart_notify_payload(event)
            mid = str(event.message_id) if event.message_id is not None else event.source.message_id
            try:
                self._restart_command_source = dataclasses.replace(event.source, message_id=mid)
            except Exception:
                self._restart_command_source = event.source
            return data

        def _dedup_payload() -> dict:
            # Platform + update_id of the triggering /restart, for redelivery detection.
            data = {"platform": event.source.platform.value if event.source.platform else None,
                    "requested_at": time.time()}
            if event.platform_update_id is not None:
                data["update_id"] = event.platform_update_id
            return data

        # Save the requester's routing info so the new gateway process can notify them once back.
        await _write_marker(".restart_notify.json", _notify_payload, "notify file")
        # Record the triggering platform + update_id in a dedicated dedup marker. Unlike
        # .restart_notify.json (unlinked once the new gateway sends its notification) this persists
        # so a delayed Telegram redelivery is still detectable. Overwritten on every /restart.
        await _write_marker(".restart_last_processed.json", _dedup_payload, "dedup marker")
        active_agents = self._running_agent_count()
        # Under a service manager (systemd/launchd) or Docker/Podman, exit 75 so the supervisor /
        # restart policy restarts us — detached setsid+bash fails there (systemd KillMode=mixed kills
        # the cgroup; tini exits with the gateway). The explicit marker covers ``sudo env -i`` wrappers.
        from gateway.restart import is_container_restart_context, is_gateway_supervisor_process
        via_service = is_gateway_supervisor_process() or is_container_restart_context()
        self.request_restart(detached=not via_service, via_service=via_service)
        # Track sessions that were active at shutdown for stuck-loop detection (#7536). On each restart, the
        # counter increments for sessions that were running. If a session hits the threshold (3 consecutive
        # restarts while active), the next startup auto-suspends it — breaking the loop.
        if active_agents:
            return t("gateway.draining", count=active_agents)
        return EphemeralReply(t("gateway.restart.restarting"))

    async def _handle_version_command(self, event: MessageEvent) -> str:
        """Handle /version — show the running Hermes Agent version."""
        return _execute("version").text

    async def _handle_help_command(self, event: MessageEvent) -> str:
        """Handle /help command - list available commands."""
        return self._telegramized_command_reply(event, _execute("help").text)

    async def _handle_commands_command(self, event: MessageEvent) -> str:
        # Page size is a surface parameter (Telegram messages are shorter).
        page_size = 15 if event.source.platform == Platform.TELEGRAM else 20
<<<<<<< HEAD
        reply = execute_command(
            "commands",
            CommandContext(
                surface="gateway",
                args=event.get_command_args(),
                options={"page_size": page_size},
            ),
        )
        return _telegramize_command_mentions(
            reply.text,
            getattr(getattr(event, "source", None), "platform", None),
        )

    async def _handle_model_command(self, event: MessageEvent) -> Optional[str]:
        """Handle /model command — switch model.

        Supports:
          /model                              — interactive picker (Telegram/Discord) or text list
          /model <name>                       — switch model (this session only)
          /model <name> --once                — switch for the next turn only
          /model <name> --session             — switch for this session only (explicit)
          /model <name> --global              — switch and persist to config.yaml
          /model <name> --provider <provider> — switch provider + model
          /model --provider <provider>        — switch to provider, auto-detect model
        """
        from gateway.run import _hermes_home, _load_gateway_config
        from hermes_cli.model_switch import (
            switch_model as _switch_model, parse_model_switch_args,
            resolve_persist_behavior,
            list_authenticated_providers,
            list_picker_providers,
        )
        from hermes_cli.providers import get_label

        raw_args = event.get_command_args().strip()
        source = event.source
        _command_profile_home = None
        if getattr(getattr(self, "config", None), "multiplex_profiles", False):
            _command_profile_home = getattr(
                self, "_resolve_profile_home_for_source"
            )(source)

        # Parse --provider, --global, --session, --once, and --refresh flags
        # via the shared single-owner parser (hermes_cli.model_switch).
        request = parse_model_switch_args(raw_args)
        model_input = request.target
        explicit_provider = request.explicit_provider
        is_global_flag = request.is_global
        force_refresh = request.force_refresh
        is_session = request.is_session
        one_turn = request.is_once
        if request.errors:
            # Gateway decoration: "❌ " prefix over the canonical error copy.
            return f"❌ {request.error_messages()[0]}"
        persist_global = resolve_persist_behavior(
            is_global_flag,
            is_session,
            is_once=one_turn,
            explicit_provider=explicit_provider,
        )

        # --refresh: bust the disk cache so the picker shows live data.
        if force_refresh:
            try:
                from hermes_cli.models import clear_provider_models_cache
                clear_provider_models_cache()
            except Exception:
                pass

        # Read current model/provider from config
        current_model = ""
        current_provider = "openrouter"
        current_base_url = ""
        current_api_key = ""
        user_provs = None
        custom_provs = None
        excluded_provs = []
        config_path = (_command_profile_home or _hermes_home) / "config.yaml"
        try:
            cfg = _load_gateway_config()
            if cfg:
                model_cfg = cfg.get("model", {})
                if isinstance(model_cfg, dict):
                    current_model = model_cfg.get("default", "")
                    current_provider = model_cfg.get("provider", current_provider)
                    current_base_url = model_cfg.get("base_url", "")
                user_provs = cfg.get("providers")
                try:
                    from hermes_cli.config import get_compatible_custom_providers
                    custom_provs = get_compatible_custom_providers(cfg)
                except Exception:
                    custom_provs = cfg.get("custom_providers")
                _excl = cfg.get("model_catalog", {}).get("excluded_providers")
                if isinstance(_excl, list):
                    excluded_provs = _excl
        except Exception:
            pass

        # Check for session override. Normalize the source the same way a normal
        # message turn does
        # (Telegram DM topic recovery) before deriving the override key, so
        # the override is stored under the key the next message turn reads
        # (#30479).
        source = await asyncio.to_thread(self._normalize_source_for_session_key, source)
        session_key = self._session_key_for_source(source)
        override = self._session_model_overrides.get(session_key, {})
        restore_snapshot = (
            self._snapshot_session_model_override(session_key) if one_turn else None
        )
        if override:
            current_model = override.get("model", current_model)
            current_provider = override.get("provider", current_provider)
            current_base_url = override.get("base_url", current_base_url)
            current_api_key = override.get("api_key", current_api_key)

        # No args: show interactive picker (Telegram/Discord) or text list
        if not model_input and not explicit_provider:
            # Try interactive picker if the platform supports it
            adapter = getattr(self, "_adapter_for_source")(source)
            has_picker = (
                adapter is not None
                and getattr(type(adapter), "send_model_picker", None) is not None
            )

            if has_picker:
                try:
                    # Offload blocking provider-listing (can fall through to a
                    # synchronous urllib HTTP fetch on a stale cache) off the
                    # event loop so the gateway doesn't freeze. See #41289.
                    providers = await asyncio.to_thread(
                        list_picker_providers,
                        current_provider=current_provider,
                        current_base_url=current_base_url,
                        current_model=current_model,
                        user_providers=user_provs,
                        custom_providers=custom_provs,
                        max_models=50,
                        include_moa=True,
                        excluded_providers=excluded_provs,
                    )
                except Exception:
                    providers = []

                if providers:
                    # Build a callback closure for when the user picks a model.
                    # Captures self + locals needed for the switch logic.
                    _self = self
                    _session_key = session_key
                    _cur_model = current_model
                    _cur_provider = current_provider
                    _cur_base_url = current_base_url
                    _cur_api_key = current_api_key
                    _picker_profile_home = _command_profile_home

                    async def _on_model_selected_scoped(
                        _chat_id: str, model_id: str, provider_slug: str
                    ) -> str:
                        """Perform the model switch and return confirmation text."""
                        skew_error = _model_switch_skew_guard()
                        if skew_error:
                            return skew_error
                        # Offload the switch off the event loop — switch_model()
                        # can fall through to a synchronous models.dev HTTP fetch
                        # (requests.get, 15s timeout) on a cold/expired cache,
                        # which freezes the gateway otherwise. See #20525, #41289.
                        result = await asyncio.to_thread(
                            _switch_model,
                            raw_input=model_id,
                            current_provider=_cur_provider,
                            current_model=_cur_model,
                            current_base_url=_cur_base_url,
                            current_api_key=_cur_api_key,
                            is_global=persist_global,
                            explicit_provider=provider_slug,
                            user_providers=user_provs,
                            custom_providers=custom_provs,
                        )
                        if not result.success:
                            return t("gateway.model.error_prefix", error=result.error_message)

                        try:
                            from hermes_cli.context_switch_guard import (
                                enrich_model_switch_warnings_for_gateway,
                            )

                            # Offload: merge_preflight_compression_warning()
                            # calls the sync resolve_display_context_length()
                            # provider probe ladder — must not run on the loop.
                            await asyncio.to_thread(
                                enrich_model_switch_warnings_for_gateway,
                                result,
                                _self,
                                session_key=_session_key,
                                source=event.source,
                                custom_providers=custom_provs,
                                load_gateway_config=_load_gateway_config,
                            )
                        except Exception as exc:
                            logger.debug("preflight-compression switch warning failed: %s", exc)

                        # Update cached agent in-place
                        cached_entry = None
                        _cache_lock = getattr(_self, "_agent_cache_lock", None)
                        _cache = getattr(_self, "_agent_cache", None)
                        if _cache_lock and _cache is not None:
                            with _cache_lock:
                                cached_entry = _cache.get(_session_key)
                        if cached_entry and cached_entry[0] is not None:
                            try:
                                cached_entry[0].switch_model(
                                    new_model=result.new_model,
                                    new_provider=result.target_provider,
                                    api_key=result.api_key,
                                    base_url=result.base_url,
                                    api_mode=result.api_mode,
                                )
                            except Exception as exc:
                                # The in-place swap rolled the agent back to the
                                # OLD working model/client and re-raised.  Abort
                                # the rest of the commit: do NOT persist the
                                # failed model to the DB, do NOT set a session
                                # override pointing at the broken model, and do
                                # NOT evict the working cached agent.  Otherwise
                                # the next message rebuilds a dead agent from the
                                # broken override and the conversation is lost
                                # (#50163).  A failed switch must be a no-op.
                                logger.warning(
                                    "Picker model switch failed for cached agent: %s", exc
                                )
                                return t(
                                    "gateway.model.error_prefix",
                                    error=(
                                        f"Model switch to {result.new_model} failed ({exc}); "
                                        f"staying on {_cur_model}."
                                    ),
                                )

                        # Persist the new model to the session DB so the
                        # dashboard shows the updated model (#34850).
                        _sess_db = getattr(_self, "_session_db", None)
                        if _sess_db is not None:
                            try:
                                _sess_entry = await _self.async_session_store.get_or_create_session(
                                    event.source
                                )
                                await _sess_db.update_session_model(
                                    _sess_entry.session_id, result.new_model
                                )
                            except Exception as exc:
                                logger.debug(
                                    "Failed to persist model switch to DB: %s", exc
                                )

                        # Store model note + session override.  Use display
                        # form (strips opaque Palantir prefix) for the user-
                        # visible note; session-override map still gets the
                        # full opaque ID, which is what the wire needs.
                        from hermes_cli.model_switch import format_model_for_display
                        _display_cur = format_model_for_display(_cur_model)
                        _display_new = format_model_for_display(result.new_model)
                        if not hasattr(_self, "_pending_model_notes"):
                            _self._pending_model_notes = {}
                        _self._pending_model_notes[_session_key] = (
                            f"[Note: model was just switched from {_display_cur} to {_display_new} "
                            f"via {result.provider_label or result.target_provider}. "
                            f"Adjust your self-identification accordingly.]"
                        )
                        _self._session_model_overrides[_session_key] = {
                            "model": result.new_model,
                            "provider": result.target_provider,
                            "api_key": result.api_key,
                            "base_url": result.base_url,
                            "api_mode": result.api_mode,
                        }

                        # Write-through the non-secret parts to the session
                        # store so the picked model survives a gateway restart
                        # (api_key is never persisted).
                        try:
                            await _self.async_session_store.set_model_override(
                                _session_key,
                                _self._session_model_overrides[_session_key],
                            )
                        except Exception:
                            logger.debug(
                                "Failed to persist session model override",
                                exc_info=True,
                            )

                        # Evict cached agent so the next turn creates a fresh
                        # agent from the override rather than relying on the
                        # stale cache signature to trigger a rebuild.
                        _self._evict_cached_agent(_session_key)

                        # Persist to config (default) unless --session opted out,
                        # mirroring the text /model command path above so a picked
                        # model survives across sessions like a typed one (#49066).
                        if persist_global:
                            try:
                                # Write-back round-trip: raw read is correct
                                # (merged defaults must not be persisted).
                                from hermes_cli.config import read_user_config_raw
                                _persist_cfg = read_user_config_raw(config_path)
                                _raw_model = _persist_cfg.get("model")
                                if isinstance(_raw_model, dict):
                                    _persist_model_cfg = _raw_model
                                elif isinstance(_raw_model, str) and _raw_model.strip():
                                    _persist_model_cfg = {"default": _raw_model.strip()}
                                    _persist_cfg["model"] = _persist_model_cfg
                                else:
                                    _persist_model_cfg = {}
                                    _persist_cfg["model"] = _persist_model_cfg
                                try:
                                    from hermes_cli.route_identity import should_clear_context_pin_async

                                    if await should_clear_context_pin_async(
                                        _persist_model_cfg.get("default")
                                        or _persist_model_cfg.get("model"),
                                        result.new_model,
                                        _persist_model_cfg.get("base_url"),
                                        result.base_url,
                                        _persist_model_cfg.get("provider"),
                                        result.target_provider,
                                    ):
                                        _persist_model_cfg.pop("context_length", None)
                                except Exception:
                                    _persist_model_cfg.pop("context_length", None)
                                _persist_model_cfg["default"] = result.new_model
                                _persist_model_cfg["provider"] = result.target_provider
                                # Named providers always resolve base_url/api_mode fresh,
                                # so any leftover is cleared unconditionally below. Custom
                                # providers have no registry entry to re-derive from, so
                                # they need an explicit set-or-clear here — the previous
                                # lone `if result.base_url:` left a stale base_url behind
                                # when switching to a custom provider whose resolver
                                # returned an empty base_url (#25107).
                                _is_custom_target = str(result.target_provider or "").strip().lower() == "custom"
                                if result.base_url:
                                    _persist_model_cfg["base_url"] = result.base_url
                                elif _is_custom_target:
                                    _persist_model_cfg.pop("base_url", None)
                                if _is_custom_target:
                                    if result.api_mode:
                                        _persist_model_cfg["api_mode"] = result.api_mode
                                    else:
                                        _persist_model_cfg.pop("api_mode", None)
                                else:
                                    clear_model_endpoint_credentials(_persist_model_cfg, clear_base_url=True)
                                from hermes_cli.config import save_config
                                save_config(_persist_cfg)
                            except Exception as e:
                                logger.warning("Failed to persist model switch: %s", e)

                        # Build confirmation text.  Use display form so opaque
                        # Palantir IDs (ri.language-model-service..*) get
                        # shortened to their trailing slug for the UI.
                        plabel = result.provider_label or result.target_provider
                        lines = [t("gateway.model.switched", model=format_model_for_display(result.new_model))]
                        lines.append(t("gateway.model.provider_label", provider=plabel))
                        mi = result.model_info
                        from hermes_cli.model_switch import resolve_display_context_length_async
                        _sw_config_ctx = None
                        _sw_model_cfg = {}
                        try:
                            _sw_cfg = _load_gateway_config()
                            _sw_model_cfg = _sw_cfg.get("model", {})
                            if isinstance(_sw_model_cfg, dict):
                                _sw_raw = _sw_model_cfg.get("context_length")
                                if _sw_raw is not None:
                                    _sw_config_ctx = int(_sw_raw)
                        except Exception:
                            pass
                        if not isinstance(_sw_model_cfg, dict):
                            _sw_model_cfg = {}
                        ctx = await resolve_display_context_length_async(
                            result.new_model,
                            result.target_provider,
                            base_url=result.base_url or current_base_url or "",
                            api_key=result.api_key or current_api_key or "",
                            model_info=mi,
                            custom_providers=custom_provs,
                            config_context_length=_sw_config_ctx,
                            configured_model=(
                                _sw_model_cfg.get("default")
                                or _sw_model_cfg.get("model")
                            ),
                            configured_provider=_sw_model_cfg.get("provider"),
                            configured_base_url=_sw_model_cfg.get("base_url"),
                        )
                        if ctx:
                            lines.append(t("gateway.model.context_label", tokens=f"{ctx:,}"))
                        if mi:
                            if mi.max_output:
                                lines.append(t("gateway.model.max_output_label", tokens=f"{mi.max_output:,}"))
                            lines.append(t("gateway.model.capabilities_label", capabilities=mi.format_capabilities()))
                        if result.warning_message:
                            lines.append(t("gateway.model.warning_prefix", warning=result.warning_message))
                        if persist_global:
                            lines.append(t("gateway.model.saved_global"))
                        else:
                            lines.append(t("gateway.model.session_only_hint"))
                        return "\n".join(lines)

                    async def _on_model_selected(
                        _chat_id: str, model_id: str, provider_slug: str
                    ) -> str:
                        if _picker_profile_home is None:
                            return await _on_model_selected_scoped(
                                _chat_id, model_id, provider_slug
                            )
                        from gateway.run import _profile_runtime_scope

                        with _profile_runtime_scope(_picker_profile_home):
                            return await _on_model_selected_scoped(
                                _chat_id, model_id, provider_slug
                            )

                    metadata = self._thread_metadata_for_source(source, self._reply_anchor_for_event(event))
                    result = await adapter.send_model_picker(
                        chat_id=source.chat_id,
                        providers=providers,
                        current_model=current_model,
                        current_provider=current_provider,
                        session_key=session_key,
                        on_model_selected=_on_model_selected,
                        metadata=metadata,
                    )
                    if result.success:
                        return None  # Picker sent — adapter handles the response

            # Fallback: text list (for platforms without picker or if picker failed)
            provider_label = get_label(current_provider)
            lines = [t("gateway.model.current_label", model=current_model or "unknown", provider=provider_label), ""]

            try:
                # Offload blocking provider-listing off the event loop so the
                # gateway doesn't freeze on a stale-cache HTTP fetch. See #41289.
                providers = await asyncio.to_thread(
                    list_authenticated_providers,
                    current_provider=current_provider,
                    current_base_url=current_base_url,
                    current_model=current_model,
                    user_providers=user_provs,
                    custom_providers=custom_provs,
                    max_models=5,
                    excluded_providers=excluded_provs,
                )
                for p in providers:
                    tag = t("gateway.model.current_tag") if p["is_current"] else ""
                    lines.append(f"**{p['name']}** `--provider {p['slug']}`{tag}:")
                    if p["models"]:
                        model_strs = ", ".join(f"`{m}`" for m in p["models"])
                        extra = t("gateway.model.more_models_suffix", count=p["total_models"] - len(p["models"])) if p["total_models"] > len(p["models"]) else ""
                        lines.append(f"  {model_strs}{extra}")
                    elif p.get("api_url"):
                        lines.append(f"  `{p['api_url']}`")
                    lines.append("")
            except Exception:
                pass

            lines.append(t("gateway.model.usage_switch_model"))
            lines.append(t("gateway.model.usage_switch_provider"))
            lines.append(t("gateway.model.usage_persist"))
            return "\n".join(lines)

        # Perform the switch
        skew_error = _model_switch_skew_guard()
        if skew_error:
            return skew_error
        # Offload the switch off the event loop — switch_model() can fall
        # through to a synchronous models.dev HTTP fetch (requests.get, 15s
        # timeout) on a cold/expired cache, which freezes the gateway
        # otherwise. See #20525, #41289.
        result = await asyncio.to_thread(
            _switch_model,
            raw_input=model_input,
            current_provider=current_provider,
            current_model=current_model,
            current_base_url=current_base_url,
            current_api_key=current_api_key,
            is_global=persist_global,
            explicit_provider=explicit_provider,
            user_providers=user_provs,
            custom_providers=custom_provs,
        )

        if not result.success:
            return t("gateway.model.error_prefix", error=result.error_message)

        try:
            from hermes_cli.context_switch_guard import (
                enrich_model_switch_warnings_for_gateway,
            )

            # Offload: merge_preflight_compression_warning() calls the sync
            # resolve_display_context_length() provider probe ladder — must
            # not run on the loop.
            await asyncio.to_thread(
                enrich_model_switch_warnings_for_gateway,
                result,
                self,
                session_key=session_key,
                source=source,
                custom_providers=custom_provs,
                load_gateway_config=_load_gateway_config,
            )
        except Exception as exc:
            logger.debug("preflight-compression switch warning failed: %s", exc)

        async def _finish_switch() -> str:
            """Apply the resolved switch (agent, session, config) and build the reply."""
            # If there's a cached agent, update it in-place
            cached_entry = None
            _cache_lock = getattr(self, "_agent_cache_lock", None)
            _cache = getattr(self, "_agent_cache", None)
            if _cache_lock and _cache is not None:
                with _cache_lock:
                    cached_entry = _cache.get(session_key)

            if cached_entry and cached_entry[0] is not None:
                try:
                    cached_entry[0].switch_model(
                        new_model=result.new_model,
                        new_provider=result.target_provider,
                        api_key=result.api_key,
                        base_url=result.base_url,
                        api_mode=result.api_mode,
                    )
                except Exception as exc:
                    # In-place swap rolled the agent back to the OLD working
                    # model/client and re-raised.  Abort the commit: skip DB
                    # persist, session override, cache eviction, and config
                    # write so a failed switch is a no-op rather than a dead
                    # conversation (#50163).  Without this early return the
                    # next message rebuilds a broken agent from the override.
                    logger.warning("In-place model switch failed for cached agent: %s", exc)
                    return t(
                        "gateway.model.error_prefix",
                        error=(
                            f"Model switch to {result.new_model} failed ({exc}); "
                            f"staying on {current_model}."
                        ),
                    )

            # Persist the new model to the session DB so the dashboard
            # shows the updated model (#34850).
            _sess_db = getattr(self, "_session_db", None)
            if _sess_db is not None:
                try:
                    _sess_entry = await self.async_session_store.get_or_create_session(source)
                    # If this session was auto-reset, consume the flag so the
                    # next regular message's cleanup does not wipe the model
                    # override just stored below (Closes #48031).
                    if getattr(_sess_entry, "was_auto_reset", False):
                        _sess_entry.was_auto_reset = False
                    await _sess_db.update_session_model(
                        _sess_entry.session_id, result.new_model
                    )
                except Exception as exc:
                    logger.debug(
                        "Failed to persist model switch to DB: %s", exc
                    )

            # Store a note to prepend to the next user message so the model
            # knows about the switch (avoids system messages mid-history).
            # Display form strips opaque Palantir RID prefixes; the override
            # map below keeps the full ID for the wire.
            from hermes_cli.model_switch import format_model_for_display
            if not hasattr(self, "_pending_model_notes"):
                self._pending_model_notes = {}
            self._pending_model_notes[session_key] = (
                f"[Note: model was just switched from {format_model_for_display(current_model)} to {format_model_for_display(result.new_model)} "
                f"via {result.provider_label or result.target_provider}. "
                f"{'This override applies to the next turn only. ' if one_turn else ''}"
                f"Adjust your self-identification accordingly.]"
            )

            # Store session override so next agent creation uses the new model
            self._session_model_overrides[session_key] = {
                "model": result.new_model,
                "provider": result.target_provider,
                "api_key": result.api_key,
                "base_url": result.base_url,
                "api_mode": result.api_mode,
            }
            if one_turn:
                if not hasattr(self, "_pending_one_turn_model_restores"):
                    self._pending_one_turn_model_restores = {}
                self._pending_one_turn_model_restores[session_key] = (
                    restore_snapshot or {"had_override": False, "override": None}
                )
            elif hasattr(self, "_pending_one_turn_model_restores"):
                self._pending_one_turn_model_restores.pop(session_key, None)

            # Write-through the non-secret parts (model/provider/base_url) to
            # the session store so the override survives a gateway restart.
            # api_key/api_mode are never persisted — they are re-resolved via
            # runtime provider resolution on rehydration.
            #
            # /model --once is intentionally EXCLUDED from the write-through:
            # a one-turn override must never survive a restart. The persisted
            # value stays at the pre-once state (the prior session override,
            # or nothing), which is exactly what the finally-restore reverts
            # the in-memory dict to. (#29923 review defect: the original
            # implementation wrote through, so a crash before the restore
            # rehydrated the once-model permanently.)
            if not one_turn:
                try:
                    await self.async_session_store.set_model_override(
                        session_key,
                        self._session_model_overrides[session_key],
                    )
                except Exception:
                    logger.debug(
                        "Failed to persist session model override", exc_info=True
                    )

            # Evict cached agent so the next turn creates a fresh agent from the
            # override rather than relying on cache signature mismatch detection.
            self._evict_cached_agent(session_key)

            # Persist to config (default) unless --session opted out
            if persist_global:
                try:
                    # Write-back round-trip: raw read is correct (merged
                    # defaults must not be persisted back to the user's file).
                    from hermes_cli.config import read_user_config_raw
                    cfg = read_user_config_raw(config_path)
                    # Coerce scalar/None ``model:`` into a dict before mutation —
                    # otherwise ``cfg.setdefault("model", {})`` returns the existing
                    # scalar and the next assignment raises
                    # ``TypeError: 'str' object does not support item assignment``.
                    # Reproduces when ``config.yaml`` has ``model: <name>`` (flat
                    # string) instead of the proper nested ``model: {default: ...}``.
                    raw_model = cfg.get("model")
                    if isinstance(raw_model, dict):
                        model_cfg = raw_model
                    elif isinstance(raw_model, str) and raw_model.strip():
                        model_cfg = {"default": raw_model.strip()}
                        cfg["model"] = model_cfg
                    else:
                        model_cfg = {}
                        cfg["model"] = model_cfg
                    try:
                        from hermes_cli.route_identity import should_clear_context_pin_async

                        if await should_clear_context_pin_async(
                            model_cfg.get("default") or model_cfg.get("model"),
                            result.new_model,
                            model_cfg.get("base_url"),
                            result.base_url,
                            model_cfg.get("provider"),
                            result.target_provider,
                        ):
                            model_cfg.pop("context_length", None)
                    except Exception:
                        model_cfg.pop("context_length", None)
                    model_cfg["default"] = result.new_model
                    model_cfg["provider"] = result.target_provider
                    # See the picker handler above for why custom providers need an
                    # explicit set-or-clear instead of the old lone truthy check (#25107).
                    _is_custom_target = str(result.target_provider or "").strip().lower() == "custom"
                    if result.base_url:
                        model_cfg["base_url"] = result.base_url
                    elif _is_custom_target:
                        model_cfg.pop("base_url", None)
                    if _is_custom_target:
                        if result.api_mode:
                            model_cfg["api_mode"] = result.api_mode
                        else:
                            model_cfg.pop("api_mode", None)
                    else:
                        clear_model_endpoint_credentials(model_cfg, clear_base_url=True)
                    from hermes_cli.config import save_config
                    save_config(cfg)
                except Exception as e:
                    logger.warning("Failed to persist model switch: %s", e)

            # Build confirmation message with full metadata
            provider_label = result.provider_label or result.target_provider
            lines = [t("gateway.model.switched", model=format_model_for_display(result.new_model))]
            lines.append(t("gateway.model.provider_label", provider=provider_label))

            # Context: always resolve via the provider-aware chain so Codex OAuth,
            # Copilot, and Nous-enforced caps win over the raw models.dev entry.
            mi = result.model_info
            from hermes_cli.model_switch import resolve_display_context_length_async
            _sw2_config_ctx = None
            _sw2_model_cfg = {}
            try:
                _sw2_cfg = _load_gateway_config()
                _sw2_model_cfg = _sw2_cfg.get("model", {})
                if isinstance(_sw2_model_cfg, dict):
                    _sw2_raw = _sw2_model_cfg.get("context_length")
                    if _sw2_raw is not None:
                        _sw2_config_ctx = int(_sw2_raw)
            except Exception:
                pass
            if not isinstance(_sw2_model_cfg, dict):
                _sw2_model_cfg = {}
            ctx = await resolve_display_context_length_async(
                result.new_model,
                result.target_provider,
                base_url=result.base_url or current_base_url or "",
                api_key=result.api_key or current_api_key or "",
                model_info=mi,
                custom_providers=custom_provs,
                config_context_length=_sw2_config_ctx,
                configured_model=(
                    _sw2_model_cfg.get("default")
                    or _sw2_model_cfg.get("model")
                ),
                configured_provider=_sw2_model_cfg.get("provider"),
                configured_base_url=_sw2_model_cfg.get("base_url"),
            )
            if ctx:
                lines.append(t("gateway.model.context_label", tokens=f"{ctx:,}"))
            if mi:
                if mi.max_output:
                    lines.append(t("gateway.model.max_output_label", tokens=f"{mi.max_output:,}"))
                lines.append(t("gateway.model.capabilities_label", capabilities=mi.format_capabilities()))

            # Cache notice
            cache_enabled = (
                (base_url_host_matches(result.base_url or "", "openrouter.ai") and "claude" in result.new_model.lower())
                or result.api_mode == "anthropic_messages"
            )
            if cache_enabled:
                lines.append(t("gateway.model.prompt_caching_enabled"))

            if result.warning_message:
                lines.append(t("gateway.model.warning_prefix", warning=result.warning_message))

            if persist_global:
                lines.append(t("gateway.model.saved_global"))
            elif one_turn:
                lines.append("    (next turn only — restores after one response)")
            else:
                lines.append(t("gateway.model.session_only_hint"))

            return "\n".join(lines)

        # Expensive-model confirmation gate (typed /model <name> path).
        # The pickers (Telegram/Discord inline keyboards, TUI, dashboard)
        # already confirm via their own UI affordances; this covers the
        # direct text command, which previously bypassed the guard.
        # expensive_model_warning() may hit models.dev or a /models endpoint
        # on a cache miss, so run it off the event loop.
        _cost_warning = None
        try:
            from hermes_cli.model_cost_guard import expensive_model_warning

            _cost_warning = await asyncio.to_thread(
                expensive_model_warning,
                result.new_model,
                provider=result.target_provider,
                base_url=result.base_url or current_base_url or "",
                api_key=result.api_key or current_api_key or "",
                model_info=result.model_info,
            )
        except Exception:
            _cost_warning = None
        if _cost_warning is not None:
            async def _on_cost_confirm(choice: str) -> str:
                if choice == "cancel":
                    return (
                        f"🟡 Model switch cancelled. Current model unchanged "
                        f"({current_model or 'unknown'})."
                    )
                # "once" and "always" both proceed — there is no persistent
                # opt-out for the cost guard (each expensive switch should be
                # an explicit decision).
                return await _finish_switch()

            _p = self._typed_command_prefix_for(event.source.platform)
            return await self._request_slash_confirm(
                event=event,
                command="model",
                title="Expensive Model Warning",
                message=(
                    f"⚠️ **Expensive Model Warning**\n\n{_cost_warning.message}\n\n"
                    f"_Text fallback: reply `{_p}approve` to switch or `{_p}cancel` to keep "
                    "the current model._"
                ),
                handler=_on_cost_confirm,
            )

        return await _finish_switch()

    async def _handle_codex_runtime_command(self, event: MessageEvent) -> str:
        """Handle /codex-runtime command in the gateway.

        Same surface as the CLI handler in cli.py:
            /codex-runtime                  — show current state
            /codex-runtime auto             — Hermes default runtime
            /codex-runtime codex_app_server — codex subprocess runtime
            /codex-runtime on / off         — synonyms

        On change, the cached agent for this session is evicted so the next
        message creates a fresh AIAgent with the new api_mode wired in
        (avoids prompt-cache invalidation mid-session)."""
        from hermes_cli import codex_runtime_switch as crs

        raw_args = event.get_command_args().strip() if event else ""
        new_value, errors = crs.parse_args(raw_args)
        if errors:
            return "❌ " + "\n❌ ".join(errors)

        # Load + persist via the same helpers used for /model and /yolo
        try:
            from hermes_cli.config import load_config, save_config
        except Exception as exc:
            return f"❌ Could not load config: {exc}"
        cfg = load_config()

        result = crs.apply(
            cfg,
            new_value,
            persist_callback=(save_config if new_value is not None else None),
        )

        # On a real change, evict the cached agent so the new runtime takes
        # effect on the next message rather than waiting for cache TTL.
        if result.success and new_value is not None and result.requires_new_session:
            try:
                session_key = self._session_key_for_source(event.source)
                self._evict_cached_agent(session_key)
            except Exception:
                logger.debug("could not evict cached agent after codex-runtime change",
                             exc_info=True)

        prefix = "✓" if result.success else "✗"
        return f"{prefix} {result.message}"

    async def _handle_personality_command(self, event: MessageEvent) -> str:
        """Handle /personality command - list or set a personality.

        All resolution/persistence goes through hermes_cli.personality —
        the single owner of personality state on every surface.
        """
        from gateway.run import _load_gateway_config
        from hermes_cli.personality import (
            active_personality_name,
            available_personalities,
            describe_personality,
            persist_personality,
            prompt_text,
            resolve_personality,
        )

        args = event.get_command_args().strip()

        try:
            config = _load_gateway_config()
        except Exception:
            config = {}
        personalities = available_personalities(config)

        if not args:
            current = active_personality_name(config)
            lines = [t("gateway.personality.header")]
            lines.append(t("gateway.personality.none_option"))
            for name, prompt in personalities.items():
                marker = " ✓" if name == current else ""
                lines.append(
                    t(
                        "gateway.personality.item",
                        name=f"{name}{marker}",
                        preview=describe_personality(prompt),
                    )
                )
            lines.append(t("gateway.personality.usage"))
            return "\n".join(lines)

        try:
            name, new_prompt = resolve_personality(args, config)
        except ValueError:
            available = "`none`, " + ", ".join(f"`{n}`" for n in personalities)
            return t("gateway.personality.unknown", name=args.lower(), available=available)

        # Persist the selection only — hermes_cli.personality never writes
        # agent.system_prompt (user-owned manual overlay).
        if not persist_personality(name):
            return t("gateway.personality.save_failed", error="config write failed")

        if not name:
            self._ephemeral_system_prompt = prompt_text(
                cfg_get(config, "agent", "system_prompt", default="")
            )
            return t("gateway.personality.cleared")

        # Update in-memory so it takes effect on the very next message.
        self._ephemeral_system_prompt = new_prompt
        return t("gateway.personality.set_to", name=name)

    async def _handle_retry_command(self, event: MessageEvent) -> str:
        """Handle /retry command - re-send the last user message."""
        source = event.source
        session_entry = await self.async_session_store.get_or_create_session(source)
        history = await self.async_session_store.load_transcript(session_entry.session_id)
        
        # Find the last *real* user message. Timeline bookkeeping rows carry
        # role=user + display_kind (model_switch / async_delegation_complete /
        # auto_continue / hidden); clients never count them as user turns.
        # Without this filter /retry rewrote the transcript around a marker
        # and re-sent opaque bookkeeping text (same class as the TUI ordinal).
        last_user_msg = None
        last_user_idx = None
        # is_user_originated_turn: excludes display_kind bookkeeping AND
        # compaction handoffs (durable role=user, sometimes without
        # display_kind on legacy sessions; #80622) — /retry must never
        # re-send a reference-only summary as if the user asked it.
        from agent.context_compressor import is_user_originated_turn

        for i in range(len(history) - 1, -1, -1):
            msg = history[i]
            if is_user_originated_turn(msg):
                last_user_msg = msg.get("content", "")
                last_user_idx = i
                break

        if not last_user_msg:
            return t("gateway.retry.no_previous")
        
        # Truncate history to before the last user message and persist only the
        # live view. After in-place compaction the pre-compaction transcript
        # lives on as active=0/compacted=1 rows under this same session id, and
        # a bare rewrite (active_only=False) would DELETE them (same class as
        # #61145). /retry never intends to purge archived history, so avoid a
        # separate existence probe: it could fail open or race with the write.
        truncated = history[:last_user_idx]
        await self.async_session_store.rewrite_transcript(
            session_entry.session_id, truncated, active_only=True
        )
        # Reset stored token count — transcript was truncated
        session_entry.last_prompt_tokens = 0

        # Re-send by creating a fake text event with the old message
        retry_event = MessageEvent(
            text=last_user_msg,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=event.raw_message,
            channel_prompt=event.channel_prompt,
        )

        # Let the normal message handler process it
        return await self._handle_message(retry_event)

    async def _handle_goal_command(self, event: "MessageEvent") -> str:
        """Handle /goal for gateway platforms.

        Subcommands: ``/goal`` / ``/goal status`` / ``/goal pause`` /
        ``/goal resume`` / ``/goal clear``. Any other text becomes the
        new goal.

        Setting a new goal queues the goal text as the next turn so the
        agent starts working on it immediately — the post-turn
        continuation hook then takes over from there.
        """
        args = (event.get_command_args() or "").strip()
        lower = args.lower()

        mgr, session_entry = await self._get_goal_manager_for_event(event)
        if mgr is None:
            return t("gateway.goal.unavailable")

        if not args or lower == "status":
            return mgr.status_line()

        # /goal show → print the active goal's completion contract
        if lower == "show":
            return f"{mgr.status_line()}\n{mgr.render_contract()}"

        if lower == "pause":
            state = mgr.pause(reason="user-paused")
            if state is None:
                return t("gateway.goal.no_goal_set")
            try:
                adapter = self.adapters.get(event.source.platform) if event.source else None
                _quick_key = self._session_key_for_source(event.source) if event.source else None
                if adapter and _quick_key:
                    self._clear_goal_pending_continuations(_quick_key, adapter)
            except Exception as exc:
                logger.debug("goal pause: pending continuation cleanup failed: %s", exc)
            return t("gateway.goal.paused", goal=state.goal)

        if lower == "resume":
            state = mgr.resume()
            if state is None:
                return t("gateway.goal.no_resume")
            return t("gateway.goal.resumed", goal=state.goal)

        if lower in {"clear", "stop", "done"}:
            had = mgr.has_goal()
            mgr.clear()
            try:
                adapter = self.adapters.get(event.source.platform) if event.source else None
                _quick_key = self._session_key_for_source(event.source) if event.source else None
                if adapter and _quick_key:
                    self._clear_goal_pending_continuations(_quick_key, adapter)
            except Exception as exc:
                logger.debug("goal clear: pending continuation cleanup failed: %s", exc)
            return t("gateway.goal_cleared") if had else t("gateway.no_active_goal")

        # /goal wait <pid> [reason] — park the loop on a background process.
        if lower == "wait" or lower.startswith("wait "):
            wait_arg = args[len("wait"):].strip()
            if not wait_arg:
                return "Usage: /goal wait <pid> [reason]"
            wtokens = wait_arg.split(None, 1)
            try:
                pid = int(wtokens[0])
            except ValueError:
                return "/goal wait: <pid> must be an integer process id."
            reason = wtokens[1].strip() if len(wtokens) > 1 else ""
            try:
                mgr.wait_on(pid, reason=reason)
            except (RuntimeError, ValueError) as exc:
                return f"/goal wait: {exc}"
            rtxt = f" ({reason})" if reason else ""
            return f"⏳ Goal parked on pid {pid}{rtxt}. Loop pauses until it exits."

        # /goal unwait — clear the wait barrier.
        if lower == "unwait":
            if mgr.stop_waiting():
                return "▶ Wait barrier cleared — goal loop resumes."
            return "No wait barrier set."

        # /goal gate ... — manage deterministic quality gates.
        if lower == "gate" or lower.startswith("gate "):
            gate_arg = args[len("gate"):].strip()
            gate_lower = gate_arg.lower()
            if not gate_arg or gate_lower == "list":
                return mgr.render_gates()
            if gate_lower.startswith("add "):
                command = gate_arg[len("add"):].strip()
                try:
                    gate = mgr.add_gate(command)
                except (RuntimeError, ValueError) as exc:
                    return f"/goal gate add: {exc}"
                return (
                    f"⚿ Gate added: $ {gate.command} "
                    f"({gate.max_retries} retries, {gate.timeout_seconds}s timeout). "
                    f"It must pass before the goal can complete."
                )
            if gate_lower.startswith("remove ") or gate_lower.startswith("rm "):
                idx_text = gate_arg.split(None, 1)[1].strip()
                try:
                    removed = mgr.remove_gate(int(idx_text))
                except (RuntimeError, ValueError, IndexError) as exc:
                    return f"/goal gate remove: {exc}"
                return f"✓ Gate removed: $ {removed}"
            if gate_lower == "clear":
                try:
                    prev = mgr.clear_gates()
                except RuntimeError as exc:
                    return f"/goal gate clear: {exc}"
                return f"✓ Cleared {prev} gate{'s' if prev != 1 else ''}."
            return "Usage: /goal gate [list | add <command> | remove <N> | clear]"

        # /goal draft <objective> → draft a structured completion contract,
        # then set it. The aux LLM call is sync; run it off the event loop.
        draft_contract_obj = None
        if lower.startswith("draft"):
            objective = args[len("draft"):].strip()
            if not objective:
                return "Usage: /goal draft <objective in plain language>"
            try:
                import asyncio
                from hermes_cli.goals import draft_contract

                draft_contract_obj = await asyncio.get_running_loop().run_in_executor(
                    None, draft_contract, objective
                )
            except Exception as exc:
                logger.debug("goal draft failed: %s", exc)
                draft_contract_obj = None
            args = objective  # the goal text is the objective
            contract = draft_contract_obj
        else:
            # Inline `field: value` lines parse into a completion contract;
            # the remaining prose is the goal headline. Plain free-form goals
            # (no such lines) behave exactly as before.
            from hermes_cli.goals import parse_contract

            headline, parsed = parse_contract(args)
            args = headline or args
            contract = parsed if not parsed.is_empty() else None

        # Otherwise — treat the remaining text as the new goal.
        try:
            state = mgr.set(args, contract=contract)
        except ValueError as exc:
            return t("gateway.goal.invalid", error=str(exc))

        # Queue the goal text as an immediate first turn so the agent
        # starts making progress. The post-turn hook takes over after.
        adapter = self.adapters.get(event.source.platform) if event.source else None
        _quick_key = self._session_key_for_source(event.source) if event.source else None
        if adapter and _quick_key:
            try:
                kickoff_event = MessageEvent(
                    text=state.goal,
                    message_type=MessageType.TEXT,
                    source=event.source,
                    message_id=event.message_id,
                    channel_prompt=event.channel_prompt,
                )
                self._enqueue_fifo(_quick_key, kickoff_event, adapter)
            except Exception as exc:
                logger.debug("goal kickoff enqueue failed: %s", exc)

        base = t("gateway.goal.set", budget=state.max_turns, goal=state.goal)
        if state.has_contract():
            return f"{base}\nCompletion contract:\n{state.contract.render_block()}"
        if lower.startswith("draft"):
            # Drafting was requested but the aux model couldn't produce one.
            return f"{base}\n(Couldn't draft a contract — running as a free-form goal.)"
        return base

    async def _handle_heartbeat_command(self, event: "MessageEvent") -> str:
        """Handle /heartbeat for gateway platforms (mirror of CLI handler).

        Sets/manages the session's one recurring re-entry prompt. The
        gateway-wide poller injects due heartbeats through the adapter FIFO
        as ordinary user turns, so alternation and caching are untouched.
        """
        from hermes_cli.heartbeat import parse_interval, format_interval, MIN_INTERVAL_SECONDS

        args = (event.get_command_args() or "").strip()
        lower = args.lower()

        mgr, session_entry = await self._get_heartbeat_manager_for_event(event)
        if mgr is None:
            return "Heartbeats unavailable (no session)."

        quick_key = self._session_key_for_source(event.source) if event.source else None

        if not args or lower == "status":
            return mgr.status_line()

        if lower == "pause":
            state = mgr.pause()
            return f"⏸ Heartbeat paused: {state.prompt}" if state else "No heartbeat set."

        if lower == "resume":
            state = mgr.resume()
            if state is None:
                return "No heartbeat to resume."
            if quick_key and event.source is not None:
                self._register_heartbeat_watch(quick_key, event.source, mgr.session_id)
            return f"▶ Heartbeat resumed (every {format_interval(state.interval_seconds)}): {state.prompt}"

        if lower in {"clear", "stop", "off"}:
            had = mgr.clear()
            if quick_key:
                self._unregister_heartbeat_watch(quick_key)
            return "✓ Heartbeat cleared." if had else "No heartbeat set."

        # Set: `/heartbeat every 10m <prompt>` (also accepts `10m <prompt>`).
        tokens = args.split(None, 2)
        interval = None
        prompt = ""
        if tokens and tokens[0].lower() == "every" and len(tokens) >= 2:
            interval = parse_interval(f"every {tokens[1]}")
            prompt = tokens[2] if len(tokens) > 2 else ""
        elif tokens:
            interval = parse_interval(tokens[0])
            prompt = args[len(tokens[0]):].strip() if interval and interval > 0 else ""

        if interval is None:
            return (
                "Usage: /heartbeat every <interval> <prompt>  (e.g. /heartbeat every 10m Check CI)\n"
                "Also: /heartbeat status | pause | resume | clear"
            )
        if interval < 0:
            return f"Interval too small — minimum is {MIN_INTERVAL_SECONDS}s."
        if not prompt.strip():
            return "Usage: /heartbeat every <interval> <prompt> — the prompt is required."

        try:
            state = mgr.set(prompt, interval)
        except ValueError as exc:
            return f"Invalid heartbeat: {exc}"
        if quick_key and event.source is not None:
            self._register_heartbeat_watch(quick_key, event.source, mgr.session_id)
        return (
            f"♥ Heartbeat set (every {format_interval(state.interval_seconds)}): {state.prompt}\n"
            "Fires as a normal turn whenever this session is idle and the interval has "
            "elapsed. Lives while the gateway runs — use `hermes cron` for durable schedules."
        )

    async def _handle_refine_command(self, event: "MessageEvent") -> str:
        """Handle /refine — run the memory/skill review fork on demand.

        Uses the session's cached AIAgent (idle agents live in
        ``_agent_cache``). The review runs in a daemon thread against a
        snapshot of the conversation; the live session and prompt cache are
        untouched. Requires the session to have at least one completed turn.
        """
        args = (event.get_command_args() or "").strip()
        quick_key = self._session_key_for_source(event.source) if event.source else None
        if not quick_key:
            return "Refine unavailable (no session)."
        if quick_key in self._running_agents:
            return "Agent is running — wait for the turn to finish, then /refine."

        agent = None
        cache_lock = getattr(self, "_agent_cache_lock", None)
        if cache_lock is not None:
            with cache_lock:
                cached = self._agent_cache.get(quick_key)
                agent = cached[0] if isinstance(cached, tuple) else cached if cached else None
        if agent is None:
            return "Nothing to refine yet — send a message first."

        snapshot = list(getattr(agent, "_session_messages", None) or [])
        if not snapshot:
            return "Nothing to refine yet — the conversation is empty."

        review_skills = "skill_manage" in getattr(agent, "valid_tool_names", set())
        try:
            agent._spawn_background_review(
                messages_snapshot=snapshot,
                review_memory=True,
                review_skills=review_skills,
                focus=args or None,
            )
        except Exception as exc:
            return f"/refine failed to start: {exc}"
        tail = f" (focus: {args})" if args else ""
        return (
            f"⚗ Reviewing this conversation in the background{tail} — "
            f"any memory/skill updates will be reported when done."
        )

    async def _handle_subgoal_command(self, event: "MessageEvent") -> str:
        """Handle /subgoal for gateway platforms (mirror of CLI handler).

        Subgoals are extra criteria appended to the active goal mid-loop.
        They modify state read at the next turn boundary, so this is safe
        to invoke while the agent is running.
        """
        args = (event.get_command_args() or "").strip()
        mgr, _session_entry = await self._get_goal_manager_for_event(event)
        if mgr is None:
            return t("gateway.goal.unavailable")
        if not mgr.has_goal():
            return "No active goal. Set one with /goal <text>."

        # No args → list current subgoals.
        if not args:
            return f"{mgr.status_line()}\n{mgr.render_subgoals()}"

        tokens = args.split(None, 1)
        verb = tokens[0].lower()
        rest = tokens[1].strip() if len(tokens) > 1 else ""

        if verb == "remove":
            if not rest:
                return "Usage: /subgoal remove <n>"
            try:
                idx = int(rest.split()[0])
            except ValueError:
                return "/subgoal remove: <n> must be an integer (1-based index)."
            try:
                removed = mgr.remove_subgoal(idx)
            except (IndexError, RuntimeError) as exc:
                return f"/subgoal remove: {exc}"
            return f"✓ Removed subgoal {idx}: {removed}"

        if verb == "clear":
            try:
                prev = mgr.clear_subgoals()
            except RuntimeError as exc:
                return f"/subgoal clear: {exc}"
            if prev:
                return f"✓ Cleared {prev} subgoal{'s' if prev != 1 else ''}."
            return "No subgoals to clear."

        try:
            text = mgr.add_subgoal(args)
        except (ValueError, RuntimeError) as exc:
            return f"/subgoal: {exc}"
        idx = len(mgr.state.subgoals) if mgr.state else 0
        return f"✓ Added subgoal {idx}: {text}"

    async def _handle_undo_command(self, event: MessageEvent) -> str:
        """Handle /undo [N] — back up N user turns (default 1), soft-deleting
        the truncated rows on disk and echoing the backed-up message text so
        the user can copy/edit and resend.

        Mirrors the CLI/TUI /undo: rewound rows stay in state.db (active=0)
        for audit and are hidden from re-prompts and search. The cached agent
        is evicted so the next message rebuilds context from the truncated
        (active-only) transcript — the gateway's equivalent of the CLI's
        in-place history surgery + memory-cache invalidation.
        """
        source = event.source

        # Parse optional turn count: "/undo" → 1, "/undo 3" → 3.
        n = 1
        raw_args = event.get_command_args().strip()
        if raw_args:
            try:
                n = int(raw_args.split()[0])
            except (ValueError, IndexError):
                return t("gateway.undo.invalid_count", arg=raw_args.split()[0])
            if n < 1:
                n = 1

        session_entry = await self.async_session_store.get_or_create_session(source)
        result = await self.async_session_store.rewind_session(session_entry.session_id, n)

        if result is None:
            return t("gateway.undo.nothing")

        # Reset stored token count — transcript was truncated.
        session_entry.last_prompt_tokens = 0
        # Evict the cached agent so the next turn rebuilds from the active-only
        # transcript and memory providers refresh their per-session caches.
        try:
            session_key = build_session_key(source)
            self._evict_cached_agent(session_key)
        except Exception as e:
            logger.debug("undo: cached-agent eviction skipped: %s", e)

        target_text = result["target_text"]
        preview = target_text[:200] + "..." if len(target_text) > 200 else target_text
        return t(
            "gateway.undo.removed",
            turns=result["turns_undone"],
            count=result["rewound_count"],
            preview=preview,
        )
=======
        reply = _execute("commands", args=event.get_command_args(), options={"page_size": page_size})
        return self._telegramized_command_reply(event, reply.text)
>>>>>>> upstream/main

    async def _handle_set_home_command(self, event: MessageEvent) -> str:
        """Handle /sethome command -- set the current chat as the platform's home channel."""
        from gateway.run import _home_target_env_var, _home_thread_env_var
        source = event.source
        platform_name = source.platform.value if source.platform else "unknown"
        chat_id = source.chat_id
        chat_name = source.chat_name or chat_id
        if source.platform is None:
            return t("gateway.set_home.save_failed", error="Missing logical platform")
        via_relay = getattr(source, "delivered_via_upstream_relay", False) is True
        if via_relay:
            adapter_for_source = getattr(self, "_adapter_for_source", None)
            relay_adapter = adapter_for_source(source) if callable(adapter_for_source) else None
            fronts_platform = getattr(relay_adapter, "fronts_platform", None)
            if (source.platform in {None, Platform.LOCAL, Platform.RELAY}
                    or not getattr(source, "user_id", None)
                    or not callable(fronts_platform) or not fronts_platform(source.platform)):
                return t("gateway.set_home.save_failed",
                         error="Relay does not authenticate this logical home target")
        thread_id = _home_thread_from_source(source)
        home = HomeChannel(
            platform=source.platform, chat_id=str(chat_id), name=chat_name, thread_id=thread_id,
            user_id=str(source.user_id) if getattr(source, "user_id", None) else None,
            scope_id=str(source.scope_id) if getattr(source, "scope_id", None) else None)
        # config.yaml is canonical because it can persist the authenticated logical-target
        # provenance required by Relay after a restart.
        try:
            persist_home_channel(home, enabled_if_new=not via_relay)
        except Exception as e:
            return t("gateway.set_home.save_failed", error=e)
        # Preserve legacy home env vars for existing cron/setup consumers.
        try:
            from hermes_cli.config import save_env_value
            save_env_value(_home_target_env_var(platform_name), str(chat_id))
            save_env_value(_home_thread_env_var(platform_name), str(thread_id or ""))
        except Exception as e:
            logger.warning("Home config saved but legacy env persistence failed: %s", e)
        # Keep the running gateway config in sync too. The pre-restart notification path reads
        # self.config before the process reloads config.
        platform_config = self.config.platforms.setdefault(source.platform, PlatformConfig(enabled=not via_relay))
        platform_config.home_channel = home
        return t("gateway.set_home.success", name=chat_name, chat_id=chat_id)

    async def _handle_voice_command(self, event: MessageEvent) -> str:
        """Handle /voice [on|off|tts|channel|leave|status] command."""
        args = event.get_command_args().strip().lower()
        chat_id = event.source.chat_id
        # Voice state belongs to the (bot, chat) pair: resolve the adapter that received the
        # command and key the mode by its owning profile so two multiplexed bots in one chat keep
        # independent /voice state.
        # See #75198.
        voice_key = self._voice_key_for_source(event.source)
        adapter = self._adapter_for_source(event.source)

        def _set_mode(mode: str) -> None:
            self._voice_mode[voice_key] = mode
            self._save_voice_modes()
            if not adapter:
                return
            if mode == "off":
                self._set_adapter_auto_tts_disabled(adapter, chat_id, disabled=True)
            else:
                self._set_adapter_auto_tts_enabled(adapter, chat_id, enabled=True)

        if args in _VOICE_MODE_BY_ARG:
            mode, reply_key = _VOICE_MODE_BY_ARG[args]
            _set_mode(mode)
            return t(reply_key)
        if args in {"channel", "join"}:
            return await self._handle_voice_channel_join(event)
        if args == "leave":
            return await self._handle_voice_channel_leave(event)
        if args == "status":
            mode = self._voice_mode.get(voice_key, "off")
            label = t(f"gateway.voice.label_{mode}") if mode in ("off", "voice_only", "all") else mode
            lines = [t("gateway.voice.status_mode", label=label)]
            guild_id = self._get_guild_id(event)  # append voice channel info if connected
            info = adapter.get_voice_channel_info(guild_id) if guild_id and hasattr(adapter, "get_voice_channel_info") else None
            if info:
                lines += [t("gateway.voice.status_channel", channel=info['channel_name']),
                          t("gateway.voice.status_participants", count=info['member_count'])]
                for m in info["members"]:
                    status = t("gateway.voice.speaking") if m.get("is_speaking") else ""
                    lines.append(t("gateway.voice.status_member", name=m['display_name'], status=status))
            return "\n".join(lines)

        # Toggle: off → on, on/all → off
        turning_on = self._voice_mode.get(voice_key, "off") == "off"
        _set_mode("voice_only" if turning_on else "off")
        toggle_line = t("gateway.voice.enabled_short" if turning_on else "gateway.voice.disabled_short")
        # Bare /voice still toggles, but append an explainer so users discover the on/off/tts/status
        # subcommands (and, on Discord, live voice-channel join/leave). Toggle result shows first.
        supports_voice_channels = adapter is not None and hasattr(adapter, "join_voice_channel")
        channels = t("gateway.voice.help_channels") if supports_voice_channels else ""
        return t("gateway.voice.help", toggle=toggle_line, channels=channels)

    async def _handle_rollback_command(self, event: MessageEvent) -> str:
        """Handle /rollback command — list or restore filesystem checkpoints."""
        from tools.checkpoint_manager import format_checkpoint_list
        mgr = self._checkpoint_manager()
        if mgr is None:
            return t("gateway.rollback.not_enabled")
        cwd = self._terminal_cwd()
        # --all / --force: classic full restore, overwriting user edits too.
        tokens = event.get_command_args().strip().split()
        restore_all = any(tok.lower() in ("--all", "--force") for tok in tokens)
        arg = " ".join(tok for tok in tokens if tok.lower() not in ("--all", "--force"))
        checkpoints = mgr.list_checkpoints(cwd)
        if not arg:
            return format_checkpoint_list(checkpoints, cwd)
        if not checkpoints:
            return t("gateway.rollback.none_found", cwd=cwd)

        # Restore by number or hash
        try:
            idx = int(arg) - 1
        except ValueError:
            target_hash = arg
        else:
            if not 0 <= idx < len(checkpoints):
                return t("gateway.rollback.invalid_number", max=len(checkpoints))
            target_hash = checkpoints[idx]["hash"]
        result = mgr.restore(cwd, target_hash, safe=not restore_all)
        if not result["success"]:
            return t("gateway.rollback.restore_failed", error=result["error"])
        msg = t("gateway.rollback.restored", hash=result["restored_to"], reason=result["reason"])
        for result_key, i18n_key in _ROLLBACK_SKIP_LINES:
            files = result.get(result_key) or []
            if files:
                more = f" (+{len(files) - 5})" if len(files) > 5 else ""
                msg += "\n" + t(i18n_key, files=", ".join(files[:5]) + more)
        return msg

    async def _handle_diff_command(self, event: MessageEvent) -> str:
        """Handle /diff — show git changes in the working directory.  Diff body is truncated hard
        here (chat is not a pager); platform senders clamp further."""
        args = [a.lower() for a in event.get_command_args().strip().split()]
        stat_only = bool({"--stat", "stat"} & set(args))
        mode = "working"
        for low in args:
            mode = _DIFF_MODE_BY_ARG.get(low, mode)
        cwd = self._terminal_cwd()
        if mode == "session":
            # Cumulative checkpoint-baseline diff.
            mgr = self._checkpoint_manager()
            if mgr is None:
                return t("gateway.diff.not_enabled")
            result = await asyncio.to_thread(mgr.session_diff, cwd)
        else:
            from tools.working_diff import collect_working_diff
            result = await asyncio.to_thread(collect_working_diff, cwd, mode)
        if not result.get("success"):
            return t("gateway.diff.failed", error=result.get("error", "Could not generate diff"))
        return self._render_diff_result(result, stat_only)

    def _render_diff_result(self, result: dict, stat_only: bool) -> str:
        """Render a working/session diff result: stat block, untracked list, fenced (truncated) diff."""
        stat = result.get("stat", "")
        diff = result.get("diff", "")
        untracked = result.get("untracked", [])
        if result.get("empty") or (not stat and not diff and not untracked):
            return t("gateway.diff.no_changes")
        out: list[str] = []
        if stat:
            out.append(f"```\n{stat}\n```")
        if untracked:
            shown = "\n".join(f"+ {rel}" for rel in untracked[:15])
            more = f"\n... and {len(untracked) - 15} more" if len(untracked) > 15 else ""
            out.append(f"**Untracked:**\n```\n{shown}{more}\n```")
        if not stat_only and diff:
            out.append(self._fenced_truncated_diff(diff))
        return "\n\n".join(out)

    @staticmethod
    def _fenced_truncated_diff(diff: str, max_lines: int = 60, max_chars: int = 3000) -> str:
        """Fence a diff body, truncating to messaging-friendly size."""
        diff_lines = diff.splitlines()
        truncated = len(diff_lines) > max_lines
        if truncated:
            diff = "\n".join(diff_lines[:max_lines])
        if len(diff) > max_chars:
            diff = diff[:max_chars]
            truncated = True
        note = ""
        if truncated:
            note = f"\n... (truncated — {len(diff_lines)} lines total; use /diff --stat for a summary)"
        return f"```diff\n{diff}{note}\n```"

    def _track_background_task(self, coro) -> None:
        """Fire-and-forget *coro*, keeping a strong ref in ``_background_tasks`` until it finishes."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _handle_background_command(self, event: MessageEvent) -> str:
        """Handle /bg <prompt> — run a prompt in a background thread with its own session; the
        result is sent to the same chat without touching the active session's history."""
        prompt = event.get_command_args().strip()
        if not prompt:
            return t("gateway.background.usage")
        task_id = f"bg_{datetime.now().strftime('%H%M%S')}_{os.urandom(3).hex()}"
        self._track_background_task(self._run_background_task(
            prompt, event.source, task_id, event_message_id=self._reply_anchor_for_event(event),
            # Forward image/audio attachments so the background agent can see them.
            media_urls=list(event.media_urls or []), media_types=list(event.media_types or [])))
        return t("gateway.background.started", preview=_preview(prompt), task_id=task_id)

    async def _handle_btw_command(self, event: MessageEvent) -> str:
        """Handle /btw <question> — one-shot auxiliary LLM call on a transcript snapshot; live history
        is never touched (alternation + prompt cache intact, current turn keeps running). Unlike /bg,
        which spawns a fresh contextless session."""
        question = event.get_command_args().strip()
        if not question:
            return t("gateway.btw.usage")
        source = event.source
        session_entry = await self.async_session_store.get_or_create_session(source)
        try:
            history = await self.async_session_store.load_transcript(session_entry.session_id)
        except TranscriptReadError:
            return HISTORY_UNREADABLE
        if not history:
            return t("gateway.btw.no_history")
        try:
            model, rt = self._resolve_session_agent_runtime(source=source)
        except Exception:
            model, rt = None, {}
        if not rt.get("api_key"):
            return t("gateway.btw.no_provider")
        main_runtime = {"model": model, **{k: rt.get(k) for k in ("provider", "base_url", "api_key", "api_mode")}}
        history_snapshot = list(history)
        # Prefer the cache-parity fork when a live cached AIAgent exists: it replays the snapshot
        # against the warm provider prefix cache, giving FULL context at cache-read prices. With no
        # cached agent the cache is cold anyway — answer_side_question's digest fallback handles it.
        try:
            parent_agent = self._cached_agent_for(self._session_key_for_source(source))
        except Exception:
            parent_agent = None
        _thread_metadata = self._reply_metadata(event)
        adapter = self._adapter_for_source(source)
        preview = _preview(question)

        async def _run_side_question() -> None:
            from agent.side_question import answer_side_question
            try:
                answer = await asyncio.to_thread(
                    answer_side_question, question, history_snapshot,
                    parent_agent=parent_agent, main_runtime=main_runtime)
                reply = t("gateway.btw.answer", preview=preview, answer=answer or "")
            except Exception as e:
                logger.warning("/btw side question failed: %s", e)
                reply = t("gateway.btw.failed", preview=preview, error=str(e))
            if adapter is not None:
                await adapter.send(source.chat_id, reply, metadata=_thread_metadata)

        self._track_background_task(_run_side_question())
        return t("gateway.btw.started", preview=preview)

    async def _handle_memory_command(self, event: MessageEvent) -> str:
        """Handle /memory — review pending memory writes + toggle the approval gate. Entries are small
        enough to review inline, so the full flow works on every platform."""
        from hermes_cli.write_approval_commands import handle_pending_subcommand
        from tools import write_approval as wa
        from tools.memory_tool import load_on_disk_store
        # Apply approved writes against a fresh on-disk store (the gateway has no long-lived agent;
        # the store persists to the same MEMORY/USER.md and honors the configured char limits).
        out = handle_pending_subcommand(
            wa.MEMORY, event.get_command_args().strip().split(), memory_store=load_on_disk_store(),
            set_mode_fn=self._write_approval_setter("memory", event))
        return out if out is not None else (
            "Unknown /memory subcommand. Use: pending, approve <id>, reject <id>, approval <on|off>."
        )

    async def _handle_skills_command(self, event: MessageEvent) -> str:
        """Handle /skills on the gateway — pending skill-write review only (hub stays CLI-only). Gated
        by ``skills.write_approval`` but still answers when staged writes exist after the gate is off
        (never stranded). ``diff`` is truncated for chat."""
        from hermes_cli.write_approval_commands import handle_pending_subcommand
        from tools import write_approval as wa
        args = event.get_command_args().strip().split()
        sub = args[0].lower() if args else ""
        gate_off = not wa.write_approval_enabled(wa.SKILLS) and sub not in {"approval", "mode"}
        if gate_off and wa.pending_count(wa.SKILLS) == 0:
            return ("Skill write approval is off (skills.write_approval). "
                    "Enable it with /skills approval on, then review staged "
                    "writes here with /skills pending.")
        out = handle_pending_subcommand(
            wa.SKILLS, args, set_mode_fn=self._write_approval_setter("skills", event))
        if out is None:
            return ("Unknown /skills subcommand on this platform. Use: pending, "
                    "approve <id>, reject <id>, diff <id>, approval <on|off>. "
                    "(Search/install are CLI-only.)")

        # Chat bubbles can't hold a full skill diff — truncate and point at the pending JSON file
        # (NOT `hermes skills diff <name>`, which diffs a bundled skill against its stock version).
        if sub == "diff" and len(out) > 3000:
            pending_id = args[1] if len(args) > 1 else "<id>"
            out = (out[:3000]
                   + "\n… (truncated — full diff in "
                     f"~/.hermes/pending/skills/{pending_id}.json)")
        return out

    async def _handle_approvals_command(self, event: MessageEvent) -> str:
        """Show or persist the profile-wide dangerous-command approval mode."""
        from gateway.slash_access import policy_for_source
        from hermes_cli.approval_mode import run_approval_mode_command
        requested = event.get_command_args().strip() or None
        # This mutates profile-wide security policy. The central slash gate can allow selected
        # commands to non-admin users, so enforce admin again at this side-effect boundary.
        # Unconfigured policies remain unrestricted.
        policy = policy_for_source(self.config, event.source)
        if requested and not policy.is_admin(event.source.user_id):
            return "Only gateway admins can change the persistent approval mode."
        # Approval checks load config dynamically; do not evict the cached agent or alter its
        # system prompt/tool schema (prompt-cache prefix is sacred).
        return run_approval_mode_command(requested).message

    async def _handle_yolo_command(self, event: MessageEvent) -> Union[str, EphemeralReply]:
        """Handle /yolo — toggle dangerous command approval bypass for this session only."""
        from tools.approval import disable_session_yolo, enable_session_yolo, is_session_yolo_enabled
        session_key = self._session_key_for_source(event.source)
        if is_session_yolo_enabled(session_key):
            disable_session_yolo(session_key)
            return EphemeralReply(t("gateway.yolo.disabled"))
        enable_session_yolo(session_key)
        return EphemeralReply(t("gateway.yolo.enabled"))

    async def _handle_verbose_command(self, event: MessageEvent) -> str:
        """Handle /verbose — cycle tool progress display mode (off → new → all → verbose → log) per
        *current platform*, saved to ``display.platforms.<platform>.tool_progress``. Gated by
        ``display.tool_progress_command`` (default off)."""
        from gateway.run import _load_gateway_config
        config_path, platform_key = self._display_config_target(event)
        try:
            user_config = _load_gateway_config()
            gate_enabled = is_truthy_value(cfg_get(user_config, "display", "tool_progress_command"),
                                           default=False)
        except Exception:
            gate_enabled = False
        if not gate_enabled:
            return t("gateway.verbose.not_enabled")
        # Cycle mode (per-platform), reading the current effective mode via the resolver.
        from gateway.display_config import resolve_display_setting
        cycle = ["off", "new", "all", "verbose", "log"]
        current = resolve_display_setting(user_config, platform_key, "tool_progress", "all")
        new_mode = cycle[(cycle.index(current if current in cycle else "all") + 1) % len(cycle)]
        description = t(f"gateway.verbose.mode_{new_mode}")
        try:
            _nested_dict(user_config, "display", "platforms", platform_key)["tool_progress"] = new_mode
            atomic_config_write(config_path, user_config)
            return f"{description}\n" + t("gateway.verbose.saved_suffix", platform=platform_key)
        except Exception as e:
            logger.warning("Failed to save tool_progress mode: %s", e)
            return f"{description}\n" + t("gateway.verbose.save_failed", error=e)

    async def _handle_busy_command(self, event: MessageEvent) -> Union[str, EphemeralReply]:
        """Handle /busy — control what happens when messaging while Hermes is working."""
        arg = event.get_command_args().strip().lower()
        if not arg or arg == "status":
            mode = self._effective_busy_input_mode(event.source)
            behavior = _BUSY_MODE_BEHAVIOR.get(mode, _BUSY_MODE_BEHAVIOR["interrupt"])[0]
            return EphemeralReply(
                f"**Busy input mode: `{mode}`\nMessages while busy: _{behavior}_\n"
                f"Change with `/busy queue`, `/busy steer`, or `/busy interrupt`.")
        if arg not in _BUSY_MODE_BEHAVIOR:
            return EphemeralReply(
                f"Unknown mode `{arg}`. Use `/busy queue`, `/busy steer`, or `/busy interrupt`.")

        # Persist before mutate
        from cli import save_config_value
        if not save_config_value("display.busy_input_mode", arg):
            return EphemeralReply("Busy input mode could not be saved to config. Mode unchanged.")
        profile_name = self._busy_profile_name_for_source(event.source)
        if profile_name:
            from gateway.run import _load_gateway_config
            self._snapshot_profile_busy_modes(profile_name, _load_gateway_config())
        else:
            self._busy_input_mode = arg
            # busy_input_mode is also the source of truth for the text mode — re-derive it so the
            # adapter refresh below doesn't keep a stale value and keep interrupting.
            self._busy_text_mode = self._load_busy_text_mode()

        adapter = self._adapter_for_source(event.source)
        if adapter is not None:
            adapter._busy_text_mode = self._effective_busy_text_mode(event.source)
        return EphemeralReply(
            f"Busy input mode set to **`{arg}`** (saved).\n_{_BUSY_MODE_BEHAVIOR[arg][1]}_")

    async def _handle_footer_command(self, event: MessageEvent) -> str:
        """Handle /footer command — toggle the runtime-metadata footer."""
        from gateway.run import _load_gateway_config, _resolve_gateway_model
        from gateway.runtime_footer import format_runtime_footer, resolve_footer_config
        config_path, platform_key = self._display_config_target(event)
        arg = ""
        try:
            text = (getattr(event, "message", None) or "").strip()
            if text.startswith("/"):
                parts = text.split(None, 1)
                arg = parts[1].strip().lower() if len(parts) > 1 else ""
        except Exception:
            arg = ""
        try:
            user_config: dict = _load_gateway_config()
        except Exception as e:
            return t("gateway.config_read_failed", error=e)
        effective = resolve_footer_config(user_config, platform_key)

        def _state(enabled: bool) -> str:
            return t("gateway.footer.state_on") if enabled else t("gateway.footer.state_off")
        if arg in {"status", "?"}:
            return t("gateway.footer.status", state=_state(effective["enabled"]),
                     fields=", ".join(effective.get("fields") or []), platform=platform_key)
        if arg and arg not in _FOOTER_STATE_BY_ARG:
            return t("gateway.footer.usage")
        new_state = _FOOTER_STATE_BY_ARG[arg] if arg else not effective["enabled"]
        try:
            _nested_dict(user_config, "display", "runtime_footer")["enabled"] = new_state
            atomic_config_write(config_path, user_config)
        except Exception as e:
            logger.warning("Failed to save runtime_footer.enabled: %s", e)
            return t("gateway.config_save_failed", error=e)
        example = ""
        if new_state:
            # Show a preview using current agent state if available.
            preview = format_runtime_footer(
                model=_resolve_gateway_model(user_config) or None, context_tokens=0, context_length=None,
                fields=effective.get("fields") or ["model", "context_pct", "cwd"])
            if preview:
                example = t("gateway.footer.example_line", preview=preview)
        return t("gateway.footer.saved", state=_state(new_state), example=example)

    async def _handle_reload_mcp_command(self, event: MessageEvent) -> Optional[str]:
        """Handle /reload-mcp — reconnect MCP servers and rebuild the cached agent. Reloading
        invalidates the provider prompt cache (tool schemas live in the system prompt), so it routes
        through slash-confirm; "Always Approve" persists ``approvals.mcp_reload_confirm: false``."""
        session_key = self._session_key_for_source(event.source)
        # Read the gate fresh from disk so a prior "always" click takes effect on the next
        # invocation without restarting the gateway.
        user_config = self._read_user_config()
        approvals = user_config.get("approvals") if isinstance(user_config, dict) else None
        if isinstance(approvals, dict) and not approvals.get("mcp_reload_confirm", True):
            return await self._execute_mcp_reload(event)
        # Route through slash-confirm. The primitive sends the prompt and stores the resume handler;
        # the button/text response triggers ``_resolve_slash_confirm`` which invokes the handler
        # with the chosen outcome.
        async def _on_confirm(choice: str) -> Optional[str]:
            if choice == "cancel":
                return t("gateway.reload_mcp.cancelled")
            if choice == "always":
                # Persist the opt-out and run the reload.
                try:
                    from cli import save_config_value
                    save_config_value("approvals.mcp_reload_confirm", False)
                    logger.info("User opted out of /reload-mcp confirmation (session=%s)", session_key)
                except Exception as exc:
                    logger.warning("Failed to persist mcp_reload_confirm=false: %s", exc)
            # once / always → run the reload
            result = await self._execute_mcp_reload(event)
            if choice == "always":
                return f"{result}\n\n" + t("gateway.reload_mcp.always_followup")
            return result
        return await self._request_slash_confirm(
            event=event, command="reload-mcp", title="/reload-mcp",
            message=t("gateway.reload_mcp.confirm_prompt"), handler=_on_confirm)

    async def _handle_reload_skills_command(self, event: MessageEvent) -> str:
        """Handle /reload-skills — rescan skills dir, queue a note for next turn. Skills are invoked at
        runtime, not baked into the system prompt, so this does NOT clear the prompt cache. The diff
        goes into ``_pending_skills_reload_notes[session_key]``, prepended to the NEXT user message —
        nothing out-of-band, so alternation is preserved."""
        try:
            from agent.skill_commands import reload_skills

            # _run_in_executor_with_context, not a bare hop: the rescan walks
            # get_hermes_home()/skills, a contextvar override under multiplex.
            result = await self._run_in_executor_with_context(reload_skills)
            added, removed = result.get("added", []), result.get("removed", [])  # [{"name", "description"}]
            total = result.get("total", 0)
            # Let adapters refresh platform-side state that cached the skill list at startup (today:
            # Discord /skill autocomplete — otherwise new skills stay invisible and deleted ones
            # error). Adapters without refresh_skill_group are skipped; the in-process reload suffices.
            for adapter in list(self.adapters.values()):
                refresh = getattr(adapter, "refresh_skill_group", None)
                try:
                    maybe = refresh() if callable(refresh) else None
                    if inspect.isawaitable(maybe):
                        await maybe
                except Exception as exc:
                    logger.warning("Adapter %s refresh_skill_group raised: %s",
                                   getattr(adapter, "name", adapter), exc)

            lines = [t("gateway.reload_skills.header")]
            if not added and not removed:
                lines += [t("gateway.reload_skills.no_new"), t("gateway.reload_skills.total", count=total)]
                return "\n".join(lines)

            def _fmt_line(item: dict) -> str:
                nm, desc = item.get("name", ""), item.get("description", "")
                return (t("gateway.reload_skills.item_with_desc", name=nm, desc=desc) if desc
                        else t("gateway.reload_skills.item_no_desc", name=nm))

            # Queue a one-shot note for the next user turn in this session too. Format matches how
            # the system prompt renders pre-existing skills (``    - name: description``) so the
            # model reads the diff in the same shape as its original skill catalog.
            sections = ["[USER INITIATED SKILLS RELOAD:"]
            for i18n_key, note_header, items in (
                ("gateway.reload_skills.added_header", "Added Skills:", added),
                ("gateway.reload_skills.removed_header", "Removed Skills:", removed)):
                if items:
                    formatted = [_fmt_line(item) for item in items]
                    lines += [t(i18n_key)] + formatted
                    sections += ["", note_header] + formatted
            lines.append(t("gateway.reload_skills.total", count=total))
            sections += ["", "Use skills_list to see the updated catalog.]"]
            session_key = self._session_key_for_source(event.source)
            if not hasattr(self, "_pending_skills_reload_notes"):
                self._pending_skills_reload_notes = {}
            if session_key:
                self._pending_skills_reload_notes[session_key] = "\n".join(sections)
            return "\n".join(lines)
        except Exception as e:
            logger.warning("Skills reload failed: %s", e)
            return t("gateway.reload_skills.failed", error=e)

    async def _handle_bundles_command(self, event: MessageEvent) -> str:
        """Handle /bundles — list installed skill bundles (mirrors the CLI handler). Bundles are
        loaded by invoking their own ``/<slug>`` command, not by this one."""
        reply = _execute("bundles")
        if "error" in reply.data:
            logger.warning("Bundles command unavailable: %s", reply.data["error"])
            return reply.text
        bundles = reply.data["bundles"]
        if not bundles:
            return ("No skill bundles installed.\nCreate one on the host with:\n"
                    "  `hermes bundles create <name> --skill <s1> --skill <s2>`\n"
                    f"Directory: `{reply.data['dir']}`")
        lines = [f"**Skill Bundles** ({len(bundles)} installed):", ""]
        for info in bundles:
            skills = info.get("skills", [])
            desc = info.get("description") or f"Load {len(skills)} skills"
            lines += [f"• `/{info['slug']}` — {desc} _({len(skills)} skills)_"] + [f"    · {s}" for s in skills]
        return "\n".join(lines + ["", "Invoke a bundle with `/<slug>` to load all its skills."])

    def _blocking_approval_or_stale(self, event: MessageEvent, stale_key: str, none_key: str):
        """``(session_key, None)`` when an agent thread is blocked on approval, else the reply to send.
        A pending-approvals entry with no blocked thread is a stale prompt: drop it and say so."""
        from tools.approval import has_blocking_approval
        session_key = self._session_key_for_source(event.source)
        if has_blocking_approval(session_key):
            return session_key, None
        if session_key in self._pending_approvals:
            self._pending_approvals.pop(session_key)
            return session_key, t(stale_key)
        return session_key, t(none_key)

    async def _handle_approve_command(self, event: MessageEvent) -> Optional[str]:
        """Handle /approve — unblock waiting agent thread(s). They block inside tools/approval.py;
        signalling the event resumes them so the command executes inline (same flow as the CLI)."""
        from tools.approval import resolve_gateway_approval
        session_key, stale = self._blocking_approval_or_stale(event, "gateway.approval_expired",
                                                              "gateway.approve.no_pending")
        if stale:
            return stale
        # Args: "all", "all session", "all always", "session", "always" ("always" beats "session").
        args = event.get_command_args().strip().lower().split()
        choices = {_APPROVE_CHOICE_BY_ARG[a] for a in args if a in _APPROVE_CHOICE_BY_ARG}
        choice = "always" if "always" in choices else "session" if "session" in choices else "once"
        count = resolve_gateway_approval(session_key, choice, resolve_all="all" in args)
        if not count:
            return t("gateway.approve.no_pending")
        confirmation_text = t(f"gateway.approve.{choice}_{'plural' if count > 1 else 'singular'}", count=count)
        logger.info("User approved %d dangerous command(s) via /approve (%s)", count, choice)
        return await self._deliver_approval_confirmation(event, confirmation_text, "approve")

    async def _handle_deny_command(self, event: MessageEvent) -> str:
        """Handle /deny — reject pending dangerous command(s) with a definitive BLOCKED result, as in
        the CLI. ``/deny`` denies the oldest; ``/deny all`` denies everything.

        ``/deny <reason>`` (or ``/deny all <reason>``) attaches a one-line reason that is relayed back to
        the agent so it can adapt instead of only hearing "denied". Ported from qwibitai/nanoclaw#2832.
        """
        from tools.approval import resolve_gateway_approval
        session_key, stale = self._blocking_approval_or_stale(event, "gateway.deny.stale",
                                                              "gateway.deny.no_pending")
        if stale:
            return stale
        # A leading "all" denies every pending command; the rest (or the whole arg string without
        # "all") is the optional deny reason relayed to the agent, capped to a sane one-liner.
        raw_args = event.get_command_args().strip()
        tokens = raw_args.split()
        resolve_all = bool(tokens) and tokens[0].lower() == "all"
        reason = (raw_args[len(tokens[0]):].strip() if resolve_all else raw_args)[:280].strip()
        count = resolve_gateway_approval(session_key, "deny", resolve_all=resolve_all, reason=reason or None)
        if not count:
            return t("gateway.deny.no_pending")
        logger.info("User denied %d dangerous command(s) via /deny%s", count,
                    " (with reason)" if reason else "")
        key = "gateway.deny.denied" + ("_reason" if reason else "") + ("_plural" if count > 1 else "_singular")
        confirmation_text = t(key, count=count, reason=reason)
        return await self._deliver_approval_confirmation(event, confirmation_text, "deny")

    async def _handle_debug_command(self, event: MessageEvent) -> str:
        """Handle /debug — upload ONLY the summary (system info + log tails), never full logs, to
        protect privacy; ``hermes debug share`` from the CLI does full uploads."""
        from hermes_cli.debug import (_GATEWAY_PRIVACY_NOTICE, _best_effort_sweep_expired_pastes,
                                      _capture_dump, _is_dpaste_url, _schedule_auto_delete,
                                      collect_debug_report, upload_to_pastebin)

        def _collect_and_upload():  # blocking I/O (dump capture, log reads, uploads) -> thread
            _best_effort_sweep_expired_pastes()
            report = collect_debug_report(log_lines=200, dump_text=_capture_dump())
            try:
                urls = {"Report": upload_to_pastebin(report)}
            except Exception as exc:
                return t("gateway.debug.upload_failed", error=exc)
            _schedule_auto_delete(list(urls.values()))  # paste.rs only; dpaste.com has no delete
            label_width = max(len(k) for k in urls)
            # The 6-hour line is only true for paste.rs; the privacy notice above already states
            # the dpaste.com fallback retention, so drop the line rather than contradict it.
            auto_delete = [] if any(map(_is_dpaste_url, urls.values())) else [
                t("gateway.debug.auto_delete")]
            return "\n".join([_GATEWAY_PRIVACY_NOTICE, "", t("gateway.debug.header"), "",
                              *(f"`{label:<{label_width}}`  {url}" for label, url in urls.items()),
                              "", *auto_delete, t("gateway.debug.full_logs_hint"),
                              t("gateway.debug.share_hint")])

        # _run_in_executor_with_context, not a bare hop: this collects the profile's logs/config off
        # ``get_hermes_home()`` and uploads them to a public paste. Losing the contextvar override
        # would publish the DEFAULT profile's diagnostics from another profile's chat.
        return await self._run_in_executor_with_context(_collect_and_upload)

    async def _handle_update_command(self, event: MessageEvent) -> str:
        """Handle /update — spawn ``hermes update`` detached (``setsid``) so it survives the gateway
        restart it may trigger; marker files let this or the next gateway process notify the user."""
        import json
        from gateway.run import _hermes_home, _resolve_hermes_bin
        from hermes_cli.config import is_managed, format_managed_message
        # Block non-messaging platforms (API server, webhooks, ACP); plugin platforms with
        # allow_update_command=True are also allowed.
        src = event.source
        if src.platform not in self._UPDATE_ALLOWED_PLATFORMS:
            try:
                from gateway.platform_registry import platform_registry
                entry = platform_registry.get(src.platform.value)
                if not entry or not entry.allow_update_command:
                    return t("gateway.update.platform_not_messaging")
            except Exception:
                return t("gateway.update.platform_not_messaging")
        if is_managed():
            return f"✗ {format_managed_message('update Hermes Agent')}"
        if not (Path(__file__).parent.parent.resolve() / '.git').exists():
            return t("gateway.update.not_git_repo")
        hermes_cmd = _resolve_hermes_bin()
        if not hermes_cmd:
            return t("gateway.update.hermes_cmd_not_found")
        pending_path = _hermes_home / ".update_pending.json"
        output_path = _hermes_home / ".update_output.txt"
        exit_code_path = _hermes_home / ".update_exit_code"
        pending = {
            "platform": src.platform.value, "chat_id": src.chat_id, "chat_type": src.chat_type,
            "user_id": src.user_id, "session_key": self._session_key_for_source(src),
            "timestamp": datetime.now().isoformat()}
        # ``profile``: the update watcher (possibly the NEXT gateway process) must answer through the
        # requester's own profile bot, not the default profile's adapter for the same platform.
        pending.update({k: v for k, v in (("thread_id", src.thread_id), ("message_id", event.message_id),
                                          ("profile", getattr(src, "profile", None))) if v})
        _tmp_pending = pending_path.with_suffix(".tmp")
        _tmp_pending.write_text(json.dumps(pending), encoding="utf-8")
        _tmp_pending.replace(pending_path)
        exit_code_path.unlink(missing_ok=True)
        try:
            _spawn_detached_update(hermes_cmd, output_path, exit_code_path)
        except Exception as e:
            pending_path.unlink(missing_ok=True)
            exit_code_path.unlink(missing_ok=True)
            return t("gateway.update.start_failed", error=e)
        self._schedule_update_notification_watch()
        return t("gateway.update.starting")


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.
from typing import Any  # noqa: F401,E402
import hashlib  # noqa: F401,E402


_PLUGIN_COMPAT_LAZY = {
    'HISTORY_UNREADABLE': ('gateway.slash_commands_status', 'HISTORY_UNREADABLE'),
    'MessageType': ('gateway.platforms.event', 'MessageType'),
    'SessionSource': ('gateway.session', 'SessionSource'),
    'base_url_host_matches': ('utils', 'base_url_host_matches'),
    'build_session_key': ('gateway.session', 'build_session_key'),
    'clear_model_endpoint_credentials': ('hermes_cli.config', 'clear_model_endpoint_credentials'),
    'extract_api_content_sidecar': ('agent.turn_context', 'extract_api_content_sidecar'),
    'fetch_account_usage': ('agent.account_usage', 'fetch_account_usage'),
    'is_shared_multi_user_session': ('gateway.session', 'is_shared_multi_user_session'),
    'render_account_usage_lines': ('agent.account_usage', 'render_account_usage_lines'),
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
