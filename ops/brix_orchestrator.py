#!/usr/bin/env python3
"""BRIX orchestrator-side handoff drain loop.

BRIX owns the runner shape, locking, Slack question/reply flow, and how a human
reply becomes the next brief text. Quay owns durable task, claim, artifact, and
handoff state behind the CLI adapter below.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol


LOGGER = logging.getLogger("brix.orchestrator")


class LockBusy(RuntimeError):
    """Raised when another orchestrator runner already owns the drain lock."""


@dataclass(frozen=True)
class Handoff:
    """A claimed orchestrator handoff.

    The field names here are BRIX-local and intentionally small so the Quay CLI
    adapter does not leak storage details into the Slack/human loop.
    """

    handoff_id: str
    task_id: str
    repo_id: str | None = None
    artifact_id: str | None = None
    claim_id: str | None = None
    reason: str = ""
    summary: str = ""
    next_brief: str | None = None
    human_question: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "Handoff":
        metadata = raw.get("metadata")
        if not isinstance(metadata, Mapping):
            metadata = {}
        return cls(
            handoff_id=str(raw.get("handoff_id") or raw.get("id") or ""),
            task_id=str(raw.get("task_id") or ""),
            repo_id=_optional_str(raw.get("repo_id")),
            artifact_id=_optional_str(raw.get("artifact_id")),
            claim_id=_optional_str(raw.get("claim_id")),
            reason=str(raw.get("reason") or ""),
            summary=str(raw.get("summary") or ""),
            next_brief=_optional_str(raw.get("next_brief")),
            human_question=_optional_str(raw.get("human_question")),
            metadata=dict(metadata),
        )


@dataclass(frozen=True)
class TaskContext:
    task_id: str
    title: str = ""
    issue: str = ""
    repo_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "TaskContext":
        metadata = raw.get("metadata")
        if not isinstance(metadata, Mapping):
            metadata = {}
        metadata = _task_metadata_with_authors(raw, metadata)
        return cls(
            task_id=str(raw.get("task_id") or raw.get("id") or ""),
            title=str(raw.get("title") or ""),
            issue=str(raw.get("issue") or raw.get("issue_id") or ""),
            repo_id=_optional_str(raw.get("repo_id")),
            metadata=metadata,
        )


@dataclass(frozen=True)
class Artifact:
    artifact_id: str
    text: str
    kind: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "Artifact":
        metadata = raw.get("metadata")
        if not isinstance(metadata, Mapping):
            metadata = {}
        return cls(
            artifact_id=str(raw.get("artifact_id") or raw.get("id") or ""),
            text=str(raw.get("text") or raw.get("content") or ""),
            kind=str(raw.get("kind") or ""),
            metadata=dict(metadata),
        )


@dataclass(frozen=True)
class SlackPostRef:
    channel_id: str
    ts: str
    thread_ts: str


@dataclass(frozen=True)
class SlackRoute:
    channel_id: str
    thread_ts: str | None = None
    source: str = ""

    @property
    def thread_ref(self) -> str | None:
        if not self.thread_ts:
            return None
        return f"{self.channel_id}:{self.thread_ts}"


@dataclass(frozen=True)
class SlackReply:
    text: str
    user_id: str = ""
    ts: str = ""
    permalink: str | None = None


@dataclass(frozen=True)
class ConversationDecision:
    """Orchestrator decision for a human discussion turn."""

    action: str
    brief: str = ""
    message: str = ""


class SlackApiError(RuntimeError):
    """Typed Slack Web API failure."""

    def __init__(self, method: str, error: str) -> None:
        self.method = method
        self.error = error
        super().__init__(f"Slack API {method} returned error: {error}")


@dataclass
class RunMetrics:
    handoffs_claimed: int = 0
    direct_briefs_submitted: int = 0
    slack_questions_posted: int = 0
    human_replies_ingested: int = 0
    human_briefs_submitted: int = 0
    claims_released: int = 0
    no_handoff: int = 0
    lock_busy: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class DrainResult:
    status: str
    handoff_id: str | None = None
    task_id: str | None = None
    metrics: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class OrchestratorConfig:
    enabled: bool = False
    default_slack_channel: str = ""
    slack_token_env: str = "SLACK_BOT_TOKEN"
    quay_command: str = "/usr/local/bin/quay"
    reply_timeout_seconds: float = 1800.0
    poll_interval_seconds: float = 15.0
    lock_path: Path | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "OrchestratorConfig":
        return cls(
            enabled=_as_bool(raw.get("enabled"), default=False),
            default_slack_channel=str(raw.get("default_slack_channel") or ""),
            slack_token_env=str(raw.get("slack_token_env") or "SLACK_BOT_TOKEN"),
            quay_command=str(raw.get("quay_command") or "/usr/local/bin/quay"),
            reply_timeout_seconds=_positive_float(
                raw.get("reply_timeout_seconds"), default=1800.0
            ),
            poll_interval_seconds=_positive_float(
                raw.get("poll_interval_seconds"), default=15.0
            ),
            lock_path=Path(str(raw["lock_path"])) if raw.get("lock_path") else None,
        )

    def with_env_overrides(self) -> "OrchestratorConfig":
        channel = os.getenv("BRIX_DEFAULT_SLACK_CHANNEL") or self.default_slack_channel
        enabled = self.enabled
        if "BRIX_ORCHESTRATOR_ENABLED" in os.environ:
            enabled = _as_bool(os.environ["BRIX_ORCHESTRATOR_ENABLED"], default=enabled)
        token_env = os.getenv("BRIX_SLACK_TOKEN_ENV") or self.slack_token_env
        timeout = _positive_float(
            os.getenv("BRIX_REPLY_TIMEOUT_SECONDS"),
            default=self.reply_timeout_seconds,
        )
        interval = _positive_float(
            os.getenv("BRIX_POLL_INTERVAL_SECONDS"),
            default=self.poll_interval_seconds,
        )
        lock_env = os.getenv("BRIX_ORCHESTRATOR_LOCK")
        return dataclasses.replace(
            self,
            enabled=enabled,
            default_slack_channel=channel,
            slack_token_env=token_env,
            quay_command=os.getenv("BRIX_QUAY_COMMAND") or self.quay_command,
            reply_timeout_seconds=timeout,
            poll_interval_seconds=interval,
            lock_path=Path(lock_env) if lock_env else self.lock_path,
        )


class QuayClient(Protocol):
    def claim_handoff(self, worker_id: str) -> Handoff | None:
        """Claim one durable handoff, or return None if no work exists."""

    def get_task_context(self, task_id: str) -> TaskContext:
        """Return enough task context for a human-readable Slack question."""

    def get_artifact(self, handoff: Handoff) -> Artifact | None:
        """Return the blocker/context artifact referenced by a handoff."""

    def escalate_human(
        self,
        handoff: Handoff,
        question: str,
        thread_ref: str | None,
    ) -> None:
        """Persist the human question/audit boundary before waiting."""

    def record_human_reply(
        self,
        handoff: Handoff,
        reply: SlackReply,
        thread_ref: str,
    ) -> None:
        """Persist the human reply before submitting the follow-up brief."""

    def submit_brief(
        self,
        handoff: Handoff,
        brief: str,
        *,
        reason: str,
    ) -> None:
        """Submit the next worker brief for a claimed handoff."""

    def complete_claim(self, handoff: Handoff) -> None:
        """Mark the claim consumed after submit_brief succeeds."""

    def release_claim(self, handoff: Handoff, reason: str) -> None:
        """Release a claim that could not be completed this run."""


class SlackQuestionClient(Protocol):
    def validate_thread(self, channel_id: str, thread_ts: str) -> SlackPostRef:
        """Prove a stored Slack thread exists and return the polling floor."""

    def post_question(
        self,
        channel_id: str,
        text: str,
        *,
        thread_ts: str | None = None,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        """Post a human question and return the Slack thread reference."""

    def wait_for_reply(
        self,
        ref: SlackPostRef,
        *,
        after_ts: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
        """Poll the question thread and return the next human reply."""

    def post_thread_message(
        self,
        ref: SlackPostRef,
        text: str,
        *,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        """Post a human-visible message in the same thread."""


class ConversationDecider(Protocol):
    def decide(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        replies: list[SlackReply],
        posted_messages: list[str],
    ) -> ConversationDecision:
        """Decide whether the discussion is ready, needs a follow-up, or should wait."""


class QuayCommandError(RuntimeError):
    def __init__(
        self,
        argv: list[str],
        *,
        returncode: int,
        stdout: str,
        stderr: str,
        code: str | None = None,
    ) -> None:
        self.argv = argv
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.code = code
        suffix = f" ({code})" if code else ""
        super().__init__(f"quay command failed{suffix}: {' '.join(argv)}")


class QuayCliClient:
    """Adapter for the Quay orchestrator handoff CLI contract."""

    def __init__(
        self,
        *,
        command: str = "/usr/local/bin/quay",
        logger: logging.Logger | None = None,
        runner: Any | None = None,
    ) -> None:
        self.command = command
        self._logger = logger or LOGGER
        self._runner = runner or subprocess.run

    def claim_handoff(self, worker_id: str) -> Handoff | None:
        rows = self._run_json(["handoff", "list", "--status", "pending"])
        if not isinstance(rows, list):
            raise RuntimeError("quay handoff list did not return a JSON array")

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            task_id = str(row.get("task_id") or "")
            if not task_id:
                continue
            try:
                claim = self._run_json(["task", "claim", task_id])
            except QuayCommandError as exc:
                if exc.code in {"wrong_state", "cancelled", "unknown_task"}:
                    log_event(
                        self._logger,
                        "handoff_claim_skipped",
                        worker_id=worker_id,
                        task_id=task_id,
                        error_code=exc.code,
                    )
                    continue
                raise
            if not isinstance(claim, Mapping):
                raise RuntimeError("quay task claim did not return a JSON object")

            metadata = self._handoff_metadata(row)
            metadata["quay_handoff"] = dict(row)
            metadata["worker_id"] = worker_id
            reason = str(row.get("reason") or "")
            return Handoff(
                handoff_id=str(row.get("handoff_id") or row.get("id") or ""),
                task_id=task_id,
                artifact_id=_artifact_kind_for_reason(reason),
                claim_id=str(claim.get("claim_id") or ""),
                reason=reason,
                summary=_summary_for_handoff(row, metadata),
                metadata=metadata,
            )
        return None

    def get_task_context(self, task_id: str) -> TaskContext:
        raw = self._run_json(["task", "get", task_id])
        if not isinstance(raw, Mapping):
            raise RuntimeError("quay task get did not return a JSON object")
        metadata = _task_metadata_with_authors(raw, raw)
        return TaskContext(
            task_id=str(raw.get("task_id") or task_id),
            title=str(raw.get("external_ref") or raw.get("branch_name") or task_id),
            issue=str(raw.get("external_ref") or ""),
            repo_id=_optional_str(raw.get("repo_id")),
            metadata=metadata,
        )

    def get_artifact(self, handoff: Handoff) -> Artifact | None:
        if not handoff.artifact_id:
            return None
        try:
            text = self._run_text(["artifact", "get", handoff.task_id, handoff.artifact_id])
        except QuayCommandError as exc:
            if exc.code == "unknown_artifact":
                log_event(
                    self._logger,
                    "artifact_missing",
                    task_id=handoff.task_id,
                    artifact_kind=handoff.artifact_id,
                )
                return None
            raise
        return Artifact(
            artifact_id=handoff.artifact_id,
            text=text,
            kind=handoff.artifact_id,
        )

    def escalate_human(
        self,
        handoff: Handoff,
        question: str,
        thread_ref: str | None,
    ) -> None:
        claim_id = self._claim_id(handoff)
        with _temp_text_file(question) as path:
            argv = [
                "escalate-human",
                handoff.task_id,
                "--claim-id",
                claim_id,
                "--question-file",
                str(path),
            ]
            if thread_ref:
                argv.extend(["--thread-ref", thread_ref])
            self._run_json(argv)

    def record_human_reply(
        self,
        handoff: Handoff,
        reply: SlackReply,
        thread_ref: str,
    ) -> None:
        claim_id = self._claim_id(handoff)
        argv = [
            "record-human-reply",
            handoff.task_id,
            "--claim-id",
            claim_id,
            "--reply-file",
        ]
        with _temp_text_file(reply.text) as path:
            argv.append(str(path))
            argv.extend(["--thread-ref", thread_ref])
            if reply.ts:
                argv.extend(["--message-ts", reply.ts])
            if reply.user_id:
                argv.extend(["--author", reply.user_id])
            self._run_json(argv)

    def submit_brief(
        self,
        handoff: Handoff,
        brief: str,
        *,
        reason: str,
    ) -> None:
        claim_id = self._claim_id(handoff)
        with _temp_text_file(brief) as path:
            self._run_json(
                [
                    "submit-brief",
                    handoff.task_id,
                    "--claim-id",
                    claim_id,
                    "--brief-file",
                    str(path),
                    "--reason",
                    reason,
                ]
            )

    def complete_claim(self, handoff: Handoff) -> None:
        # Quay submit-brief completes the claimed handoff transactionally.
        return None

    def release_claim(self, handoff: Handoff, reason: str) -> None:
        claim_id = self._claim_id(handoff)
        self._run_json(
            [
                "task",
                "release-claim",
                handoff.task_id,
                "--claim-id",
                claim_id,
            ]
        )

    def _claim_id(self, handoff: Handoff) -> str:
        if handoff.claim_id:
            return handoff.claim_id
        raw = handoff.metadata.get("claim_id")
        if isinstance(raw, str) and raw:
            return raw
        raise RuntimeError(f"handoff {handoff.handoff_id} has no claim_id")

    def _run_json(self, argv: list[str]) -> Any:
        text = self._run_text(argv)
        if not text.strip():
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"quay returned invalid JSON for {' '.join(argv)}") from exc

    def _run_text(self, argv: list[str]) -> str:
        full_argv = [self.command, *argv]
        proc = self._runner(
            full_argv,
            check=False,
            capture_output=True,
            text=True,
        )
        stdout = getattr(proc, "stdout", "") or ""
        stderr = getattr(proc, "stderr", "") or ""
        returncode = int(getattr(proc, "returncode", 0))
        if returncode != 0:
            raise QuayCommandError(
                full_argv,
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
                code=_cli_error_code(stderr),
            )
        return stdout

    def _handoff_metadata(self, row: Mapping[str, Any]) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        payload = row.get("payload_json")
        if isinstance(payload, str) and payload:
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = {"raw_payload_json": payload}
            if isinstance(parsed, Mapping):
                metadata.update(dict(parsed))
            else:
                metadata["payload"] = parsed
        return metadata


class HermesConversationDecider:
    """LLM-backed BRIX orchestrator decision step.

    Humans talk normally in Slack. This component decides whether that
    discussion has enough information to resume the worker, or whether the
    orchestrator should ask/confirm one more thing.
    """

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        agent_factory: Any | None = None,
        runtime_resolver: Any | None = None,
        default_model_resolver: Any | None = None,
        model_catalog_resolver: Any | None = None,
        model_normalizer: Any | None = None,
        agent_class: Any | None = None,
    ) -> None:
        self._logger = logger or LOGGER
        self._agent_factory = agent_factory
        self._runtime_resolver = runtime_resolver
        self._default_model_resolver = default_model_resolver
        self._model_catalog_resolver = model_catalog_resolver
        self._model_normalizer = model_normalizer
        self._agent_class = agent_class

    def decide(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        replies: list[SlackReply],
        posted_messages: list[str],
    ) -> ConversationDecision:
        if not replies:
            return ConversationDecision(action="wait")

        agent = self._build_agent(handoff)
        prompt = build_orchestrator_decision_prompt(
            handoff=handoff,
            task=task,
            artifact=artifact,
            question=question,
            replies=replies,
            posted_messages=posted_messages,
        )
        result = agent.run_conversation(
            prompt,
            system_message=ORCHESTRATOR_DECISION_SYSTEM,
            conversation_history=[],
            task_id=f"brix-orchestrator:{handoff.task_id}",
        )
        text = ""
        if isinstance(result, Mapping):
            text = str(result.get("final_response") or "")
        else:
            text = str(result or "")
        decision = parse_decision_response(text)
        log_event(
            self._logger,
            "orchestrator_conversation_decision",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            action=decision.action,
        )
        return decision

    def _build_agent(self, handoff: Handoff) -> Any:
        if self._agent_factory is not None:
            return self._agent_factory()
        runtime = self._resolve_runtime()
        agent_class = self._agent_class
        if agent_class is None:
            from run_agent import AIAgent

            agent_class = AIAgent

        return agent_class(
            model=self._resolve_model(runtime),
            api_key=runtime.get("api_key"),
            base_url=runtime.get("base_url"),
            provider=runtime.get("provider"),
            api_mode=runtime.get("api_mode"),
            acp_command=runtime.get("command"),
            acp_args=list(runtime.get("args") or []),
            credential_pool=runtime.get("credential_pool"),
            max_iterations=2,
            quiet_mode=True,
            enabled_toolsets=[],
            skip_context_files=True,
            skip_memory=True,
            platform="brix-orchestrator",
            session_id=orchestrator_session_id(handoff),
        )

    def _resolve_runtime(self) -> Mapping[str, Any]:
        resolver = self._runtime_resolver
        if resolver is None:
            from hermes_cli.runtime_provider import resolve_runtime_provider

            resolver = resolve_runtime_provider
        requested = os.getenv("BRIX_ORCHESTRATOR_PROVIDER", "").strip() or None
        runtime = resolver(requested=requested)
        if not isinstance(runtime, Mapping):
            raise RuntimeError("orchestrator provider resolver returned invalid runtime")
        return runtime

    def _resolve_model(self, runtime: Mapping[str, Any]) -> str:
        provider = str(runtime.get("provider") or "").strip()
        runtime_model = runtime.get("model")
        if isinstance(runtime_model, str) and runtime_model.strip():
            resolved = self._provider_compatible_model(runtime_model, provider)
            if resolved:
                return resolved

        try:
            from hermes_cli.config import load_config

            cfg = load_config()
            model_cfg = cfg.get("model") if isinstance(cfg, Mapping) else {}
            if isinstance(model_cfg, Mapping):
                configured_provider = str(model_cfg.get("provider") or "").strip().lower()
                if configured_provider == "auto":
                    configured_provider = ""
                runtime_provider = provider.lower()
                configured = (
                    str(model_cfg.get("default") or model_cfg.get("model") or "")
                    .strip()
                )
                if (
                    configured
                    and (not configured_provider or configured_provider == runtime_provider)
                ):
                    resolved = self._provider_compatible_model(configured, provider)
                    if resolved:
                        return resolved
        except Exception:
            pass

        return self._default_model_for_provider(provider)

    def _provider_compatible_model(self, model: str, provider: str) -> str:
        model = str(model or "").strip()
        if not model:
            return ""
        if not provider:
            return model
        normalized = self._normalize_model_for_provider(model, provider)
        if not normalized:
            return ""
        if self._model_in_provider_catalog(normalized, provider):
            return normalized
        return ""

    def _normalize_model_for_provider(self, model: str, provider: str) -> str:
        normalizer = self._model_normalizer
        if normalizer is None:
            from hermes_cli.model_normalize import normalize_model_for_provider

            normalizer = normalize_model_for_provider
        try:
            return str(normalizer(model, provider) or "").strip()
        except Exception:
            return str(model or "").strip()

    def _model_in_provider_catalog(self, model: str, provider: str) -> bool:
        resolver = self._model_catalog_resolver
        if resolver is None:
            from hermes_cli.models import provider_model_ids

            resolver = provider_model_ids
        try:
            catalog = [str(item).strip() for item in (resolver(provider) or [])]
        except Exception:
            return True
        catalog = [item for item in catalog if item]
        if not catalog:
            return True
        model_lower = model.lower()
        return any(item.lower() == model_lower for item in catalog)

    def _default_model_for_provider(self, provider: str) -> str:
        if not provider:
            return ""
        resolver = self._default_model_resolver
        if resolver is None:
            from hermes_cli.models import get_default_model_for_provider

            resolver = get_default_model_for_provider
        try:
            return str(resolver(provider) or "").strip()
        except Exception:
            return ""


class SlackWebApiClient:
    """Small stdlib Slack Web API client used by the standalone runner."""

    def __init__(self, token: str, logger: logging.Logger | None = None) -> None:
        if not token:
            raise ValueError("Slack token is required")
        self._token = token
        self._logger = logger or LOGGER

    def validate_thread(self, channel_id: str, thread_ts: str) -> SlackPostRef:
        data = self._api(
            "conversations.replies",
            {"channel": channel_id, "ts": thread_ts, "limit": 200},
        )
        messages = data.get("messages") or []
        if not messages:
            raise SlackApiError("conversations.replies", "thread_not_found")
        floor_ts = thread_ts
        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            ts = str(msg.get("ts") or "")
            if not ts:
                continue
            if msg.get("subtype") == "bot_message" or msg.get("bot_id"):
                if _slack_ts_key(ts) > _slack_ts_key(floor_ts):
                    floor_ts = ts
        return SlackPostRef(channel_id=channel_id, ts=floor_ts, thread_ts=thread_ts)

    def post_question(
        self,
        channel_id: str,
        text: str,
        *,
        thread_ts: str | None = None,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        payload = {
            "channel": channel_id,
            "text": text,
            "thread_ts": thread_ts,
            "unfurl_links": False,
            "unfurl_media": False,
            "metadata": {
                "event_type": "brix_orchestrator_handoff",
                "event_payload": {
                    "handoff_id": handoff.handoff_id,
                    "task_id": task.task_id,
                },
            },
        }
        data = self._api("chat.postMessage", payload)
        ts = str(data.get("ts") or "")
        if not ts:
            raise RuntimeError("Slack chat.postMessage returned no ts")
        return SlackPostRef(channel_id=channel_id, ts=ts, thread_ts=thread_ts or ts)

    def wait_for_reply(
        self,
        ref: SlackPostRef,
        *,
        after_ts: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
        deadline = time.monotonic() + timeout_seconds
        floor_ts = after_ts or ref.ts
        while time.monotonic() <= deadline:
            replies = self._api(
                "conversations.replies",
                {"channel": ref.channel_id, "ts": ref.thread_ts},
            )
            for msg in replies.get("messages") or []:
                if not isinstance(msg, Mapping):
                    continue
                ts = str(msg.get("ts") or "")
                if not ts or _slack_ts_key(ts) <= _slack_ts_key(floor_ts):
                    continue
                if msg.get("subtype") == "bot_message" or msg.get("bot_id"):
                    continue
                text = str(msg.get("text") or "").strip()
                if not text:
                    continue
                return SlackReply(
                    text=text,
                    user_id=str(msg.get("user") or ""),
                    ts=ts,
                    permalink=_optional_str(msg.get("permalink")),
                )
            time.sleep(poll_interval_seconds)
        return None

    def post_thread_message(
        self,
        ref: SlackPostRef,
        text: str,
        *,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        data = self._api(
            "chat.postMessage",
            {
                "channel": ref.channel_id,
                "text": text,
                "thread_ts": ref.thread_ts,
                "unfurl_links": False,
                "unfurl_media": False,
            },
        )
        ts = str(data.get("ts") or "")
        if not ts:
            raise SlackApiError("chat.postMessage", "missing_ts")
        return SlackPostRef(channel_id=ref.channel_id, ts=ts, thread_ts=ref.thread_ts)

    def _api(self, method: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = urllib.parse.urlencode(
            {
                k: json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v)
                for k, v in payload.items()
                if v is not None
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"https://slack.com/api/{method}",
            data=body,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise SlackApiError(method, f"transport_error:{exc}") from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Slack API {method} returned invalid JSON") from exc
        if not isinstance(parsed, dict) or not parsed.get("ok"):
            err = parsed.get("error") if isinstance(parsed, dict) else "invalid_response"
            raise SlackApiError(method, str(err or "unknown_error"))
        return parsed


class MissingSlackClient:
    def __init__(self, token_env: str) -> None:
        self.token_env = token_env

    def validate_thread(self, channel_id: str, thread_ts: str) -> SlackPostRef:
        raise RuntimeError(f"Slack token env var {self.token_env} is not set")

    def post_question(
        self,
        channel_id: str,
        text: str,
        *,
        thread_ts: str | None = None,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        raise RuntimeError(f"Slack token env var {self.token_env} is not set")

    def wait_for_reply(
        self,
        ref: SlackPostRef,
        *,
        after_ts: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
        raise RuntimeError(f"Slack token env var {self.token_env} is not set")

    def post_thread_message(
        self,
        ref: SlackPostRef,
        text: str,
        *,
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        raise RuntimeError(f"Slack token env var {self.token_env} is not set")


class FileLock:
    """Non-blocking advisory lock around the singleton drain loop."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Any = None

    def __enter__(self) -> "FileLock":
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._handle.close()
            self._handle = None
            raise LockBusy(str(self.path)) from exc
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(
            json.dumps({"pid": os.getpid(), "started_at": time.time()}, sort_keys=True)
            + "\n"
        )
        self._handle.flush()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        import fcntl

        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


class HandoffDrainer:
    def __init__(
        self,
        *,
        quay: QuayClient,
        slack: SlackQuestionClient,
        config: OrchestratorConfig,
        worker_id: str,
        decider: ConversationDecider | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.quay = quay
        self.slack = slack
        self.config = config
        self.worker_id = worker_id
        self.decider = decider or HermesConversationDecider(logger=logger or LOGGER)
        self.logger = logger or LOGGER
        self.metrics = RunMetrics()

    def drain_one(self) -> DrainResult:
        handoff = self.quay.claim_handoff(self.worker_id)
        if handoff is None:
            self.metrics.no_handoff += 1
            log_event(self.logger, "drain_no_handoff", worker_id=self.worker_id)
            return DrainResult(status="no_handoff", metrics=self.metrics.as_dict())
        self.metrics.handoffs_claimed += 1
        log_event(
            self.logger,
            "handoff_claimed",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
        )

        try:
            result = self._handle_claimed_handoff(handoff)
        except SlackApiError as exc:
            self.metrics.errors += 1
            reason = f"slack_api_error:{exc.method}:{exc.error}"
            try:
                self.quay.release_claim(handoff, reason=reason)
                self.metrics.claims_released += 1
            except Exception:
                self.logger.exception("failed to release claim after Slack API error")
            log_event(
                self.logger,
                "claim_released",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                reason=reason,
                slack_method=exc.method,
                slack_error=exc.error,
            )
            return DrainResult(
                status="slack_api_error",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )
        except Exception as exc:
            self.metrics.errors += 1
            reason = f"runner_error: {type(exc).__name__}: {exc}"
            try:
                self.quay.release_claim(handoff, reason=reason)
                self.metrics.claims_released += 1
            except Exception:
                self.logger.exception("failed to release claim after runner error")
            raise
        return result

    def _handle_claimed_handoff(self, handoff: Handoff) -> DrainResult:
        task = self.quay.get_task_context(handoff.task_id)
        artifact = self.quay.get_artifact(handoff) if handoff.artifact_id else None

        brief = choose_direct_brief(handoff)
        if brief:
            self.quay.submit_brief(handoff, brief, reason="blocker_resolved")
            self.quay.complete_claim(handoff)
            self.metrics.direct_briefs_submitted += 1
            log_event(
                self.logger,
                "brief_submitted_direct",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
            )
            return DrainResult(
                status="submitted_direct",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )

        route = resolve_slack_route(handoff, task, self.config)
        if route is None:
            reason = "missing_default_slack_channel"
            self.quay.release_claim(handoff, reason=reason)
            self.metrics.claims_released += 1
            log_event(
                self.logger,
                "claim_released",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                reason=reason,
            )
            return DrainResult(
                status=reason,
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )

        question = build_human_question(handoff, task, artifact)
        question_result = self._post_question_and_wait(
            handoff=handoff,
            task=task,
            artifact=artifact,
            question=question,
            route=route,
        )
        if question_result is None:
            return self._handle_stale_slack_route(
                handoff=handoff,
                task=task,
                artifact=artifact,
                question=question,
                stale_route=route,
            )

        post_ref, thread_ref, reply, decision = question_result
        if reply is None or decision is None:
            reason = "human_reply_timeout"
            self.quay.release_claim(handoff, reason=reason)
            self.metrics.claims_released += 1
            log_event(
                self.logger,
                "claim_released",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                thread_ref=thread_ref,
                reason=reason,
            )
            return DrainResult(
                status=reason,
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )

        return self._submit_human_resume(
            handoff=handoff,
            task=task,
            post_ref=post_ref,
            thread_ref=thread_ref,
            reply=reply,
            decision=decision,
        )

    def _post_question_and_wait(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        route: SlackRoute,
    ) -> tuple[SlackPostRef, str, SlackReply | None, ConversationDecision | None] | None:
        prepared = self._prepare_thread_before_escalation(
            handoff=handoff,
            task=task,
            question=question,
            route=route,
        )
        if prepared is None:
            return None

        post_ref, posted_question = prepared
        thread_ref = slack_thread_ref(post_ref)
        self.quay.escalate_human(handoff, question, thread_ref)
        if posted_question:
            self.metrics.slack_questions_posted += 1
            log_event(
                self.logger,
                "slack_question_posted",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                channel_id=post_ref.channel_id,
                thread_ts=post_ref.thread_ts,
                route_source=route.source,
            )
        else:
            log_event(
                self.logger,
                "slack_thread_attached",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                channel_id=post_ref.channel_id,
                thread_ts=post_ref.thread_ts,
                route_source=route.source,
            )

        reply, decision = self._drive_human_discussion(
            handoff=handoff,
            task=task,
            artifact=artifact,
            question=question,
            post_ref=post_ref,
        )
        return post_ref, thread_ref, reply, decision

    def _prepare_thread_before_escalation(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        question: str,
        route: SlackRoute,
    ) -> tuple[SlackPostRef, bool] | None:
        try:
            if route.thread_ref:
                thread_ts = route.thread_ts or ""
                return self.slack.validate_thread(route.channel_id, thread_ts), False
            post_ref = self.slack.post_question(
                route.channel_id,
                question,
                thread_ts=route.thread_ts,
                handoff=handoff,
                task=task,
            )
            return post_ref, True
        except SlackApiError as exc:
            if exc.error == "thread_not_found" and route_is_metadata(route):
                log_event(
                    self.logger,
                    "slack_route_stale",
                    handoff_id=handoff.handoff_id,
                    task_id=handoff.task_id,
                    route_source=route.source,
                    channel_id=route.channel_id,
                    thread_ts=route.thread_ts,
                    slack_method=exc.method,
                    slack_error=exc.error,
                )
                return None
            raise

    def _drive_human_discussion(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        post_ref: SlackPostRef,
    ) -> tuple[SlackReply | None, ConversationDecision | None]:
        replies: list[SlackReply] = []
        posted_messages: list[str] = []
        decision_error_prompted = False
        after_ts = post_ref.ts
        deadline = time.monotonic() + self.config.reply_timeout_seconds

        while time.monotonic() <= deadline:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            reply = self.slack.wait_for_reply(
                post_ref,
                after_ts=after_ts,
                timeout_seconds=remaining,
                poll_interval_seconds=self.config.poll_interval_seconds,
            )
            if reply is None:
                return None, None

            replies.append(reply)
            after_ts = reply.ts or after_ts
            try:
                decision = normalize_decision(
                    self.decider.decide(
                        handoff=handoff,
                        task=task,
                        artifact=artifact,
                        question=question,
                        replies=list(replies),
                        posted_messages=list(posted_messages),
                    )
                )
            except Exception as exc:
                self.metrics.errors += 1
                log_event(
                    self.logger,
                    "orchestrator_decision_error",
                    handoff_id=handoff.handoff_id,
                    task_id=handoff.task_id,
                    reply_ts=reply.ts,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                if not decision_error_prompted:
                    message = decision_error_retry_message()
                    self.slack.post_thread_message(
                        post_ref,
                        message,
                        handoff=handoff,
                        task=task,
                    )
                    posted_messages.append(message)
                    decision_error_prompted = True
                    log_event(
                        self.logger,
                        "orchestrator_decision_retry_prompt_posted",
                        handoff_id=handoff.handoff_id,
                        task_id=handoff.task_id,
                        reply_ts=reply.ts,
                    )
                continue
            if decision.action == "ready" and decision.brief.strip():
                return reply, decision
            if decision.action == "ask" and decision.message.strip():
                message = decision.message.strip()
                self.slack.post_thread_message(
                    post_ref,
                    message,
                    handoff=handoff,
                    task=task,
                )
                posted_messages.append(message)
                log_event(
                    self.logger,
                    "orchestrator_followup_posted",
                    handoff_id=handoff.handoff_id,
                    task_id=handoff.task_id,
                    reply_ts=reply.ts,
                )
                continue
            log_event(
                self.logger,
                "orchestrator_waiting_for_more_context",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                reply_ts=reply.ts,
                action=decision.action,
            )

        return None, None

    def _handle_stale_slack_route(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        stale_route: SlackRoute,
    ) -> DrainResult:
        fallback_channel = (self.config.default_slack_channel or "").strip()
        if fallback_channel:
            fallback_route = SlackRoute(
                channel_id=fallback_channel,
                thread_ts=None,
                source="fallback_channel",
            )
            log_event(
                self.logger,
                "slack_route_fallback",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                stale_route_source=stale_route.source,
                stale_channel_id=stale_route.channel_id,
                stale_thread_ts=stale_route.thread_ts,
                fallback_channel_id=fallback_channel,
                reason="stale_slack_thread_ref",
            )
            question_result = self._post_question_and_wait(
                handoff=handoff,
                task=task,
                artifact=artifact,
                question=question,
                route=fallback_route,
            )
            if question_result is None:
                raise RuntimeError("fallback Slack route was unexpectedly stale")
            post_ref, thread_ref, reply, decision = question_result
            if reply is None or decision is None:
                reason = "human_reply_timeout"
                self.quay.release_claim(handoff, reason=reason)
                self.metrics.claims_released += 1
                log_event(
                    self.logger,
                    "claim_released",
                    handoff_id=handoff.handoff_id,
                    task_id=handoff.task_id,
                    thread_ref=thread_ref,
                    reason=reason,
                    route_source=fallback_route.source,
                    channel_id=post_ref.channel_id,
                    thread_ts=post_ref.thread_ts,
                )
                return DrainResult(
                    status=reason,
                    handoff_id=handoff.handoff_id,
                    task_id=handoff.task_id,
                    metrics=self.metrics.as_dict(),
                )
            return self._submit_human_resume(
                handoff=handoff,
                task=task,
                post_ref=post_ref,
                thread_ref=thread_ref,
                reply=reply,
                decision=decision,
            )

        reason = "stale_slack_thread_ref"
        self.quay.release_claim(handoff, reason=reason)
        self.metrics.claims_released += 1
        log_event(
            self.logger,
            "claim_released",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            reason=reason,
            route_source=stale_route.source,
            channel_id=stale_route.channel_id,
            thread_ts=stale_route.thread_ts,
        )
        return DrainResult(
            status=reason,
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            metrics=self.metrics.as_dict(),
        )

    def _submit_human_resume(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        post_ref: SlackPostRef,
        thread_ref: str,
        reply: SlackReply,
        decision: ConversationDecision,
    ) -> DrainResult:
        self.metrics.human_replies_ingested += 1
        self.quay.record_human_reply(handoff, reply, thread_ref)
        accepted_brief = decision.brief.strip()
        if not accepted_brief:
            raise RuntimeError("orchestrator marked discussion ready without a worker brief")
        next_brief = human_reply_to_brief(
            reply,
            accepted_brief=accepted_brief,
            handoff=handoff,
            task=task,
        )
        self.quay.submit_brief(handoff, next_brief, reason="advice_answered")
        self.quay.complete_claim(handoff)
        self.metrics.human_briefs_submitted += 1
        self._post_resume_ack(
            post_ref=post_ref,
            handoff=handoff,
            task=task,
            reply=reply,
            accepted_brief=accepted_brief,
        )
        log_event(
            self.logger,
            "brief_submitted_from_human_reply",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            reply_ts=reply.ts,
            user_id=reply.user_id,
        )
        return DrainResult(
            status="submitted_from_human_reply",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            metrics=self.metrics.as_dict(),
        )

    def _post_resume_ack(
        self,
        *,
        post_ref: SlackPostRef,
        handoff: Handoff,
        task: TaskContext,
        reply: SlackReply,
        accepted_brief: str,
    ) -> None:
        ack = build_resume_ack(task=task, reply=reply, accepted_brief=accepted_brief)
        try:
            self.slack.post_thread_message(
                post_ref,
                ack,
                handoff=handoff,
                task=task,
            )
        except Exception as exc:
            log_event(
                self.logger,
                "slack_ack_failed",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                channel_id=post_ref.channel_id,
                thread_ts=post_ref.thread_ts,
                error_type=type(exc).__name__,
                error=str(exc),
            )


def choose_direct_brief(handoff: Handoff) -> str | None:
    if handoff.next_brief and handoff.next_brief.strip():
        return handoff.next_brief.strip()
    for key in ("next_brief", "brief", "proposed_brief"):
        val = handoff.metadata.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def resolve_author_mention(handoff: Handoff, task: TaskContext) -> str | None:
    user_id = _find_slack_user_id(task.metadata) or _find_slack_user_id(handoff.metadata)
    if not user_id:
        return None
    return f"<@{user_id}>"


def _task_metadata_with_authors(
    raw: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(metadata)
    authors = result.get("authors", raw.get("authors"))
    if authors is None:
        authors = _parse_json_value(result.get("authors_json") or raw.get("authors_json"))
    elif isinstance(authors, str):
        parsed_authors = _parse_json_value(authors)
        if parsed_authors is not None:
            authors = parsed_authors
    if authors is not None:
        result["authors"] = authors
    return result


def _parse_json_value(value: Any) -> Any:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _find_slack_user_id(value: Any) -> str | None:
    if isinstance(value, str):
        found = _extract_slack_user_id(value)
        if found:
            return found
        parsed = _parse_json_value(value)
        if parsed is not None:
            return _find_slack_user_id(parsed)
        return None
    if isinstance(value, Mapping):
        preferred_keys = (
            "contributor_slack_id",
            "contributor_slack_user_id",
            "author_slack_id",
            "author_slack_user_id",
            "requester_slack_id",
            "requester_slack_user_id",
            "creator_slack_id",
            "creator_slack_user_id",
            "slack_user_id",
            "slack_id",
            "author.slack_id",
            "authors_json",
        )
        for key in preferred_keys:
            found = _find_slack_user_id(value.get(key))
            if found:
                return found
        for key in (
            "contributor",
            "author",
            "requester",
            "creator",
            "created_by",
            "submitted_by",
            "submitter",
            "source_author",
            "authors",
            "contributors",
            "slack",
            "ticket",
            "source",
            "flash",
        ):
            found = _find_slack_user_id(value.get(key))
            if found:
                return found
        for key, nested in value.items():
            key_text = str(key).lower()
            if any(token in key_text for token in ("author", "contributor", "requester", "creator", "slack")):
                found = _find_slack_user_id(nested)
                if found:
                    return found
    if isinstance(value, list):
        for item in value:
            found = _find_slack_user_id(item)
            if found:
                return found
    return None


def _extract_slack_user_id(text: str) -> str | None:
    raw = (text or "").strip()
    if raw.startswith("<@") and raw.endswith(">"):
        raw = raw[2:-1].split("|", 1)[0].strip()
    for prefix in ("U", "W"):
        if raw.startswith(prefix) and len(raw) >= 7 and raw.replace("_", "").isalnum():
            return raw
    marker = "<@"
    start = raw.find(marker)
    if start >= 0:
        end = raw.find(">", start + len(marker))
        if end > start:
            candidate = raw[start + len(marker) : end].split("|", 1)[0].strip()
            if candidate.startswith(("U", "W")) and len(candidate) >= 7:
                return candidate
    return None


def build_human_question(
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
) -> str:
    if handoff.human_question and handoff.human_question.strip():
        return handoff.human_question.strip()
    meta_question = handoff.metadata.get("human_question")
    if isinstance(meta_question, str) and meta_question.strip():
        return meta_question.strip()

    lines = [
        "A Quay task needs human guidance before the next worker brief.",
        f"Task: {task.title or task.task_id}",
    ]
    author_mention = resolve_author_mention(handoff, task)
    if author_mention:
        lines.append(f"Contributor: {author_mention}")
    if task.issue:
        lines.append(f"Issue: {task.issue}")
    if task.repo_id or handoff.repo_id:
        lines.append(f"Repo: {task.repo_id or handoff.repo_id}")
    if handoff.summary:
        lines.extend(["", "Handoff summary:", handoff.summary.strip()])
    if artifact and artifact.text.strip():
        lines.extend(["", "Blocker/context:", artifact.text.strip()])
    lines.extend(
        [
            "",
            "Discuss freely in this thread. I will resume the worker once the path is clear.",
        ]
    )
    return "\n".join(lines)


ORCHESTRATOR_DECISION_SYSTEM = (
    "You are the BRIX orchestrator deciding whether a blocked worker can resume. "
    "Read the Slack discussion and return only JSON. "
    "Use action='ready' only when the worker can continue with a clear, "
    "self-contained brief. Use action='ask' when there is remaining doubt; "
    "message must summarize the worker-facing instruction you would send and ask "
    "for confirmation or the missing detail without mentioning Quay internals. "
    "Use action='wait' when the latest message does not add enough signal. "
    "Schema: {\"action\":\"wait\"} or {\"action\":\"ask\",\"message\":\"...\"} "
    "or {\"action\":\"ready\",\"brief\":\"...\"}."
)


def build_orchestrator_decision_prompt(
    *,
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
    question: str,
    replies: list[SlackReply],
    posted_messages: list[str],
) -> str:
    payload = {
        "task": {
            "task_id": task.task_id,
            "title": task.title,
            "issue": task.issue,
            "repo_id": task.repo_id or handoff.repo_id,
        },
        "handoff": {
            "handoff_id": handoff.handoff_id,
            "reason": handoff.reason,
            "summary": handoff.summary,
        },
        "question_posted_to_humans": question,
        "blocker_context": artifact.text if artifact else "",
        "orchestrator_messages_already_posted": posted_messages,
        "human_replies": [
            {
                "text": reply.text,
                "user_id": reply.user_id,
                "ts": reply.ts,
                "permalink": reply.permalink,
            }
            for reply in replies
        ],
    }
    return (
        "Decide the next orchestrator action for this blocked worker discussion.\n"
        "Return only the JSON object described in the system message.\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
    )


def parse_decision_response(text: str) -> ConversationDecision:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = _strip_json_fence(raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return ConversationDecision(action="wait")
        else:
            return ConversationDecision(action="wait")
    if not isinstance(data, Mapping):
        return ConversationDecision(action="wait")
    return normalize_decision(
        ConversationDecision(
            action=str(data.get("action") or "wait"),
            brief=str(data.get("brief") or ""),
            message=str(data.get("message") or ""),
        )
    )


def _strip_json_fence(text: str) -> str:
    lines = text.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def normalize_decision(decision: ConversationDecision) -> ConversationDecision:
    action = (decision.action or "wait").strip().lower()
    if action in {"ready", "resume", "continue"}:
        return ConversationDecision(action="ready", brief=decision.brief.strip())
    if action in {"ask", "question", "confirm"}:
        return ConversationDecision(action="ask", message=decision.message.strip())
    return ConversationDecision(action="wait")


def human_reply_to_brief(
    reply: SlackReply,
    *,
    accepted_brief: str,
    handoff: Handoff,
    task: TaskContext,
) -> str:
    header = [
        "Use the following human guidance as the next worker brief.",
        f"Task: {task.title or task.task_id}",
    ]
    if task.issue:
        header.append(f"Issue: {task.issue}")
    if reply.user_id or reply.ts:
        who = f"<@{reply.user_id}>" if reply.user_id else "human"
        suffix = f" at {reply.ts}" if reply.ts else ""
        header.append(f"Human reply: {who}{suffix}")
    if reply.permalink:
        header.append(f"Slack reference: {reply.permalink}")
    return "\n".join(header + ["", accepted_brief.strip()])


def build_resume_ack(
    *,
    task: TaskContext,
    reply: SlackReply,
    accepted_brief: str,
) -> str:
    target = task.issue or task.title or task.task_id
    source = f"<@{reply.user_id}>" if reply.user_id else "the Slack thread"
    if reply.ts:
        source = f"{source} at {reply.ts}"
    preview = accepted_brief.strip()
    if len(preview) > 500:
        preview = preview[:497].rstrip() + "..."
    return (
        f"Quay resumed for {target} from {source}.\n\n"
        f"Accepted brief:\n> {preview}"
    )


def decision_error_retry_message() -> str:
    return (
        "I received the reply, but my decision engine hit an internal "
        "orchestrator error. No need to rephrase; I will retry this thread "
        "when the runtime is healthy."
    )


def orchestrator_session_id(handoff: Handoff) -> str:
    """Return a short stable session id for provider prompt-cache keys."""
    raw = ":".join(
        [
            handoff.task_id,
            handoff.claim_id or "",
            handoff.handoff_id,
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"brix-orch-{digest}"


def slack_thread_ref(ref: SlackPostRef) -> str:
    return f"{ref.channel_id}:{ref.thread_ts}"


def route_is_metadata(route: SlackRoute) -> bool:
    return route.source.startswith(("task.", "handoff."))


def resolve_slack_route(
    handoff: Handoff,
    task: TaskContext,
    config: OrchestratorConfig,
) -> SlackRoute | None:
    route = _route_from_metadata(task.metadata, source="task")
    if route is not None:
        return route
    route = _route_from_metadata(handoff.metadata, source="handoff")
    if route is not None:
        return route
    channel_id = (config.default_slack_channel or "").strip()
    if channel_id:
        return SlackRoute(channel_id=channel_id, source="fallback_channel")
    return None


def _route_from_metadata(metadata: Mapping[str, Any], *, source: str) -> SlackRoute | None:
    keys = (
        "slack_thread_ref",
        "thread_ref",
        "original_slack_thread_ref",
        "source_slack_thread_ref",
    )
    for key in keys:
        route = _coerce_slack_route(metadata.get(key), source=f"{source}.{key}")
        if route is not None:
            return route

    nested_keys = (
        "slack",
        "slack_thread",
        "original_slack_thread",
        "source_slack_thread",
    )
    for key in nested_keys:
        route = _coerce_slack_route(metadata.get(key), source=f"{source}.{key}")
        if route is not None:
            return route
    return None


def _coerce_slack_route(value: Any, *, source: str) -> SlackRoute | None:
    if isinstance(value, str):
        parsed = _parse_slack_thread_ref(value)
        if parsed is None:
            return None
        channel_id, thread_ts = parsed
        return SlackRoute(channel_id=channel_id, thread_ts=thread_ts, source=source)
    if not isinstance(value, Mapping):
        return None

    for key in ("slack_thread_ref", "thread_ref", "original_thread_ref"):
        route = _coerce_slack_route(value.get(key), source=f"{source}.{key}")
        if route is not None:
            return route

    channel = value.get("channel_id") or value.get("channel")
    thread = value.get("thread_ts") or value.get("ts")
    if isinstance(channel, str) and isinstance(thread, str):
        channel = channel.strip()
        thread = thread.strip()
        if channel and thread:
            return SlackRoute(channel_id=channel, thread_ts=thread, source=source)
    return None


def _parse_slack_thread_ref(value: str) -> tuple[str, str] | None:
    left, sep, right = value.strip().partition(":")
    if not sep:
        return None
    channel_id = left.strip()
    thread_ts = right.strip()
    if not channel_id or not thread_ts:
        return None
    return channel_id, thread_ts


def load_config(path: Path | None) -> OrchestratorConfig:
    raw: dict[str, Any] = {}
    if path and path.is_file():
        with path.open("r", encoding="utf-8") as f:
            parsed = json.load(f)
        if not isinstance(parsed, dict):
            raise ValueError(f"{path} must contain a JSON object")
        raw = parsed
    return OrchestratorConfig.from_mapping(raw).with_env_overrides()


def default_config_path() -> Path:
    if os.getenv("BRIX_ORCHESTRATOR_CONFIG"):
        return Path(os.environ["BRIX_ORCHESTRATOR_CONFIG"])
    home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")
    return home / "quay" / "orchestrator.json"


def default_lock_path(config: OrchestratorConfig) -> Path:
    if config.lock_path is not None:
        return config.lock_path
    home = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")
    return home / "quay" / "orchestrator.lock"


def build_slack_client(config: OrchestratorConfig) -> SlackQuestionClient:
    token = os.getenv(config.slack_token_env) or os.getenv("SLACK_TOKEN") or ""
    if not token:
        return MissingSlackClient(config.slack_token_env)
    return SlackWebApiClient(token=token, logger=LOGGER)


def build_quay_client(config: OrchestratorConfig) -> QuayClient:
    return QuayCliClient(command=config.quay_command, logger=LOGGER)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    record = {"event": event, **fields}
    logger.info(json.dumps(record, sort_keys=True))


def run_drain_one(args: argparse.Namespace) -> int:
    setup_logging(verbose=args.verbose)
    config = load_config(Path(args.config) if args.config else default_config_path())
    if not config.enabled and not args.ignore_disabled:
        result = DrainResult(status="disabled", metrics=RunMetrics().as_dict())
        print(json.dumps(dataclasses.asdict(result), sort_keys=True))
        return 0

    metrics = RunMetrics()
    try:
        lock_path = Path(args.lock_path) if args.lock_path else default_lock_path(config)
        with FileLock(lock_path):
            drainer = HandoffDrainer(
                quay=build_quay_client(config),
                slack=build_slack_client(config),
                config=config,
                worker_id=args.worker_id,
                logger=LOGGER,
            )
            result = drainer.drain_one()
    except LockBusy:
        metrics.lock_busy += 1
        result = DrainResult(status="lock_busy", metrics=metrics.as_dict())
    print(json.dumps(dataclasses.asdict(result), sort_keys=True))
    return 0


def setup_logging(*, verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Drain BRIX/Quay orchestrator handoffs")
    sub = parser.add_subparsers(dest="command", required=True)

    p_drain = sub.add_parser("drain-one", help="claim and process one handoff")
    p_drain.add_argument("--config", help="path to orchestrator JSON config")
    p_drain.add_argument("--lock-path", help="override the singleton lock path")
    p_drain.add_argument(
        "--worker-id",
        default=os.getenv("BRIX_ORCHESTRATOR_WORKER_ID") or f"brix-orchestrator:{os.uname().nodename}",
        help="claim owner id passed to the Quay adapter",
    )
    p_drain.add_argument(
        "--ignore-disabled",
        action="store_true",
        help="run even when config.enabled is false",
    )
    p_drain.add_argument("--verbose", action="store_true")
    p_drain.set_defaults(func=run_drain_one)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _artifact_kind_for_reason(reason: str) -> str | None:
    if reason == "worker_blocker":
        return "blocker"
    if reason == "human_reply_ingested":
        return "slack_reply"
    return None


def _summary_for_handoff(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    for key in ("summary", "message", "reason"):
        val = metadata.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    reason = str(row.get("reason") or "")
    if reason:
        return f"Quay handoff reason: {reason}"
    return ""


def _cli_error_code(stderr: str) -> str | None:
    first = stderr.strip().splitlines()[0] if stderr.strip() else ""
    if not first:
        return None
    try:
        parsed = json.loads(first)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, Mapping):
        return None
    code = parsed.get("error")
    return str(code) if isinstance(code, str) and code else None


@contextmanager
def _temp_text_file(text: str):
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        prefix="brix-orchestrator-",
        suffix=".md",
        delete=False,
    )
    path = Path(handle.name)
    try:
        with handle:
            handle.write(text)
        yield path
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _as_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


def _positive_float(value: Any, *, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _slack_ts_key(ts: str) -> tuple[int, int]:
    left, _, right = ts.partition(".")
    try:
        return int(left or "0"), int((right or "0").ljust(6, "0")[:6])
    except ValueError:
        return 0, 0


if __name__ == "__main__":
    raise SystemExit(main())
