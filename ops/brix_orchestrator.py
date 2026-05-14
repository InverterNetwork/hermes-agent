#!/usr/bin/env python3
"""BRIX orchestrator-side handoff drain loop.

BRIX owns the runner shape, locking, Slack question/reply flow, and how a human
reply becomes the next brief text. Quay owns durable task, claim, artifact, and
handoff state behind the CLI adapter below.
"""

from __future__ import annotations

import argparse
import dataclasses
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
        return cls(
            task_id=str(raw.get("task_id") or raw.get("id") or ""),
            title=str(raw.get("title") or ""),
            issue=str(raw.get("issue") or raw.get("issue_id") or ""),
            repo_id=_optional_str(raw.get("repo_id")),
            metadata=dict(metadata),
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
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
        """Poll the question thread and return the first human reply."""


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
        return TaskContext(
            task_id=str(raw.get("task_id") or task_id),
            title=str(raw.get("external_ref") or raw.get("branch_name") or task_id),
            issue=str(raw.get("external_ref") or ""),
            repo_id=_optional_str(raw.get("repo_id")),
            metadata=dict(raw),
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


class SlackWebApiClient:
    """Small stdlib Slack Web API client used by the standalone runner."""

    def __init__(self, token: str, logger: logging.Logger | None = None) -> None:
        if not token:
            raise ValueError("Slack token is required")
        self._token = token
        self._logger = logger or LOGGER

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
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() <= deadline:
            replies = self._api(
                "conversations.replies",
                {"channel": ref.channel_id, "ts": ref.thread_ts},
            )
            for msg in replies.get("messages") or []:
                if not isinstance(msg, Mapping):
                    continue
                ts = str(msg.get("ts") or "")
                if not ts or _slack_ts_key(ts) <= _slack_ts_key(ref.ts):
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
            raise RuntimeError(f"Slack API {method} failed: {exc}") from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Slack API {method} returned invalid JSON") from exc
        if not isinstance(parsed, dict) or not parsed.get("ok"):
            err = parsed.get("error") if isinstance(parsed, dict) else "invalid_response"
            raise RuntimeError(f"Slack API {method} returned error: {err}")
        return parsed


class MissingSlackClient:
    def __init__(self, token_env: str) -> None:
        self.token_env = token_env

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
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> SlackReply | None:
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
        logger: logging.Logger | None = None,
    ) -> None:
        self.quay = quay
        self.slack = slack
        self.config = config
        self.worker_id = worker_id
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
        if route.thread_ref:
            self.quay.escalate_human(handoff, question, route.thread_ref)
        post_ref = self.slack.post_question(
            route.channel_id,
            question,
            thread_ts=route.thread_ts,
            handoff=handoff,
            task=task,
        )
        thread_ref = slack_thread_ref(post_ref)
        if not route.thread_ref:
            self.quay.escalate_human(handoff, question, thread_ref)
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

        reply = self.slack.wait_for_reply(
            post_ref,
            timeout_seconds=self.config.reply_timeout_seconds,
            poll_interval_seconds=self.config.poll_interval_seconds,
        )
        if reply is None:
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

        self.metrics.human_replies_ingested += 1
        self.quay.record_human_reply(handoff, reply, thread_ref)
        next_brief = human_reply_to_brief(reply, handoff=handoff, task=task)
        self.quay.submit_brief(handoff, next_brief, reason="advice_answered")
        self.quay.complete_claim(handoff)
        self.metrics.human_briefs_submitted += 1
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


def choose_direct_brief(handoff: Handoff) -> str | None:
    if handoff.next_brief and handoff.next_brief.strip():
        return handoff.next_brief.strip()
    for key in ("next_brief", "brief", "proposed_brief"):
        val = handoff.metadata.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
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
    if task.issue:
        lines.append(f"Issue: {task.issue}")
    if task.repo_id or handoff.repo_id:
        lines.append(f"Repo: {task.repo_id or handoff.repo_id}")
    if handoff.summary:
        lines.extend(["", "Handoff summary:", handoff.summary.strip()])
    if artifact and artifact.text.strip():
        lines.extend(["", "Blocker/context:", artifact.text.strip()])
    lines.extend(["", "Reply in this thread with the guidance to use as the next brief."])
    return "\n".join(lines)


def human_reply_to_brief(
    reply: SlackReply,
    *,
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
    return "\n".join(header + ["", reply.text.strip()])


def slack_thread_ref(ref: SlackPostRef) -> str:
    return f"{ref.channel_id}:{ref.thread_ts}"


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
