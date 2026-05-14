#!/usr/bin/env python3
"""BRIX orchestrator-side handoff drain loop.

This file deliberately stops at the Quay adapter boundary. AST-121 owns the
final Quay CLI/API contract; BRIX owns the runner shape, locking, Slack
question/reply flow, and how a human reply becomes the next brief text.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol


LOGGER = logging.getLogger("brix.orchestrator")


class LockBusy(RuntimeError):
    """Raised when another orchestrator runner already owns the drain lock."""


@dataclass(frozen=True)
class Handoff:
    """A claimed orchestrator handoff.

    The field names here are BRIX-local and intentionally small. The AST-121
    adapter can map Quay's eventual JSON/API shape onto this object without
    leaking those details into the Slack/human loop.
    """

    handoff_id: str
    task_id: str
    repo_id: str | None = None
    artifact_id: str | None = None
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
    reply_timeout_seconds: float = 1800.0
    poll_interval_seconds: float = 15.0
    lock_path: Path | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "OrchestratorConfig":
        return cls(
            enabled=_as_bool(raw.get("enabled"), default=False),
            default_slack_channel=str(raw.get("default_slack_channel") or ""),
            slack_token_env=str(raw.get("slack_token_env") or "SLACK_BOT_TOKEN"),
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
            reply_timeout_seconds=timeout,
            poll_interval_seconds=interval,
            lock_path=Path(lock_env) if lock_env else self.lock_path,
        )


class QuayClient(Protocol):
    def claim_handoff(self, worker_id: str) -> Handoff | None:
        """Claim one durable handoff, or return None if no work exists."""

    def get_task_context(self, task_id: str) -> TaskContext:
        """Return enough task context for a human-readable Slack question."""

    def get_artifact(self, artifact_id: str) -> Artifact:
        """Return the blocker/context artifact referenced by a handoff."""

    def submit_brief(self, handoff: Handoff, brief: str) -> None:
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


class PendingQuayClient:
    """No-op adapter until AST-121 finalizes the Quay handoff contract."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or LOGGER

    def claim_handoff(self, worker_id: str) -> Handoff | None:
        log_event(
            self._logger,
            "quay_adapter_pending",
            worker_id=worker_id,
            dependency="AST-121",
        )
        return None

    def get_task_context(self, task_id: str) -> TaskContext:
        raise NotImplementedError("AST-121 must provide the Quay adapter")

    def get_artifact(self, artifact_id: str) -> Artifact:
        raise NotImplementedError("AST-121 must provide the Quay adapter")

    def submit_brief(self, handoff: Handoff, brief: str) -> None:
        raise NotImplementedError("AST-121 must provide the Quay adapter")

    def complete_claim(self, handoff: Handoff) -> None:
        raise NotImplementedError("AST-121 must provide the Quay adapter")

    def release_claim(self, handoff: Handoff, reason: str) -> None:
        raise NotImplementedError("AST-121 must provide the Quay adapter")


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
        handoff: Handoff,
        task: TaskContext,
    ) -> SlackPostRef:
        payload = {
            "channel": channel_id,
            "text": text,
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
        return SlackPostRef(channel_id=channel_id, ts=ts, thread_ts=ts)

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
        artifact = self.quay.get_artifact(handoff.artifact_id) if handoff.artifact_id else None

        brief = choose_direct_brief(handoff)
        if brief:
            self.quay.submit_brief(handoff, brief)
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

        channel_id = (self.config.default_slack_channel or "").strip()
        if not channel_id:
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
        post_ref = self.slack.post_question(
            channel_id,
            question,
            handoff=handoff,
            task=task,
        )
        self.metrics.slack_questions_posted += 1
        log_event(
            self.logger,
            "slack_question_posted",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            channel_id=post_ref.channel_id,
            thread_ts=post_ref.thread_ts,
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
                reason=reason,
            )
            return DrainResult(
                status=reason,
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )

        self.metrics.human_replies_ingested += 1
        next_brief = human_reply_to_brief(reply, handoff=handoff, task=task)
        self.quay.submit_brief(handoff, next_brief)
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
                quay=PendingQuayClient(LOGGER),
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
