#!/usr/bin/env python3
"""Quay orchestrator-side delivery outbox and handoff drain loop.

This component owns the runner shape, locking, Slack question/reply flow, and
how a human reply becomes the next brief text. Quay owns durable task, claim,
artifact, outbox, and handoff state behind the CLI adapter below.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import os
import re
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


LOGGER = logging.getLogger("quay.orchestrator")


class LockBusy(RuntimeError):
    """Raised when another orchestrator runner already owns the drain lock."""


@dataclass(frozen=True)
class Handoff:
    """A claimed orchestrator handoff.

    The field names here are orchestrator-local and intentionally small so the
    Quay CLI adapter does not leak storage details into the Slack/human loop.
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
    status: str = ""

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
            status=str(raw.get("status") or raw.get("state") or ""),
        )


@dataclass(frozen=True)
class OutboxItem:
    """A claimed Quay outbox item owned by the orchestrator sidecar."""

    outbox_item_id: str
    task_id: str
    kind: str
    handler_class: str
    claim_id: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    route_hint: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "OutboxItem":
        payload = _json_mapping_field(raw.get("payload_json"), raw.get("payload"))
        route_hint = _json_mapping_field(raw.get("route_hint_json"), raw.get("route_hint"))
        payload_route_hint = payload.get("route_hint")
        merged_route_hint = (
            dict(payload_route_hint) if isinstance(payload_route_hint, Mapping) else {}
        )
        merged_route_hint.update(route_hint)
        metadata: dict[str, Any] = {}
        metadata.update(payload)
        metadata.update(merged_route_hint)
        metadata["route_hint"] = dict(merged_route_hint)
        metadata["quay_outbox"] = dict(raw)
        return cls(
            outbox_item_id=str(raw.get("outbox_item_id") or raw.get("id") or ""),
            task_id=str(raw.get("task_id") or ""),
            kind=str(raw.get("kind") or ""),
            handler_class=str(raw.get("handler_class") or ""),
            claim_id=_optional_str(raw.get("claim_id")),
            payload=payload,
            route_hint=merged_route_hint,
            metadata=metadata,
        )

    def as_handoff(self, task: TaskContext | None = None) -> Handoff:
        return Handoff(
            handoff_id=f"outbox:{self.outbox_item_id}",
            task_id=self.task_id,
            claim_id=self.claim_id,
            reason=self.kind,
            summary=delivery_message_from_outbox(self, task),
            metadata=dict(self.metadata),
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
    outbox_items_claimed: int = 0
    delivery_items_delivered: int = 0
    direct_briefs_submitted: int = 0
    slack_questions_posted: int = 0
    human_replies_ingested: int = 0
    human_briefs_submitted: int = 0
    human_waits_parked: int = 0
    claims_released: int = 0
    no_handoff: int = 0
    lock_busy: int = 0
    errors: int = 0
    agent_briefs_submitted: int = 0
    agent_fyi_posted: int = 0
    remediation_escalated: int = 0

    def as_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


@dataclass(frozen=True)
class DrainResult:
    status: str
    handoff_id: str | None = None
    task_id: str | None = None
    metrics: Mapping[str, int] = field(default_factory=dict)


# Default Linear team for Otto's Quay-CLI friction tickets (BRIX-1878).
DEFAULT_REMEDIATION_LINEAR_TEAM_ID = "28294e03-c1ce-4c2b-ba24-49e36179a321"


@dataclass(frozen=True)
class OrchestratorConfig:
    enabled: bool = False
    default_slack_channel: str = ""
    slack_token_env: str = "SLACK_BOT_TOKEN"
    quay_command: str = "/usr/local/bin/quay"
    reply_timeout_seconds: float = 1800.0
    poll_interval_seconds: float = 15.0
    lock_path: Path | None = None
    # Agentic blocker remediation (BRIX-1878). Default OFF: Otto never touches a
    # live handoff unless every one of these flags is explicitly enabled.
    remediation_enabled: bool = False
    remediation_max_iterations: int = 3
    remediation_max_tokens: int = 4000
    remediation_max_attempts: int = 2
    remediation_friction_enabled: bool = False
    # Model-authored human-facing escalation message (BRIX-1878). Decoupled from
    # ``remediation_enabled``: this flag only lets the (already bounded) turn
    # author the Slack escalation text on the escalate path. It NEVER enables an
    # auto-resume — a gate-cleared resume is still only submitted under
    # ``remediation_enabled``. Default OFF: fully inert until explicitly enabled.
    remediation_escalation_message_enabled: bool = False
    remediation_linear_team_id: str = DEFAULT_REMEDIATION_LINEAR_TEAM_ID

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
            remediation_enabled=_as_bool(
                raw.get("remediation_enabled"), default=False
            ),
            remediation_max_iterations=_positive_int(
                raw.get("remediation_max_iterations"), default=3
            ),
            remediation_max_tokens=_positive_int(
                raw.get("remediation_max_tokens"), default=4000
            ),
            remediation_max_attempts=_positive_int(
                raw.get("remediation_max_attempts"), default=2
            ),
            remediation_friction_enabled=_as_bool(
                raw.get("remediation_friction_enabled"), default=False
            ),
            remediation_escalation_message_enabled=_as_bool(
                raw.get("remediation_escalation_message_enabled"), default=False
            ),
            remediation_linear_team_id=str(
                raw.get("remediation_linear_team_id")
                or DEFAULT_REMEDIATION_LINEAR_TEAM_ID
            ),
        )

    def with_env_overrides(self) -> "OrchestratorConfig":
        channel = _env_first(
            "QUAY_ORCHESTRATOR_DEFAULT_SLACK_CHANNEL",
            "BRIX_DEFAULT_SLACK_CHANNEL",
        ) or self.default_slack_channel
        enabled = self.enabled
        enabled_env = _env_first(
            "QUAY_ORCHESTRATOR_ENABLED",
            "BRIX_ORCHESTRATOR_ENABLED",
        )
        if enabled_env is not None:
            enabled = _as_bool(enabled_env, default=enabled)
        token_env = _env_first(
            "QUAY_ORCHESTRATOR_SLACK_TOKEN_ENV",
            "BRIX_SLACK_TOKEN_ENV",
        ) or self.slack_token_env
        timeout = _positive_float(
            _env_first(
                "QUAY_ORCHESTRATOR_REPLY_TIMEOUT_SECONDS",
                "BRIX_REPLY_TIMEOUT_SECONDS",
            ),
            default=self.reply_timeout_seconds,
        )
        interval = _positive_float(
            _env_first(
                "QUAY_ORCHESTRATOR_POLL_INTERVAL_SECONDS",
                "BRIX_POLL_INTERVAL_SECONDS",
            ),
            default=self.poll_interval_seconds,
        )
        lock_env = _env_first(
            "QUAY_ORCHESTRATOR_LOCK",
            "BRIX_ORCHESTRATOR_LOCK",
        )
        remediation_enabled = self.remediation_enabled
        remediation_enabled_env = os.getenv("QUAY_ORCHESTRATOR_REMEDIATION_ENABLED")
        if remediation_enabled_env is not None:
            remediation_enabled = _as_bool(
                remediation_enabled_env, default=remediation_enabled
            )
        remediation_friction_enabled = self.remediation_friction_enabled
        remediation_friction_env = os.getenv(
            "QUAY_ORCHESTRATOR_REMEDIATION_FRICTION_ENABLED"
        )
        if remediation_friction_env is not None:
            remediation_friction_enabled = _as_bool(
                remediation_friction_env, default=remediation_friction_enabled
            )
        remediation_escalation_message_enabled = (
            self.remediation_escalation_message_enabled
        )
        remediation_escalation_message_env = os.getenv(
            "QUAY_ORCHESTRATOR_REMEDIATION_ESCALATION_MESSAGE_ENABLED"
        )
        if remediation_escalation_message_env is not None:
            remediation_escalation_message_enabled = _as_bool(
                remediation_escalation_message_env,
                default=remediation_escalation_message_enabled,
            )
        return dataclasses.replace(
            self,
            enabled=enabled,
            default_slack_channel=channel,
            slack_token_env=token_env,
            quay_command=_env_first(
                "QUAY_ORCHESTRATOR_QUAY_COMMAND",
                "BRIX_QUAY_COMMAND",
            ) or self.quay_command,
            reply_timeout_seconds=timeout,
            poll_interval_seconds=interval,
            lock_path=Path(lock_env) if lock_env else self.lock_path,
            remediation_enabled=remediation_enabled,
            remediation_max_iterations=_positive_int(
                os.getenv("QUAY_ORCHESTRATOR_REMEDIATION_MAX_ITERATIONS"),
                default=self.remediation_max_iterations,
            ),
            remediation_max_tokens=_positive_int(
                os.getenv("QUAY_ORCHESTRATOR_REMEDIATION_MAX_TOKENS"),
                default=self.remediation_max_tokens,
            ),
            remediation_max_attempts=_positive_int(
                os.getenv("QUAY_ORCHESTRATOR_REMEDIATION_MAX_ATTEMPTS"),
                default=self.remediation_max_attempts,
            ),
            remediation_friction_enabled=remediation_friction_enabled,
            remediation_escalation_message_enabled=(
                remediation_escalation_message_enabled
            ),
            remediation_linear_team_id=(
                os.getenv("QUAY_ORCHESTRATOR_REMEDIATION_LINEAR_TEAM_ID")
                or self.remediation_linear_team_id
            ),
        )


class QuayClient(Protocol):
    def claim_work(self, worker_id: str) -> Handoff | OutboxItem | None:
        """Claim one durable outbox item or legacy handoff."""

    def claim_handoff(self, worker_id: str) -> Handoff | None:
        """Claim one legacy durable handoff, or return None if no work exists."""

    def claim_waiting_human(self, worker_id: str) -> Handoff | None:
        """Return one parked handoff waiting for Slack reply ingestion."""

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

    def complete_outbox_item(self, item: OutboxItem) -> None:
        """Mark a delivery-only outbox item delivered."""

    def fail_outbox_item(self, item: OutboxItem, reason: str) -> None:
        """Reopen a claimed outbox item with an explicit retry reason."""


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


# Quay's `submit-brief --reason` accepts ONLY these two values (verified via
# `quay submit-brief --help`). Any other value fails at the CLI mid-drain, so we
# validate at the call site to fail loudly instead. In particular the agentic
# remediation path resolves a blocker, so it reuses `blocker_resolved` — the
# auto-vs-human distinction lives in the `brief_submitted_remediated` log event
# and the Slack FYI, not in this enum.
_QUAY_SUBMIT_REASONS: frozenset[str] = frozenset({"blocker_resolved", "advice_answered"})


def _validate_submit_reason(reason: str) -> str:
    if reason not in _QUAY_SUBMIT_REASONS:
        raise ValueError(
            f"invalid submit-brief reason {reason!r}; "
            f"quay accepts only {sorted(_QUAY_SUBMIT_REASONS)}"
        )
    return reason


class QuayCliClient:
    """Adapter for Quay's shared outbox and legacy handoff CLI contracts."""

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

    def claim_work(self, worker_id: str) -> Handoff | OutboxItem | None:
        try:
            item = self._claim_delivery_outbox_work(worker_id)
        except QuayCommandError as exc:
            if _outbox_unsupported(exc):
                log_event(
                    self._logger,
                    "outbox_contract_unavailable",
                    worker_id=worker_id,
                    error_code=exc.code,
                )
                return self.claim_handoff(worker_id)
            raise
        if item is not None:
            return item
        return self.claim_handoff(worker_id)

    def claim_waiting_human(self, worker_id: str) -> Handoff | None:
        try:
            rows = self._run_json(["handoff", "list", "--status", "waiting_human"])
        except QuayCommandError as exc:
            if exc.code in {"unknown_status", "usage_error"}:
                log_event(
                    self._logger,
                    "waiting_human_contract_unavailable",
                    worker_id=worker_id,
                    error_code=exc.code,
                )
                return None
            raise
        if not isinstance(rows, list):
            raise RuntimeError("quay handoff list did not return a JSON array")

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            task_id = str(row.get("task_id") or "")
            if not task_id:
                continue
            handoff = self._handoff_from_row(
                row,
                worker_id=worker_id,
                status_override="waiting_human",
            )
            if not handoff.claim_id:
                log_event(
                    self._logger,
                    "waiting_human_handoff_skipped",
                    worker_id=worker_id,
                    task_id=task_id,
                    handoff_id=handoff.handoff_id,
                    reason="missing_claim_id",
                )
                continue
            return handoff
        return None

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

            claimed = dict(row)
            claimed.update(dict(claim))
            return self._handoff_from_row(
                claimed,
                worker_id=worker_id,
                claim_id=str(claim.get("claim_id") or ""),
            )
        return None

    def _handoff_from_row(
        self,
        row: Mapping[str, Any],
        *,
        worker_id: str,
        claim_id: str | None = None,
        status_override: str | None = None,
    ) -> Handoff:
        metadata = self._handoff_metadata(row)
        metadata["quay_handoff"] = dict(row)
        metadata["worker_id"] = worker_id
        for key in (
            "slack_thread_ref",
            "thread_ref",
            "original_slack_thread_ref",
            "source_slack_thread_ref",
        ):
            value = row.get(key)
            if isinstance(value, str) and value.strip():
                metadata.setdefault(key, value.strip())
        reason = str(row.get("reason") or metadata.get("reason") or "")
        status = status_override or str(row.get("status") or row.get("state") or "")
        return Handoff(
            handoff_id=str(row.get("handoff_id") or row.get("id") or ""),
            task_id=str(row.get("task_id") or ""),
            repo_id=_optional_str(row.get("repo_id") or metadata.get("repo_id")),
            artifact_id=_artifact_kind_for_reason(reason)
            or _optional_str(row.get("artifact_id") or metadata.get("artifact_id")),
            claim_id=_optional_str(claim_id or row.get("claim_id") or metadata.get("claim_id")),
            reason=reason,
            summary=_summary_for_handoff(row, metadata),
            next_brief=_optional_str(row.get("next_brief") or metadata.get("next_brief")),
            human_question=_optional_str(
                row.get("human_question")
                or row.get("question")
                or metadata.get("human_question")
            ),
            metadata=metadata,
            status=status,
        )

    def _claim_delivery_outbox_work(self, worker_id: str) -> OutboxItem | None:
        rows = self._run_json(
            ["outbox", "list", "--status", "pending", "--handler-class", "delivery"]
        )
        if not isinstance(rows, list):
            raise RuntimeError("quay outbox list did not return a JSON array")

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            outbox_item_id = str(row.get("outbox_item_id") or row.get("id") or "")
            if not outbox_item_id:
                continue
            terminal_reason = _terminal_outbox_row_reason(row)
            if terminal_reason:
                log_event(
                    self._logger,
                    "outbox_claim_skipped",
                    worker_id=worker_id,
                    outbox_item_id=outbox_item_id,
                    task_id=str(row.get("task_id") or ""),
                    reason=terminal_reason,
                )
                continue
            try:
                claim = self._run_json(["outbox", "claim", outbox_item_id])
            except QuayCommandError as exc:
                if exc.code in {"wrong_state", "unknown_outbox_item"}:
                    log_event(
                        self._logger,
                        "outbox_claim_skipped",
                        worker_id=worker_id,
                        outbox_item_id=outbox_item_id,
                        error_code=exc.code,
                    )
                    continue
                raise
            if not isinstance(claim, Mapping):
                raise RuntimeError("quay outbox claim did not return a JSON object")

            claimed = dict(row)
            claimed.update(dict(claim))
            item = OutboxItem.from_mapping(claimed)
            item = dataclasses.replace(
                item,
                metadata={**dict(item.metadata), "worker_id": worker_id},
            )
            if item.handler_class == "delivery":
                return item

            reason = f"unsupported_handler_class:{item.handler_class or 'missing'}"
            self.fail_outbox_item(item, reason)
            log_event(
                self._logger,
                "outbox_claim_skipped",
                worker_id=worker_id,
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=reason,
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
        _validate_submit_reason(reason)
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

    def complete_outbox_item(self, item: OutboxItem) -> None:
        claim_id = self._outbox_claim_id(item)
        self._run_json(
            [
                "outbox",
                "complete",
                item.outbox_item_id,
                "--claim-id",
                claim_id,
            ]
        )

    def fail_outbox_item(self, item: OutboxItem, reason: str) -> None:
        claim_id = self._outbox_claim_id(item)
        self._run_json(
            [
                "outbox",
                "fail",
                item.outbox_item_id,
                "--claim-id",
                claim_id,
                "--error",
                reason,
            ]
        )

    def _claim_id(self, handoff: Handoff) -> str:
        if handoff.claim_id:
            return handoff.claim_id
        raw = handoff.metadata.get("claim_id")
        if isinstance(raw, str) and raw:
            return raw
        raise RuntimeError(f"handoff {handoff.handoff_id} has no claim_id")

    def _outbox_claim_id(self, item: OutboxItem) -> str:
        if item.claim_id:
            return item.claim_id
        raw = item.metadata.get("claim_id")
        if isinstance(raw, str) and raw:
            return raw
        raise RuntimeError(f"outbox item {item.outbox_item_id} has no claim_id")

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


def _assert_agent_tool_less(agent: Any) -> None:
    """Guarantee an orchestrator agent resolved to ZERO tools.

    The orchestrator's ``AIAgent`` instances (Slack decider + blocker
    remediator) must never hold a tool: the model is a pure reasoner and every
    state change is performed by orchestrator code. ``enabled_toolsets=[]``
    alone is NOT sufficient because ``model_tools`` force-appends the ``kanban``
    toolset whenever ``HERMES_KANBAN_TASK`` is set in the ambient env. This
    check fails loudly if any tool survives, so the agent can never silently
    gain a capability from the environment.
    """
    tools = getattr(agent, "tools", None)
    if tools:
        try:
            names = sorted(t["function"]["name"] for t in tools)
        except (TypeError, KeyError, AttributeError):
            names = [str(t) for t in tools]
        raise RuntimeError(
            "orchestrator agent must be tool-less but resolved tools: "
            + ", ".join(names)
        )


class _OrchestratorAgentBuilder:
    """Shared bounded-``AIAgent`` runtime/model resolution for the orchestrator.

    Both the Slack conversation decider and the blocker remediator run a scoped,
    tool-less ``AIAgent`` under the same provider/model resolution rules. This
    base owns that plumbing so the two callers cannot drift apart; each supplies
    its own iteration/token caps via ``_build_agent``.
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

    def _build_agent(
        self,
        handoff: Handoff,
        *,
        max_iterations: int,
        max_tokens: int | None = None,
    ) -> Any:
        if self._agent_factory is not None:
            agent = self._agent_factory()
            _assert_agent_tool_less(agent)
            return agent
        runtime = self._resolve_runtime()
        agent_class = self._agent_class
        if agent_class is None:
            from run_agent import AIAgent

            agent_class = AIAgent

        kwargs: dict[str, Any] = dict(
            model=self._resolve_model(runtime),
            api_key=runtime.get("api_key"),
            base_url=runtime.get("base_url"),
            provider=runtime.get("provider"),
            api_mode=runtime.get("api_mode"),
            acp_command=runtime.get("command"),
            acp_args=list(runtime.get("args") or []),
            credential_pool=runtime.get("credential_pool"),
            max_iterations=max_iterations,
            quiet_mode=True,
            # Tool-less by construction. An empty ``enabled_toolsets`` is not
            # enough: ``model_tools`` force-appends the ``kanban`` toolset
            # whenever ``HERMES_KANBAN_TASK`` is set in the ambient env.
            # Explicitly disabling ``kanban`` overrides that force-append so the
            # orchestrator agent resolves to ZERO tools regardless of env — an
            # explicit API rather than mutating global ``os.environ``. The
            # post-build ``_assert_agent_tool_less`` check is the backstop.
            enabled_toolsets=[],
            disabled_toolsets=["kanban"],
            skip_context_files=True,
            skip_memory=True,
            platform="quay-orchestrator",
            session_id=orchestrator_session_id(handoff),
        )
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        agent = agent_class(**kwargs)
        _assert_agent_tool_less(agent)
        return agent

    def _resolve_runtime(self) -> Mapping[str, Any]:
        resolver = self._runtime_resolver
        if resolver is None:
            from hermes_cli.runtime_provider import resolve_runtime_provider

            resolver = resolve_runtime_provider
        requested = (
            _env_first("QUAY_ORCHESTRATOR_PROVIDER", "BRIX_ORCHESTRATOR_PROVIDER")
            or ""
        ).strip() or None
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


class HermesConversationDecider(_OrchestratorAgentBuilder):
    """LLM-backed Quay orchestrator decision step.

    Humans talk normally in Slack. This component decides whether that
    discussion has enough information to resume the worker, or whether the
    orchestrator should ask/confirm one more thing.
    """

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

        agent = self._build_agent(handoff, max_iterations=2)
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
            task_id=f"quay-orchestrator:{handoff.task_id}",
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


# ---------------------------------------------------------------------------
# Agentic blocker remediation (BRIX-1878)
#
# SAFETY: Otto may auto-answer a blocked worker only when EVERY guardrail below
# agrees. The model NEVER holds a tool; it only proposes JSON. All state
# transitions (``submit_brief``, Linear writes) are performed by orchestrator
# code AFTER the deterministic gates below. When anything is unclear the code
# escalates to a human — the model's opinion cannot override that.
# ---------------------------------------------------------------------------

# Deterministic never-auto categories. If any substring here appears in the
# blocker/handoff/model text, the handoff is escalated to a human, full stop.
_NEVER_AUTO_CATEGORIES: dict[str, tuple[str, ...]] = {
    "product_or_requirements_decision": (
        "product decision",
        "product call",
        "requirement",
        "requirements",
        "acceptance criteria",
        "scope change",
        "change scope",
        "design decision",
        "which approach",
        "which option",
        "which feature",
        "business logic",
        "business decision",
        "should we",
        "do we want",
        "product owner",
        "stakeholder",
        "roadmap",
    ),
    "irreversible_or_production_change": (
        "production",
        "prod deploy",
        "deploy to prod",
        "irreversible",
        "cannot be undone",
        "can't be undone",
        "not reversible",
        "force push",
        "force-push",
        "rollback",
        "mainnet",
        "live environment",
        "destructive",
        "delete",
        "drop table",
        "tear down",
        "teardown",
        "release to production",
    ),
    "security": (
        "security",
        "vulnerability",
        "vulnerabilities",
        "cve-",
        "exploit",
        "auth bypass",
        "authentication bypass",
        "authorization",
        "access control",
        "privilege",
        "rce",
        "xss",
        "csrf",
        "sql injection",
        "injection attack",
        "malicious",
    ),
    "spend_or_budget": (
        "budget",
        "spend",
        "overspend",
        "invoice",
        "billing",
        "payment",
        "purchase",
        "procurement",
        "pay for",
        "pricing tier",
        "upgrade plan",
    ),
    "credentials_or_secrets": (
        "credential",
        "secret",
        "api key",
        "api_key",
        "apikey",
        "password",
        "private key",
        "ssh key",
        "access token",
        "auth token",
        "bearer token",
        "mnemonic",
        "seed phrase",
        "wallet key",
        "keystore",
        ".env secret",
    ),
    "data_changes": (
        "data migration",
        "migrate data",
        "database migration",
        "schema migration",
        "drop column",
        "delete rows",
        "delete records",
        "truncate",
        "alter table",
        "wipe data",
        "purge data",
        "production data",
        "customer data",
        "user data",
        "backfill",
    ),
    "ambiguous_intent": (
        "unclear",
        "ambiguous",
        "not sure what",
        "unsure",
        "need clarification",
        "needs clarification",
        "please clarify",
        "what did you mean",
        "which one",
        "don't know what",
        "cannot determine",
        "can't determine",
        "undecided",
    ),
}

# Category tokens that, if the model claims them, are treated as never-auto.
_NEVER_AUTO_CATEGORY_TOKENS: tuple[str, ...] = (
    "product",
    "requirement",
    "design",
    "security",
    "vuln",
    "credential",
    "secret",
    "password",
    "token",
    "spend",
    "budget",
    "billing",
    "payment",
    "cost",
    "data",
    "migration",
    "migrate",
    "irreversible",
    "prod",
    "production",
    "deploy",
    "release",
    "destructive",
    "delete",
    "ambiguous",
    "unclear",
    "unknown",
    "escalate",
)

# Canonical auto-answerable categories (companion skill hermes-state #32). This
# allowlist is the ENFORCEABLE FLOOR for auto-resume: a model resume is accepted
# only when its category normalizes to one of these five. Everything else — a
# novel label, a relabeled danger, an empty/miscased/non-string value —
# escalates. The keyword denylist above (`_never_auto_reason` /
# `_is_never_auto_category`) is kept as defense-in-depth over the brief text; it
# does NOT replace this floor.
_AUTO_ANSWERABLE_CATEGORIES: frozenset[str] = frozenset(
    {
        "prerequisite-baseline",
        "stale-baseline",
        "missing-invocation",
        "transient-retry",
        "scoped-known-fix",
    }
)


def _normalize_category(category: Any) -> str:
    """Strip + lowercase a model-proposed category; non-strings normalize to ''."""
    if not isinstance(category, str):
        return ""
    return category.strip().lower()


def _is_auto_answerable_category(category: Any) -> bool:
    """True only when ``category`` is a member of the canonical allowlist."""
    return _normalize_category(category) in _AUTO_ANSWERABLE_CATEGORIES


REMEDIATION_GUARDRAIL_SYSTEM = (
    "You are the Quay orchestrator's blocker-remediation guardrail. A worker is "
    "blocked and you decide whether the orchestrator may auto-resume it WITHOUT a "
    "human in the loop, running on a live orchestrator.\n"
    "You have NO tools. You only emit ONE JSON object and nothing else.\n"
    "NEVER auto-resume (always output {\"action\":\"escalate\"}) when the blocker "
    "involves ANY of: a product or requirements decision; an irreversible or "
    "production change; security; spend or budget; credentials or secrets; data "
    "changes; or any ambiguous, unclear, or under-specified intent.\n"
    "When you are unsure for ANY reason, output {\"action\":\"escalate\"}.\n"
    "Only propose a resume when the fix is objectively safe, reversible, and fully "
    "determined by the blocker context (for example a lint or formatting fix, a "
    "trivial and obvious test correction, or a mechanical follow-up that the "
    "worker already described).\n"
    "You never perform actions; the orchestrator code, not you, decides whether to "
    "act on your proposal and re-checks every guardrail first.\n"
    "Output schema (JSON only, no prose, no code fence):\n"
    "  {\"action\":\"escalate\"}\n"
    "  On escalate you MAY add an optional \"message\": a concise, human-facing "
    "recommendation of how to proceed plus the one confirmation or input you need "
    "(follow the skill's \"Writing the escalation message\" guidance — recommend "
    "only when it is directly grounded in the blocker; when evidence is thin, ask "
    "for the missing input instead of guessing). Omit it if you cannot summarize "
    "usefully. Example: {\"action\":\"escalate\",\"message\":\"...\"}\n"
    "OR\n"
    "  {\"action\":\"resume\",\"brief\":\"<resume guidance for the worker>\","
    "\"category\":\"<auto-answerable category>\",\"rationale\":\"<why it is safe "
    "and reversible>\",\"friction\":{\"title\":\"...\",\"detail\":\"...\","
    "\"signature\":\"...\"}}\n"
    "The friction object is optional; include it only to record a recurring "
    "Quay-CLI friction the worker hit.\n"
    "--- BEGIN SKILL ---\n"
)

# Hard caps: any response beyond these is treated as malformed -> escalate.
_MAX_REMEDIATION_RESPONSE_CHARS = 20000
_MAX_REMEDIATION_BRIEF_CHARS = 6000
_MAX_FRICTION_FIELD_CHARS = 4000
# The optional model-authored escalation message is human-facing prose; cap it so
# a single blocked worker cannot flood a Slack thread. Applied both when parsing
# and when sanitizing (defence in depth).
_MAX_ESCALATION_MESSAGE_CHARS = 800


@dataclass(frozen=True)
class RemediationOutcome:
    brief: str
    category: str
    rationale: str = ""
    friction: Mapping[str, str] | None = None
    # Optional model-authored human-facing escalation message, carried ONLY for
    # the escalate path. NEVER read by any gate / allowlist / loop-guard / resume
    # decision — the renderer reads it after the decision has been made.
    message: str | None = None
    tokens: int = 0


def _never_auto_reason(
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
    *,
    proposed_category: str = "",
    proposed_brief: str = "",
) -> str | None:
    """Deterministic (no-model) never-auto classifier.

    Returns a human-readable reason string when the handoff must NOT be
    auto-remediated (the caller escalates), or ``None`` when the deterministic
    checks find no reason to block. Conservative by design: missing blocker
    context is itself a reason to escalate.
    """
    blocker_text = artifact.text if artifact else ""
    # Pre-model gate: with no blocker context we cannot reason safely.
    if not proposed_category and not proposed_brief:
        if not blocker_text or not blocker_text.strip():
            return "no_blocker_context"

    parts = [
        handoff.reason or "",
        handoff.summary or "",
        blocker_text,
        proposed_category or "",
        proposed_brief or "",
    ]
    try:
        parts.append(json.dumps(task.metadata, ensure_ascii=False, default=str))
    except (TypeError, ValueError):
        parts.append(str(task.metadata))
    haystack = "\n".join(parts).lower()

    for category, needles in _NEVER_AUTO_CATEGORIES.items():
        for needle in needles:
            if needle in haystack:
                return f"{category}:{needle.strip()}"

    if proposed_category:
        category_lower = proposed_category.strip().lower()
        for token in _NEVER_AUTO_CATEGORY_TOKENS:
            if token in category_lower:
                return f"never_auto_category:{category_lower}"

    return None


def _is_never_auto_category(category: str) -> bool:
    category_lower = (category or "").strip().lower()
    if not category_lower:
        return True
    return any(token in category_lower for token in _NEVER_AUTO_CATEGORY_TOKENS)


def _parse_friction(value: Any) -> dict[str, str] | None:
    if not isinstance(value, Mapping):
        return None
    title = value.get("title")
    detail = value.get("detail")
    signature = value.get("signature")
    fields = {}
    for key, raw in (("title", title), ("detail", detail), ("signature", signature)):
        if not isinstance(raw, str) or not raw.strip():
            return None
        text = raw.strip()
        if len(text) > _MAX_FRICTION_FIELD_CHARS:
            return None
        fields[key] = text
    return fields


def _coerce_escalation_message(value: Any) -> str | None:
    """Defensively coerce a raw escalate ``message`` into a capped string.

    Returns ``None`` (message treated as absent) unless ``value`` is a non-empty
    string. The message is optional: escalate is valid with or without it, and a
    missing / blank / non-string / oversized value must never block the escalate.
    Oversized text is truncated with an ellipsis rather than rejected.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if len(text) > _MAX_ESCALATION_MESSAGE_CHARS:
        text = text[: _MAX_ESCALATION_MESSAGE_CHARS - 1].rstrip() + "…"
    return text


def parse_remediation_response(text: str) -> dict[str, Any] | None:
    """Strictly parse the remediation turn's JSON.

    Accepts ONLY ``{"action":"escalate"}`` or a fully-formed resume object.
    Anything malformed, missing a required field, or oversized returns ``None``
    so the caller escalates to a human.
    """
    raw = (text or "").strip()
    if not raw or len(raw) > _MAX_REMEDIATION_RESPONSE_CHARS:
        return None
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
                return None
        else:
            return None
    if not isinstance(data, Mapping):
        return None

    action = str(data.get("action") or "").strip().lower()
    if action == "escalate":
        # F4: preserve a valid friction object on the escalate path so the
        # orchestrator can still capture a recurring Quay-CLI friction even when
        # Otto cannot self-resolve. Invalid/oversized friction is dropped (we
        # still escalate); a bad friction never blocks the escalation.
        escalation: dict[str, Any] = {"action": "escalate"}
        # Optional human-facing escalation message. Carried ONLY on the escalate
        # path; it does not (and must not) influence the resume/gate logic below.
        message = _coerce_escalation_message(data.get("message"))
        if message is not None:
            escalation["message"] = message
        if data.get("friction") is not None:
            parsed_friction = _parse_friction(data.get("friction"))
            if parsed_friction is not None:
                escalation["friction"] = parsed_friction
        return escalation
    if action != "resume":
        return None

    brief = data.get("brief")
    category = data.get("category")
    rationale = data.get("rationale")
    if not isinstance(brief, str) or not brief.strip():
        return None
    if not isinstance(category, str) or not category.strip():
        return None
    if not isinstance(rationale, str) or not rationale.strip():
        return None
    if len(brief) > _MAX_REMEDIATION_BRIEF_CHARS:
        return None

    result: dict[str, Any] = {
        "action": "resume",
        "brief": brief.strip(),
        "category": category.strip(),
        "rationale": rationale.strip(),
    }
    if "friction" in data and data.get("friction") is not None:
        parsed_friction = _parse_friction(data.get("friction"))
        if parsed_friction is None:
            return None
        result["friction"] = parsed_friction
    return result


def build_remediation_prompt(
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
) -> str:
    payload = {
        "task": {
            "task_id": task.task_id,
            "title": task.title,
            "issue": task.issue,
            "repo_id": task.repo_id or handoff.repo_id,
            "metadata": task.metadata,
        },
        "handoff": {
            "handoff_id": handoff.handoff_id,
            "reason": handoff.reason,
            "summary": handoff.summary,
        },
        "blocker": artifact.text if artifact else "",
    }
    return (
        "A Quay worker is blocked. Decide whether it can be safely auto-resumed "
        "without a human, following every rule in the system message.\n"
        "Return only the JSON object described in the system message.\n\n"
        + json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    )


def build_remediation_fyi(
    handoff: Handoff,
    task: TaskContext,
    outcome: RemediationOutcome,
) -> str:
    target = task.issue or task.title or task.task_id
    blocker = ""
    if handoff.summary and handoff.summary.strip():
        blocker = handoff.summary.strip().splitlines()[0].strip()
    preview = outcome.brief.strip()
    if len(preview) > 400:
        preview = preview[:397].rstrip() + "..."
    lines = [f"Otto auto-resumed a blocked Quay worker for {target}."]
    lines.append(f"Task: {task.task_id}")
    if blocker:
        lines.append(f"Blocker: {blocker}")
    lines.append(f"Category: {outcome.category}")
    lines.append(f"Brief: {preview}")
    lines.append("Auto-resumed; reply here to override.")
    return "\n".join(lines)


class HandoffRemediator(_OrchestratorAgentBuilder):
    """Bounded, guardrailed agentic remediation for blocked worker handoffs.

    ``remediate`` returns a resume brief ONLY when both the pre-model and
    post-model deterministic gates agree it is safe; any doubt, crash, or model
    malfunction degrades to ``None`` (human escalation). The model is a pure
    reasoner with no tools: the orchestrator, not the model, performs every
    state change.
    """

    def __init__(
        self,
        *,
        config: OrchestratorConfig,
        logger: logging.Logger | None = None,
        agent_factory: Any | None = None,
        runtime_resolver: Any | None = None,
        default_model_resolver: Any | None = None,
        model_catalog_resolver: Any | None = None,
        model_normalizer: Any | None = None,
        agent_class: Any | None = None,
        skill_loader: Any | None = None,
        linear_request: Any | None = None,
    ) -> None:
        super().__init__(
            logger=logger,
            agent_factory=agent_factory,
            runtime_resolver=runtime_resolver,
            default_model_resolver=default_model_resolver,
            model_catalog_resolver=model_catalog_resolver,
            model_normalizer=model_normalizer,
            agent_class=agent_class,
        )
        self.config = config
        self._skill_loader = skill_loader
        self._linear_request = linear_request
        self.last_outcome: RemediationOutcome | None = None

    # -- public API --------------------------------------------------------

    def remediate(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
    ) -> str | None:
        self.last_outcome = None
        try:
            # Gate #1a (pre-model): deterministic never-auto classifier.
            never = _never_auto_reason(handoff, task, artifact)
            if never is not None:
                log_event(
                    self._logger,
                    "remediation_gate_blocked",
                    gate="pre_model_never_auto",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                    reason=never,
                )
                return None

            # Gate #1b (pre-model): durable per-task loop guard.
            attempts = self._read_attempts(handoff.task_id)
            if attempts >= self.config.remediation_max_attempts:
                log_event(
                    self._logger,
                    "remediation_gate_blocked",
                    gate="loop_guard",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                    attempts=attempts,
                    max_attempts=self.config.remediation_max_attempts,
                )
                return None

            skill_body = self._load_skill()
            if not skill_body:
                log_event(
                    self._logger,
                    "remediation_gate_blocked",
                    gate="missing_skill",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                )
                return None

            # Consume one attempt now: we are about to invoke the model. Fail
            # CLOSED — if we cannot durably record the attempt we treat
            # ourselves as OVER-BUDGET and escalate WITHOUT calling the model
            # ("if I can't prove I'm under budget, I don't spend an attempt").
            try:
                self._bump_attempts(handoff.task_id)
            except OSError:
                log_event(
                    self._logger,
                    "remediation_gate_blocked",
                    gate="loop_guard_write_failed",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                )
                return None

            agent = self._build_agent(
                handoff,
                max_iterations=self.config.remediation_max_iterations,
                max_tokens=self.config.remediation_max_tokens,
            )
            system_message = REMEDIATION_GUARDRAIL_SYSTEM + skill_body
            prompt = build_remediation_prompt(handoff, task, artifact)
            result = agent.run_conversation(
                prompt,
                system_message=system_message,
                conversation_history=[],
                task_id=f"quay-orchestrator:{handoff.task_id}",
            )
            if isinstance(result, Mapping):
                text = str(result.get("final_response") or "")
                tokens = _safe_int(result.get("total_tokens"))
            else:
                text = str(result or "")
                tokens = 0

            parsed = parse_remediation_response(text)
            if parsed is None or parsed.get("action") != "resume":
                # F4: the model escalated (or its output was rejected). If it
                # still supplied a valid friction object, surface it via
                # ``last_outcome`` so the orchestrator can record it (code-only,
                # behind ``remediation_friction_enabled``) while still escalating.
                # BRIX-1878: likewise carry any model-authored escalation message
                # so the orchestrator can render it in place of the deterministic
                # template (gated by ``remediation_escalation_message_enabled``).
                # Both are escalate-path-only and read AFTER this decision — they
                # never affect the gate/allowlist/loop-guard logic above.
                friction = (
                    parsed.get("friction") if isinstance(parsed, Mapping) else None
                )
                message = (
                    parsed.get("message") if isinstance(parsed, Mapping) else None
                )
                if friction or message:
                    self.last_outcome = RemediationOutcome(
                        brief="",
                        category="",
                        friction=friction,
                        message=message,
                        tokens=tokens,
                    )
                log_event(
                    self._logger,
                    "remediation_attempt",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                    decision="escalate",
                    tokens=tokens,
                )
                return None

            brief = str(parsed.get("brief") or "").strip()
            category = str(parsed.get("category") or "").strip()

            # Gate #2 (post-model): a resume is accepted ONLY IF BOTH hold:
            #   (a) the model's category is a member of the canonical
            #       auto-answerable allowlist (the ENFORCEABLE FLOOR), AND
            #   (b) the deterministic never-auto keyword scan over the model's
            #       own category+brief finds nothing.
            # The allowlist is the primary, enforceable gate; the keyword scan
            # (`_never_auto_reason` / `_is_never_auto_category`) is kept as
            # defense-in-depth. Both are layers among several: LLM judgment, a
            # narrow allowlist, worker-sandbox limits, and an FYI-with-undo. Any
            # category that is novel, empty, relabeled, miscased, or non-string
            # fails (a) and escalates.
            if not brief:
                return None
            allowlisted = _is_auto_answerable_category(category)
            gate2 = _never_auto_reason(
                handoff,
                task,
                artifact,
                proposed_category=category,
                proposed_brief=brief,
            )
            if not allowlisted or gate2 is not None or _is_never_auto_category(category):
                if not allowlisted:
                    reason = (
                        "category_not_allowlisted:"
                        + (_normalize_category(category) or "<empty>")
                    )
                elif gate2 is not None:
                    reason = gate2
                else:
                    reason = f"never_auto_category:{category.lower()}"
                log_event(
                    self._logger,
                    "remediation_gate_blocked",
                    gate="post_model_allowlist",
                    task_id=handoff.task_id,
                    handoff_id=handoff.handoff_id,
                    category=category,
                    reason=reason,
                    tokens=tokens,
                )
                return None

            outcome = RemediationOutcome(
                brief=brief,
                category=category,
                rationale=str(parsed.get("rationale") or ""),
                friction=parsed.get("friction"),
                tokens=tokens,
            )
            self.last_outcome = outcome
            log_event(
                self._logger,
                "remediation_attempt",
                task_id=handoff.task_id,
                handoff_id=handoff.handoff_id,
                decision="resume",
                category=category,
                tokens=tokens,
            )
            return brief
        except Exception:
            # A crash must degrade to human escalation, never drop the handoff.
            self._logger.exception("remediation crashed; escalating to human")
            self.last_outcome = None
            return None

    def record_friction(
        self,
        *,
        outcome: RemediationOutcome | None,
        task: TaskContext,
    ) -> None:
        """Best-effort: file recurring Quay-CLI friction to Linear.

        Never raises. Deduped both by a local per-signature marker (avoids
        re-hitting Linear across ticks) and by a GraphQL title search.
        """
        if not self.config.remediation_friction_enabled:
            return
        if outcome is None or not outcome.friction:
            return
        friction = outcome.friction
        signature = str(friction.get("signature") or "").strip()
        if not signature:
            return
        try:
            if self._friction_marker_exists(signature):
                return
            team_id = self.config.remediation_linear_team_id
            existing = self._linear_find_friction(team_id, signature)
            if existing:
                log_event(
                    self._logger,
                    "remediation_friction_deduped",
                    task_id=task.task_id,
                    signature=signature,
                    issue=existing,
                )
            else:
                created = self._linear_create_friction(team_id, friction, signature)
                log_event(
                    self._logger,
                    "remediation_friction_filed",
                    task_id=task.task_id,
                    signature=signature,
                    issue=created,
                )
            self._write_friction_marker(signature)
        except Exception:
            self._logger.exception("friction recording failed; ignoring")

    # -- skill loading -----------------------------------------------------

    def _load_skill(self) -> str | None:
        if self._skill_loader is not None:
            try:
                body = self._skill_loader()
            except Exception:
                self._logger.exception("remediation skill loader failed")
                return None
            body = (body or "").strip() if isinstance(body, str) else ""
            return body or None
        path = self._skill_path()
        try:
            if not path.is_file():
                return None
            body = path.read_text(encoding="utf-8").strip()
        except OSError:
            self._logger.exception("failed to read remediation skill")
            return None
        return body or None

    def _skill_path(self) -> Path:
        return (
            _hermes_home_dir()
            / "skills"
            / "quay"
            / "quay-blocker-remediation"
            / "SKILL.md"
        )

    # -- durable loop guard ------------------------------------------------

    def _attempts_path(self, task_id: str) -> Path:
        safe = _sanitize_state_key(task_id)
        return _hermes_home_dir() / "quay" / "remediation-attempts" / safe

    def _read_attempts(self, task_id: str) -> int:
        path = self._attempts_path(task_id)
        try:
            return int((path.read_text(encoding="utf-8") or "0").strip() or "0")
        except (OSError, ValueError):
            return 0

    def _bump_attempts(self, task_id: str) -> int:
        # Fail CLOSED: a failure to durably record the attempt MUST propagate so
        # the caller treats it as over-budget and escalates. Swallowing the
        # error here (fail-open) would let a read-only/full filesystem re-invoke
        # the model every tick without ever advancing the counter — unbounded.
        path = self._attempts_path(task_id)
        nxt = self._read_attempts(task_id) + 1
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(nxt), encoding="utf-8")
        return nxt

    # -- friction markers --------------------------------------------------

    def _friction_marker_path(self, signature: str) -> Path:
        digest = hashlib.sha256(signature.encode("utf-8")).hexdigest()[:32]
        return _hermes_home_dir() / "quay" / "friction-markers" / digest

    def _friction_marker_exists(self, signature: str) -> bool:
        return self._friction_marker_path(signature).exists()

    def _write_friction_marker(self, signature: str) -> None:
        path = self._friction_marker_path(signature)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(signature, encoding="utf-8")
        except OSError:
            self._logger.exception("failed to persist friction marker")

    # -- Linear (code-only, never an agent tool) ---------------------------

    def _linear_find_friction(self, team_id: str, signature: str) -> str | None:
        query = (
            "query($team: ID!, $needle: String!) { "
            "issues(filter: {team: {id: {eq: $team}}, "
            "title: {containsIgnoreCase: $needle}}, first: 1) "
            "{ nodes { id identifier url } } }"
        )
        data = self._linear_call(
            query, {"team": team_id, "needle": signature}
        )
        nodes = (
            ((data.get("data") or {}).get("issues") or {}).get("nodes")
            if isinstance(data, Mapping)
            else None
        )
        if isinstance(nodes, list) and nodes:
            first = nodes[0]
            if isinstance(first, Mapping):
                return str(first.get("identifier") or first.get("id") or "")
        return None

    def _linear_create_friction(
        self,
        team_id: str,
        friction: Mapping[str, str],
        signature: str,
    ) -> str | None:
        title = f"{friction.get('title', '').strip()} [otto-friction:{signature}]"
        detail = friction.get("detail", "").strip()
        description = (
            f"{detail}\n\n"
            f"_Filed automatically by Otto (Quay-CLI friction capture)._\n"
            f"Signature: `{signature}`\n"
            f"Tag: `[otto-friction]`"
        )
        mutation = (
            "mutation($input: IssueCreateInput!) { "
            "issueCreate(input: $input) { success issue { id identifier url } } }"
        )
        data = self._linear_call(
            mutation,
            {"input": {"teamId": team_id, "title": title, "description": description}},
        )
        issue = (
            ((data.get("data") or {}).get("issueCreate") or {}).get("issue")
            if isinstance(data, Mapping)
            else None
        )
        if isinstance(issue, Mapping):
            return str(issue.get("identifier") or issue.get("id") or "")
        return None

    def _linear_call(
        self, query: str, variables: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        if self._linear_request is not None:
            return self._linear_request(query, dict(variables))
        api_key = (os.getenv("LINEAR_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("LINEAR_API_KEY is not configured")
        body = json.dumps({"query": query, "variables": dict(variables)}).encode("utf-8")
        request = urllib.request.Request(
            "https://api.linear.app/graphql",
            data=body,
            headers={
                "Authorization": api_key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, Mapping):
            raise RuntimeError("Linear GraphQL returned a non-object response")
        return parsed


def _hermes_home_dir() -> Path:
    return Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")


def _sanitize_state_key(value: str) -> str:
    safe = "".join(
        ch if (ch.isalnum() or ch in "._-") else "_" for ch in (value or "")
    )
    safe = safe.strip("._") or "unknown"
    return safe[:200]


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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
                "event_type": "quay_orchestrator_handoff",
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
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        floor_ts = after_ts or ref.ts
        while True:
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
            now = time.monotonic()
            if now >= deadline:
                return None
            time.sleep(min(poll_interval_seconds, max(0.0, deadline - now)))
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
        self._locked = False

    def __enter__(self) -> "FileLock":
        self.acquire(blocking=False)
        return self

    def acquire(self, *, blocking: bool = False) -> None:
        import fcntl

        if self._locked:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._handle is None:
            self._handle = self.path.open("a+", encoding="utf-8")
        operation = fcntl.LOCK_EX
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(self._handle.fileno(), operation)
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
        self._locked = True

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release(close=True)

    def release(self, *, close: bool = False) -> None:
        import fcntl

        if self._handle is None:
            return
        try:
            if self._locked:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
                self._locked = False
        finally:
            if close:
                self._handle.close()
                self._handle = None

    @contextmanager
    def suspended(self):
        was_locked = self._locked
        if was_locked:
            self.release(close=False)
        try:
            yield
        finally:
            if was_locked:
                self.acquire(blocking=True)


class HandoffDrainer:
    def __init__(
        self,
        *,
        quay: QuayClient,
        slack: SlackQuestionClient,
        config: OrchestratorConfig,
        worker_id: str,
        decider: ConversationDecider | None = None,
        remediator: HandoffRemediator | None = None,
        coordination_lock: FileLock | None = None,
        park_human_waits: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.quay = quay
        self.slack = slack
        self.config = config
        self.worker_id = worker_id
        self.decider = decider or HermesConversationDecider(logger=logger or LOGGER)
        self.remediator = remediator or HandoffRemediator(
            config=config, logger=logger or LOGGER
        )
        self.coordination_lock = coordination_lock
        self.park_human_waits = park_human_waits
        self.logger = logger or LOGGER
        self.metrics = RunMetrics()

    def drain_one(self) -> DrainResult:
        claim_work = getattr(self.quay, "claim_work", None)
        work = (
            claim_work(self.worker_id)
            if callable(claim_work)
            else self.quay.claim_handoff(self.worker_id)
        )
        if work is None and self.park_human_waits:
            claim_waiting_human = getattr(self.quay, "claim_waiting_human", None)
            if callable(claim_waiting_human):
                work = claim_waiting_human(self.worker_id)
        if work is None:
            self.metrics.no_handoff += 1
            log_event(self.logger, "drain_no_handoff", worker_id=self.worker_id)
            return DrainResult(status="no_handoff", metrics=self.metrics.as_dict())

        if isinstance(work, OutboxItem):
            self.metrics.outbox_items_claimed += 1
            log_event(
                self.logger,
                "outbox_item_claimed",
                outbox_item_id=work.outbox_item_id,
                task_id=work.task_id,
                kind=work.kind,
                handler_class=work.handler_class,
            )
            if work.handler_class == "delivery":
                return self._handle_claimed_delivery_item(work)
            reason = f"unsupported_handler_class:{work.handler_class or 'missing'}"
            self.quay.fail_outbox_item(work, reason)
            self.metrics.claims_released += 1
            return DrainResult(
                status="unsupported_outbox_handler",
                handoff_id=f"outbox:{work.outbox_item_id}",
                task_id=work.task_id,
                metrics=self.metrics.as_dict(),
            )

        handoff = work
        if handoff.metadata.get("outbox_item_id"):
            self.metrics.outbox_items_claimed += 1
        self.metrics.handoffs_claimed += 1
        log_event(
            self.logger,
            "handoff_claimed",
            handoff_id=handoff.handoff_id,
            task_id=handoff.task_id,
            status=handoff.status,
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

    def _handle_claimed_delivery_item(self, item: OutboxItem) -> DrainResult:
        # A single poisoned row must never crash the drain or block the rows
        # behind it: any Slack error is classified, and any other unexpected
        # exception is retried within a bounded budget and then quarantined —
        # nothing here re-raises.
        try:
            return self._deliver_claimed_item(item)
        except SlackApiError as exc:
            return self._fail_delivery_slack_error(item, exc)
        except Exception as exc:  # noqa: BLE001 - one bad row must not crash drain
            return self._fail_delivery_unexpected(item, exc)

    def _deliver_claimed_item(self, item: OutboxItem) -> DrainResult:
        task = self.quay.get_task_context(item.task_id)
        handoff = item.as_handoff(task)
        route = resolve_slack_route(handoff, task, self.config)
        if route is None:
            reason = "missing_default_slack_channel"
            self._fail_terminal_delivery_item(item, reason)
            self.metrics.claims_released += 1
            log_event(
                self.logger,
                "outbox_item_quarantined",
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=reason,
            )
            return DrainResult(
                status=reason,
                handoff_id=handoff.handoff_id,
                task_id=item.task_id,
                metrics=self.metrics.as_dict(),
            )

        message = delivery_message_from_outbox(item, task)
        post_ref = self._post_delivery_message(
            item=item,
            handoff=handoff,
            task=task,
            route=route,
            message=message,
        )

        # Slack delivery is at-least-once: if the post succeeds but marking the
        # outbox item complete fails, a later retry may post the same message.
        self.quay.complete_outbox_item(item)
        self.metrics.delivery_items_delivered += 1
        log_event(
            self.logger,
            "outbox_item_delivered",
            outbox_item_id=item.outbox_item_id,
            task_id=item.task_id,
            kind=item.kind,
            channel_id=post_ref.channel_id,
            thread_ts=post_ref.thread_ts,
            route_source=route.source,
        )
        return DrainResult(
            status="delivery_delivered",
            handoff_id=handoff.handoff_id,
            task_id=item.task_id,
            metrics=self.metrics.as_dict(),
        )

    def _fail_delivery_slack_error(
        self, item: OutboxItem, exc: SlackApiError
    ) -> DrainResult:
        self.metrics.errors += 1
        reason = f"slack_api_error:{exc.method}:{exc.error}"
        disposition = _delivery_slack_error_disposition(exc)
        if disposition == "terminal":
            self._fail_terminal_delivery_item(item, reason)
            self.metrics.claims_released += 1
            log_event(
                self.logger,
                "outbox_item_quarantined",
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=reason,
                slack_method=exc.method,
                slack_error=exc.error,
            )
            return DrainResult(
                status="slack_api_error",
                handoff_id=f"outbox:{item.outbox_item_id}",
                task_id=item.task_id,
                metrics=self.metrics.as_dict(),
            )
        if disposition == "operator":
            # Human-recoverable (invite the bot / unarchive). Never silently
            # dropped: alert loudly and retry within the budget so a quick fix
            # auto-delivers before the row is quarantined.
            log_event(
                self.logger,
                "outbox_item_operator_action_required",
                level=logging.ERROR,
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=reason,
                slack_method=exc.method,
                slack_error=exc.error,
                remediation="invite the Slack bot to the channel or unarchive it, then requeue",
            )
        return self._retry_or_quarantine_delivery_item(
            item,
            reason,
            status="slack_api_error",
            slack_method=exc.method,
            slack_error=exc.error,
        )

    def _fail_delivery_unexpected(self, item: OutboxItem, exc: Exception) -> DrainResult:
        self.metrics.errors += 1
        detail = str(exc).replace("\n", " ").strip()
        reason = f"runner_error:{type(exc).__name__}:{detail}"
        self.logger.exception(
            "unexpected error delivering outbox item %s", item.outbox_item_id
        )
        return self._retry_or_quarantine_delivery_item(
            item, reason, status="runner_error"
        )

    def _retry_or_quarantine_delivery_item(
        self,
        item: OutboxItem,
        reason: str,
        *,
        status: str,
        slack_method: str | None = None,
        slack_error: str | None = None,
    ) -> DrainResult:
        attempts = _delivery_attempt_count(item) + 1
        if attempts >= MAX_DELIVERY_ATTEMPTS:
            terminal_reason = f"exhausted_retries:attempt={attempts}:{reason}"
            try:
                self._fail_terminal_delivery_item(item, terminal_reason)
                self.metrics.claims_released += 1
            except Exception:
                self.logger.exception(
                    "failed to quarantine exhausted delivery outbox item"
                )
            log_event(
                self.logger,
                "outbox_item_quarantined",
                level=logging.ERROR,
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=terminal_reason,
                attempts=attempts,
                slack_method=slack_method,
                slack_error=slack_error,
            )
        else:
            retry_reason = f"{_DELIVERY_ATTEMPT_MARKER}{attempts}:{reason}"
            try:
                self.quay.fail_outbox_item(item, retry_reason)
                self.metrics.claims_released += 1
            except Exception:
                self.logger.exception(
                    "failed to mark delivery outbox item for retry"
                )
            log_event(
                self.logger,
                "outbox_item_failed",
                outbox_item_id=item.outbox_item_id,
                task_id=item.task_id,
                reason=retry_reason,
                attempts=attempts,
                slack_method=slack_method,
                slack_error=slack_error,
            )
        return DrainResult(
            status=status,
            handoff_id=f"outbox:{item.outbox_item_id}",
            task_id=item.task_id,
            metrics=self.metrics.as_dict(),
        )

    def _fail_terminal_delivery_item(self, item: OutboxItem, reason: str) -> None:
        terminal_reason = f"terminal:{reason}"
        self.quay.fail_outbox_item(item, terminal_reason)

    def _post_delivery_message(
        self,
        *,
        item: OutboxItem | None = None,
        handoff: Handoff,
        task: TaskContext,
        route: SlackRoute,
        message: str,
    ) -> SlackPostRef:
        if route.thread_ref:
            thread_ts = route.thread_ts or ""
            try:
                ref = self.slack.validate_thread(route.channel_id, thread_ts)
            except SlackApiError as exc:
                if exc.error == "thread_not_found" and route_is_metadata(route):
                    fallback_channel = (self.config.default_slack_channel or "").strip()
                    if fallback_channel:
                        fallback = SlackRoute(
                            channel_id=fallback_channel,
                            source="fallback_channel",
                        )
                        return self._post_delivery_message(
                            item=item,
                            handoff=handoff,
                            task=task,
                            route=fallback,
                            message=message,
                        )
                raise
            return self.slack.post_thread_message(
                ref,
                message,
                handoff=handoff,
                task=task,
            )
        return self.slack.post_question(
            route.channel_id,
            message,
            thread_ts=route.thread_ts,
            handoff=handoff,
            task=task,
        )

    def _post_remediation_fyi(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        outcome: RemediationOutcome,
    ) -> None:
        """Best-effort FYI after an auto-resume. A failed FYI must not undo it."""
        try:
            route = resolve_slack_route(handoff, task, self.config)
            if route is None:
                return
            message = build_remediation_fyi(handoff, task, outcome)
            self._post_delivery_message(
                handoff=handoff,
                task=task,
                route=route,
                message=message,
            )
            self.metrics.agent_fyi_posted += 1
            log_event(
                self.logger,
                "remediation_fyi_posted",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                category=outcome.category,
            )
        except Exception:
            self.logger.exception("remediation FYI post failed; ignoring")

    def _handle_claimed_handoff(self, handoff: Handoff) -> DrainResult:
        task = self.quay.get_task_context(handoff.task_id)
        artifact = self.quay.get_artifact(handoff) if handoff.artifact_id else None

        brief = choose_direct_brief(handoff)
        remediated = False
        # Run the (bounded, guardrailed) remediation turn when EITHER auto-resume
        # OR the escalation-message feature is enabled. The two flags are
        # independent: the message flag lets the turn run purely to author a
        # human-facing escalation, and NEVER authorizes an auto-resume.
        if brief is None and (
            self.config.remediation_enabled
            or self.config.remediation_escalation_message_enabled
        ):
            brief = self.remediator.remediate(
                handoff=handoff, task=task, artifact=artifact
            )
            # A gate-cleared resume is submitted ONLY under remediation_enabled.
            # In message-only mode a proposed resume is deliberately dropped
            # (brief -> None) so we fall through to escalate; the message flag
            # must never enable an auto-resume.
            if brief is not None and self.config.remediation_enabled:
                remediated = True
            else:
                if brief is not None:
                    # Message-only mode: the model proposed a gate-cleared resume
                    # but auto-resume is disabled, so we escalate instead.
                    log_event(
                        self.logger,
                        "remediation_resume_suppressed_message_only",
                        handoff_id=handoff.handoff_id,
                        task_id=handoff.task_id,
                    )
                    brief = None
                self.metrics.remediation_escalated += 1
                # F4: even when Otto escalates, still capture any recurring
                # Quay-CLI friction the model surfaced. Code-only and gated by
                # remediation_friction_enabled inside record_friction; a no-op
                # when there is no friction to file.
                self.remediator.record_friction(
                    outcome=self.remediator.last_outcome, task=task
                )
        if brief and remediated:
            outcome = self.remediator.last_outcome
            # Resolving a blocker -> the only Quay-valid resume reason for this
            # handoff kind. The "auto-remediated" distinction is recorded in the
            # log event and FYI below, not the reason enum.
            self.quay.submit_brief(handoff, brief, reason="blocker_resolved")
            self.quay.complete_claim(handoff)
            self.metrics.agent_briefs_submitted += 1
            log_event(
                self.logger,
                "brief_submitted_remediated",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                category=outcome.category if outcome else "",
            )
            if outcome is not None:
                self._post_remediation_fyi(
                    handoff=handoff, task=task, outcome=outcome
                )
                self.remediator.record_friction(outcome=outcome, task=task)
            return DrainResult(
                status="submitted_remediated",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )
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

        # Use a model-authored escalation body ONLY when the message feature is
        # enabled AND the remediation turn actually supplied one; otherwise the
        # renderer falls back to the deterministic build_human_question. Read
        # after the decision, this can never affect any gate or the resume path.
        model_message = None
        if (
            self.config.remediation_escalation_message_enabled
            and self.remediator.last_outcome is not None
        ):
            model_message = self.remediator.last_outcome.message
        question = build_escalation_message(handoff, task, artifact, model_message)
        if handoff_is_waiting_for_human(handoff):
            return self._poll_waiting_human_handoff(
                handoff=handoff,
                task=task,
                artifact=artifact,
                question=question,
                route=route,
            )
        if self.park_human_waits:
            question_result = self._post_question_and_escalate(
                handoff=handoff,
                task=task,
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
            post_ref, thread_ref = question_result
            return self._park_human_wait(
                handoff=handoff,
                task=task,
                post_ref=post_ref,
                thread_ref=thread_ref,
                route_source=route.source,
            )

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

    def _poll_waiting_human_handoff(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        artifact: Artifact | None,
        question: str,
        route: SlackRoute,
    ) -> DrainResult:
        if not route.thread_ref:
            reason = "missing_waiting_thread_ref"
            self.quay.release_claim(handoff, reason=reason)
            self.metrics.claims_released += 1
            log_event(
                self.logger,
                "claim_released",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                reason=reason,
                route_source=route.source,
            )
            return DrainResult(
                status=reason,
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                metrics=self.metrics.as_dict(),
            )

        thread_ts = route.thread_ts or ""
        try:
            post_ref = self.slack.validate_thread(route.channel_id, thread_ts)
        except SlackApiError as exc:
            if exc.error == "thread_not_found" and route_is_metadata(route):
                return self._handle_stale_slack_route(
                    handoff=handoff,
                    task=task,
                    artifact=artifact,
                    question=question,
                    stale_route=route,
                )
            raise

        thread_ref = slack_thread_ref(post_ref)
        reply, decision = self._drive_human_discussion(
            handoff=handoff,
            task=task,
            artifact=artifact,
            question=question,
            post_ref=post_ref,
        )
        if reply is not None and decision is not None:
            return self._submit_human_resume(
                handoff=handoff,
                task=task,
                post_ref=post_ref,
                thread_ref=thread_ref,
                reply=reply,
                decision=decision,
            )
        return self._park_human_wait(
            handoff=handoff,
            task=task,
            post_ref=post_ref,
            thread_ref=thread_ref,
            route_source=route.source,
        )

    def _post_question_and_escalate(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        question: str,
        route: SlackRoute,
    ) -> tuple[SlackPostRef, str] | None:
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
        return post_ref, thread_ref

    def _park_human_wait(
        self,
        *,
        handoff: Handoff,
        task: TaskContext,
        post_ref: SlackPostRef,
        thread_ref: str,
        route_source: str,
    ) -> DrainResult:
        self.metrics.human_waits_parked += 1
        log_event(
            self.logger,
            "human_wait_parked",
            handoff_id=handoff.handoff_id,
            task_id=task.task_id,
            channel_id=post_ref.channel_id,
            thread_ts=post_ref.thread_ts,
            thread_ref=thread_ref,
            route_source=route_source,
        )
        return DrainResult(
            status="waiting_for_human",
            handoff_id=handoff.handoff_id,
            task_id=task.task_id,
            metrics=self.metrics.as_dict(),
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
        question_result = self._post_question_and_escalate(
            handoff=handoff,
            task=task,
            question=question,
            route=route,
        )
        if question_result is None:
            return None

        post_ref, thread_ref = question_result
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
                ref = self.slack.validate_thread(route.channel_id, thread_ts)
                post_ref = self.slack.post_thread_message(
                    ref,
                    question,
                    handoff=handoff,
                    task=task,
                )
                return post_ref, True
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
            if remaining <= 0 and not self.park_human_waits:
                break
            wait_timeout = 0.0 if self.park_human_waits else remaining
            with self._suspend_lock_for_human_wait(handoff=handoff, task=task):
                reply = self.slack.wait_for_reply(
                    post_ref,
                    after_ts=after_ts,
                    timeout_seconds=wait_timeout,
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
                if self.park_human_waits:
                    return None, None
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
                if self.park_human_waits:
                    return None, None
                continue
            log_event(
                self.logger,
                "orchestrator_waiting_for_more_context",
                handoff_id=handoff.handoff_id,
                task_id=handoff.task_id,
                reply_ts=reply.ts,
                action=decision.action,
            )
            if self.park_human_waits:
                return None, None

        return None, None

    @contextmanager
    def _suspend_lock_for_human_wait(self, *, handoff: Handoff, task: TaskContext):
        if self.coordination_lock is None or self.park_human_waits:
            yield
            return

        log_event(
            self.logger,
            "drain_lock_suspended_for_human_wait",
            handoff_id=handoff.handoff_id,
            task_id=task.task_id,
        )
        with self.coordination_lock.suspended():
            yield
        log_event(
            self.logger,
            "drain_lock_reacquired_after_human_wait",
            handoff_id=handoff.handoff_id,
            task_id=task.task_id,
        )

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
            if self.park_human_waits:
                question_result = self._post_question_and_escalate(
                    handoff=handoff,
                    task=task,
                    question=question,
                    route=fallback_route,
                )
                if question_result is None:
                    raise RuntimeError("fallback Slack route was unexpectedly stale")
                post_ref, thread_ref = question_result
                return self._park_human_wait(
                    handoff=handoff,
                    task=task,
                    post_ref=post_ref,
                    thread_ref=thread_ref,
                    route_source=fallback_route.source,
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


def handoff_is_waiting_for_human(handoff: Handoff) -> bool:
    if _handoff_status_text(handoff.status) == "waiting_human":
        return True
    for key in ("status", "state"):
        value = handoff.metadata.get(key)
        if _handoff_status_text(value) == "waiting_human":
            return True
    raw = handoff.metadata.get("quay_handoff")
    if isinstance(raw, Mapping):
        for key in ("status", "state"):
            if _handoff_status_text(raw.get(key)) == "waiting_human":
                return True
    return False


def _handoff_status_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower().replace("-", "_")


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


# ── Compact human-escalation message ────────────────────────────────────────
#
# When a Quay worker blocks, the orchestrator asks a human for guidance in
# Slack. Humans triage these fast, so the renderer surfaces only signal-bearing
# fields: a reason-specific header, the real blocker line, and the concrete
# action we need — with generic handoff scaffolding and ``Quay handoff reason:``
# filler stripped. The helpers below are small and pure so they stay testable.

_HUMAN_QUESTION_FOOTER = "Reply in thread and I'll resume the worker once it's clear."

# Lines the legacy renderer (or Quay's ``_summary_for_handoff`` fallback)
# injects that carry no signal for a human. Matched case-insensitively against a
# fully stripped line.
_HANDOFF_FILLER_PREFIXES = ("quay handoff reason:",)
_HANDOFF_FILLER_LINES = frozenset(
    {
        "a quay task needs human guidance before the next worker brief.",
        "handoff summary:",
        "blocker/context:",
        "blocker:",
        "context:",
        "discuss freely in this thread. i will resume the worker once the path is clear.",
        "reply in thread and i'll resume the worker once it's clear.",
        "worker_blocker",
    }
)

# A line WITHOUT a trailing ``?`` only counts as the ask when it opens with a
# clear imperative / request. Ambiguous interrogative leads (does/what/how/…)
# are deliberately excluded here so declaratives like "Does not compile after
# the refactor." are not mistaken for the ask; a real question keeps its ``?``.
_HANDOFF_IMPERATIVE_PREFIXES = (
    "please", "confirm", "decide", "approve", "clarify", "provide",
    "advise", "let me know", "need to", "needs to",
    "should we", "should i", "shall we", "shall i",
)

# Softer signals used only as a fallback: when no strong ask is found and the
# generic default would otherwise fire, a leftover line opening with one of
# these (an uncertainty or a request) is promoted into ``Need:`` instead of
# being left to overflow into ``Context:``. Kept conservative on purpose.
_HANDOFF_SOFT_ASK_PREFIXES = (
    "unsure", "not sure", "unclear", "uncertain", "wondering", "wonder ",
    "should", "shall", "which", "whether", "need ", "needs ", "please",
    "can we", "can i", "could we", "could i", "would we", "would it",
    "let me know", "confirm", "decide", "clarify", "advise",
    "want to know", "requesting", "request ",
)


def _clean_handoff_lines(text: str) -> list[str]:
    """Split ``text`` into stripped, signal-bearing lines.

    Drops blank lines plus the generic handoff scaffolding and
    ``Quay handoff reason: <x>`` filler the renderer must not surface.
    """
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered in _HANDOFF_FILLER_LINES:
            continue
        if any(lowered.startswith(prefix) for prefix in _HANDOFF_FILLER_PREFIXES):
            continue
        lines.append(line)
    return lines


def _is_action_line(line: str) -> bool:
    """True when ``line`` clearly reads as a question or a concrete ask.

    A trailing ``?`` always qualifies. Without one, only a clear imperative /
    request does — ambiguous interrogative leads are left to the softer,
    fallback-only :func:`_promote_soft_ask` so plain declaratives are not
    mislabelled as the ask.
    """
    lowered = line.strip().lower()
    if not lowered:
        return False
    if lowered.endswith("?"):
        return True
    return lowered.startswith(_HANDOFF_IMPERATIVE_PREFIXES)


def _first_blocker_line(lines: list[str]) -> str:
    """The first real blocker statement — preferring a non-question line."""
    for line in lines:
        if not _is_action_line(line):
            return line
    return lines[0] if lines else ""


def _promote_action_line(lines: list[str]) -> str:
    """The first question/ask line, promotable into ``Need:`` (else empty)."""
    for line in lines:
        if _is_action_line(line):
            return line
    return ""


def _promote_soft_ask(lines: list[str]) -> str:
    """A leftover line that reads like an uncertainty/request (else empty).

    Fallback used only when no strong ask was promoted, so a real ask phrased
    without a ``?`` (e.g. "Unsure whether we should roll back or push forward")
    still reaches the human as ``Need:`` rather than the generic default.
    """
    for line in lines:
        if line.strip().lower().startswith(_HANDOFF_SOFT_ASK_PREFIXES):
            return line
    return ""


def _handoff_reason_kind(handoff: Handoff, blocker_text: str) -> str:
    """Classify the escalation so the header and default ``Need`` fit the case."""
    reason = (handoff.reason or "").lower()
    metadata = handoff.metadata if isinstance(handoff.metadata, Mapping) else {}
    budget_artifact = metadata.get("budget_exhausted_artifact_id")
    text = f"{blocker_text}\n{reason}".lower()
    budget_signalled = (
        "budget" in reason
        or budget_artifact not in (None, "", 0, "0", "null", "none")
        or (
            "budget" in text
            and any(
                token in text
                for token in ("exhaust", "exceed", "ran out", "run out", "depleted", "out of")
            )
        )
    )
    if budget_signalled:
        return "budget"
    if "manual" in reason or "manual resume" in text or "resume manually" in text:
        return "manual_resume"
    return "worker_blocker"


def _handoff_header(kind: str) -> str:
    if kind == "budget":
        return "*Quay worker blocked — budget exhausted*"
    if kind == "manual_resume":
        return "*Quay worker blocked — manual resume needed*"
    return "*Quay worker blocked*"


def _handoff_default_need(kind: str) -> str:
    if kind == "budget":
        return "approve more budget or stop the task."
    if kind == "manual_resume":
        return "confirm it's safe to resume and I'll continue."
    return "advise how the worker should proceed."


def build_human_question(
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
) -> str:
    # An explicit worker- or operator-authored question always wins verbatim;
    # only the auto-generated scaffolding below is compacted.
    if handoff.human_question and handoff.human_question.strip():
        return handoff.human_question.strip()
    meta_question = handoff.metadata.get("human_question")
    if isinstance(meta_question, str) and meta_question.strip():
        return meta_question.strip()

    artifact_lines = _clean_handoff_lines(artifact.text if artifact else "")
    summary_lines = _clean_handoff_lines(handoff.summary)
    # The artifact is the primary blocker; the summary supplements it. Consider
    # both (artifact first) when picking the blocker statement and the ask.
    combined_lines = list(dict.fromkeys(artifact_lines + summary_lines))

    reason_line = _first_blocker_line(combined_lines)
    kind = _handoff_reason_kind(handoff, "\n".join(combined_lines))

    promoted = _promote_action_line(combined_lines)
    if promoted and promoted != reason_line:
        need_line = promoted
    else:
        # No strong ask: try to rescue a softly-phrased ask from the leftover
        # before falling back to the reason-specific default.
        leftover_for_ask = [line for line in combined_lines if line != reason_line]
        need_line = _promote_soft_ask(leftover_for_ask) or _handoff_default_need(kind)

    identifier = task.issue or task.task_id
    repo = task.repo_id or handoff.repo_id
    header = _handoff_header(kind)
    header_suffix = " · ".join(part for part in (identifier, repo) if part)
    if header_suffix:
        header = f"{header} — {header_suffix}"

    lines = [header]

    title = (task.title or "").strip()
    if title and title not in {identifier, repo, reason_line}:
        lines.append(f"Task: {title}")

    author_mention = resolve_author_mention(handoff, task)
    if author_mention:
        lines.append(f"Contributor: {author_mention}")

    if reason_line:
        lines.append(f"Reason: {reason_line}")

    if need_line:
        lines.append(f"Need: {need_line}")

    # Context: every non-filler blocker/summary line we have not already
    # surfaced as Reason or Need. This guarantees no artifact signal — a second
    # question, a soft ask, or diagnostic detail — is ever silently dropped,
    # while staying empty for the common single-statement + single-question case.
    used = {reason_line, need_line}
    context_lines = [line for line in combined_lines if line not in used]
    if context_lines:
        if len(context_lines) == 1:
            lines.append(f"Context: {context_lines[0]}")
        else:
            lines.append("Context:")
            lines.extend(f"• {line}" for line in context_lines)

    lines.append(_HUMAN_QUESTION_FOOTER)
    return "\n".join(lines)


# A zero-width space wedged into a mention token breaks the exact-substring that
# Slack (and a human's eye) reads as a ping, while keeping the text legible.
_MENTION_NEUTRALIZER = "\u200b"

# Slack broadcast tokens and raw mention tokens. The model message is authored
# from attacker-influenceable blocker text, so it must never be able to @-ping a
# channel or a person. Each pattern is de-fanged so the visible text survives but
# cannot broadcast.
# NOTE: consume the optional label — Slack parses `<!channel|urgent>` as the same
# broadcast as `<!channel>`, so `\b[^>]*>` (mirroring the subteam pattern) is
# required; an anchored `...)>` regex misses the labeled/spaced form and it pings.
_BROADCAST_TOKEN_RE = re.compile(r"<!(channel|here|everyone)\b[^>]*>", re.IGNORECASE)
_SUBTEAM_TOKEN_RE = re.compile(r"<!subteam\^[^>]*>", re.IGNORECASE)
_RAW_USER_MENTION_RE = re.compile(r"<@([^>]*)>")
_BARE_BROADCAST_RE = re.compile(r"@(channel|here|everyone)\b", re.IGNORECASE)
# Backstop for every other Slack angle-bracket entity: channel refs `<#C..>`,
# disguised hyperlinks `<https://..|label>`, or any `<!..>`/`<@..>` form the
# passes above didn't fully consume. The lookahead requires a Slack sigil or URL
# scheme right after `<`, so ordinary text like "a < b" is left untouched.
_SLACK_ENTITY_OPEN_RE = re.compile(r"<(?=[@#!]|https?://|mailto:|tel:)", re.IGNORECASE)


def _defang_raw_user_mention(match: "re.Match[str]") -> str:
    inner = match.group(1).strip()
    label = inner.split("|", 1)[-1].strip() or inner.split("|", 1)[0].strip()
    return "@" + _MENTION_NEUTRALIZER + (label or "user")


def _sanitize_escalation_message(text: str) -> str:
    """Neutralize broadcast/mention injection in a model-authored message.

    The message is composed from attacker-influenceable blocker text, so before
    it can reach Slack we defang every token that could ping: ``@channel`` /
    ``@here`` / ``@everyone``, the Slack broadcast tokens ``<!channel>`` /
    ``<!here>`` / ``<!everyone>`` (including their labeled ``<!channel|label>``
    forms), user-group ``<!subteam^...>`` tokens, raw ``<@Uxxxx>`` user mentions,
    and any other Slack angle-bracket entity (channel refs, disguised
    hyperlinks). Each is rewritten so the visible text is preserved but the exact
    ping/link substring no longer exists. Also collapses whitespace and caps the
    length. Pure and side-effect free.
    """
    if not isinstance(text, str):
        return ""
    cleaned = text
    # Slack broadcast tokens -> visible but inert (no literal ``<!channel>`` and
    # no literal ``@channel`` left behind).
    cleaned = _BROADCAST_TOKEN_RE.sub(
        lambda m: "@" + _MENTION_NEUTRALIZER + m.group(1).lower(), cleaned
    )
    # User-group pings.
    cleaned = _SUBTEAM_TOKEN_RE.sub("@" + _MENTION_NEUTRALIZER + "group", cleaned)
    # Raw user mentions <@Uxxxx> / <@Uxxxx|name>.
    cleaned = _RAW_USER_MENTION_RE.sub(_defang_raw_user_mention, cleaned)
    # Bare @channel / @here / @everyone (both the originals and none of the
    # already-neutralized forms above, which carry a zero-width space).
    cleaned = _BARE_BROADCAST_RE.sub(
        lambda m: "@" + _MENTION_NEUTRALIZER + m.group(1).lower(), cleaned
    )
    # Break any remaining Slack angle-bracket entity (disguised hyperlink,
    # channel ref, or a mention/broadcast form missed above) by wedging a
    # zero-width space after the opening `<`, so Slack renders it as inert
    # literal text instead of a live link/ref/ping.
    cleaned = _SLACK_ENTITY_OPEN_RE.sub("<" + _MENTION_NEUTRALIZER, cleaned)
    # Collapse to a compact single block and cap the size.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > _MAX_ESCALATION_MESSAGE_CHARS:
        cleaned = cleaned[: _MAX_ESCALATION_MESSAGE_CHARS - 1].rstrip() + "…"
    return cleaned


def build_escalation_message(
    handoff: Handoff,
    task: TaskContext,
    artifact: Artifact | None,
    model_message: str | None,
) -> str:
    """Render the human escalation, optionally using a model-authored body.

    When ``model_message`` sanitizes to a non-empty string, it becomes the BODY
    of the escalation, wrapped in the SAME deterministic routing scaffolding
    :func:`build_human_question` uses (reason-specific header + identifier·repo,
    a ``Contributor:`` mention, and the shared footer). When it is ``None`` /
    blank, this falls back to :func:`build_human_question` unchanged. An explicit
    worker/operator ``human_question`` still wins verbatim (highest priority).
    """
    # Highest priority: an explicit worker/operator question is authoritative and
    # must win over any model-authored message (mirrors build_human_question).
    if handoff.human_question and handoff.human_question.strip():
        return handoff.human_question.strip()
    meta_question = handoff.metadata.get("human_question")
    if isinstance(meta_question, str) and meta_question.strip():
        return meta_question.strip()

    body = _sanitize_escalation_message(model_message or "")
    if not body:
        # No usable model message -> byte-identical to today's escalation.
        return build_human_question(handoff, task, artifact)

    artifact_lines = _clean_handoff_lines(artifact.text if artifact else "")
    summary_lines = _clean_handoff_lines(handoff.summary)
    combined_lines = list(dict.fromkeys(artifact_lines + summary_lines))
    kind = _handoff_reason_kind(handoff, "\n".join(combined_lines))

    identifier = task.issue or task.task_id
    repo = task.repo_id or handoff.repo_id
    header = _handoff_header(kind)
    header_suffix = " · ".join(part for part in (identifier, repo) if part)
    if header_suffix:
        header = f"{header} — {header_suffix}"

    lines = [header]
    author_mention = resolve_author_mention(handoff, task)
    if author_mention:
        lines.append(f"Contributor: {author_mention}")
    lines.append(body)
    lines.append(_HUMAN_QUESTION_FOOTER)
    return "\n".join(lines)


ORCHESTRATOR_DECISION_SYSTEM = (
    "You are the Quay orchestrator deciding whether a blocked worker can resume. "
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
    return f"quay-orch-{digest}"


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
        "route_hint",
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
    if isinstance(channel, str):
        channel = channel.strip()
        if channel:
            return SlackRoute(channel_id=channel, source=source)
    return None


def _parse_slack_thread_ref(value: str) -> tuple[str, str] | None:
    parts = [part.strip() for part in value.strip().split(":")]
    if parts and parts[0].lower() == "slack":
        # A `slack:` prefix is only valid in the full `slack:CHANNEL:THREAD_TS`
        # shape. Reject anything else (e.g. a truncated `slack:CHANNEL`) rather
        # than mis-reading "slack" as the channel id.
        if len(parts) != 3:
            return None
        channel_id, thread_ts = parts[1], parts[2]
    elif len(parts) == 2:
        channel_id, thread_ts = parts
    else:
        return None
    if not channel_id or not thread_ts:
        return None
    return channel_id, thread_ts


# --- Delivery outbox quarantine contract ------------------------------------
#
# One deterministically-bad delivery row must never starve the rows behind it.
# The orchestrator marks such rows with a `terminal:` error prefix and skips
# them on later drains. This only works if `quay outbox list --status pending`
# faithfully round-trips what `quay outbox fail --error <reason>` wrote, i.e.:
#
#   * a failed row is re-listed under `--status pending` (retryable), OR is
#     dropped from the pending list entirely once it is dead-lettered — either
#     way it stops being re-claimed; and
#   * the `--error` string survives on the row under one of `last_error`,
#     `error`, or `last_error_message`, with our `terminal:` marker intact
#     (optionally behind a Quay wrapper prefix such as "... : terminal:...").
#
# `MAX_DELIVERY_ATTEMPTS` also bounds retries for genuinely transient failures
# and for unexpected (non-Slack) exceptions, so even if Quay exposes no native
# attempt counter the row is quarantined after a few ticks. The fallback
# counter is carried in the `delivery_attempt=<n>` marker embedded in the
# retryable failure reason (read back via the same last_error round-trip).
MAX_DELIVERY_ATTEMPTS = 5

_DELIVERY_ATTEMPT_MARKER = "delivery_attempt="

# Slack Web API `error` values (and the synthetic `transport_error:` prefix
# used for network / 5xx / 429 failures) that are genuinely transient: the same
# payload and route can succeed on a later tick, so these are retried within the
# attempt budget rather than quarantined immediately.
_RETRYABLE_SLACK_ERRORS = frozenset(
    {
        "ratelimited",
        "rate_limited",
        "service_unavailable",
        "internal_error",
        "fatal_error",
        "backend_error",
        "request_timeout",
        "timeout",
        "gateway_timeout",
        "connection_error",
    }
)

# Deterministic Slack errors a human can still clear by fixing the destination
# channel (invite the bot / unarchive). Retried within the attempt budget so a
# quick fix auto-delivers, and surfaced loudly (see the operator alert) so the
# message is never silently dropped.
_OPERATOR_RECOVERABLE_SLACK_ERRORS = frozenset(
    {
        "not_in_channel",
        "is_archived",
    }
)


def _delivery_slack_error_disposition(exc: SlackApiError) -> str:
    """Classify a Slack delivery failure.

    Returns ``retryable`` (transient — retry within the attempt budget),
    ``operator`` (channel-level, human-recoverable — retry within budget *and*
    alert), or ``terminal`` (deterministic bad payload/route — quarantine
    immediately so it cannot starve the outbox).
    """
    error = (exc.error or "").strip().lower()
    if error.startswith("transport_error:") or error in _RETRYABLE_SLACK_ERRORS:
        return "retryable"
    if error in _OPERATOR_RECOVERABLE_SLACK_ERRORS:
        return "operator"
    return "terminal"


def _delivery_attempt_count(item: OutboxItem) -> int:
    """Best-effort count of prior delivery attempts for a claimed row.

    Prefers a native counter on the raw Quay outbox row; otherwise falls back
    to the ``delivery_attempt=<n>`` marker this orchestrator embeds in the
    retryable failure reason, so bounded retry still works when Quay exposes no
    attempt counter.
    """
    raw = item.metadata.get("quay_outbox")
    if not isinstance(raw, Mapping):
        return 0
    for key in ("delivery_attempts", "attempt_count", "attempts", "retry_count"):
        value = raw.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
    return _attempt_from_marker(_row_error(raw))


def _row_error(row: Mapping[str, Any]) -> str:
    return str(
        row.get("last_error")
        or row.get("error")
        or row.get("last_error_message")
        or ""
    )


def _attempt_from_marker(reason: str) -> int:
    marker_at = reason.find(_DELIVERY_ATTEMPT_MARKER)
    if marker_at < 0:
        return 0
    tail = reason[marker_at + len(_DELIVERY_ATTEMPT_MARKER) :]
    digits = ""
    for ch in tail:
        if ch.isdigit():
            digits += ch
        else:
            break
    return int(digits) if digits else 0


# Reason tokens that legitimately follow a `terminal:` / `quarantine:` marker.
# Wrapper-tolerance only fires when the marker is followed by one of these, so
# arbitrary error free-text that merely contains the word "terminal" (e.g.
# "connection terminal: reset by peer") is never misread as a terminal row.
_TERMINAL_REASON_TOKENS = frozenset(
    {
        "missing_default_slack_channel",
        "slack_api_error",
        "exhausted_retries",
        "channel_not_found",
        "thread_not_found",
        "not_in_channel",
        "is_archived",
        "msg_too_long",
        "invalid_blocks",
        "invalid_arguments",
        "restricted_action",
        "quarantined",
    }
)


def _leading_reason_token(text: str) -> str:
    token = ""
    for ch in text:
        if ch == ":" or ch.isspace():
            break
        token += ch
    return token


def _wrapped_terminal_reason(lowered: str) -> bool:
    """True only if a `terminal:`/`quarantine:` marker sits behind a wrapper
    boundary AND is immediately followed by a recognized terminal reason token.
    """
    for marker in ("terminal:", "quarantine:"):
        for sep in (" " + marker, ":" + marker):
            idx = lowered.find(sep)
            while idx >= 0:
                after = lowered[idx + len(sep) :]
                if _leading_reason_token(after) in _TERMINAL_REASON_TOKENS:
                    return True
                idx = lowered.find(sep, idx + 1)
    return False


def _terminal_outbox_row_reason(row: Mapping[str, Any]) -> str | None:
    status = str(row.get("status") or row.get("state") or "").strip().lower()
    if status in {
        "terminal",
        "quarantined",
        "quarantine",
        "failed_terminal",
        "dead",
        "dead_letter",
    }:
        return f"terminal_status:{status}"
    last_error = _row_error(row).strip()
    if not last_error:
        return None
    lowered = last_error.lower()
    # Canonical marker the orchestrator itself writes — always authoritative.
    if lowered.startswith("terminal:") or lowered.startswith("quarantine:"):
        return last_error
    # A row still carrying a delivery_attempt= retry marker is retryable-in-flight:
    # never let wrapper-tolerance misread free text inside its reason as terminal.
    if _DELIVERY_ATTEMPT_MARKER in lowered:
        return None
    # Tolerate a Quay wrapper prefix, e.g. "quay-wrapper: terminal:exhausted_retries",
    # but only when the marker is followed by a recognized terminal reason token.
    if _wrapped_terminal_reason(lowered):
        return last_error
    return None


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
    config = _env_first("QUAY_ORCHESTRATOR_CONFIG", "BRIX_ORCHESTRATOR_CONFIG")
    if config:
        return Path(config)
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


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    record = {"event": event, **fields}
    logger.log(level, json.dumps(record, sort_keys=True))


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
        with FileLock(lock_path) as lock:
            drainer = HandoffDrainer(
                quay=build_quay_client(config),
                slack=build_slack_client(config),
                config=config,
                worker_id=args.worker_id,
                coordination_lock=lock,
                park_human_waits=args.park_human_waits,
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
    parser = argparse.ArgumentParser(
        description="Drain Quay orchestrator delivery outbox items and handoffs"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_drain = sub.add_parser(
        "drain-one",
        help="claim and process one delivery outbox item or legacy handoff",
    )
    p_drain.add_argument("--config", help="path to orchestrator JSON config")
    p_drain.add_argument("--lock-path", help="override the singleton lock path")
    p_drain.add_argument(
        "--worker-id",
        default=_env_first(
            "QUAY_ORCHESTRATOR_WORKER_ID",
            "BRIX_ORCHESTRATOR_WORKER_ID",
        )
        or f"quay-orchestrator:{os.uname().nodename}",
        help="claim owner id passed to the Quay adapter",
    )
    p_drain.add_argument(
        "--ignore-disabled",
        action="store_true",
        help="run even when config.enabled is false",
    )
    p_drain.add_argument(
        "--park-human-waits",
        action="store_true",
        help=(
            "post or poll one human-wait handoff without blocking for the full "
            "reply timeout"
        ),
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


def _json_mapping_field(encoded: Any, fallback: Any = None) -> dict[str, Any]:
    value = fallback
    if isinstance(encoded, str) and encoded:
        try:
            value = json.loads(encoded)
        except json.JSONDecodeError:
            return {"raw_json": encoded}
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    return {"value": value}


def delivery_message_from_outbox(
    item: OutboxItem,
    task: TaskContext | None = None,
) -> str:
    if _is_pr_ready_approved_delivery(item):
        message = _pr_ready_approved_message_from_outbox(item, task)
        if message:
            return message

    for key in ("text", "message", "body", "summary"):
        value = item.payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    title = item.payload.get("title")
    url = item.payload.get("url") or item.payload.get("html_url")
    if isinstance(title, str) and title.strip():
        text = title.strip()
        if isinstance(url, str) and url.strip():
            return f"{text}\n{url.strip()}"
        return text
    if item.payload:
        return f"Quay delivery item {item.kind}:\n{json.dumps(item.payload, sort_keys=True)}"
    return f"Quay delivery item {item.kind} for task {item.task_id}"


def _is_pr_ready_approved_delivery(item: OutboxItem) -> bool:
    kind = item.kind.strip().lower().replace("-", "_")
    return kind in {"pr_ready_approved", "slack.pr_ready_approved"}


def _pr_ready_approved_message_from_outbox(
    item: OutboxItem,
    task: TaskContext | None,
) -> str:
    payload = item.payload
    task_id = _first_text(
        payload.get("task_id"),
        item.task_id,
        task.task_id if task else None,
    )
    external_ref = _first_text(
        payload.get("external_ref"),
        payload.get("ticket_ref"),
        payload.get("issue"),
        payload.get("issue_id"),
        payload.get("linear_id"),
        task.issue if task else None,
    )
    repo = _first_text(
        payload.get("repo_id"),
        payload.get("repository"),
        payload.get("repository_full_name"),
        payload.get("repo"),
        payload.get("repo_name"),
        task.repo_id if task else None,
    )
    pr_number = _first_text(
        payload.get("pr_number"),
        payload.get("pull_request_number"),
        payload.get("github_pr_number"),
        _nested_value(payload, "pr", "number"),
        _nested_value(payload, "pull_request", "number"),
    )
    pr_url = _first_text(
        payload.get("pr_url"),
        payload.get("pull_request_url"),
        payload.get("html_url"),
        _nested_value(payload, "pr", "url"),
        _nested_value(payload, "pr", "html_url"),
        _nested_value(payload, "pull_request", "url"),
        _nested_value(payload, "pull_request", "html_url"),
    )
    review_id = _first_text(
        payload.get("review_id"),
        payload.get("pull_request_review_id"),
        _nested_value(payload, "review", "id"),
    )
    review_url = _first_text(
        payload.get("review_url"),
        payload.get("pull_request_review_url"),
        _nested_value(payload, "review", "url"),
        _nested_value(payload, "review", "html_url"),
    )
    title = _first_text(
        payload.get("title"),
        payload.get("pr_title"),
        payload.get("pull_request_title"),
    )
    branch = _first_text(
        payload.get("branch"),
        payload.get("branch_name"),
        payload.get("head_branch"),
        payload.get("source_branch"),
    )
    head_sha = _first_text(
        payload.get("head_sha"),
        payload.get("commit_sha"),
        payload.get("sha"),
    )
    note = _first_text(payload.get("message"), payload.get("text"), payload.get("summary"))

    lines = ["*Quay PR ready and reviewer-approved*"]
    rendered_values: set[str] = set()

    def add_line(label: str, value: str, *aliases: Any) -> None:
        lines.append(f"{label}: {value}")
        for item in (value, *aliases):
            text = _first_text(item)
            if text:
                rendered_values.add(text)

    if external_ref:
        add_line("Ticket", external_ref)
    if repo:
        add_line("Repo", repo)
    pr_ref = _format_pr_ref(pr_number, pr_url)
    if pr_ref:
        add_line("PR", pr_ref, pr_number, pr_url)
    if review_id:
        add_line("Review", _format_link(review_id, review_url), review_id, review_url)
    if task_id:
        add_line("Task", task_id)
    if title:
        add_line("Title", title)
    if branch:
        add_line("Branch", branch)
    if head_sha:
        add_line("Head", head_sha[:12], head_sha)
    if note and note not in rendered_values:
        add_line("Note", note)

    return "\n".join(lines)


def _format_pr_ref(pr_number: str | None, pr_url: str | None) -> str:
    if pr_number and pr_url:
        label = pr_number if pr_number.startswith("#") else f"#{pr_number}"
        return _format_link(label, pr_url)
    if pr_number:
        return pr_number if pr_number.startswith("#") else f"#{pr_number}"
    if pr_url:
        return pr_url
    return ""


def _format_link(label: str, url: str | None) -> str:
    if not url:
        return label
    return f"<{url}|{label}>"


def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _nested_value(mapping: Mapping[str, Any], *path: str) -> Any:
    value: Any = mapping
    for key in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


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


def _outbox_unsupported(exc: QuayCommandError) -> bool:
    if len(exc.argv) < 3 or exc.argv[1:3] != ["outbox", "list"]:
        return False
    if exc.code in {"unknown_command", "unknown_subcommand"}:
        return True
    text = f"{exc.stdout}\n{exc.stderr}".lower()
    return "outbox" in text and (
        "unknown command" in text
        or "unknown subcommand" in text
        or "invalid command" in text
    )


@contextmanager
def _temp_text_file(text: str):
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        prefix="quay-orchestrator-",
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


def _positive_int(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        try:
            parsed = int(float(value))
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
