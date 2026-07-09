from __future__ import annotations

import dataclasses
import importlib.util
import json
import logging
import sys
import threading
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "ops" / "quay_orchestrator.py"
SPEC = importlib.util.spec_from_file_location("quay_orchestrator", MODULE_PATH)
assert SPEC and SPEC.loader
quay = importlib.util.module_from_spec(SPEC)
sys.modules["quay_orchestrator"] = quay
SPEC.loader.exec_module(quay)


class FakeQuayClient:
    def __init__(
        self,
        handoff,
        *,
        task=None,
        artifact=None,
    ) -> None:
        self._handoff = handoff
        self.task = task or quay.TaskContext(
            task_id=handoff.task_id,
            title="Fix stuck worker",
            issue="BRIX-1405",
            repo_id="hermes-agent",
        )
        self.artifact = artifact or quay.Artifact(
            artifact_id=handoff.artifact_id or "artifact-1",
            text="Worker needs product guidance.",
            kind="blocker",
        )
        self.escalations: list[tuple[object, str, str | None]] = []
        self.submitted: list[tuple[object, str, str]] = []
        self.completed: list[object] = []
        self.released: list[tuple[object, str]] = []
        self.state = "pending" if handoff is not None else "empty"
        self.last_thread_ref: str | None = None

    def claim_handoff(self, worker_id: str):
        if self.state != "pending":
            return None
        handoff = self._handoff
        self._handoff = None
        if handoff is not None:
            self.state = "claimed"
        return handoff

    def claim_work(self, worker_id: str):
        return self.claim_handoff(worker_id)

    def get_task_context(self, task_id: str):
        return self.task

    def get_artifact(self, handoff):
        return self.artifact

    def escalate_human(self, handoff, question: str, thread_ref: str) -> None:
        if self.state != "claimed":
            raise RuntimeError(f"wrong_state: cannot escalate from {self.state}")
        self.state = "waiting_human"
        self.last_thread_ref = thread_ref
        self.escalated = (handoff, question, thread_ref)
        self.escalations.append((handoff, question, thread_ref))

    def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
        if self.state != "waiting_human":
            raise RuntimeError(f"wrong_state: cannot record reply from {self.state}")
        self.state = "claimed"
        self.recorded_reply = (handoff, reply, thread_ref)

    def submit_brief(self, handoff, brief: str, *, reason: str) -> None:
        if self.state != "claimed":
            raise RuntimeError(f"wrong_state: cannot submit brief from {self.state}")
        self.state = "queued"
        self.submitted.append((handoff, brief, reason))

    def complete_claim(self, handoff) -> None:
        if self.state != "queued":
            raise RuntimeError(f"wrong_state: cannot complete from {self.state}")
        self.state = "completed"
        self.completed.append(handoff)

    def release_claim(self, handoff, reason: str) -> None:
        if self.state not in {"claimed", "waiting_human"}:
            raise RuntimeError(f"wrong_state: cannot release from {self.state}")
        self.state = "pending"
        self.released.append((handoff, reason))
        if self.last_thread_ref:
            handoff_metadata = dict(handoff.metadata)
            handoff_metadata.setdefault("slack_thread_ref", self.last_thread_ref)
            handoff = dataclasses.replace(handoff, metadata=handoff_metadata)
            task_metadata = dict(self.task.metadata)
            task_metadata.setdefault("slack_thread_ref", self.last_thread_ref)
            self.task = dataclasses.replace(self.task, metadata=task_metadata)
        self._handoff = handoff

    def complete_outbox_item(self, item) -> None:
        raise AssertionError("legacy fake should not complete outbox items")

    def fail_outbox_item(self, item, reason: str) -> None:
        raise AssertionError("legacy fake should not fail outbox items")


class FakeOutboxQuayClient:
    def __init__(self, item, *, task=None) -> None:
        self._item = item
        self.task = task or quay.TaskContext(
            task_id=item.task_id,
            title="Review approved PR",
            issue="BRIX-1447",
            repo_id="hermes-agent",
        )
        self.completed: list[object] = []
        self.failed: list[tuple[object, str]] = []
        self.state = "pending" if item is not None else "empty"

    def claim_work(self, worker_id: str):
        if self.state != "pending":
            return None
        self.state = "claimed"
        return self._item

    def claim_handoff(self, worker_id: str):
        return None

    def get_task_context(self, task_id: str):
        return self.task

    def get_artifact(self, handoff):
        raise AssertionError("delivery items do not fetch artifacts")

    def escalate_human(self, handoff, question: str, thread_ref: str | None) -> None:
        raise AssertionError("delivery items do not escalate humans")

    def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
        raise AssertionError("delivery items do not record human replies")

    def submit_brief(self, handoff, brief: str, *, reason: str) -> None:
        raise AssertionError("delivery items do not submit briefs")

    def complete_claim(self, handoff) -> None:
        raise AssertionError("delivery items do not complete task claims")

    def release_claim(self, handoff, reason: str) -> None:
        raise AssertionError("delivery items do not release task claims")

    def complete_outbox_item(self, item) -> None:
        if self.state != "claimed":
            raise RuntimeError(f"wrong_state: cannot complete outbox from {self.state}")
        self.state = "completed"
        self.completed.append(item)

    def fail_outbox_item(self, item, reason: str) -> None:
        if self.state != "claimed":
            raise RuntimeError(f"wrong_state: cannot fail outbox from {self.state}")
        self.state = "pending"
        self.failed.append((item, reason))


class ReclaimingOutboxQuayClient:
    """Round-trips fail_outbox_item reasons back as the row's ``last_error`` on
    the next claim, and honors terminal markers by refusing to re-claim.

    This mirrors the exact Quay outbox contract the quarantine logic depends
    on (documented at ops/quay_orchestrator.py), so bounded retry and the
    terminal-skip can be exercised across successive drains instead of being
    faked with a hard-coded ``last_error``.
    """

    def __init__(self, raw, *, task=None) -> None:
        self._raw = dict(raw)
        self.task = task or quay.TaskContext(
            task_id=str(raw.get("task_id") or ""),
            title="Review approved PR",
            issue="BRIX-1447",
            repo_id="hermes-agent",
        )
        self.completed: list[object] = []
        self.failed: list[tuple[object, str]] = []
        self.claims = 0

    def claim_work(self, worker_id: str):
        if quay._terminal_outbox_row_reason(self._raw):
            return None
        self.claims += 1
        return quay.OutboxItem.from_mapping(self._raw)

    def claim_handoff(self, worker_id: str):
        return None

    def get_task_context(self, task_id: str):
        return self.task

    def complete_outbox_item(self, item) -> None:
        self._raw["status"] = "completed"
        self.completed.append(item)

    def fail_outbox_item(self, item, reason: str) -> None:
        self._raw["last_error"] = reason
        self._raw["status"] = "pending"
        self.failed.append((item, reason))


def delivery_raw(*, route_hint=None, payload=None, **extra):
    raw = {
        "outbox_item_id": 31,
        "task_id": "task-delivery",
        "kind": "slack.pr_ready_approved",
        "handler_class": "delivery",
        "claim_id": "outbox-claim-31",
        "payload_json": json.dumps(
            payload or {"message": "PR #44 is approved and ready."}
        ),
        "route_hint_json": json.dumps(
            route_hint if route_hint is not None else {"channel_id": "CDELIVERY"}
        ),
        "status": "pending",
        "last_error": None,
    }
    raw.update(extra)
    return raw


class _CapturingLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _capturing_logger(name: str) -> tuple[logging.Logger, _CapturingLogHandler]:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    handler = _CapturingLogHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger, handler


class FakeSlackClient:
    def __init__(
        self,
        reply=None,
        *,
        wait_results=None,
        post_error=None,
        validate_results=None,
    ) -> None:
        self.wait_results = list(wait_results or ([] if reply is None else [reply]))
        self.post_error = post_error
        self.validate_results = list(validate_results or [])
        self.validations: list[tuple[str, str]] = []
        self.questions: list[tuple[str, str, str | None]] = []
        self.waits: list[tuple[object, float, float]] = []
        self.acks: list[tuple[object, str]] = []
        self.thread_floor_ts: dict[tuple[str, str], str] = {}

    def validate_thread(self, channel_id: str, thread_ts: str):
        self.validations.append((channel_id, thread_ts))
        if self.validate_results:
            result = self.validate_results.pop(0)
            if isinstance(result, Exception):
                raise result
            if isinstance(result, quay.SlackPostRef):
                return result
        floor_ts = self.thread_floor_ts.get((channel_id, thread_ts), thread_ts)
        return quay.SlackPostRef(
            channel_id=channel_id,
            ts=floor_ts,
            thread_ts=thread_ts,
        )

    def post_question(self, channel_id: str, text: str, *, thread_ts=None, handoff, task):
        if self.post_error:
            raise self.post_error
        self.questions.append((channel_id, text, thread_ts))
        ref = quay.SlackPostRef(
            channel_id=channel_id,
            ts="1000.000000",
            thread_ts=thread_ts or "1000.000000",
        )
        self.thread_floor_ts[(ref.channel_id, ref.thread_ts)] = ref.ts
        return ref

    def wait_for_reply(self, ref, *, after_ts: str, timeout_seconds: float, poll_interval_seconds: float):
        self.waits.append((ref, timeout_seconds, poll_interval_seconds))
        if self.wait_results:
            while self.wait_results:
                result = self.wait_results.pop(0)
                if isinstance(result, Exception):
                    raise result
                if result is None:
                    return None
                if result.ts and quay._slack_ts_key(result.ts) <= quay._slack_ts_key(after_ts):
                    continue
                return result
        return None

    def post_thread_message(self, ref, text: str, *, handoff, task):
        self.acks.append((ref, text))
        post_ref = quay.SlackPostRef(
            channel_id=ref.channel_id,
            ts="1002.000000",
            thread_ts=ref.thread_ts,
        )
        self.thread_floor_ts[(post_ref.channel_id, post_ref.thread_ts)] = post_ref.ts
        return post_ref


class FakeDecider:
    def __init__(self, decisions=None) -> None:
        self.decisions = list(decisions or [])
        self.calls = []

    def decide(self, **kwargs):
        self.calls.append(kwargs)
        if self.decisions:
            decision = self.decisions.pop(0)
            if isinstance(decision, Exception):
                raise decision
            return decision
        return quay.ConversationDecision(action="wait")


def ready(brief: str) -> quay.ConversationDecision:
    return quay.ConversationDecision(action="ready", brief=brief)


def ask(message: str) -> quay.ConversationDecision:
    return quay.ConversationDecision(action="ask", message=message)


def delivery_item(*, route_hint=None, payload=None):
    return quay.OutboxItem.from_mapping(
        {
            "outbox_item_id": 31,
            "task_id": "task-delivery",
            "kind": "slack.pr_ready_approved",
            "handler_class": "delivery",
            "claim_id": "outbox-claim-31",
            "payload_json": json.dumps(
                payload or {"message": "PR #44 is approved and ready."}
            ),
            "route_hint_json": json.dumps(route_hint or {}),
        }
    )


class FakeAgent:
    instances = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.prompts = []
        FakeAgent.instances.append(self)

    def run_conversation(self, prompt, **kwargs):
        self.prompts.append((prompt, kwargs))
        return {"final_response": '{"action":"ready","brief":"Resume with the answer."}'}


class FakeCompletedProcess:
    def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class RecordingQuayRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.file_payloads: list[str] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        command = argv[1:]
        if command == [
            "outbox",
            "list",
            "--status",
            "pending",
            "--handler-class",
            "delivery",
        ]:
            return FakeCompletedProcess("[]\n")
        if command[:4] == ["handoff", "list", "--status", "pending"]:
            return FakeCompletedProcess(
                '[{"handoff_id":7,"task_id":"task-cli","reason":"worker_blocker",'
                '"state_event_id":99,"idempotency_key":"task-cli:99:worker_blocker",'
                '"payload_json":"{\\"attempt_id\\":1,\\"artifact_id\\":42,'
                '\\"blocker_content_hash\\":\\"hash\\",\\"blocker_bytes\\":24,'
                '\\"budget_exhausted_artifact_id\\":null}","status":"pending",'
                '"claim_id":null,"claimed_at":null,"completed_at":null,'
                '"created_at":"2026-05-14T00:00:00.000Z",'
                '"updated_at":"2026-05-14T00:00:00.000Z"}]\n'
            )
        if command == ["task", "claim", "task-cli"]:
            return FakeCompletedProcess('{"task_id":"task-cli","claim_id":"claim-1","state":"claimed-by-orchestrator"}\n')
        if command == ["task", "get", "task-cli"]:
            return FakeCompletedProcess(
                '{"task_id":"task-cli","repo_id":"repo-1","state":"claimed-by-orchestrator",'
                '"external_ref":"BRIX-1405","branch_name":"quay/task",'
                '"slack_thread_ref":"GPRIVATE123:999.000000",'
                '"authors_json":"[{\\"slack_id\\":\\"U06TDC56VJB\\"}]"}\n'
            )
        if command == ["artifact", "get", "task-cli", "blocker"]:
            return FakeCompletedProcess("blocked on product input\n")
        if command[0] in {"escalate-human", "record-human-reply", "submit-brief"}:
            for flag in ("--question-file", "--reply-file", "--brief-file"):
                if flag in command:
                    self.file_payloads.append(Path(command[command.index(flag) + 1]).read_text())
            if command[0] == "escalate-human":
                return FakeCompletedProcess('{"task_id":"task-cli","state":"waiting_human","artifact_id":1,"escalation_seq":1,"escalation_nonce":"n","thread_ref":"C1:1"}\n')
            if command[0] == "record-human-reply":
                return FakeCompletedProcess('{"task_id":"task-cli","state":"claimed-by-orchestrator","artifact_id":2}\n')
            return FakeCompletedProcess('{"task_id":"task-cli","state":"queued","attempt_id":3}\n')
        raise AssertionError(f"unexpected quay command: {command!r}")


class RecordingDeliveryOutboxRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        command = argv[1:]
        if command == [
            "outbox",
            "list",
            "--status",
            "pending",
            "--handler-class",
            "delivery",
        ]:
            return FakeCompletedProcess(
                '[{"outbox_item_id":21,"task_id":"task-delivery-cli",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"source_event_id":null,'
                '"idempotency_key":"task-delivery-cli:ready-approved",'
                '"payload_json":"{\\"message\\":\\"PR #44 is approved and ready.\\"}",'
                '"route_hint_json":"{\\"channel_id\\":\\"CDELIVERY\\"}",'
                '"status":"pending","claim_id":null,"claimed_at":null,'
                '"delivered_at":null,"completed_at":null,"last_error":null,'
                '"next_eligible_at":null,'
                '"created_at":"2026-05-22T00:00:00.000Z",'
                '"updated_at":"2026-05-22T00:00:00.000Z"}]\n'
            )
        if command == ["outbox", "claim", "21"]:
            return FakeCompletedProcess(
                '{"outbox_item_id":21,"task_id":"task-delivery-cli",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"status":"claimed","claim_id":"delivery-claim-1"}\n'
            )
        if command == ["task", "get", "task-delivery-cli"]:
            return FakeCompletedProcess(
                '{"task_id":"task-delivery-cli","repo_id":"repo-1",'
                '"external_ref":"BRIX-1447","branch_name":"quay/delivery"}\n'
            )
        if command == [
            "outbox",
            "complete",
            "21",
            "--claim-id",
            "delivery-claim-1",
        ]:
            return FakeCompletedProcess(
                '{"outbox_item_id":21,"task_id":"task-delivery-cli",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"status":"completed","delivered_at":"2026-05-22T00:01:00.000Z",'
                '"completed_at":"2026-05-22T00:01:00.000Z"}\n'
            )
        raise AssertionError(f"unexpected quay command: {command!r}")


class RecordingWrongClassOutboxRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        command = argv[1:]
        if command == [
            "outbox",
            "list",
            "--status",
            "pending",
            "--handler-class",
            "delivery",
        ]:
            return FakeCompletedProcess(
                '[{"outbox_item_id":22,"task_id":"task-workflow-cli",'
                '"kind":"workflow_intervention.worker_blocker",'
                '"handler_class":"workflow_intervention","status":"pending"}]\n'
            )
        if command == ["outbox", "claim", "22"]:
            return FakeCompletedProcess(
                '{"outbox_item_id":22,"task_id":"task-workflow-cli",'
                '"kind":"workflow_intervention.worker_blocker",'
                '"handler_class":"workflow_intervention","status":"claimed",'
                '"claim_id":"wrong-class-claim"}\n'
            )
        if command == [
            "outbox",
            "fail",
            "22",
            "--claim-id",
            "wrong-class-claim",
            "--error",
            "unsupported_handler_class:workflow_intervention",
        ]:
            return FakeCompletedProcess('{"outbox_item_id":22,"status":"pending"}\n')
        if command[:4] == ["handoff", "list", "--status", "pending"]:
            return FakeCompletedProcess("[]\n")
        raise AssertionError(f"unexpected quay command: {command!r}")


class RecordingTerminalThenReadyOutboxRunner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        command = argv[1:]
        if command == [
            "outbox",
            "list",
            "--status",
            "pending",
            "--handler-class",
            "delivery",
        ]:
            return FakeCompletedProcess(
                '[{"outbox_item_id":22,"task_id":"task-poisoned",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"status":"pending","last_error":"terminal:missing_default_slack_channel"},'
                '{"outbox_item_id":23,"task_id":"task-ready",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"payload_json":"{\\"message\\":\\"PR #45 is approved.\\"}",'
                '"route_hint_json":"{\\"channel_id\\":\\"CDELIVERY\\"}",'
                '"status":"pending","last_error":null}]\n'
            )
        if command == ["outbox", "claim", "23"]:
            return FakeCompletedProcess(
                '{"outbox_item_id":23,"task_id":"task-ready",'
                '"kind":"slack.pr_ready_approved","handler_class":"delivery",'
                '"status":"claimed","claim_id":"delivery-claim-23"}\n'
            )
        if command == ["task", "get", "task-ready"]:
            return FakeCompletedProcess(
                '{"task_id":"task-ready","repo_id":"repo-1",'
                '"external_ref":"BRIX-1842","branch_name":"quay/ready"}\n'
            )
        if command == [
            "outbox",
            "complete",
            "23",
            "--claim-id",
            "delivery-claim-23",
        ]:
            return FakeCompletedProcess('{"outbox_item_id":23,"status":"completed"}\n')
        raise AssertionError(f"unexpected quay command: {command!r}")


def test_drain_one_submits_direct_next_brief_without_slack():
    handoff = quay.Handoff(
        handoff_id="handoff-1",
        task_id="task-1",
        next_brief="Apply the reviewer suggestion and rerun tests.",
    )
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "submitted_direct"
    assert quay_client.submitted == [
        (handoff, "Apply the reviewer suggestion and rerun tests.", "blocker_resolved")
    ]
    assert quay_client.completed == [handoff]
    assert quay_client.released == []
    assert slack.questions == []
    assert result.metrics["direct_briefs_submitted"] == 1


def test_drain_one_delivers_outbox_item_to_existing_thread():
    item = delivery_item(
        payload={
            "external_ref": "BRIX-1447",
            "repo_id": "InverterNetwork/hermes-agent",
            "pr_number": 44,
            "pr_url": "https://github.com/InverterNetwork/hermes-agent/pull/44",
            "review_id": "RVW_kwDOExample",
            "head_sha": "abcdef1234567890",
            "branch": "quay/task-delivery",
            "route_hint": {"slack_thread_ref": "C1234567890:1000.000000"},
        }
    )
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.validations == [("C1234567890", "1000.000000")]
    assert slack.questions == []
    delivered_text = slack.acks[0][1]
    assert "Quay PR ready and reviewer-approved" in delivered_text
    assert "Ticket: BRIX-1447" in delivered_text
    assert "Repo: InverterNetwork/hermes-agent" in delivered_text
    assert "PR: <https://github.com/InverterNetwork/hermes-agent/pull/44|#44>" in delivered_text
    assert "Review: RVW_kwDOExample" in delivered_text
    assert "Task: task-delivery" in delivered_text
    assert "Head: abcdef123456" in delivered_text
    assert quay_client.completed == [item]
    assert quay_client.failed == []
    assert result.metrics["outbox_items_claimed"] == 1
    assert result.metrics["delivery_items_delivered"] == 1


def test_drain_one_delivers_outbox_item_with_prefixed_slack_thread_ref():
    item = delivery_item(
        payload={
            "external_ref": "BRIX-1842",
            "repo_id": "InverterNetwork/hermes-agent",
            "pr_number": 44,
            "route_hint": {"slack_thread_ref": "slack:C1234567890:1000.000000"},
        }
    )
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.validations == [("C1234567890", "1000.000000")]
    assert slack.acks[0][0] == quay.SlackPostRef(
        channel_id="C1234567890",
        ts="1000.000000",
        thread_ts="1000.000000",
    )
    assert quay_client.completed == [item]
    assert quay_client.failed == []


def test_pr_ready_approved_message_dedupes_note_against_rendered_fields():
    item = delivery_item(
        payload={
            "title": "Ready after approval",
            "message": "Ready after approval",
            "repo_id": "InverterNetwork/hermes-agent",
        }
    )

    message = quay.delivery_message_from_outbox(
        item,
        quay.TaskContext(task_id=item.task_id, issue="BRIX-1447"),
    )

    assert "Title: Ready after approval" in message
    assert "Note: Ready after approval" not in message


def test_outbox_handoff_summary_uses_task_context_fallbacks():
    item = delivery_item(payload={})
    handoff = item.as_handoff(
        quay.TaskContext(
            task_id=item.task_id,
            issue="BRIX-1447",
            repo_id="InverterNetwork/hermes-agent",
        )
    )

    assert "Ticket: BRIX-1447" in handoff.summary
    assert "Repo: InverterNetwork/hermes-agent" in handoff.summary
    assert "Task: task-delivery" in handoff.summary


def test_drain_one_delivers_outbox_item_to_default_channel_without_route():
    item = delivery_item(route_hint={})
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="CFALLBACK1",
        ),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.validations == []
    assert slack.questions[0][0] == "CFALLBACK1"
    assert "Task: task-delivery" in slack.questions[0][1]
    assert quay_client.completed == [item]
    assert quay_client.failed == []


def test_drain_one_delivery_stale_thread_falls_back_to_default_channel():
    item = delivery_item(route_hint={"slack_thread_ref": "CSTALE1234:1000.000000"})
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient(
        validate_results=[
            quay.SlackApiError("conversations.replies", "thread_not_found"),
        ]
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="CFALLBACK1",
        ),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.validations == [("CSTALE1234", "1000.000000")]
    assert slack.questions[0][0] == "CFALLBACK1"
    assert slack.questions[0][2] is None
    assert quay_client.completed == [item]
    assert quay_client.failed == []


def test_drain_one_delivery_missing_route_marks_item_terminal():
    item = delivery_item(route_hint={})
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "missing_default_slack_channel"
    assert slack.questions == []
    assert quay_client.completed == []
    assert quay_client.failed == [(item, "terminal:missing_default_slack_channel")]


def test_drain_one_delivery_terminal_slack_failure_marks_item_terminal():
    item = delivery_item(route_hint={})
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient(
        post_error=quay.SlackApiError("chat.postMessage", "channel_not_found")
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="CFALLBACK1",
        ),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "slack_api_error"
    assert quay_client.completed == []
    assert quay_client.failed == [
        (item, "terminal:slack_api_error:chat.postMessage:channel_not_found")
    ]


def test_drain_one_delivery_msg_too_long_quarantines_immediately():
    item = delivery_item(route_hint={"channel_id": "CDELIVERY"})
    quay_client = FakeOutboxQuayClient(item)
    slack = FakeSlackClient(
        post_error=quay.SlackApiError("chat.postMessage", "msg_too_long")
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    # msg_too_long is a deterministic bad payload: quarantined on the first
    # failure so it cannot starve the rows behind it.
    assert result.status == "slack_api_error"
    assert quay_client.completed == []
    assert quay_client.failed == [
        (item, "terminal:slack_api_error:chat.postMessage:msg_too_long")
    ]


def test_drain_one_delivery_transient_slack_error_retries_then_quarantines():
    quay_client = ReclaimingOutboxQuayClient(delivery_raw())
    slack = FakeSlackClient(
        post_error=quay.SlackApiError("chat.postMessage", "ratelimited")
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    statuses = [
        drainer.drain_one().status
        for _ in range(quay.MAX_DELIVERY_ATTEMPTS + 2)
    ]
    reasons = [reason for _, reason in quay_client.failed]

    # ratelimited is transient: retried with an incrementing marker for the
    # first MAX-1 attempts, then quarantined once the budget is exhausted.
    retryable = [r for r in reasons if r.startswith(quay._DELIVERY_ATTEMPT_MARKER)]
    assert len(retryable) == quay.MAX_DELIVERY_ATTEMPTS - 1
    assert reasons[0].startswith("delivery_attempt=1:")
    assert reasons[-1].startswith("terminal:exhausted_retries:")
    assert "ratelimited" in reasons[-1]
    # Once quarantined the row is no longer re-claimed, so later drains find
    # no work rather than re-poisoning the outbox forever.
    assert statuses[-1] == "no_handoff"
    assert quay_client.completed == []


def test_drain_one_delivery_unexpected_error_is_bounded_and_never_crashes_drain():
    quay_client = ReclaimingOutboxQuayClient(delivery_raw())
    slack = FakeSlackClient(post_error=ValueError("deterministic render bug"))
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    statuses = []
    for _ in range(quay.MAX_DELIVERY_ATTEMPTS + 1):
        # A deterministic non-Slack exception must not propagate out of the
        # drain (previously it re-raised and crashed the tick every time).
        statuses.append(drainer.drain_one().status)

    reasons = [reason for _, reason in quay_client.failed]
    assert all(status in {"runner_error", "no_handoff"} for status in statuses)
    assert reasons[0].startswith("delivery_attempt=1:runner_error:ValueError:")
    assert reasons[-1].startswith("terminal:exhausted_retries:")
    assert "ValueError" in reasons[-1]
    assert statuses[-1] == "no_handoff"
    assert quay_client.completed == []


def test_drain_one_delivery_not_in_channel_alerts_and_retries_not_silently_dropped():
    logger, handler = _capturing_logger("test.operator-alert")
    quay_client = ReclaimingOutboxQuayClient(delivery_raw())
    slack = FakeSlackClient(
        post_error=quay.SlackApiError("chat.postMessage", "not_in_channel")
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
        logger=logger,
    )

    result = drainer.drain_one()

    # not_in_channel is human-recoverable: NOT silently quarantined on the
    # first failure (retried within the budget so a quick channel fix
    # auto-delivers) and surfaced as a loud ERROR-level operator alert.
    assert result.status == "slack_api_error"
    first_reason = quay_client.failed[0][1]
    assert first_reason.startswith("delivery_attempt=1:")
    assert "not_in_channel" in first_reason
    alerts = [
        rec
        for rec in handler.records
        if "outbox_item_operator_action_required" in rec.getMessage()
    ]
    assert alerts, "expected a loud operator-action alert"
    assert alerts[0].levelno == logging.ERROR


def test_cli_quay_adapter_claims_delivery_outbox_and_leaves_workflow_for_handoff():
    runner = RecordingDeliveryOutboxRunner()
    quay_client = quay.QuayCliClient(command="/bin/quay", runner=runner)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.questions[0][0] == "CDELIVERY"
    assert "Ticket: BRIX-1447" in slack.questions[0][1]
    assert "Repo: repo-1" in slack.questions[0][1]
    assert "Task: task-delivery-cli" in slack.questions[0][1]
    assert "Note: PR #44 is approved and ready." in slack.questions[0][1]
    assert slack.questions[0][2] is None
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert calls == [
        "outbox list --status pending --handler-class delivery",
        "outbox claim 21",
        "task get task-delivery-cli",
        "outbox complete 21 --claim-id delivery-claim-1",
    ]


def test_cli_quay_adapter_fails_claimed_outbox_item_with_wrong_handler_class():
    runner = RecordingWrongClassOutboxRunner()
    quay_client = quay.QuayCliClient(command="/bin/quay", runner=runner)

    assert quay_client.claim_work("worker-1") is None

    calls = [" ".join(call[1:]) for call in runner.calls]
    assert calls == [
        "outbox list --status pending --handler-class delivery",
        "outbox claim 22",
        "outbox fail 22 --claim-id wrong-class-claim "
        "--error unsupported_handler_class:workflow_intervention",
        "handoff list --status pending",
    ]


def test_cli_quay_adapter_skips_terminal_outbox_row_and_claims_later_ready_item():
    runner = RecordingTerminalThenReadyOutboxRunner()
    quay_client = quay.QuayCliClient(command="/bin/quay", runner=runner)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "delivery_delivered"
    assert slack.questions[0][0] == "CDELIVERY"
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert calls == [
        "outbox list --status pending --handler-class delivery",
        "outbox claim 23",
        "task get task-ready",
        "outbox complete 23 --claim-id delivery-claim-23",
    ]


def test_parse_slack_thread_ref_accepts_both_shapes_and_rejects_truncated_prefix():
    assert quay._parse_slack_thread_ref("C123:1000.0") == ("C123", "1000.0")
    assert quay._parse_slack_thread_ref("slack:C123:1000.0") == ("C123", "1000.0")
    assert quay._parse_slack_thread_ref("SLACK:C123:1000.0") == ("C123", "1000.0")
    # Truncated legacy prefix must be rejected, not read as channel="slack".
    assert quay._parse_slack_thread_ref("slack:C123") is None
    assert quay._parse_slack_thread_ref("slack:") is None
    assert quay._parse_slack_thread_ref("slack") is None
    assert quay._parse_slack_thread_ref("slack:slack:C123:1000.0") is None
    assert quay._parse_slack_thread_ref("C123") is None
    assert quay._parse_slack_thread_ref("C123:") is None
    assert quay._parse_slack_thread_ref("") is None


def test_delivery_slack_error_disposition_splits_transient_operator_terminal():
    def disp(error: str) -> str:
        return quay._delivery_slack_error_disposition(
            quay.SlackApiError("chat.postMessage", error)
        )

    # Transient -> retryable within the attempt budget.
    assert disp("ratelimited") == "retryable"
    assert disp("service_unavailable") == "retryable"
    assert disp("internal_error") == "retryable"
    assert disp("transport_error:<urlopen error timed out>") == "retryable"
    assert disp("transport_error:HTTP Error 503: Service Unavailable") == "retryable"
    # Human-recoverable channel state -> operator alert (still retried).
    assert disp("not_in_channel") == "operator"
    assert disp("is_archived") == "operator"
    # Deterministic bad payload/route -> quarantine immediately.
    assert disp("msg_too_long") == "terminal"
    assert disp("invalid_blocks") == "terminal"
    assert disp("channel_not_found") == "terminal"
    assert disp("thread_not_found") == "terminal"


def test_terminal_outbox_row_reason_tolerates_field_and_wrapper_variance():
    # Marker under each accepted error-field name.
    assert quay._terminal_outbox_row_reason(
        {"last_error": "terminal:missing_default_slack_channel"}
    )
    assert quay._terminal_outbox_row_reason({"error": "quarantine:bad"})
    assert quay._terminal_outbox_row_reason(
        {"last_error_message": "terminal:slack_api_error"}
    )
    # Terminal status name variants.
    assert quay._terminal_outbox_row_reason({"status": "dead_letter"})
    assert quay._terminal_outbox_row_reason({"state": "quarantined"})
    # A Quay wrapper prefix before our marker is still detected.
    assert quay._terminal_outbox_row_reason(
        {"last_error": "delivery handler failed: terminal:msg_too_long"}
    )
    # Retryable rows (attempt marker / plain transient) are NOT skipped.
    assert (
        quay._terminal_outbox_row_reason(
            {"last_error": "delivery_attempt=2:slack_api_error:chat.postMessage:ratelimited"}
        )
        is None
    )
    assert quay._terminal_outbox_row_reason({"last_error": None, "status": "pending"}) is None
    assert quay._terminal_outbox_row_reason({}) is None


def test_terminal_outbox_row_reason_rejects_free_text_terminal_false_positive():
    # A RETRYABLE-in-flight row whose embedded exception text merely CONTAINS
    # " terminal:" as free text must NOT be misdetected as terminal (would be a
    # silent early skip / drop).
    retryable_free_text = {
        "last_error": (
            "delivery_attempt=1:runner_error:RuntimeError: "
            "connection terminal: reset by peer"
        )
    }
    assert quay._terminal_outbox_row_reason(retryable_free_text) is None

    # Even without a delivery_attempt= marker, free text after the marker that
    # isn't a recognized terminal reason token stays non-terminal.
    assert (
        quay._terminal_outbox_row_reason(
            {"last_error": "socket read: terminal: reset by peer"}
        )
        is None
    )

    # Genuine terminal writes are still detected: the orchestrator's own
    # canonical prefix, and a Quay wrapper in front of a recognized token.
    assert quay._terminal_outbox_row_reason(
        {"last_error": "terminal:channel_not_found"}
    )
    assert quay._terminal_outbox_row_reason(
        {"last_error": "quay-wrapper: terminal:exhausted_retries:attempt=5:runner_error"}
    )


def test_outbox_unsupported_only_masks_missing_outbox_list_contract():
    assert quay._outbox_unsupported(
        quay.QuayCommandError(
            ["/bin/quay", "outbox", "list"],
            returncode=1,
            stdout="",
            stderr="unknown command: outbox\n",
            code="usage_error",
        )
    )
    assert not quay._outbox_unsupported(
        quay.QuayCommandError(
            ["/bin/quay", "outbox", "list"],
            returncode=1,
            stdout="",
            stderr='{"error":"usage_error"}\nusage: quay outbox list ...\n',
            code="usage_error",
        )
    )
    assert not quay._outbox_unsupported(
        quay.QuayCommandError(
            ["/bin/quay", "outbox", "claim", "22"],
            returncode=1,
            stdout="",
            stderr="unknown command: outbox\n",
            code="unknown_command",
        )
    )


def test_drain_one_posts_guidance_prompt_when_reattaching_to_original_thread():
    runner = RecordingQuayRunner()
    quay_client = quay.QuayCliClient(command="/bin/quay", runner=runner)
    reply = quay.SlackReply(
        text="Use the existing importer and keep the old endpoint as a fallback.",
        user_id="U123",
        ts="1003.000000",
        permalink="https://example.slack/thread",
    )
    slack = FakeSlackClient(reply)
    decider = FakeDecider([
        ready("Use the existing importer and keep the old endpoint as a fallback.")
    ])
    config = quay.OrchestratorConfig(
        enabled=True,
        reply_timeout_seconds=60,
        poll_interval_seconds=2,
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=config,
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.validations == [("GPRIVATE123", "999.000000")]
    assert slack.questions == []
    assert slack.acks
    assert "*Quay worker blocked* — BRIX-1405 · repo-1" in slack.acks[0][1]
    assert "Reason: blocked on product input" in slack.acks[0][1]
    assert "Reply in thread and I'll resume the worker once it's clear." in slack.acks[0][1]
    assert slack.waits[0][0] == quay.SlackPostRef(
        channel_id="GPRIVATE123",
        ts="1002.000000",
        thread_ts="999.000000",
    )
    assert slack.waits[0][1] <= 60
    assert slack.waits[0][1] > 59
    assert slack.waits[0][2] == 2
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert calls[0] == "outbox list --status pending --handler-class delivery"
    assert "handoff list --status pending" in calls[1]
    assert "task claim task-cli" in calls[2]
    assert "escalate-human task-cli --claim-id claim-1" in calls[5]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[5]
    assert "record-human-reply task-cli --claim-id claim-1" in calls[6]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[6]
    assert "submit-brief task-cli --claim-id claim-1" in calls[7]
    assert "--reason advice_answered" in calls[7]
    assert "Quay handoff reason" not in runner.file_payloads[0]
    assert "Reason: blocked on product input" in runner.file_payloads[0]
    assert "resume the worker once it's clear" in runner.file_payloads[0]
    assert "resume:" not in runner.file_payloads[0]
    assert "Use the following human guidance" in runner.file_payloads[2]
    assert "U123" in runner.file_payloads[2]
    assert "Use the existing importer" in runner.file_payloads[2]
    assert "Quay resumed for BRIX-1405" in slack.acks[1][1]
    assert result.metrics["slack_questions_posted"] == 1
    assert result.metrics["human_briefs_submitted"] == 1


def test_drain_one_falls_back_to_default_channel_and_persists_post_thread():
    handoff = quay.Handoff(
        handoff_id="handoff-fallback",
        task_id="task-fallback",
        artifact_id="artifact-fallback",
        summary="Worker needs a product decision.",
    )
    reply = quay.SlackReply(
        text="Use the default route.",
        user_id="U123",
        ts="1001.000000",
    )
    task = quay.TaskContext(
        task_id=handoff.task_id,
        title="Fix stuck worker",
        issue="BRIX-1405",
        repo_id="hermes-agent",
        metadata={"authors_json": '[{"slack_id":"U06TDC56VJB"}]'},
    )
    quay_client = FakeQuayClient(handoff, task=task)
    slack = FakeSlackClient(reply)
    decider = FakeDecider([ready("Use the default route.")])
    config = quay.OrchestratorConfig(
        enabled=True,
        default_slack_channel="C1234567890",
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=config,
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.questions[0][0] == "C1234567890"
    assert "Contributor: <@U06TDC56VJB>" in slack.questions[0][1]
    assert slack.questions[0][2] is None
    assert quay_client.escalated[2] == "C1234567890:1000.000000"
    assert quay_client.recorded_reply[2] == "C1234567890:1000.000000"
    assert quay_client.submitted[0][2] == "advice_answered"
    assert slack.acks


def test_missing_default_slack_channel_releases_claim():
    handoff = quay.Handoff(handoff_id="handoff-3", task_id="task-3")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient()
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "missing_default_slack_channel"
    assert quay_client.submitted == []
    assert quay_client.completed == []
    assert quay_client.released == [(handoff, "missing_default_slack_channel")]
    assert slack.questions == []


def test_human_reply_timeout_releases_claim():
    handoff = quay.Handoff(handoff_id="handoff-4", task_id="task-4")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(reply=None)
    decider = FakeDecider([ready("Use the existing thread on retry.")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
            reply_timeout_seconds=1,
            poll_interval_seconds=1,
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert quay_client.submitted == []
    assert quay_client.completed == []
    assert quay_client.released == [(handoff, "human_reply_timeout")]
    assert slack.questions
    slack.wait_results.append(
        quay.SlackReply(
            text="Use the existing thread on retry.",
            user_id="U123",
            ts="1003.000000",
        )
    )

    retry = drainer.drain_one()

    assert retry.status == "submitted_from_human_reply"
    assert len(slack.questions) == 1
    assert slack.validations == [("C1234567890", "1000.000000")]
    assert quay_client.recorded_reply[2] == "C1234567890:1000.000000"
    assert quay_client.submitted[0][2] == "advice_answered"


def test_plain_thread_chatter_does_not_resume_quay():
    handoff = quay.Handoff(handoff_id="handoff-chatter", task_id="task-chatter")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        quay.SlackReply(
            text="Could I fix CI and ping you here?",
            user_id="U123",
            ts="1001.000000",
        )
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
            reply_timeout_seconds=1,
            poll_interval_seconds=1,
        ),
        worker_id="test-worker",
        decider=FakeDecider([quay.ConversationDecision(action="wait")]),
    )

    result = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert not hasattr(quay_client, "recorded_reply")
    assert quay_client.submitted == []
    assert slack.acks == []
    assert quay_client.released == [(handoff, "human_reply_timeout")]


def test_orchestrator_ready_decision_records_submits_and_acks():
    handoff = quay.Handoff(handoff_id="handoff-resume", task_id="task-resume")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        quay.SlackReply(
            text="CI is blocked because the auth fixture is only available in CI.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Use the CI-only auth fixture.")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert quay_client.recorded_reply[1].text == "CI is blocked because the auth fixture is only available in CI."
    assert quay_client.submitted[0][1].endswith("Use the CI-only auth fixture.")
    assert quay_client.submitted[0][2] == "advice_answered"
    assert quay_client.completed == [handoff]
    assert slack.acks
    assert "Quay resumed for BRIX-1405" in slack.acks[0][1]
    assert "Use the CI-only auth fixture" in slack.acks[0][1]


def test_orchestrator_can_ask_for_confirmation_before_resuming():
    handoff = quay.Handoff(handoff_id="handoff-confirm", task_id="task-confirm")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            quay.SlackReply(
                text="I think we should use the CI-only auth fixture.",
                user_id="U123",
                ts="1001.000000",
            ),
            quay.SlackReply(
                text="Yes, that's the right worker instruction.",
                user_id="U123",
                ts="1003.000000",
            ),
        ]
    )
    decider = FakeDecider(
        [
            ask("I would tell the worker to use the CI-only auth fixture. Is that right?"),
            ready("Use the CI-only auth fixture."),
        ]
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert len(decider.calls) == 2
    assert "I would tell the worker" in slack.acks[0][1]
    assert quay_client.recorded_reply[1].text == "Yes, that's the right worker instruction."
    assert quay_client.submitted[0][1].endswith("Use the CI-only auth fixture.")


def test_orchestrator_decider_error_prompts_once_and_keeps_waiting():
    handoff = quay.Handoff(handoff_id="handoff-decider", task_id="task-decider")
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            quay.SlackReply(
                text="The fixture is only available in CI.",
                user_id="U123",
                ts="1001.000000",
            ),
            quay.SlackReply(
                text="Tell the worker to use the CI-only auth fixture.",
                user_id="U123",
                ts="1003.000000",
            ),
        ]
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=FakeDecider(
            [
                RuntimeError("model unavailable"),
                ready("Use the CI-only auth fixture."),
            ]
        ),
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert len(slack.questions) == 1
    assert len(slack.acks) == 2
    assert "decision engine hit an internal orchestrator error" in slack.acks[0][1]
    assert "No need to rephrase" in slack.acks[0][1]
    assert "Quay resumed" in slack.acks[1][1]
    assert quay_client.escalations[0][2] == "C1234567890:1000.000000"
    assert quay_client.released == []
    assert quay_client.submitted[0][1].endswith("Use the CI-only auth fixture.")
    assert quay_client.completed == [handoff]
    assert result.metrics["errors"] == 1
    assert result.metrics["claims_released"] == 0


def test_orchestrator_decider_error_retry_does_not_reprocess_old_reply():
    handoff = quay.Handoff(
        handoff_id="handoff-decider-timeout",
        task_id="task-decider-timeout",
    )
    quay_client = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            quay.SlackReply(
                text="The fixture is only available in CI.",
                user_id="U123",
                ts="1001.000000",
            ),
        ]
    )
    decider = FakeDecider([RuntimeError("model unavailable")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
            reply_timeout_seconds=1,
            poll_interval_seconds=1,
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()
    slack.wait_results.append(
        quay.SlackReply(
            text="The fixture is only available in CI.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    retry = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert retry.status == "human_reply_timeout"
    assert len(slack.questions) == 1
    assert len(slack.acks) == 2
    assert "decision engine hit an internal orchestrator error" in slack.acks[0][1]
    assert "No need to rephrase" in slack.acks[0][1]
    assert "*Quay worker blocked*" in slack.acks[1][1]
    assert slack.validations == [("C1234567890", "1000.000000")]
    assert len(decider.calls) == 1


def test_orchestrator_decider_resolves_runtime_before_building_agent(monkeypatch):
    FakeAgent.instances = []
    calls = []

    def runtime_resolver(*, requested=None):
        calls.append(requested)
        return {
            "api_key": "token",
            "base_url": "https://chatgpt.com/backend-api/codex",
            "provider": "openai-codex",
            "api_mode": "codex_responses",
            "command": "codex-acp",
            "args": ["--json"],
            "credential_pool": "pool",
        }

    monkeypatch.setenv("QUAY_ORCHESTRATOR_PROVIDER", "openai-codex")
    monkeypatch.setenv("HERMES_INFERENCE_PROVIDER", "anthropic")
    decider = quay.HermesConversationDecider(
        runtime_resolver=runtime_resolver,
        default_model_resolver=lambda provider: f"default-for-{provider}",
        agent_class=FakeAgent,
    )
    decision = decider.decide(
        handoff=quay.Handoff(handoff_id="handoff-runtime", task_id="task-runtime"),
        task=quay.TaskContext(task_id="task-runtime"),
        artifact=quay.Artifact(artifact_id="artifact-runtime", text="blocked"),
        question="What should happen?",
        replies=[
            quay.SlackReply(
                text="Resume with the answer.",
                user_id="U123",
                ts="1001.000000",
            )
        ],
        posted_messages=[],
    )

    assert decision == quay.ConversationDecision(
        action="ready",
        brief="Resume with the answer.",
    )
    assert calls == ["openai-codex"]
    assert len(FakeAgent.instances) == 1
    kwargs = FakeAgent.instances[0].kwargs
    assert kwargs["api_key"] == "token"
    assert kwargs["base_url"] == "https://chatgpt.com/backend-api/codex"
    assert kwargs["provider"] == "openai-codex"
    assert kwargs["api_mode"] == "codex_responses"
    assert kwargs["acp_command"] == "codex-acp"
    assert kwargs["acp_args"] == ["--json"]
    assert kwargs["credential_pool"] == "pool"
    assert kwargs["model"] == "default-for-openai-codex"
    assert kwargs["platform"] == "quay-orchestrator"
    assert kwargs["session_id"].startswith("quay-orch-")
    assert len(kwargs["session_id"]) <= 64


def test_orchestrator_decider_uses_short_session_id_for_long_quay_ids():
    FakeAgent.instances = []
    long_handoff = quay.Handoff(
        handoff_id="handoff-with-a-long-human-readable-id",
        task_id="5bb225ff-fde7-40d9-acac-1a9f8e88e5d9",
        claim_id="1f6f9630-291f-4a9b-a169-eb2d7690da14",
    )
    decider = quay.HermesConversationDecider(
        runtime_resolver=lambda *, requested=None: {"provider": "openai-codex"},
        default_model_resolver=lambda provider: "gpt-5.5",
        agent_class=FakeAgent,
    )

    decider.decide(
        handoff=long_handoff,
        task=quay.TaskContext(task_id=long_handoff.task_id),
        artifact=quay.Artifact(artifact_id="artifact-runtime", text="blocked"),
        question="What should happen?",
        replies=[
            quay.SlackReply(
                text="I will fix CI manually and report back.",
                user_id="U123",
                ts="1001.000000",
            )
        ],
        posted_messages=[],
    )

    session_id = FakeAgent.instances[0].kwargs["session_id"]
    assert session_id.startswith("quay-orch-")
    assert len(session_id) <= 64
    assert long_handoff.task_id not in session_id
    assert long_handoff.claim_id not in session_id


def test_orchestrator_decider_lets_shared_resolver_handle_unset_provider(monkeypatch):
    calls = []

    def runtime_resolver(*, requested=None):
        calls.append(requested)
        return {"provider": "openai-codex"}

    monkeypatch.delenv("QUAY_ORCHESTRATOR_PROVIDER", raising=False)
    monkeypatch.setenv("HERMES_INFERENCE_PROVIDER", "anthropic")

    decider = quay.HermesConversationDecider(runtime_resolver=runtime_resolver)

    assert decider._resolve_runtime() == {"provider": "openai-codex"}
    assert calls == [None]


def test_orchestrator_decider_falls_back_from_stale_config_model(monkeypatch):
    from hermes_cli import config as config_mod

    monkeypatch.setattr(
        config_mod,
        "load_config",
        lambda: {
            "model": {
                "provider": "openai-codex",
                "default": "anthropic/claude-sonnet-4.6",
            }
        },
    )
    decider = quay.HermesConversationDecider(
        default_model_resolver=lambda provider: f"default-for-{provider}",
        model_catalog_resolver=lambda provider: ["gpt-5.3-codex", "gpt-5.4"],
    )

    assert (
        decider._resolve_model({"provider": "openai-codex"})
        == "default-for-openai-codex"
    )


def test_orchestrator_decider_uses_normalized_compatible_config_model(monkeypatch):
    from hermes_cli import config as config_mod

    monkeypatch.setattr(
        config_mod,
        "load_config",
        lambda: {
            "model": {
                "provider": "openai-codex",
                "default": "openai/gpt-5.4",
            }
        },
    )
    decider = quay.HermesConversationDecider(
        default_model_resolver=lambda provider: f"default-for-{provider}",
        model_catalog_resolver=lambda provider: ["gpt-5.3-codex", "gpt-5.4"],
    )

    assert decider._resolve_model({"provider": "openai-codex"}) == "gpt-5.4"


def test_fake_quay_rejects_double_human_escalation_without_reply():
    handoff = quay.Handoff(handoff_id="handoff-stateful", task_id="task-stateful")
    quay_client = FakeQuayClient(handoff)

    assert quay_client.claim_handoff("test-worker") == handoff
    quay_client.escalate_human(handoff, "Question", "C1234567890:1000.000000")

    with pytest.raises(RuntimeError, match="wrong_state"):
        quay_client.escalate_human(handoff, "Fallback question", "CFALLBACK1:1000.000000")


def test_orchestrator_decision_parser_accepts_ready_ask_and_invalid_json():
    assert quay.parse_decision_response('{"action":"ready","brief":"do the thing"}') == quay.ConversationDecision(
        action="ready",
        brief="do the thing",
    )
    assert quay.parse_decision_response('```json\n{"action":"ask","message":"Can I tell the worker X?"}\n```') == quay.ConversationDecision(
        action="ask",
        message="Can I tell the worker X?",
    )
    assert quay.parse_decision_response("not json") == quay.ConversationDecision(action="wait")


def test_human_question_mentions_author_from_task_or_handoff_metadata():
    task = quay.TaskContext(
        task_id="task-author",
        title="Needs author",
        metadata={"author": {"slack_id": "U1234567"}},
    )
    handoff = quay.Handoff(
        handoff_id="handoff-author",
        task_id="task-author",
        metadata={"contributor": {"slack_id": "U7654321"}},
    )

    question = quay.build_human_question(handoff, task, None)

    assert "Contributor: <@U1234567>" in question


def test_human_question_mentions_legacy_flash_tag_author_on_fallback_route():
    task = quay.TaskContext(task_id="task-legacy", title="Legacy flash")
    handoff = quay.Handoff(
        handoff_id="handoff-legacy",
        task_id="task-legacy",
        metadata={"source": {"flash": {"author_slack_id": "<@UFLASH1>"}}},
    )

    question = quay.build_human_question(handoff, task, None)
    route = quay.resolve_slack_route(
        handoff,
        task,
        quay.OrchestratorConfig(enabled=True, default_slack_channel="C1234567890"),
    )

    assert "Contributor: <@UFLASH1>" in question
    assert route == quay.SlackRoute(channel_id="C1234567890", source="fallback_channel")


def test_human_question_omits_contributor_without_author_metadata():
    task = quay.TaskContext(
        task_id="task-no-author",
        title="No author",
        metadata={"authors_json": "not-json"},
    )
    handoff = quay.Handoff(handoff_id="handoff-no-author", task_id=task.task_id)

    question = quay.build_human_question(handoff, task, None)

    assert "Contributor:" not in question
    assert "<@" not in question


def test_human_question_promotes_question_and_strips_scaffolding():
    task = quay.TaskContext(
        task_id="task-q",
        title="Wire up the importer",
        issue="BRIX-1870",
        repo_id="hermes-agent",
    )
    handoff = quay.Handoff(
        handoff_id="handoff-q",
        task_id=task.task_id,
        reason="worker_blocker",
        summary="Quay handoff reason: worker_blocker",
    )
    artifact = quay.Artifact(
        artifact_id="a1",
        text=(
            "Blocked: the importer schema changed and two endpoints now exist.\n"
            "Should we keep the old endpoint or migrate to the new one?"
        ),
        kind="blocker",
    )

    question = quay.build_human_question(handoff, task, artifact)
    lines = question.splitlines()

    assert lines[0] == "*Quay worker blocked* — BRIX-1870 · hermes-agent"
    assert "Task: Wire up the importer" in lines
    assert "Reason: Blocked: the importer schema changed and two endpoints now exist." in lines
    assert "Need: Should we keep the old endpoint or migrate to the new one?" in lines
    assert lines[-1] == "Reply in thread and I'll resume the worker once it's clear."
    # Generic scaffolding + handoff-reason filler are gone.
    assert "Quay handoff reason" not in question
    assert "needs human guidance" not in question
    assert "Handoff summary" not in question
    assert "Blocker/context" not in question


def test_human_question_budget_exhaustion_uses_reason_specific_defaults():
    task = quay.TaskContext(
        task_id="task-budget",
        title="Refactor the client",
        issue="BRIX-2001",
        repo_id="hermes-agent",
    )
    handoff = quay.Handoff(
        handoff_id="handoff-budget",
        task_id=task.task_id,
        reason="worker_blocker",
        metadata={"budget_exhausted_artifact_id": 42},
    )
    artifact = quay.Artifact(
        artifact_id="a2",
        text="Attempt budget exhausted after 5 attempts without a passing build.",
        kind="blocker",
    )

    question = quay.build_human_question(handoff, task, artifact)
    lines = question.splitlines()

    assert lines[0] == "*Quay worker blocked — budget exhausted* — BRIX-2001 · hermes-agent"
    assert "Reason: Attempt budget exhausted after 5 attempts without a passing build." in lines
    assert "Need: approve more budget or stop the task." in lines


def test_human_question_vague_blocker_falls_back_sanely():
    task = quay.TaskContext(
        task_id="task-vague",
        title="Fix the flow",
        issue="BRIX-3003",
        repo_id="hermes-agent",
    )
    handoff = quay.Handoff(
        handoff_id="handoff-vague",
        task_id=task.task_id,
        reason="worker_blocker",
        summary="Quay handoff reason: worker_blocker",
    )
    artifact = quay.Artifact(
        artifact_id="a3",
        text="blocked on product input",
        kind="blocker",
    )

    question = quay.build_human_question(handoff, task, artifact)
    lines = question.splitlines()

    assert lines[0] == "*Quay worker blocked* — BRIX-3003 · hermes-agent"
    assert "Reason: blocked on product input" in lines
    assert "Need: advise how the worker should proceed." in lines
    # No filler leaked in from the summary.
    assert "Quay handoff reason" not in question
    assert "Context:" not in question


def test_human_question_promotes_question_from_artifact_over_summary_statement():
    # Artifact carries only the question; the summary carries the statement.
    # The statement becomes the Reason and the question is promoted to Need.
    task = quay.TaskContext(
        task_id="task-ctx",
        title="Ship the feature",
        issue="BRIX-4004",
        repo_id="hermes-agent",
    )
    handoff = quay.Handoff(
        handoff_id="handoff-ctx",
        task_id=task.task_id,
        reason="worker_blocker",
        summary="Worker paused after the third failed migration.",
    )
    artifact = quay.Artifact(
        artifact_id="a4",
        text="Should we roll back the migration or push forward?",
        kind="blocker",
    )

    question = quay.build_human_question(handoff, task, artifact)
    lines = question.splitlines()

    assert "Reason: Worker paused after the third failed migration." in lines
    assert "Need: Should we roll back the migration or push forward?" in lines
    # The summary statement is already the Reason, so it is not repeated.
    assert "Context:" not in question


def test_human_question_surfaces_summary_context_distinct_from_blocker():
    task = quay.TaskContext(
        task_id="task-ctx2",
        title="Ship the feature",
        issue="BRIX-4040",
        repo_id="hermes-agent",
    )
    handoff = quay.Handoff(
        handoff_id="handoff-ctx2",
        task_id=task.task_id,
        reason="worker_blocker",
        summary="Third migration attempt; the prior two were rolled back.",
    )
    artifact = quay.Artifact(
        artifact_id="a4b",
        text=(
            "Blocked: the migration keeps failing on the users table.\n"
            "Should we roll back or push forward?"
        ),
        kind="blocker",
    )

    question = quay.build_human_question(handoff, task, artifact)
    lines = question.splitlines()

    assert "Reason: Blocked: the migration keeps failing on the users table." in lines
    assert "Need: Should we roll back or push forward?" in lines
    assert "Context: Third migration attempt; the prior two were rolled back." in lines


def test_human_question_returns_explicit_question_verbatim():
    task = quay.TaskContext(task_id="task-explicit", title="Explicit", issue="BRIX-5005")
    handoff = quay.Handoff(
        handoff_id="handoff-explicit",
        task_id=task.task_id,
        human_question="Can you confirm the staging DB URL before I continue?",
        summary="Quay handoff reason: worker_blocker",
    )
    artifact = quay.Artifact(artifact_id="a5", text="unused", kind="blocker")

    question = quay.build_human_question(handoff, task, artifact)

    assert question == "Can you confirm the staging DB URL before I continue?"


def test_human_question_returns_metadata_question_verbatim():
    task = quay.TaskContext(task_id="task-meta-q", title="Meta", issue="BRIX-6006")
    handoff = quay.Handoff(
        handoff_id="handoff-meta-q",
        task_id=task.task_id,
        metadata={"human_question": "Which region should the worker target?"},
    )

    question = quay.build_human_question(handoff, task, None)

    assert question == "Which region should the worker target?"


def test_stale_metadata_route_falls_back_and_records_fallback_thread():
    handoff = quay.Handoff(handoff_id="handoff-stale", task_id="task-stale")
    task = quay.TaskContext(
        task_id=handoff.task_id,
        title="Fix stale route",
        issue="BRIX-1396",
        metadata={"slack_thread_ref": "CSTALE1234:1778757925.556349"},
    )
    quay_client = FakeQuayClient(handoff, task=task)
    slack = FakeSlackClient(
        validate_results=[
            quay.SlackApiError("conversations.replies", "thread_not_found"),
        ],
        wait_results=[
            quay.SlackReply(
                text="The fallback thread has the context we need.",
                user_id="U123",
                ts="1001.000000",
            ),
        ]
    )
    decider = FakeDecider([ready("Use the fallback thread.")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="CFALLBACK1",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.validations == [("CSTALE1234", "1778757925.556349")]
    assert len(slack.questions) == 1
    assert slack.questions[0][0] == "CFALLBACK1"
    assert slack.questions[0][2] is None
    assert quay_client.escalations == [
        (handoff, slack.questions[0][1], "CFALLBACK1:1000.000000")
    ]
    assert quay_client.recorded_reply[2] == "CFALLBACK1:1000.000000"


def test_stale_metadata_route_without_fallback_releases_and_does_not_bubble():
    handoff = quay.Handoff(handoff_id="handoff-stale", task_id="task-stale")
    task = quay.TaskContext(
        task_id=handoff.task_id,
        title="Fix stale route",
        issue="BRIX-1396",
        metadata={"slack_thread_ref": "CSTALE1234:1778757925.556349"},
    )
    quay_client = FakeQuayClient(handoff, task=task)
    slack = FakeSlackClient(
        validate_results=[
            quay.SlackApiError("conversations.replies", "thread_not_found"),
        ]
    )
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "stale_slack_thread_ref"
    assert slack.validations == [("CSTALE1234", "1778757925.556349")]
    assert slack.questions == []
    assert quay_client.escalations == []
    assert quay_client.submitted == []
    assert quay_client.released == [(handoff, "stale_slack_thread_ref")]


def test_failed_record_human_reply_does_not_post_success_ack():
    class FailingRecordQuay(FakeQuayClient):
        def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
            raise RuntimeError("record failed")

    handoff = quay.Handoff(handoff_id="handoff-record-fails", task_id="task-record-fails")
    quay_client = FailingRecordQuay(handoff)
    slack = FakeSlackClient(
        quay.SlackReply(
            text="Try the fixture.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Try the fixture.")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    with pytest.raises(RuntimeError, match="record failed"):
        drainer.drain_one()

    assert slack.acks == []


def test_failed_submit_brief_does_not_post_success_ack():
    class FailingSubmitQuay(FakeQuayClient):
        def submit_brief(self, handoff, brief: str, *, reason: str) -> None:
            raise RuntimeError("submit failed")

    handoff = quay.Handoff(handoff_id="handoff-submit-fails", task_id="task-submit-fails")
    quay_client = FailingSubmitQuay(handoff)
    slack = FakeSlackClient(
        quay.SlackReply(
            text="Try the fixture.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Try the fixture.")])
    drainer = quay.HandoffDrainer(
        quay=quay_client,
        slack=slack,
        config=quay.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    with pytest.raises(RuntimeError, match="submit failed"):
        drainer.drain_one()

    assert slack.acks == []


def test_file_lock_is_nonblocking(tmp_path: Path):
    lock_path = tmp_path / "orchestrator.lock"
    with quay.FileLock(lock_path):
        with pytest.raises(quay.LockBusy):
            with quay.FileLock(lock_path):
                pass


def test_delivery_outbox_drains_while_unrelated_handoff_waits_for_human(tmp_path: Path):
    lock_path = tmp_path / "orchestrator.lock"
    wait_started = threading.Event()
    reply_allowed = threading.Event()

    class BlockingSlackClient(FakeSlackClient):
        def wait_for_reply(
            self,
            ref,
            *,
            after_ts: str,
            timeout_seconds: float,
            poll_interval_seconds: float,
        ):
            self.waits.append((ref, timeout_seconds, poll_interval_seconds))
            wait_started.set()
            if not reply_allowed.wait(timeout=5):
                return None
            return quay.SlackReply(
                text="Resume with the approved importer fallback.",
                user_id="U123",
                ts="1001.000000",
            )

    handoff = quay.Handoff(handoff_id="handoff-waiting", task_id="task-waiting")
    handoff_quay = FakeQuayClient(handoff)
    human_slack = BlockingSlackClient()
    human_result: list[quay.DrainResult] = []
    human_errors: list[BaseException] = []

    def run_handoff_wait() -> None:
        try:
            with quay.FileLock(lock_path) as lock:
                drainer = quay.HandoffDrainer(
                    quay=handoff_quay,
                    slack=human_slack,
                    config=quay.OrchestratorConfig(
                        enabled=True,
                        default_slack_channel="C1234567890",
                        reply_timeout_seconds=60,
                        poll_interval_seconds=1,
                    ),
                    worker_id="human-worker",
                    decider=FakeDecider([ready("Use the approved importer fallback.")]),
                    coordination_lock=lock,
                )
                human_result.append(drainer.drain_one())
        except BaseException as exc:
            human_errors.append(exc)

    human_thread = threading.Thread(target=run_handoff_wait)
    human_thread.start()
    assert wait_started.wait(timeout=2)
    assert handoff_quay.state == "waiting_human"

    item = delivery_item(route_hint={})
    delivery_quay = FakeOutboxQuayClient(item)
    delivery_slack = FakeSlackClient()
    with quay.FileLock(lock_path):
        delivery_result = quay.HandoffDrainer(
            quay=delivery_quay,
            slack=delivery_slack,
            config=quay.OrchestratorConfig(
                enabled=True,
                default_slack_channel="CDELIVERY",
            ),
            worker_id="delivery-worker",
        ).drain_one()

    assert delivery_result.status == "delivery_delivered"
    assert delivery_quay.completed == [item]
    assert human_thread.is_alive()
    assert handoff_quay.state == "waiting_human"

    reply_allowed.set()
    human_thread.join(timeout=5)

    assert not human_thread.is_alive()
    assert human_errors == []
    assert human_result[0].status == "submitted_from_human_reply"
    assert handoff_quay.submitted[0][2] == "advice_answered"
    assert handoff_quay.completed == [handoff]


def test_deployed_parked_handoff_exits_before_pending_handoff_drains_next_timer(
    tmp_path: Path,
):
    lock_path = tmp_path / "orchestrator.lock"

    class MultiHandoffQuayClient:
        def __init__(self, handoffs) -> None:
            self._pending = list(handoffs)
            self._waiting: list[object] = []
            self.states = {handoff.handoff_id: "pending" for handoff in handoffs}
            self.submitted: list[tuple[object, str, str]] = []
            self.completed: list[object] = []
            self.released: list[tuple[object, str]] = []
            self.escalations: list[tuple[object, str, str]] = []

        def claim_handoff(self, worker_id: str):
            if not self._pending:
                return None
            handoff = self._pending.pop(0)
            self.states[handoff.handoff_id] = "claimed"
            return handoff

        def claim_work(self, worker_id: str):
            return self.claim_handoff(worker_id)

        def claim_waiting_human(self, worker_id: str):
            if not self._waiting:
                return None
            return self._waiting[0]

        def get_task_context(self, task_id: str):
            return quay.TaskContext(
                task_id=task_id,
                title=f"Fix {task_id}",
                issue="BRIX-1405",
                repo_id="hermes-agent",
            )

        def get_artifact(self, handoff):
            return quay.Artifact(
                artifact_id=handoff.artifact_id or "artifact-1",
                text=f"{handoff.task_id} needs product guidance.",
                kind="blocker",
            )

        def escalate_human(self, handoff, question: str, thread_ref: str) -> None:
            metadata = dict(handoff.metadata)
            metadata["slack_thread_ref"] = thread_ref
            metadata["status"] = "waiting_human"
            waiting = dataclasses.replace(
                handoff,
                metadata=metadata,
                status="waiting_human",
            )
            self.states[handoff.handoff_id] = "waiting_human"
            self._waiting.append(waiting)
            self.escalations.append((handoff, question, thread_ref))

        def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
            assert self.states[handoff.handoff_id] == "waiting_human"
            self.states[handoff.handoff_id] = "claimed"

        def submit_brief(self, handoff, brief: str, *, reason: str) -> None:
            self.states[handoff.handoff_id] = "queued"
            self.submitted.append((handoff, brief, reason))

        def complete_claim(self, handoff) -> None:
            self.states[handoff.handoff_id] = "completed"
            self.completed.append(handoff)
            self._waiting = [
                waiting
                for waiting in self._waiting
                if waiting.handoff_id != handoff.handoff_id
            ]

        def release_claim(self, handoff, reason: str) -> None:
            self.states[handoff.handoff_id] = "pending"
            self.released.append((handoff, reason))

    waiting_handoff = quay.Handoff(
        handoff_id="handoff-waiting",
        task_id="task-waiting",
    )
    pending_handoff = quay.Handoff(
        handoff_id="handoff-pending",
        task_id="task-pending",
        next_brief="Apply the already-approved next brief.",
    )
    quay_client = MultiHandoffQuayClient([waiting_handoff, pending_handoff])
    human_slack = FakeSlackClient()

    with quay.FileLock(lock_path) as lock:
        parked_result = quay.HandoffDrainer(
            quay=quay_client,
            slack=human_slack,
            config=quay.OrchestratorConfig(
                enabled=True,
                default_slack_channel="C1234567890",
                reply_timeout_seconds=60,
                poll_interval_seconds=1,
            ),
            worker_id="systemd-worker",
            decider=FakeDecider([ready("Use the approved worker path.")]),
            coordination_lock=lock,
            park_human_waits=True,
        ).drain_one()

    assert parked_result.status == "waiting_for_human"
    assert quay_client.states[waiting_handoff.handoff_id] == "waiting_human"
    assert quay_client.states[pending_handoff.handoff_id] == "pending"
    assert len(quay_client.escalations) == 1
    assert human_slack.waits == []

    with quay.FileLock(lock_path):
        pending_result = quay.HandoffDrainer(
            quay=quay_client,
            slack=human_slack,
            config=quay.OrchestratorConfig(
                enabled=True,
                default_slack_channel="C1234567890",
            ),
            worker_id="second-worker",
            park_human_waits=True,
        ).drain_one()

    assert pending_result.status == "submitted_direct"
    assert [handoff.handoff_id for handoff in quay_client.completed] == [
        pending_handoff.handoff_id
    ]
    assert quay_client.states[waiting_handoff.handoff_id] == "waiting_human"
    assert human_slack.waits == []

    human_slack.wait_results.append(
        quay.SlackReply(
            text="Resume with the approved worker path.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    with quay.FileLock(lock_path):
        reply_result = quay.HandoffDrainer(
            quay=quay_client,
            slack=human_slack,
            config=quay.OrchestratorConfig(
                enabled=True,
                default_slack_channel="C1234567890",
            ),
            worker_id="reply-worker",
            decider=FakeDecider([ready("Use the approved worker path.")]),
            park_human_waits=True,
        ).drain_one()

    assert reply_result.status == "submitted_from_human_reply"
    assert human_slack.waits[0][1] == 0.0
    assert [handoff.handoff_id for handoff in quay_client.completed] == [
        pending_handoff.handoff_id,
        waiting_handoff.handoff_id,
    ]


def test_installed_quay_orchestrator_runner_uses_parked_human_waits():
    runner = (REPO_ROOT / "ops" / "quay-orchestrator-runner").read_text(
        encoding="utf-8"
    )
    service = (REPO_ROOT / "ops" / "quay-orchestrator.service").read_text(
        encoding="utf-8"
    )
    timer = (REPO_ROOT / "ops" / "quay-orchestrator.timer").read_text(
        encoding="utf-8"
    )

    assert "--park-human-waits" in runner
    assert "TimeoutStartSec=300" in service
    assert "Unit=quay-orchestrator.service" in timer


def test_cli_quay_adapter_noops_without_slack_token_when_no_handoffs(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    quay_bin = tmp_path / "quay"
    quay_bin.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1 $2 $3\" == \"outbox list --status\" ]]; then\n"
        "  printf '[]\\n'\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$1 $2 $3\" == \"handoff list --status\" ]]; then\n"
        "  printf '[]\\n'\n"
        "  exit 0\n"
        "fi\n"
        "exit 99\n",
        encoding="utf-8",
    )
    quay_bin.chmod(0o755)
    config = tmp_path / "orchestrator.json"
    config.write_text(
        '{"enabled": true, "quay_command": "' + str(quay_bin) + '"}\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)

    rc = quay.main(
        [
            "drain-one",
            "--config",
            str(config),
            "--lock-path",
            str(tmp_path / "orchestrator.lock"),
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert '"status": "no_handoff"' in out


def test_quay_cli_client_uses_ast_121_human_reply_contract():
    runner = RecordingQuayRunner()
    client = quay.QuayCliClient(command="/bin/quay", runner=runner)

    handoff = client.claim_handoff("worker-1")
    assert handoff is not None
    assert handoff.claim_id == "claim-1"
    assert handoff.artifact_id == "blocker"
    assert handoff.summary == "Quay handoff reason: worker_blocker"
    task = client.get_task_context(handoff.task_id)
    assert task.repo_id == "repo-1"
    assert task.metadata["slack_thread_ref"] == "GPRIVATE123:999.000000"
    assert task.metadata["authors"] == [{"slack_id": "U06TDC56VJB"}]
    artifact = client.get_artifact(handoff)
    assert artifact.text == "blocked on product input\n"

    client.escalate_human(handoff, "Question?", None)
    client.record_human_reply(
        handoff,
        quay.SlackReply(text="Answer.", user_id="U1", ts="1001.000000"),
        "C123:1000.000000",
    )
    client.submit_brief(handoff, "Next brief.", reason="advice_answered")

    calls = [" ".join(call[1:]) for call in runner.calls]
    assert "escalate-human task-cli --claim-id claim-1" in calls[4]
    assert "--thread-ref" not in calls[4]
    assert "record-human-reply task-cli --claim-id claim-1" in calls[5]
    assert "--message-ts 1001.000000 --author U1" in calls[5]
    assert "submit-brief task-cli --claim-id claim-1" in calls[6]
    assert "--reason advice_answered" in calls[6]
    assert runner.file_payloads == ["Question?", "Answer.", "Next brief."]


def test_quay_cli_client_claims_waiting_human_handoff_for_parked_poll():
    class WaitingHumanRunner:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def __call__(self, argv, **kwargs):
            self.calls.append(list(argv))
            command = argv[1:]
            if command == ["handoff", "list", "--status", "waiting_human"]:
                return FakeCompletedProcess(
                    '[{"handoff_id":8,"task_id":"task-waiting-cli",'
                    '"reason":"worker_blocker","status":"waiting_human",'
                    '"claim_id":"claim-waiting",'
                    '"thread_ref":"CWAIT12345:1000.000000",'
                    '"payload_json":"{\\"human_question\\":\\"Question?\\"}"}]\n'
                )
            raise AssertionError(f"unexpected quay command: {command!r}")

    runner = WaitingHumanRunner()
    client = quay.QuayCliClient(command="/bin/quay", runner=runner)

    handoff = client.claim_waiting_human("worker-1")

    assert handoff is not None
    assert handoff.status == "waiting_human"
    assert handoff.claim_id == "claim-waiting"
    assert handoff.metadata["thread_ref"] == "CWAIT12345:1000.000000"
    assert handoff.human_question == "Question?"
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert calls == ["handoff list --status waiting_human"]
