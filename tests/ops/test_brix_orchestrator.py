from __future__ import annotations

import dataclasses
import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "ops" / "brix_orchestrator.py"
SPEC = importlib.util.spec_from_file_location("brix_orchestrator", MODULE_PATH)
assert SPEC and SPEC.loader
brix = importlib.util.module_from_spec(SPEC)
sys.modules["brix_orchestrator"] = brix
SPEC.loader.exec_module(brix)


class FakeQuayClient:
    def __init__(
        self,
        handoff,
        *,
        task=None,
        artifact=None,
    ) -> None:
        self._handoff = handoff
        self.task = task or brix.TaskContext(
            task_id=handoff.task_id,
            title="Fix stuck worker",
            issue="BRIX-1405",
            repo_id="hermes-agent",
        )
        self.artifact = artifact or brix.Artifact(
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
            if isinstance(result, brix.SlackPostRef):
                return result
        floor_ts = self.thread_floor_ts.get((channel_id, thread_ts), thread_ts)
        return brix.SlackPostRef(
            channel_id=channel_id,
            ts=floor_ts,
            thread_ts=thread_ts,
        )

    def post_question(self, channel_id: str, text: str, *, thread_ts=None, handoff, task):
        if self.post_error:
            raise self.post_error
        self.questions.append((channel_id, text, thread_ts))
        ref = brix.SlackPostRef(
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
                if result.ts and brix._slack_ts_key(result.ts) <= brix._slack_ts_key(after_ts):
                    continue
                return result
        return None

    def post_thread_message(self, ref, text: str, *, handoff, task):
        self.acks.append((ref, text))
        post_ref = brix.SlackPostRef(
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
        return brix.ConversationDecision(action="wait")


def ready(brief: str) -> brix.ConversationDecision:
    return brix.ConversationDecision(action="ready", brief=brief)


def ask(message: str) -> brix.ConversationDecision:
    return brix.ConversationDecision(action="ask", message=message)


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
                '"slack_thread_ref":"GPRIVATE123:999.000000"}\n'
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


def test_drain_one_submits_direct_next_brief_without_slack():
    handoff = brix.Handoff(
        handoff_id="handoff-1",
        task_id="task-1",
        next_brief="Apply the reviewer suggestion and rerun tests.",
    )
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient()
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "submitted_direct"
    assert quay.submitted == [
        (handoff, "Apply the reviewer suggestion and rerun tests.", "blocker_resolved")
    ]
    assert quay.completed == [handoff]
    assert quay.released == []
    assert slack.questions == []
    assert result.metrics["direct_briefs_submitted"] == 1


def test_drain_one_reattaches_to_original_thread_without_reposting_question():
    runner = RecordingQuayRunner()
    quay = brix.QuayCliClient(command="/bin/quay", runner=runner)
    reply = brix.SlackReply(
        text="Use the existing importer and keep the old endpoint as a fallback.",
        user_id="U123",
        ts="1001.000000",
        permalink="https://example.slack/thread",
    )
    slack = FakeSlackClient(reply)
    decider = FakeDecider([
        ready("Use the existing importer and keep the old endpoint as a fallback.")
    ])
    config = brix.OrchestratorConfig(
        enabled=True,
        reply_timeout_seconds=60,
        poll_interval_seconds=2,
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=config,
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.validations == [("GPRIVATE123", "999.000000")]
    assert slack.questions == []
    assert slack.waits[0][0] == brix.SlackPostRef(
        channel_id="GPRIVATE123",
        ts="999.000000",
        thread_ts="999.000000",
    )
    assert slack.waits[0][1] <= 60
    assert slack.waits[0][1] > 59
    assert slack.waits[0][2] == 2
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert "escalate-human task-cli --claim-id claim-1" in calls[4]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[4]
    assert "record-human-reply task-cli --claim-id claim-1" in calls[5]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[5]
    assert "submit-brief task-cli --claim-id claim-1" in calls[6]
    assert "--reason advice_answered" in calls[6]
    assert "Quay handoff reason: worker_blocker" in runner.file_payloads[0]
    assert "blocked on product input" in runner.file_payloads[0]
    assert "resume the worker once the path is clear" in runner.file_payloads[0]
    assert "resume:" not in runner.file_payloads[0]
    assert "Use the following human guidance" in runner.file_payloads[2]
    assert "U123" in runner.file_payloads[2]
    assert "Use the existing importer" in runner.file_payloads[2]
    assert slack.acks
    assert "Quay resumed for BRIX-1405" in slack.acks[0][1]
    assert result.metrics["slack_questions_posted"] == 0
    assert result.metrics["human_briefs_submitted"] == 1


def test_drain_one_falls_back_to_default_channel_and_persists_post_thread():
    handoff = brix.Handoff(
        handoff_id="handoff-fallback",
        task_id="task-fallback",
        artifact_id="artifact-fallback",
        summary="Worker needs a product decision.",
    )
    reply = brix.SlackReply(
        text="Use the default route.",
        user_id="U123",
        ts="1001.000000",
    )
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(reply)
    decider = FakeDecider([ready("Use the default route.")])
    config = brix.OrchestratorConfig(
        enabled=True,
        default_slack_channel="C1234567890",
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=config,
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.questions[0][0] == "C1234567890"
    assert slack.questions[0][2] is None
    assert quay.escalated[2] == "C1234567890:1000.000000"
    assert quay.recorded_reply[2] == "C1234567890:1000.000000"
    assert quay.submitted[0][2] == "advice_answered"
    assert slack.acks


def test_missing_default_slack_channel_releases_claim():
    handoff = brix.Handoff(handoff_id="handoff-3", task_id="task-3")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient()
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "missing_default_slack_channel"
    assert quay.submitted == []
    assert quay.completed == []
    assert quay.released == [(handoff, "missing_default_slack_channel")]
    assert slack.questions == []


def test_human_reply_timeout_releases_claim():
    handoff = brix.Handoff(handoff_id="handoff-4", task_id="task-4")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(reply=None)
    decider = FakeDecider([ready("Use the existing thread on retry.")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
    assert quay.submitted == []
    assert quay.completed == []
    assert quay.released == [(handoff, "human_reply_timeout")]
    assert slack.questions
    slack.wait_results.append(
        brix.SlackReply(
            text="Use the existing thread on retry.",
            user_id="U123",
            ts="1001.000000",
        )
    )

    retry = drainer.drain_one()

    assert retry.status == "submitted_from_human_reply"
    assert len(slack.questions) == 1
    assert slack.validations == [("C1234567890", "1000.000000")]
    assert quay.recorded_reply[2] == "C1234567890:1000.000000"
    assert quay.submitted[0][2] == "advice_answered"


def test_plain_thread_chatter_does_not_resume_quay():
    handoff = brix.Handoff(handoff_id="handoff-chatter", task_id="task-chatter")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        brix.SlackReply(
            text="Could I fix CI and ping you here?",
            user_id="U123",
            ts="1001.000000",
        )
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
            reply_timeout_seconds=1,
            poll_interval_seconds=1,
        ),
        worker_id="test-worker",
        decider=FakeDecider([brix.ConversationDecision(action="wait")]),
    )

    result = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert not hasattr(quay, "recorded_reply")
    assert quay.submitted == []
    assert slack.acks == []
    assert quay.released == [(handoff, "human_reply_timeout")]


def test_orchestrator_ready_decision_records_submits_and_acks():
    handoff = brix.Handoff(handoff_id="handoff-resume", task_id="task-resume")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        brix.SlackReply(
            text="CI is blocked because the auth fixture is only available in CI.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Use the CI-only auth fixture.")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
            enabled=True,
            default_slack_channel="C1234567890",
        ),
        worker_id="test-worker",
        decider=decider,
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert quay.recorded_reply[1].text == "CI is blocked because the auth fixture is only available in CI."
    assert quay.submitted[0][1].endswith("Use the CI-only auth fixture.")
    assert quay.submitted[0][2] == "advice_answered"
    assert quay.completed == [handoff]
    assert slack.acks
    assert "Quay resumed for BRIX-1405" in slack.acks[0][1]
    assert "Use the CI-only auth fixture" in slack.acks[0][1]


def test_orchestrator_can_ask_for_confirmation_before_resuming():
    handoff = brix.Handoff(handoff_id="handoff-confirm", task_id="task-confirm")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            brix.SlackReply(
                text="I think we should use the CI-only auth fixture.",
                user_id="U123",
                ts="1001.000000",
            ),
            brix.SlackReply(
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
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
    assert quay.recorded_reply[1].text == "Yes, that's the right worker instruction."
    assert quay.submitted[0][1].endswith("Use the CI-only auth fixture.")


def test_orchestrator_decider_error_prompts_once_and_keeps_waiting():
    handoff = brix.Handoff(handoff_id="handoff-decider", task_id="task-decider")
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            brix.SlackReply(
                text="The fixture is only available in CI.",
                user_id="U123",
                ts="1001.000000",
            ),
            brix.SlackReply(
                text="Tell the worker to use the CI-only auth fixture.",
                user_id="U123",
                ts="1003.000000",
            ),
        ]
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
    assert quay.escalations[0][2] == "C1234567890:1000.000000"
    assert quay.released == []
    assert quay.submitted[0][1].endswith("Use the CI-only auth fixture.")
    assert quay.completed == [handoff]
    assert result.metrics["errors"] == 1
    assert result.metrics["claims_released"] == 0


def test_orchestrator_decider_error_retry_does_not_reprocess_old_reply():
    handoff = brix.Handoff(
        handoff_id="handoff-decider-timeout",
        task_id="task-decider-timeout",
    )
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(
        wait_results=[
            brix.SlackReply(
                text="The fixture is only available in CI.",
                user_id="U123",
                ts="1001.000000",
            ),
        ]
    )
    decider = FakeDecider([RuntimeError("model unavailable")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
        brix.SlackReply(
            text="The fixture is only available in CI.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    retry = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert retry.status == "human_reply_timeout"
    assert len(slack.questions) == 1
    assert len(slack.acks) == 1
    assert "decision engine hit an internal orchestrator error" in slack.acks[0][1]
    assert "No need to rephrase" in slack.acks[0][1]
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

    monkeypatch.setenv("BRIX_ORCHESTRATOR_PROVIDER", "openai-codex")
    monkeypatch.setenv("HERMES_INFERENCE_PROVIDER", "anthropic")
    decider = brix.HermesConversationDecider(
        runtime_resolver=runtime_resolver,
        default_model_resolver=lambda provider: f"default-for-{provider}",
        agent_class=FakeAgent,
    )
    decision = decider.decide(
        handoff=brix.Handoff(handoff_id="handoff-runtime", task_id="task-runtime"),
        task=brix.TaskContext(task_id="task-runtime"),
        artifact=brix.Artifact(artifact_id="artifact-runtime", text="blocked"),
        question="What should happen?",
        replies=[
            brix.SlackReply(
                text="Resume with the answer.",
                user_id="U123",
                ts="1001.000000",
            )
        ],
        posted_messages=[],
    )

    assert decision == brix.ConversationDecision(
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
    assert kwargs["platform"] == "brix-orchestrator"
    assert kwargs["session_id"].startswith("brix-orch-")
    assert len(kwargs["session_id"]) <= 64


def test_orchestrator_decider_uses_short_session_id_for_long_quay_ids():
    FakeAgent.instances = []
    long_handoff = brix.Handoff(
        handoff_id="handoff-with-a-long-human-readable-id",
        task_id="5bb225ff-fde7-40d9-acac-1a9f8e88e5d9",
        claim_id="1f6f9630-291f-4a9b-a169-eb2d7690da14",
    )
    decider = brix.HermesConversationDecider(
        runtime_resolver=lambda *, requested=None: {"provider": "openai-codex"},
        default_model_resolver=lambda provider: "gpt-5.5",
        agent_class=FakeAgent,
    )

    decider.decide(
        handoff=long_handoff,
        task=brix.TaskContext(task_id=long_handoff.task_id),
        artifact=brix.Artifact(artifact_id="artifact-runtime", text="blocked"),
        question="What should happen?",
        replies=[
            brix.SlackReply(
                text="I will fix CI manually and report back.",
                user_id="U123",
                ts="1001.000000",
            )
        ],
        posted_messages=[],
    )

    session_id = FakeAgent.instances[0].kwargs["session_id"]
    assert session_id.startswith("brix-orch-")
    assert len(session_id) <= 64
    assert long_handoff.task_id not in session_id
    assert long_handoff.claim_id not in session_id


def test_orchestrator_decider_lets_shared_resolver_handle_unset_provider(monkeypatch):
    calls = []

    def runtime_resolver(*, requested=None):
        calls.append(requested)
        return {"provider": "openai-codex"}

    monkeypatch.delenv("BRIX_ORCHESTRATOR_PROVIDER", raising=False)
    monkeypatch.setenv("HERMES_INFERENCE_PROVIDER", "anthropic")

    decider = brix.HermesConversationDecider(runtime_resolver=runtime_resolver)

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
    decider = brix.HermesConversationDecider(
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
    decider = brix.HermesConversationDecider(
        default_model_resolver=lambda provider: f"default-for-{provider}",
        model_catalog_resolver=lambda provider: ["gpt-5.3-codex", "gpt-5.4"],
    )

    assert decider._resolve_model({"provider": "openai-codex"}) == "gpt-5.4"


def test_fake_quay_rejects_double_human_escalation_without_reply():
    handoff = brix.Handoff(handoff_id="handoff-stateful", task_id="task-stateful")
    quay = FakeQuayClient(handoff)

    assert quay.claim_handoff("test-worker") == handoff
    quay.escalate_human(handoff, "Question", "C1234567890:1000.000000")

    with pytest.raises(RuntimeError, match="wrong_state"):
        quay.escalate_human(handoff, "Fallback question", "CFALLBACK1:1000.000000")


def test_orchestrator_decision_parser_accepts_ready_ask_and_invalid_json():
    assert brix.parse_decision_response('{"action":"ready","brief":"do the thing"}') == brix.ConversationDecision(
        action="ready",
        brief="do the thing",
    )
    assert brix.parse_decision_response('```json\n{"action":"ask","message":"Can I tell the worker X?"}\n```') == brix.ConversationDecision(
        action="ask",
        message="Can I tell the worker X?",
    )
    assert brix.parse_decision_response("not json") == brix.ConversationDecision(action="wait")


def test_human_question_mentions_author_from_task_or_handoff_metadata():
    task = brix.TaskContext(
        task_id="task-author",
        title="Needs author",
        metadata={"author": {"slack_id": "U1234567"}},
    )
    handoff = brix.Handoff(
        handoff_id="handoff-author",
        task_id="task-author",
        metadata={"contributor": {"slack_id": "U7654321"}},
    )

    question = brix.build_human_question(handoff, task, None)

    assert "Contributor: <@U1234567>" in question


def test_human_question_mentions_legacy_flash_tag_author_on_fallback_route():
    task = brix.TaskContext(task_id="task-legacy", title="Legacy flash")
    handoff = brix.Handoff(
        handoff_id="handoff-legacy",
        task_id="task-legacy",
        metadata={"source": {"flash": {"author_slack_id": "<@UFLASH1>"}}},
    )

    question = brix.build_human_question(handoff, task, None)
    route = brix.resolve_slack_route(
        handoff,
        task,
        brix.OrchestratorConfig(enabled=True, default_slack_channel="C1234567890"),
    )

    assert "Contributor: <@UFLASH1>" in question
    assert route == brix.SlackRoute(channel_id="C1234567890", source="fallback_channel")


def test_stale_metadata_route_falls_back_and_records_fallback_thread():
    handoff = brix.Handoff(handoff_id="handoff-stale", task_id="task-stale")
    task = brix.TaskContext(
        task_id=handoff.task_id,
        title="Fix stale route",
        issue="BRIX-1396",
        metadata={"slack_thread_ref": "CSTALE1234:1778757925.556349"},
    )
    quay = FakeQuayClient(handoff, task=task)
    slack = FakeSlackClient(
        validate_results=[
            brix.SlackApiError("conversations.replies", "thread_not_found"),
        ],
        wait_results=[
            brix.SlackReply(
                text="The fallback thread has the context we need.",
                user_id="U123",
                ts="1001.000000",
            ),
        ]
    )
    decider = FakeDecider([ready("Use the fallback thread.")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
    assert quay.escalations == [
        (handoff, slack.questions[0][1], "CFALLBACK1:1000.000000")
    ]
    assert quay.recorded_reply[2] == "CFALLBACK1:1000.000000"


def test_stale_metadata_route_without_fallback_releases_and_does_not_bubble():
    handoff = brix.Handoff(handoff_id="handoff-stale", task_id="task-stale")
    task = brix.TaskContext(
        task_id=handoff.task_id,
        title="Fix stale route",
        issue="BRIX-1396",
        metadata={"slack_thread_ref": "CSTALE1234:1778757925.556349"},
    )
    quay = FakeQuayClient(handoff, task=task)
    slack = FakeSlackClient(
        validate_results=[
            brix.SlackApiError("conversations.replies", "thread_not_found"),
        ]
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(enabled=True),
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "stale_slack_thread_ref"
    assert slack.validations == [("CSTALE1234", "1778757925.556349")]
    assert slack.questions == []
    assert quay.escalations == []
    assert quay.submitted == []
    assert quay.released == [(handoff, "stale_slack_thread_ref")]


def test_failed_record_human_reply_does_not_post_success_ack():
    class FailingRecordQuay(FakeQuayClient):
        def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
            raise RuntimeError("record failed")

    handoff = brix.Handoff(handoff_id="handoff-record-fails", task_id="task-record-fails")
    quay = FailingRecordQuay(handoff)
    slack = FakeSlackClient(
        brix.SlackReply(
            text="Try the fixture.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Try the fixture.")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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

    handoff = brix.Handoff(handoff_id="handoff-submit-fails", task_id="task-submit-fails")
    quay = FailingSubmitQuay(handoff)
    slack = FakeSlackClient(
        brix.SlackReply(
            text="Try the fixture.",
            user_id="U123",
            ts="1001.000000",
        )
    )
    decider = FakeDecider([ready("Try the fixture.")])
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=brix.OrchestratorConfig(
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
    with brix.FileLock(lock_path):
        with pytest.raises(brix.LockBusy):
            with brix.FileLock(lock_path):
                pass


def test_cli_quay_adapter_noops_without_slack_token_when_no_handoffs(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    quay = tmp_path / "quay"
    quay.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1 $2 $3\" == \"handoff list --status\" ]]; then\n"
        "  printf '[]\\n'\n"
        "  exit 0\n"
        "fi\n"
        "exit 99\n",
        encoding="utf-8",
    )
    quay.chmod(0o755)
    config = tmp_path / "orchestrator.json"
    config.write_text(
        '{"enabled": true, "quay_command": "' + str(quay) + '"}\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_TOKEN", raising=False)

    rc = brix.main(
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
    client = brix.QuayCliClient(command="/bin/quay", runner=runner)

    handoff = client.claim_handoff("worker-1")
    assert handoff is not None
    assert handoff.claim_id == "claim-1"
    assert handoff.artifact_id == "blocker"
    assert handoff.summary == "Quay handoff reason: worker_blocker"
    task = client.get_task_context(handoff.task_id)
    assert task.repo_id == "repo-1"
    assert task.metadata["slack_thread_ref"] == "GPRIVATE123:999.000000"
    artifact = client.get_artifact(handoff)
    assert artifact.text == "blocked on product input\n"

    client.escalate_human(handoff, "Question?", None)
    client.record_human_reply(
        handoff,
        brix.SlackReply(text="Answer.", user_id="U1", ts="1001.000000"),
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
