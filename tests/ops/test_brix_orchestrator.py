from __future__ import annotations

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
        self.submitted: list[tuple[object, str, str]] = []
        self.completed: list[object] = []
        self.released: list[tuple[object, str]] = []

    def claim_handoff(self, worker_id: str):
        handoff = self._handoff
        self._handoff = None
        return handoff

    def get_task_context(self, task_id: str):
        return self.task

    def get_artifact(self, handoff):
        return self.artifact

    def escalate_human(self, handoff, question: str, thread_ref: str) -> None:
        self.escalated = (handoff, question, thread_ref)

    def record_human_reply(self, handoff, reply, thread_ref: str) -> None:
        self.recorded_reply = (handoff, reply, thread_ref)

    def submit_brief(self, handoff, brief: str, *, reason: str) -> None:
        self.submitted.append((handoff, brief, reason))

    def complete_claim(self, handoff) -> None:
        self.completed.append(handoff)

    def release_claim(self, handoff, reason: str) -> None:
        self.released.append((handoff, reason))


class FakeSlackClient:
    def __init__(self, reply=None) -> None:
        self.reply = reply
        self.questions: list[tuple[str, str, str | None]] = []
        self.waits: list[tuple[object, float, float]] = []

    def post_question(self, channel_id: str, text: str, *, thread_ts=None, handoff, task):
        self.questions.append((channel_id, text, thread_ts))
        return brix.SlackPostRef(
            channel_id=channel_id,
            ts="1000.000000",
            thread_ts=thread_ts or "1000.000000",
        )

    def wait_for_reply(self, ref, *, timeout_seconds: float, poll_interval_seconds: float):
        self.waits.append((ref, timeout_seconds, poll_interval_seconds))
        return self.reply


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


def test_drain_one_posts_slack_question_to_original_thread_without_fallback():
    runner = RecordingQuayRunner()
    quay = brix.QuayCliClient(command="/bin/quay", runner=runner)
    reply = brix.SlackReply(
        text="Use the existing importer and keep the old endpoint as a fallback.",
        user_id="U123",
        ts="1001.000000",
        permalink="https://example.slack/thread",
    )
    slack = FakeSlackClient(reply)
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
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.questions[0][0] == "GPRIVATE123"
    assert slack.questions[0][2] == "999.000000"
    assert "Quay handoff reason: worker_blocker" in slack.questions[0][1]
    assert "blocked on product input" in slack.questions[0][1]
    assert slack.waits[0][1:] == (60, 2)
    calls = [" ".join(call[1:]) for call in runner.calls]
    assert "escalate-human task-cli --claim-id claim-1" in calls[4]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[4]
    assert "record-human-reply task-cli --claim-id claim-1" in calls[5]
    assert "--thread-ref GPRIVATE123:999.000000" in calls[5]
    assert "submit-brief task-cli --claim-id claim-1" in calls[6]
    assert "--reason advice_answered" in calls[6]
    assert "Use the following human guidance" in runner.file_payloads[2]
    assert "U123" in runner.file_payloads[2]
    assert "Use the existing importer" in runner.file_payloads[2]
    assert result.metrics["slack_questions_posted"] == 1
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
    config = brix.OrchestratorConfig(
        enabled=True,
        default_slack_channel="C1234567890",
    )
    drainer = brix.HandoffDrainer(
        quay=quay,
        slack=slack,
        config=config,
        worker_id="test-worker",
    )

    result = drainer.drain_one()

    assert result.status == "submitted_from_human_reply"
    assert slack.questions[0][0] == "C1234567890"
    assert slack.questions[0][2] is None
    assert quay.escalated[2] == "C1234567890:1000.000000"
    assert quay.recorded_reply[2] == "C1234567890:1000.000000"
    assert quay.submitted[0][2] == "advice_answered"


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
    )

    result = drainer.drain_one()

    assert result.status == "human_reply_timeout"
    assert quay.submitted == []
    assert quay.completed == []
    assert quay.released == [(handoff, "human_reply_timeout")]
    assert slack.questions


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
