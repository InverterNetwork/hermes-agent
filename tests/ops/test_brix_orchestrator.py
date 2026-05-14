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
        self.submitted: list[tuple[object, str]] = []
        self.completed: list[object] = []
        self.released: list[tuple[object, str]] = []

    def claim_handoff(self, worker_id: str):
        handoff = self._handoff
        self._handoff = None
        return handoff

    def get_task_context(self, task_id: str):
        return self.task

    def get_artifact(self, artifact_id: str):
        return self.artifact

    def submit_brief(self, handoff, brief: str) -> None:
        self.submitted.append((handoff, brief))

    def complete_claim(self, handoff) -> None:
        self.completed.append(handoff)

    def release_claim(self, handoff, reason: str) -> None:
        self.released.append((handoff, reason))


class FakeSlackClient:
    def __init__(self, reply=None) -> None:
        self.reply = reply
        self.questions: list[tuple[str, str]] = []
        self.waits: list[tuple[object, float, float]] = []

    def post_question(self, channel_id: str, text: str, *, handoff, task):
        self.questions.append((channel_id, text))
        return brix.SlackPostRef(channel_id=channel_id, ts="1000.000000", thread_ts="1000.000000")

    def wait_for_reply(self, ref, *, timeout_seconds: float, poll_interval_seconds: float):
        self.waits.append((ref, timeout_seconds, poll_interval_seconds))
        return self.reply


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
    assert quay.submitted == [(handoff, "Apply the reviewer suggestion and rerun tests.")]
    assert quay.completed == [handoff]
    assert quay.released == []
    assert slack.questions == []
    assert result.metrics["direct_briefs_submitted"] == 1


def test_drain_one_posts_slack_question_and_maps_reply_to_next_brief():
    handoff = brix.Handoff(
        handoff_id="handoff-2",
        task_id="task-2",
        repo_id="brix-indexer",
        artifact_id="artifact-2",
        summary="Worker could not choose the migration path.",
    )
    reply = brix.SlackReply(
        text="Use the existing importer and keep the old endpoint as a fallback.",
        user_id="U123",
        ts="1001.000000",
        permalink="https://example.slack/thread",
    )
    quay = FakeQuayClient(handoff)
    slack = FakeSlackClient(reply)
    config = brix.OrchestratorConfig(
        enabled=True,
        default_slack_channel="C1234567890",
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
    assert slack.questions[0][0] == "C1234567890"
    assert "Worker could not choose the migration path" in slack.questions[0][1]
    assert "Worker needs product guidance" in slack.questions[0][1]
    assert slack.waits[0][1:] == (60, 2)
    submitted = quay.submitted[0][1]
    assert "Use the following human guidance" in submitted
    assert "U123" in submitted
    assert "Use the existing importer" in submitted
    assert quay.completed == [handoff]
    assert quay.released == []
    assert result.metrics["slack_questions_posted"] == 1
    assert result.metrics["human_briefs_submitted"] == 1


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


def test_cli_pending_adapter_noops_without_slack_token(tmp_path: Path, monkeypatch, capsys):
    config = tmp_path / "orchestrator.json"
    config.write_text('{"enabled": true}\n', encoding="utf-8")
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
