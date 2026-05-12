"""Schema tests for ``slack_triggers:`` in ``installer/values_helper.py``.

Covers:
* validate-schema accepts a well-formed top-level ``slack_triggers:`` block.
* validate-schema rejects bad shapes (channel_id, skill name, missing
  synthetic_author when accept_from_bots is true, duplicate channels).
* validate-schema with ``--skills-root`` enforces skill existence at
  install time (the spec's "unknown skills fail loud at install, not at
  runtime" requirement).
* render-runtime-config flattens the top-level block into
  ``slack.triggers:`` in the rendered config.yaml (where the gateway
  bridge in ``gateway/config.py`` picks it up).
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER = REPO_ROOT / "installer" / "values_helper.py"


def _run(values_path: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HELPER), "--values", str(values_path), *args],
        capture_output=True,
        text=True,
    )


@pytest.fixture
def base_values_body() -> str:
    """Minimal valid base — adds slack_triggers per test."""
    return textwrap.dedent(
        """
        org:
          name: TestOrg
          agent_identity_name: alice
        gateway:
          model_provider: openai-codex
          model_base_url: ""
        slack:
          app:
            display_name: TestBot
            description: testing
            background_color: "#000000"
          runtime:
            allowed_users: []
            allowed_channels: []
            require_mention: true
            home_channel: ""
            channel_prompts: {}
        repos: []
        """
    ).strip() + "\n"


def _write_values(tmp_path: Path, body: str, triggers_yaml: str = "") -> Path:
    p = tmp_path / "deploy.values.yaml"
    if triggers_yaml:
        body = body + triggers_yaml
    p.write_text(body, encoding="utf-8")
    return p


class TestValidateSchema:
    def test_empty_slack_triggers_passes(self, tmp_path: Path, base_values_body: str):
        p = _write_values(tmp_path, base_values_body, "slack_triggers: []\n")
        r = _run(p, "validate-schema")
        assert r.returncode == 0, r.stderr

    def test_well_formed_trigger_passes(self, tmp_path: Path, base_values_body: str):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                channel_name: feedback
                skill: feedback-intake
                require_top_level: true
                accept_from_bots: false
                rate_limit:
                  max_per_hour: 30
                  on_overflow: skip
                default_repo: iTRY-monorepo
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        r = _run(p, "validate-schema")
        assert r.returncode == 0, r.stderr

    def test_bad_channel_id_rejected(self, tmp_path: Path, base_values_body: str):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: lowercase
                skill: feedback-intake
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        r = _run(p, "validate-schema")
        assert r.returncode == 1
        assert "channel_id" in r.stderr

    def test_bad_skill_name_rejected(self, tmp_path: Path, base_values_body: str):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: "Bad Name!"
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        r = _run(p, "validate-schema")
        assert r.returncode == 1
        assert "skill" in r.stderr

    def test_accept_from_bots_without_synthetic_author_rejected(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0NOTIFY
                skill: newrelic-triage
                accept_from_bots: true
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        r = _run(p, "validate-schema")
        assert r.returncode == 1
        assert "synthetic_author" in r.stderr

    def test_duplicate_channel_rejected(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: feedback-intake
              - channel_id: C0FEEDBACK
                skill: feedback-intake
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        r = _run(p, "validate-schema")
        assert r.returncode == 1
        assert "already bound" in r.stderr or "channel_id" in r.stderr

    def test_skills_root_typo_fails(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: feedback-intaek
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        skills_root = tmp_path / "skills"
        (skills_root / "feedback-intake").mkdir(parents=True)
        (skills_root / "feedback-intake" / "SKILL.md").write_text("# real\n")
        r = _run(p, "validate-schema", "--skills-root", str(skills_root))
        assert r.returncode == 1
        assert "not present" in r.stderr

    def test_skills_root_match_passes(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: feedback-intake
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        skills_root = tmp_path / "skills"
        (skills_root / "feedback-intake").mkdir(parents=True)
        (skills_root / "feedback-intake" / "SKILL.md").write_text("# real\n")
        r = _run(p, "validate-schema", "--skills-root", str(skills_root))
        assert r.returncode == 0, r.stderr


class TestMergeSlackTriggers:
    """Every-run reconciliation of slack.triggers — the blocking issue from
    PR #62 review. render-runtime-config seeds on first install only, so
    without this command, adding a new trigger to deploy.values.yaml and
    re-running the installer would never reach the gateway."""

    def _seed_config(self, tmp_path: Path, body: str = "") -> Path:
        out = tmp_path / "config.yaml"
        out.write_text(body or "slack:\n  allowed_channels:\n    - C_TEST\n",
                       encoding="utf-8")
        return out

    def test_adds_triggers_on_first_merge(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: feedback-intake
                default_repo: iTRY-monorepo
            """
        )
        values = _write_values(tmp_path, base_values_body, triggers)
        out = self._seed_config(tmp_path)
        r = _run(values, "merge-slack-triggers", "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert rendered["slack"]["triggers"][0]["channel_id"] == "C0FEEDBACK"
        # Pre-existing operator-edited keys survive the merge.
        assert rendered["slack"]["allowed_channels"] == ["C_TEST"]

    def test_overwrites_existing_triggers(
        self, tmp_path: Path, base_values_body: str,
    ):
        """Declarative reconciliation: a values.yaml edit replaces the
        live triggers list verbatim. Stale entries that the operator
        removed from values.yaml must disappear from config.yaml."""
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0NEW01
                skill: feedback-intake
            """
        )
        values = _write_values(tmp_path, base_values_body, triggers)
        out = self._seed_config(
            tmp_path,
            body=(
                "slack:\n"
                "  allowed_channels:\n    - C_TEST\n"
                "  triggers:\n"
                "    - channel_id: C0STALE01\n"
                "      skill: feedback-intake\n"
            ),
        )
        r = _run(values, "merge-slack-triggers", "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = yaml.safe_load(out.read_text(encoding="utf-8"))
        channel_ids = [t["channel_id"] for t in rendered["slack"]["triggers"]]
        assert channel_ids == ["C0NEW01"]

    def test_empty_block_clears_triggers(
        self, tmp_path: Path, base_values_body: str,
    ):
        values = _write_values(tmp_path, base_values_body, "slack_triggers: []\n")
        out = self._seed_config(
            tmp_path,
            body=(
                "slack:\n"
                "  triggers:\n"
                "    - channel_id: C0STALE01\n"
                "      skill: feedback-intake\n"
            ),
        )
        r = _run(values, "merge-slack-triggers", "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert "triggers" not in (rendered.get("slack") or {})

    def test_missing_config_is_an_error(
        self, tmp_path: Path, base_values_body: str,
    ):
        values = _write_values(tmp_path, base_values_body, "slack_triggers: []\n")
        out = tmp_path / "nonexistent.yaml"
        r = _run(values, "merge-slack-triggers", "--out", str(out))
        assert r.returncode == 1
        assert "render-runtime-config must seed" in r.stderr


class TestRenderRuntimeConfigEmitsTriggers:
    def test_top_level_block_becomes_slack_triggers(
        self, tmp_path: Path, base_values_body: str,
    ):
        triggers = textwrap.dedent(
            """
            slack_triggers:
              - channel_id: C0FEEDBACK
                skill: feedback-intake
                default_repo: iTRY-monorepo
            """
        )
        p = _write_values(tmp_path, base_values_body, triggers)
        out = tmp_path / "config.yaml"
        r = _run(p, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = yaml.safe_load(out.read_text(encoding="utf-8"))
        assert "slack" in rendered
        assert "triggers" in rendered["slack"]
        assert rendered["slack"]["triggers"][0]["channel_id"] == "C0FEEDBACK"
        assert rendered["slack"]["triggers"][0]["skill"] == "feedback-intake"

    def test_empty_block_omits_triggers_key(
        self, tmp_path: Path, base_values_body: str,
    ):
        p = _write_values(tmp_path, base_values_body, "slack_triggers: []\n")
        out = tmp_path / "config.yaml"
        r = _run(p, "render-runtime-config", "--out", str(out))
        assert r.returncode == 0, r.stderr
        rendered = yaml.safe_load(out.read_text(encoding="utf-8"))
        # An empty block doesn't emit a triggers key (matching the "no
        # opt-in flags" convention — absence is the explicit empty signal).
        assert "triggers" not in (rendered.get("slack") or {})
