"""Tests for declarative, operator-whitelisted secret injection into cron
scripts (BRIX-1870).

The feature adds a NARROW, two-gate opt-in on top of the default strip-all
posture for cron subprocess environments (SECURITY.md §2.3):

  * Job gate (declaration): a job's ``secrets: [...]`` list. Dev-editable,
    PR-reviewed, lives in hermes-state.
  * Platform gate (authorization): ``cron.injectable_secrets`` in the HOST
    config.yaml. Operator-controlled allowlist, NOT in hermes-state.

Injection is the INTERSECTION of both gates, minus a never-injectable hard
floor, and only for names that actually have a value in the host environment.

Coverage:
  * Job model: the ``secrets`` field is stored, normalized, updated, cleared.
  * ``build_cron_secret_injection`` helper: each gate + hard floor + loud warn.
  * ``_cron_injectable_secrets_allowlist`` config reader.
  * ``_run_job_script`` end to end: allowlisted+declared → injected;
    declared-not-allowlisted → stripped + loud; allowlisted-not-declared →
    stripped; hard-floor declared+allowlisted → stripped + loud; undeclared →
    stripped.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest


@pytest.fixture
def cron_env(tmp_path, monkeypatch):
    """Isolated cron environment with a temp HERMES_HOME (mirrors test_cron_script)."""
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "cron").mkdir()
    (hermes_home / "cron" / "output").mkdir()
    (hermes_home / "scripts").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    import cron.jobs as jobs_mod
    monkeypatch.setattr(jobs_mod, "HERMES_DIR", hermes_home)
    monkeypatch.setattr(jobs_mod, "CRON_DIR", hermes_home / "cron")
    monkeypatch.setattr(jobs_mod, "JOBS_FILE", hermes_home / "cron" / "jobs.json")
    monkeypatch.setattr(jobs_mod, "OUTPUT_DIR", hermes_home / "cron" / "output")

    return hermes_home


def _write_probe(scripts_dir: Path, var: str, name: str = "probe.py") -> str:
    """Write a script that prints PRESENT/ABSENT for a single env var."""
    script = scripts_dir / name
    script.write_text(
        textwrap.dedent(
            f"""\
            import os
            key = {var!r}
            print("PRESENT" if os.environ.get(key) else "ABSENT")
            """
        )
    )
    return name


# ---------------------------------------------------------------------------
# Job model: the ``secrets`` declaration field
# ---------------------------------------------------------------------------


class TestJobSecretsField:
    def test_create_job_stores_secrets_list(self, cron_env):
        from cron.jobs import create_job, get_job

        job = create_job(
            prompt=None,
            schedule="every 5m",
            script="w.sh",
            no_agent=True,
            deliver="local",
            secrets=["SLACK_BOT_TOKEN"],
        )
        assert job["secrets"] == ["SLACK_BOT_TOKEN"]
        assert get_job(job["id"])["secrets"] == ["SLACK_BOT_TOKEN"]

    def test_create_job_normalizes_and_dedupes(self, cron_env):
        from cron.jobs import create_job

        job = create_job(
            prompt="hi",
            schedule="every 5m",
            secrets=["  SLACK_BOT_TOKEN ", "", "SLACK_BOT_TOKEN", "MY_TOK"],
        )
        assert job["secrets"] == ["SLACK_BOT_TOKEN", "MY_TOK"]

    def test_create_job_default_is_empty_list(self, cron_env):
        from cron.jobs import create_job

        job = create_job(prompt="hi", schedule="every 5m")
        assert job["secrets"] == []

    def test_update_job_add_secrets(self, cron_env):
        from cron.jobs import create_job, update_job

        job = create_job(prompt="hi", schedule="every 5m")
        updated = update_job(job["id"], {"secrets": ["SLACK_BOT_TOKEN", " SLACK_BOT_TOKEN "]})
        assert updated["secrets"] == ["SLACK_BOT_TOKEN"]

    def test_update_job_clear_secrets(self, cron_env):
        from cron.jobs import create_job, update_job

        job = create_job(prompt="hi", schedule="every 5m", secrets=["SLACK_BOT_TOKEN"])
        assert job["secrets"] == ["SLACK_BOT_TOKEN"]
        updated = update_job(job["id"], {"secrets": []})
        assert updated["secrets"] == []

    def test_get_job_normalizes_legacy_record_without_field(self, cron_env):
        """A hand-written/legacy job missing ``secrets`` reads back as []."""
        from cron.jobs import _normalize_job_record

        normalized = _normalize_job_record({"id": "x", "prompt": "p"})
        assert normalized["secrets"] == []


class TestCronjobToolSecrets:
    def test_create_with_secrets_via_tool(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        import json
        from tools.cronjob_tools import cronjob

        (cron_env / "scripts" / "post.sh").write_text("#!/bin/bash\necho hi\n")
        result = json.loads(
            cronjob(
                action="create",
                schedule="every 5m",
                script="post.sh",
                no_agent=True,
                deliver="local",
                secrets=["SLACK_BOT_TOKEN"],
            )
        )
        assert result["success"] is True
        assert result["job"]["secrets"] == ["SLACK_BOT_TOKEN"]

    def test_update_clear_secrets_via_tool(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        import json
        from tools.cronjob_tools import cronjob

        created = json.loads(
            cronjob(action="create", schedule="every 5m", prompt="hi", secrets=["SLACK_BOT_TOKEN"])
        )
        job_id = created["job_id"]
        updated = json.loads(cronjob(action="update", job_id=job_id, secrets=[]))
        assert updated["success"] is True
        # _format_job omits an empty secrets list.
        assert "secrets" not in updated["job"]

    def test_schema_exposes_secrets_property(self):
        from tools.cronjob_tools import CRONJOB_SCHEMA

        props = CRONJOB_SCHEMA["parameters"]["properties"]
        assert "secrets" in props
        assert props["secrets"]["type"] == "array"


# ---------------------------------------------------------------------------
# build_cron_secret_injection: the intersection + hard floor + loud warnings
# ---------------------------------------------------------------------------


class TestBuildCronSecretInjection:
    def test_declared_and_allowlisted_is_injected(self):
        from tools.environments.local import (
            build_cron_secret_injection,
            _HERMES_PROVIDER_ENV_FORCE_PREFIX,
        )

        extra, warns = build_cron_secret_injection(
            ["SLACK_BOT_TOKEN"],
            {"SLACK_BOT_TOKEN"},
            source_env={"SLACK_BOT_TOKEN": "xoxb-1"},
        )
        assert extra == {f"{_HERMES_PROVIDER_ENV_FORCE_PREFIX}SLACK_BOT_TOKEN": "xoxb-1"}
        assert warns == []

    def test_uses_force_prefix_so_sanitize_reinjects(self):
        """The injected key must carry the force prefix so _sanitize re-adds the
        bare name POST-strip."""
        from tools.environments.local import (
            build_cron_secret_injection,
            _sanitize_subprocess_env,
        )

        extra, _ = build_cron_secret_injection(
            ["SLACK_BOT_TOKEN"], {"SLACK_BOT_TOKEN"}, source_env={"SLACK_BOT_TOKEN": "xoxb-1"}
        )
        sanitized = _sanitize_subprocess_env({"SLACK_BOT_TOKEN": "xoxb-1", "PATH": "/bin"}, extra)
        assert sanitized["SLACK_BOT_TOKEN"] == "xoxb-1"

    def test_declared_not_allowlisted_skipped_and_loud(self):
        from tools.environments.local import build_cron_secret_injection

        extra, warns = build_cron_secret_injection(
            ["SLACK_BOT_TOKEN"], set(), source_env={"SLACK_BOT_TOKEN": "xoxb-1"}
        )
        assert extra == {}
        assert len(warns) == 1
        assert "not in the operator" in warns[0]
        assert "SLACK_BOT_TOKEN" in warns[0]

    def test_allowlisted_not_declared_is_not_injected(self):
        """The declaration list drives — an allowlisted-but-undeclared name is
        never injected."""
        from tools.environments.local import build_cron_secret_injection

        extra, warns = build_cron_secret_injection(
            [], {"SLACK_BOT_TOKEN"}, source_env={"SLACK_BOT_TOKEN": "xoxb-1"}
        )
        assert extra == {}
        assert warns == []

    def test_hard_floor_blocked_even_when_allowlisted_and_loud(self):
        from tools.environments.local import build_cron_secret_injection

        # Operator (mis)configures GH_TOKEN into the allowlist and the job declares
        # it — the hard floor must still refuse it.
        extra, warns = build_cron_secret_injection(
            ["GH_TOKEN"], {"GH_TOKEN"}, source_env={"GH_TOKEN": "ghp-secret"}
        )
        assert extra == {}
        assert len(warns) == 1
        assert "hard floor" in warns[0]

    def test_no_value_in_env_skipped_and_loud(self):
        from tools.environments.local import build_cron_secret_injection

        extra, warns = build_cron_secret_injection(
            ["SLACK_BOT_TOKEN"], {"SLACK_BOT_TOKEN"}, source_env={}
        )
        assert extra == {}
        assert len(warns) == 1
        assert "no value" in warns[0]

    def test_empty_declaration_returns_empty(self):
        from tools.environments.local import build_cron_secret_injection

        assert build_cron_secret_injection(None, {"SLACK_BOT_TOKEN"}) == ({}, [])
        assert build_cron_secret_injection([], {"SLACK_BOT_TOKEN"}) == ({}, [])


class TestHardFloor:
    def test_slack_bot_token_is_eligible(self):
        from tools.environments.local import cron_never_injectable_keys

        assert "SLACK_BOT_TOKEN" not in cron_never_injectable_keys()

    def test_high_value_secrets_are_on_the_floor(self):
        from tools.environments.local import cron_never_injectable_keys

        floor = cron_never_injectable_keys()
        for name in (
            "GH_TOKEN",
            "GITHUB_TOKEN",
            "SLACK_APP_TOKEN",
            "SLACK_SIGNING_SECRET",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "HERMES_DASHBOARD_SESSION_TOKEN",
        ):
            assert name in floor, f"{name} must be on the never-injectable hard floor"

    def test_floor_reuses_always_strip_keys(self):
        from tools.environments.local import (
            cron_never_injectable_keys,
            _ALWAYS_STRIP_KEYS,
            _CRON_INJECTABLE_EXCEPTIONS,
        )

        floor = cron_never_injectable_keys()
        # Every always-strip key except the blessed exceptions is on the floor.
        assert (_ALWAYS_STRIP_KEYS - _CRON_INJECTABLE_EXCEPTIONS) <= floor


# ---------------------------------------------------------------------------
# _cron_injectable_secrets_allowlist: the operator config reader
# ---------------------------------------------------------------------------


class TestAllowlistConfigReader:
    def test_reads_list_from_config(self, monkeypatch):
        import cron.scheduler as sched

        monkeypatch.setattr(
            sched, "load_config", lambda: {"cron": {"injectable_secrets": ["SLACK_BOT_TOKEN", " X "]}}
        )
        assert sched._cron_injectable_secrets_allowlist() == frozenset({"SLACK_BOT_TOKEN", "X"})

    def test_string_scalar_is_coerced(self, monkeypatch):
        import cron.scheduler as sched

        monkeypatch.setattr(
            sched, "load_config", lambda: {"cron": {"injectable_secrets": "SLACK_BOT_TOKEN"}}
        )
        assert sched._cron_injectable_secrets_allowlist() == frozenset({"SLACK_BOT_TOKEN"})

    def test_absent_config_is_empty(self, monkeypatch):
        import cron.scheduler as sched

        monkeypatch.setattr(sched, "load_config", lambda: {})
        assert sched._cron_injectable_secrets_allowlist() == frozenset()

    def test_fails_closed_on_error(self, monkeypatch):
        import cron.scheduler as sched

        def _boom():
            raise RuntimeError("config broken")

        monkeypatch.setattr(sched, "load_config", _boom)
        assert sched._cron_injectable_secrets_allowlist() == frozenset()


# ---------------------------------------------------------------------------
# _run_job_script: end-to-end injection at the real script-env build
# ---------------------------------------------------------------------------


class TestRunJobScriptInjection:
    def test_allowlisted_and_declared_is_injected(self, cron_env, monkeypatch):
        import cron.scheduler as sched

        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-live")
        monkeypatch.setattr(
            sched, "_cron_injectable_secrets_allowlist", lambda: frozenset({"SLACK_BOT_TOKEN"})
        )
        _write_probe(cron_env / "scripts", "SLACK_BOT_TOKEN")

        ok, output = sched._run_job_script("probe.py", secrets=["SLACK_BOT_TOKEN"])
        assert ok is True
        assert output == "PRESENT"

    def test_undeclared_secret_still_stripped(self, cron_env, monkeypatch):
        """Baseline: with no declaration the strip-all posture is unchanged."""
        import cron.scheduler as sched

        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-live")
        monkeypatch.setattr(
            sched, "_cron_injectable_secrets_allowlist", lambda: frozenset({"SLACK_BOT_TOKEN"})
        )
        _write_probe(cron_env / "scripts", "SLACK_BOT_TOKEN")

        ok, output = sched._run_job_script("probe.py")  # no secrets declared
        assert ok is True
        assert output == "ABSENT"

    def test_declared_not_allowlisted_stripped_and_warns(self, cron_env, monkeypatch, caplog):
        import logging
        import cron.scheduler as sched

        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-live")
        monkeypatch.setattr(sched, "_cron_injectable_secrets_allowlist", lambda: frozenset())
        _write_probe(cron_env / "scripts", "SLACK_BOT_TOKEN")

        with caplog.at_level(logging.WARNING, logger="cron.scheduler"):
            ok, output = sched._run_job_script("probe.py", secrets=["SLACK_BOT_TOKEN"])
        assert ok is True
        assert output == "ABSENT"
        assert "not in the operator" in caplog.text
        assert "SLACK_BOT_TOKEN" in caplog.text

    def test_allowlisted_not_declared_stripped(self, cron_env, monkeypatch):
        import cron.scheduler as sched

        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-live")
        monkeypatch.setattr(
            sched, "_cron_injectable_secrets_allowlist", lambda: frozenset({"SLACK_BOT_TOKEN"})
        )
        _write_probe(cron_env / "scripts", "SLACK_BOT_TOKEN")

        # Job declares a DIFFERENT (custom) secret, not SLACK_BOT_TOKEN.
        ok, output = sched._run_job_script("probe.py", secrets=["SOME_OTHER_NAME"])
        assert ok is True
        assert output == "ABSENT"

    def test_hard_floor_declared_and_allowlisted_still_stripped(self, cron_env, monkeypatch, caplog):
        import logging
        import cron.scheduler as sched

        monkeypatch.setenv("GH_TOKEN", "ghp-live")
        # Even a misconfigured operator allowlist cannot grant a hard-floor name.
        monkeypatch.setattr(
            sched, "_cron_injectable_secrets_allowlist", lambda: frozenset({"GH_TOKEN"})
        )
        _write_probe(cron_env / "scripts", "GH_TOKEN")

        with caplog.at_level(logging.WARNING, logger="cron.scheduler"):
            ok, output = sched._run_job_script("probe.py", secrets=["GH_TOKEN"])
        assert ok is True
        assert output == "ABSENT"
        assert "hard floor" in caplog.text

    def test_custom_secret_injected_end_to_end(self, cron_env, monkeypatch):
        """A non-blocklisted custom secret still flows through the two gates."""
        import cron.scheduler as sched

        monkeypatch.setenv("MY_SERVICE_TOKEN", "s3cr3t")
        monkeypatch.setattr(
            sched, "_cron_injectable_secrets_allowlist", lambda: frozenset({"MY_SERVICE_TOKEN"})
        )
        _write_probe(cron_env / "scripts", "MY_SERVICE_TOKEN")

        ok, output = sched._run_job_script("probe.py", secrets=["MY_SERVICE_TOKEN"])
        assert ok is True
        assert output == "PRESENT"
