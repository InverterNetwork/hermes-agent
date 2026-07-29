from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "ops" / "quay-orchestrator-runner"


def _python_stub(tmp_path: Path) -> tuple[Path, Path]:
    py = tmp_path / "python"
    log = tmp_path / "python.args"
    py.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" > {log}\n',
        encoding="utf-8",
    )
    py.chmod(0o755)
    return py, log


def _base_env(tmp_path: Path, py: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "HERMES_HOME": str(tmp_path / "home"),
            "QUAY_ORCHESTRATOR_PYTHON": str(py),
            "QUAY_ORCHESTRATOR_SCRIPT": str(tmp_path / "quay_orchestrator.py"),
        }
    )
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    env.pop("QUAY_WORKER_GH_TOKEN", None)
    return env


def test_orchestrator_runner_warns_when_helper_file_missing(tmp_path: Path):
    # Copy the runner into a directory with NO co-located quay-github-auth so
    # the BASH_SOURCE fallback also misses it and the true helper-not-found
    # WARN branch is exercised (not the mint path).
    runner_copy = tmp_path / "quay-orchestrator-runner"
    shutil.copy2(RUNNER, runner_copy)

    py, log = _python_stub(tmp_path)
    env = _base_env(tmp_path, py)
    env["HERMES_QUAY_GITHUB_AUTH"] = str(tmp_path / "missing-auth-helper")

    result = subprocess.run(
        ["bash", str(runner_copy)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "GitHub auth helper not found" in result.stderr
    assert "continuing without worker GitHub token" in result.stderr
    # Slack-only delivery still drains even with no auth helper present.
    assert "drain-one --config" in log.read_text(encoding="utf-8")


def test_orchestrator_runner_continues_when_token_mint_unavailable(tmp_path: Path):
    # Helper IS found (via the co-located repo copy), but no token helper is
    # staged under HERMES_HOME, so the optional preflight mint fails and the
    # runner drains Slack-only work without a worker GitHub token.
    py, log = _python_stub(tmp_path)
    env = _base_env(tmp_path, py)
    env["HERMES_QUAY_GITHUB_AUTH"] = str(tmp_path / "missing-auth-helper")

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "missing GitHub worker token" in result.stderr
    assert "token helper unavailable" in result.stderr
    assert "continuing without worker GitHub token" in result.stderr
    assert "drain-one --config" in log.read_text(encoding="utf-8")
