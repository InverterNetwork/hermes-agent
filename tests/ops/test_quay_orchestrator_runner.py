from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "ops" / "quay-orchestrator-runner"


def test_orchestrator_runner_continues_without_github_auth_helper(tmp_path: Path):
    py = tmp_path / "python"
    log = tmp_path / "python.args"
    py.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" > {log}\n',
        encoding="utf-8",
    )
    py.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "HERMES_HOME": str(tmp_path / "home"),
            "QUAY_ORCHESTRATOR_PYTHON": str(py),
            "QUAY_ORCHESTRATOR_SCRIPT": str(tmp_path / "quay_orchestrator.py"),
            "HERMES_QUAY_GITHUB_AUTH": str(tmp_path / "missing-auth-helper"),
        }
    )
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    env.pop("QUAY_WORKER_GH_TOKEN", None)

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
