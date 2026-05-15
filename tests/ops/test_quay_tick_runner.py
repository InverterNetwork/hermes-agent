from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "ops" / "quay-tick-runner"


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def _runner_env(tmp_path: Path, reviewer_env: Path) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    # macOS developer machines do not always have GNU timeout installed.
    _write_executable(
        bin_dir / "timeout",
        "#!/usr/bin/env bash\n"
        "shift\n"
        'exec "$@"\n',
    )

    py = tmp_path / "python"
    _write_executable(
        py,
        "#!/usr/bin/env bash\n"
        'helper="$1"\n'
        "shift\n"
        'exec "$helper" "$@"\n',
    )

    calls = tmp_path / "helper.calls"
    helper = tmp_path / "hermes_github_token.py"
    _write_executable(
        helper,
        "#!/usr/bin/env bash\n"
        'printf "config=%s app_id=%s override=%s\\n" '
        '"${HERMES_GH_CONFIG:-}" "${HERMES_GH_APP_ID:-}" '
        f'"${{HERMES_GH_TOKEN_OVERRIDE:-}}" >> {calls}\n'
        'if [[ "${1:-}" != "mint" ]]; then exit 2; fi\n'
        f'if [[ "${{HERMES_GH_CONFIG:-}}" == "{reviewer_env}" ]]; then\n'
        '  printf "reviewer-token\\n"\n'
        "else\n"
        '  printf "worker-token\\n"\n'
        "fi\n",
    )

    quay_log = tmp_path / "quay.env"
    quay = tmp_path / "quay"
    _write_executable(
        quay,
        "#!/usr/bin/env bash\n"
        f'printf "args=%s\\n" "$*" > {quay_log}\n'
        f'printf "GH_TOKEN=%s\\n" "${{GH_TOKEN:-}}" >> {quay_log}\n'
        f'printf "QUAY_REVIEWER_GH_TOKEN=%s\\n" '
        f'"${{QUAY_REVIEWER_GH_TOKEN:-}}" >> {quay_log}\n',
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{env.get('PATH', '')}",
            "HERMES_HOME": str(tmp_path / "home"),
            "HERMES_TOKEN_HELPER": str(helper),
            "HERMES_TOKEN_PYTHON": str(py),
            "HERMES_REVIEWER_GH_CONFIG": str(reviewer_env),
            "QUAY_BIN": str(quay),
            "HELPER_CALLS": str(calls),
            "QUAY_LOG": str(quay_log),
        }
    )
    env.pop("GH_TOKEN", None)
    env.pop("GITHUB_TOKEN", None)
    env.pop("QUAY_REVIEWER_GH_TOKEN", None)
    return env


def test_quay_tick_runner_exports_worker_and_reviewer_tokens(tmp_path: Path):
    reviewer_env = tmp_path / "reviewer.env"
    reviewer_env.write_text("HERMES_GH_APP_ID=reviewer\n", encoding="utf-8")
    env = _runner_env(tmp_path, reviewer_env)

    result = subprocess.run(
        ["bash", str(RUNNER), "--once"],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    log = Path(env["QUAY_LOG"]).read_text(encoding="utf-8")
    assert "args=tick --once" in log
    assert "GH_TOKEN=worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=reviewer-token" in log


def test_reviewer_mint_ignores_generic_worker_helper_env(tmp_path: Path):
    reviewer_env = tmp_path / "reviewer.env"
    reviewer_env.write_text("HERMES_GH_APP_ID=reviewer\n", encoding="utf-8")
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "GH_TOKEN": "caller-worker-token",
            "HERMES_GH_CONFIG": str(tmp_path / "worker.env"),
            "HERMES_GH_APP_ID": "worker-app-id",
            "HERMES_GH_TOKEN_OVERRIDE": "worker-override-token",
        }
    )

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    log = Path(env["QUAY_LOG"]).read_text(encoding="utf-8")
    assert "GH_TOKEN=caller-worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=reviewer-token" in log

    calls = Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines()
    assert calls == [f"config={reviewer_env} app_id= override="]


def test_quay_tick_runner_skips_reviewer_token_without_config(tmp_path: Path):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    log = Path(env["QUAY_LOG"]).read_text(encoding="utf-8")
    assert "GH_TOKEN=worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=" in log
    calls = Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["config= app_id= override="]
