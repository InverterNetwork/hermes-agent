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

    gh_log = tmp_path / "gh.calls"
    gh = bin_dir / "gh"
    _write_executable(
        gh,
        "#!/usr/bin/env bash\n"
        f'printf "args=%s GH_TOKEN=%s GITHUB_TOKEN=%s\\n" "$*" "${{GH_TOKEN:-}}" "${{GITHUB_TOKEN:-}}" >> {gh_log}\n'
        'exit_code=0\n'
        'stderr=""\n'
        'case "$*" in\n'
        '  "repo view "*)\n'
        '    exit_code="${GH_REPO_VIEW_EXIT:-${GH_VALIDATE_EXIT:-0}}"\n'
        '    stderr="${GH_REPO_VIEW_STDERR:-${GH_VALIDATE_STDERR:-gh: Bad credentials}}"\n'
        '    ;;\n'
        '  "pr list "*)\n'
        '    exit_code="${GH_PR_LIST_EXIT:-${GH_VALIDATE_EXIT:-0}}"\n'
        '    stderr="${GH_PR_LIST_STDERR:-${GH_VALIDATE_STDERR:-gh: Bad credentials}}"\n'
        '    ;;\n'
        'esac\n'
        'if [[ -n "${GH_FAIL_TOKEN:-}" && "${GH_TOKEN:-}" != "$GH_FAIL_TOKEN" ]]; then\n'
        '  exit_code=0\n'
        'fi\n'
        'if [[ -n "${GH_FAIL_REPO:-}" && "$*" != *"$GH_FAIL_REPO"* ]]; then\n'
        '  exit_code=0\n'
        'fi\n'
        'if [[ "$exit_code" != "0" ]]; then\n'
        '  printf "%s\\n" "$stderr" >&2\n'
        "fi\n"
        'exit "$exit_code"\n',
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
        f'printf "QUAY_WORKER_GH_TOKEN=%s\\n" '
        f'"${{QUAY_WORKER_GH_TOKEN:-}}" >> {quay_log}\n'
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
            "GH_LOG": str(gh_log),
            "HERMES_QUAY_GITHUB_AUTH_REPO": "InverterNetwork/hermes-agent",
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
    assert "QUAY_WORKER_GH_TOKEN=worker-token" in log
    assert "GH_TOKEN=worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=reviewer-token" in log


def test_reviewer_app_repo_coverage_gap_fails_before_quay(tmp_path: Path):
    reviewer_env = tmp_path / "reviewer.env"
    reviewer_env.write_text("HERMES_GH_APP_ID=reviewer\n", encoding="utf-8")
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "HERMES_QUAY_GITHUB_AUTH_REPOS": (
                "InverterNetwork/hermes-agent,InverterNetwork/hermes-state"
            ),
            "GH_FAIL_TOKEN": "reviewer-token",
            "GH_FAIL_REPO": "InverterNetwork/hermes-state",
            "GH_PR_LIST_EXIT": "1",
            "GH_PR_LIST_STDERR": "HTTP 404: Not Found",
        }
    )

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 75
    assert not Path(env["QUAY_LOG"]).exists()
    assert (
        "GitHub App coverage gap: reviewer App cannot access "
        "InverterNetwork/hermes-state during pr list: HTTP 404: Not Found"
    ) in result.stderr
    assert (
        "minted reviewer GitHub token does not have required Quay repo/PR access"
        in result.stderr
    )


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
    assert "QUAY_WORKER_GH_TOKEN=caller-worker-token" in log
    assert "GH_TOKEN=caller-worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=reviewer-token" in log

    calls = Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines()
    assert calls == [f"config={reviewer_env} app_id= override="]


def test_valid_existing_worker_token_uses_repo_pr_scoped_probe(
    tmp_path: Path,
):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env["GH_TOKEN"] = "caller-worker-token"

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    log = Path(env["QUAY_LOG"]).read_text(encoding="utf-8")
    assert "QUAY_WORKER_GH_TOKEN=caller-worker-token" in log
    assert "GH_TOKEN=caller-worker-token" in log
    assert not Path(env["HELPER_CALLS"]).exists()
    gh_log = Path(env["GH_LOG"]).read_text(encoding="utf-8")
    assert (
        "args=repo view InverterNetwork/hermes-agent --json viewerPermission "
        "GH_TOKEN=caller-worker-token"
    ) in gh_log
    assert (
        "args=pr list -R InverterNetwork/hermes-agent --limit 1 "
        "GH_TOKEN=caller-worker-token"
    ) in gh_log
    assert "args=api rate_limit" not in gh_log


def test_pr_unauthorized_existing_worker_token_mints_replacement(tmp_path: Path):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "GH_TOKEN": "stale-worker-token",
            "GH_FAIL_TOKEN": "stale-worker-token",
            "GH_PR_LIST_EXIT": "1",
            "GH_PR_LIST_STDERR": "HTTP 401: Bad credentials",
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
    assert "QUAY_WORKER_GH_TOKEN=worker-token" in log
    assert "GH_TOKEN=worker-token" in log
    assert "inherited GitHub token is stale or unauthorized; minting replacement" in result.stderr
    calls = Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["config= app_id= override="]
    gh_log = Path(env["GH_LOG"]).read_text(encoding="utf-8")
    assert "args=api rate_limit" not in gh_log


def test_worker_auth_fails_without_token_helper(tmp_path: Path):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env["HERMES_TOKEN_HELPER"] = str(tmp_path / "missing-helper.py")

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 64
    assert "missing GitHub worker token" in result.stderr
    assert "token helper unavailable" in result.stderr
    assert not Path(env["QUAY_LOG"]).exists()


def test_invalid_minted_worker_token_fails_before_quay(tmp_path: Path):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "GH_FAIL_TOKEN": "worker-token",
            "GH_PR_LIST_EXIT": "1",
            "GH_PR_LIST_STDERR": "HTTP 403: Resource not accessible by integration",
        }
    )

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 75
    assert "minted GitHub worker token does not have required Quay repo/PR access" in result.stderr
    assert Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines() == [
        "config= app_id= override="
    ]
    assert not Path(env["QUAY_LOG"]).exists()


def test_inherited_token_validation_outage_fails_without_minting(tmp_path: Path):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "GH_TOKEN": "caller-worker-token",
            "GH_REPO_VIEW_EXIT": "2",
            "GH_REPO_VIEW_STDERR": "dial tcp: i/o timeout",
        }
    )

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 75
    assert "cannot validate inherited GitHub token" in result.stderr
    assert "possibly stale GH_TOKEN/GITHUB_TOKEN" in result.stderr
    assert not Path(env["HELPER_CALLS"]).exists()
    assert not Path(env["QUAY_LOG"]).exists()


def test_inherited_token_rate_limit_fails_without_stale_token_replacement(
    tmp_path: Path,
):
    reviewer_env = tmp_path / "missing-reviewer.env"
    env = _runner_env(tmp_path, reviewer_env)
    env.update(
        {
            "GH_TOKEN": "caller-worker-token",
            "GH_PR_LIST_EXIT": "1",
            "GH_PR_LIST_STDERR": "HTTP 403: API rate limit exceeded",
        }
    )

    result = subprocess.run(
        ["bash", str(RUNNER)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 75
    assert "GitHub auth validation hit rate limiting" in result.stderr
    assert "stale or unauthorized" not in result.stderr
    assert not Path(env["HELPER_CALLS"]).exists()
    assert not Path(env["QUAY_LOG"]).exists()


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
    assert "QUAY_WORKER_GH_TOKEN=worker-token" in log
    assert "GH_TOKEN=worker-token" in log
    assert "QUAY_REVIEWER_GH_TOKEN=" in log
    calls = Path(env["HELPER_CALLS"]).read_text(encoding="utf-8").splitlines()
    assert calls == ["config= app_id= override="]


def test_quay_orchestrator_runner_prepares_worker_github_auth(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_executable(bin_dir / "timeout", "#!/usr/bin/env bash\nshift\nexec \"$@\"\n")
    _write_executable(
        bin_dir / "gh",
        "#!/usr/bin/env bash\n"
        'printf "gh-token=%s\\n" "${GH_TOKEN:-}" >> "$GH_LOG"\n'
        'if [[ -n "${GH_FAIL_TOKEN:-}" && "${GH_TOKEN:-}" != "$GH_FAIL_TOKEN" ]]; then\n'
        "  exit 0\n"
        "fi\n"
        'if [[ "${GH_VALIDATE_EXIT:-0}" != "0" ]]; then\n'
        '  printf "%s\\n" "${GH_VALIDATE_STDERR:-gh: Bad credentials}" >&2\n'
        "fi\n"
        'exit "${GH_VALIDATE_EXIT:-0}"\n',
    )

    helper = tmp_path / "hermes_github_token.py"
    _write_executable(
        helper,
        "#!/usr/bin/env bash\n"
        'printf "helper-called\\n" >> "$HELPER_CALLS"\n'
        'printf "worker-token\\n"\n',
    )
    token_py = tmp_path / "token-python"
    _write_executable(
        token_py,
        "#!/usr/bin/env bash\n"
        'helper="$1"\n'
        "shift\n"
        'exec "$helper" "$@"\n',
    )
    py = tmp_path / "python"
    _write_executable(
        py,
        "#!/usr/bin/env bash\n"
        'printf "args=%s\\n" "$*" > "$ORCH_LOG"\n'
        'printf "GH_TOKEN=%s\\n" "${GH_TOKEN:-}" >> "$ORCH_LOG"\n',
    )
    script = tmp_path / "quay_orchestrator.py"
    script.write_text("# stub\n", encoding="utf-8")

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{env.get('PATH', '')}",
            "HERMES_HOME": str(tmp_path / "home"),
            "HERMES_TOKEN_HELPER": str(helper),
            "HERMES_TOKEN_PYTHON": str(token_py),
            "QUAY_ORCHESTRATOR_PYTHON": str(py),
            "QUAY_ORCHESTRATOR_SCRIPT": str(script),
            "QUAY_ORCHESTRATOR_CONFIG": str(tmp_path / "orchestrator.json"),
            "HELPER_CALLS": str(tmp_path / "helper.calls"),
            "GH_LOG": str(tmp_path / "gh.calls"),
            "ORCH_LOG": str(tmp_path / "orch.calls"),
            "GH_TOKEN": "stale-worker-token",
            "GH_FAIL_TOKEN": "stale-worker-token",
            "GH_VALIDATE_EXIT": "1",
            "HERMES_QUAY_GITHUB_AUTH_REPO": "InverterNetwork/hermes-agent",
        }
    )

    result = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "quay-orchestrator-runner")],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "stale or unauthorized; minting replacement" in result.stderr
    assert "helper-called" in Path(env["HELPER_CALLS"]).read_text(encoding="utf-8")
    orch_log = Path(env["ORCH_LOG"]).read_text(encoding="utf-8")
    assert "drain-one --config" in orch_log
    assert "GH_TOKEN=worker-token" in orch_log
