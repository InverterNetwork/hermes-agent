"""Behavior of the rendered ops/quay-as-hermes wrapper.

The wrapper has two invocation paths:

* operator path: caller is root or fabian-on-host → drop to AGENT_USER
  via `sudo -u`.
* agent path: caller already runs as AGENT_USER (e.g. hermes-gateway
  shelling out to quay from the agent context). The agent user is
  intentionally not in sudoers, so `sudo -u self` would reject — the
  wrapper must skip sudo and exec the preamble + quay directly.

Both branches must apply the same preamble (cd /, load quay.env and
quay-worker-env, mint $GH_TOKEN). These tests render the wrapper the same way
setup-hermes.sh does (sed-substitute __AGENT_USER__ + __TARGET_DIR__),
stub sudo + quay so we can assert which branch was taken without
actually privilege-escalating, and stub gh/token-helper binaries so token
validation and fallback minting stay local to the test.
"""

from __future__ import annotations

import getpass
import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
WRAPPER_SRC = REPO_ROOT / "ops" / "quay-as-hermes"


def _render_wrapper(dst: Path, agent_user: str, target_dir: Path, quay_bin: Path) -> None:
    """Materialize the wrapper exactly the way setup-hermes.sh does, plus
    two extra substitutions so quay and the worker env file point at tmp
    fixtures instead of host paths."""
    worker_env = target_dir / "auth" / "quay-worker.env"
    body = WRAPPER_SRC.read_text()
    body = body.replace("__AGENT_USER__", agent_user)
    body = body.replace("__TARGET_DIR__", str(target_dir))
    body = body.replace("/usr/local/bin/quay", str(quay_bin))
    body = body.replace("/etc/default/quay-worker-env", str(worker_env))
    dst.write_text(body)
    dst.chmod(0o755)


@pytest.fixture
def wrapper_env(tmp_path: Path) -> dict:
    target = tmp_path / "target"
    (target / "auth").mkdir(parents=True)
    (target / "quay").mkdir()

    (target / "auth" / "quay.env").write_text(
        "GH_TOKEN=stub-from-envfile\n"
        "LINEAR_API_KEY=lin-stub\n"
    )
    (target / "auth" / "quay-worker.env").write_text(
        "# host-managed worker env\n"
        "RPC_URL_4326=https://rpc.example\n"
        "1BAD=ignored\n"
        "lowercase=ignored\n",
        encoding="utf-8",
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    timeout = bin_dir / "timeout"
    timeout.write_text("#!/usr/bin/env bash\nshift\nexec \"$@\"\n")
    timeout.chmod(0o755)

    gh_log = tmp_path / "gh.log"
    gh = bin_dir / "gh"
    gh.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "ARGS: %s GH_TOKEN: %s GITHUB_TOKEN: %s\\n" "$*" "${{GH_TOKEN:-}}" "${{GITHUB_TOKEN:-}}" >> {gh_log}\n'
        'exit_code=0\n'
        'case "$*" in\n'
        '  "repo view "*) exit_code="${GH_REPO_VIEW_EXIT:-${GH_VALIDATE_EXIT:-0}}" ;;\n'
        '  "pr list "*) exit_code="${GH_PR_LIST_EXIT:-${GH_VALIDATE_EXIT:-0}}" ;;\n'
        'esac\n'
        'if [[ -n "${GH_FAIL_TOKEN:-}" && "${GH_TOKEN:-}" != "$GH_FAIL_TOKEN" ]]; then\n'
        '  exit_code=0\n'
        'fi\n'
        'exit "$exit_code"\n'
    )
    gh.chmod(0o755)

    py = target / "hermes-agent" / "venv" / "bin" / "python"
    py.parent.mkdir(parents=True)
    py.write_text(
        "#!/usr/bin/env bash\n"
        'helper="$1"\n'
        "shift\n"
        'exec "$helper" "$@"\n'
    )
    py.chmod(0o755)

    helper_calls = tmp_path / "helper.calls"
    helper = target / "hermes-agent" / "installer" / "hermes_github_token.py"
    helper.parent.mkdir(parents=True)
    helper.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" >> {helper_calls}\n'
        'if [[ "${1:-}" != "mint" ]]; then exit 2; fi\n'
        'printf "minted-worker-token\\n"\n'
    )
    helper.chmod(0o755)

    # Records argv + a few env vars and cwd to a log file the test reads.
    quay_log = tmp_path / "quay.log"
    quay_bin = bin_dir / "quay"
    quay_bin.write_text(
        "#!/usr/bin/env bash\n"
        f'{{\n'
        f'  printf "ARGS: %s\\n" "$*"\n'
        f'  printf "PWD: %s\\n" "$PWD"\n'
        f'  printf "QUAY_DATA_DIR: %s\\n" "${{QUAY_DATA_DIR:-}}"\n'
        f'  printf "HERMES_HOME: %s\\n" "${{HERMES_HOME:-}}"\n'
        f'  printf "QUAY_WORKER_GH_TOKEN: %s\\n" "${{QUAY_WORKER_GH_TOKEN:-}}"\n'
        f'  printf "GH_TOKEN: %s\\n" "${{GH_TOKEN:-}}"\n'
        f'  printf "LINEAR_API_KEY: %s\\n" "${{LINEAR_API_KEY:-}}"\n'
        f'  printf "RPC_URL_4326: %s\\n" "${{RPC_URL_4326:-}}"\n'
        f'}} >> {quay_log}\n'
        "exit 0\n"
    )
    quay_bin.chmod(0o755)

    # Stub sudo: records that sudo was invoked, then exec's the rest of
    # argv so the preamble + quay still run (without privilege change).
    # Real sudo's flag layout is `sudo [-u USER] env K=V… sh -c BODY -- ARGS`.
    # We just need to skip past `-u USER` and forward the rest.
    sudo_log = tmp_path / "sudo.log"
    sudo_stub = bin_dir / "sudo"
    sudo_stub.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "SUDO_USED: %s\\n" "$*" >> {sudo_log}\n'
        'if [[ "$1" == "-u" ]]; then shift 2; fi\n'
        'exec "$@"\n'
    )
    sudo_stub.chmod(0o755)

    return {
        "tmp": tmp_path,
        "target": target,
        "bin": bin_dir,
        "quay_bin": quay_bin,
        "quay_log": quay_log,
        "sudo_log": sudo_log,
        "gh_log": gh_log,
        "helper_calls": helper_calls,
    }


def _run_wrapper(
    wrapper: Path,
    env: dict,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    proc_env = os.environ.copy()
    proc_env["PATH"] = f"{env['bin']}{os.pathsep}{proc_env['PATH']}"
    # Wipe any GH_TOKEN already in the parent env so the wrapper's
    # env-file load is the only source; otherwise the assertion that
    # quay.env reached quay becomes ambiguous.
    proc_env.pop("GH_TOKEN", None)
    proc_env.pop("GITHUB_TOKEN", None)
    if extra_env:
        proc_env.update(extra_env)
    proc_env.setdefault(
        "HERMES_QUAY_GITHUB_AUTH_REPO",
        "InverterNetwork/hermes-agent",
    )
    return subprocess.run(
        [str(wrapper), "repo", "list"],
        env=proc_env, check=False, capture_output=True, text=True,
    )


def _parse_quay_log(log_path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in log_path.read_text().splitlines():
        if ": " in line:
            k, v = line.split(": ", 1)
            out[k] = v
    return out


class TestQuayAsHermesWrapper:
    def test_agent_path_skips_sudo_when_caller_matches_agent_user(self, wrapper_env):
        """hermes-gateway runs as the agent user and shells out to
        quay-as-hermes. The agent is not in sudoers, so a sudo
        invocation rejects with `user NOT in sudoers` and the whole
        agent-driven enqueue flow halts. The wrapper must detect this
        and skip sudo."""
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(wrapper, wrapper_env)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert not wrapper_env["sudo_log"].exists(), (
            "wrapper invoked sudo despite caller already being the agent user:\n"
            + wrapper_env["sudo_log"].read_text()
        )
        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["ARGS"] == "repo list"
        assert log["PWD"] == "/"
        assert log["QUAY_DATA_DIR"] == f"{wrapper_env['target']}/quay"
        assert log["HERMES_HOME"] == str(wrapper_env["target"])
        # Confirms the preamble ran in the agent branch too, not just
        # the sudo branch: env file got parsed and exported.
        assert log["QUAY_WORKER_GH_TOKEN"] == "stub-from-envfile"
        assert log["GH_TOKEN"] == "stub-from-envfile"
        assert log["LINEAR_API_KEY"] == "lin-stub"
        assert log["RPC_URL_4326"] == "https://rpc.example"

    def test_agent_path_preserves_quay_data_dir_override(self, wrapper_env):
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        override = wrapper_env["tmp"] / "profile-quay"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(
            wrapper,
            wrapper_env,
            extra_env={"QUAY_DATA_DIR": str(override)},
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert not wrapper_env["sudo_log"].exists()
        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["QUAY_DATA_DIR"] == str(override)
        assert log["HERMES_HOME"] == str(wrapper_env["target"])

    def test_operator_path_drops_privileges_via_sudo(self, wrapper_env):
        """Counterpart to the agent-path test: when caller != AGENT_USER
        (operator on host, root via ssh), the wrapper must still go
        through `sudo -u AGENT_USER`. We pin AGENT_USER to a name the
        test runner is not, so the sudo branch is forced."""
        other_user = "not-" + getpass.getuser()
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=other_user,
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(wrapper, wrapper_env)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert wrapper_env["sudo_log"].exists(), (
            "wrapper skipped sudo when caller != AGENT_USER; operator path "
            "must still drop privileges:\n" + result.stderr
        )
        sudo_call = wrapper_env["sudo_log"].read_text()
        assert f"-u {other_user}" in sudo_call, sudo_call
        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["ARGS"] == "repo list"
        assert log["PWD"] == "/"
        assert log["QUAY_DATA_DIR"] == f"{wrapper_env['target']}/quay"
        assert log["HERMES_HOME"] == str(wrapper_env["target"])
        assert log["QUAY_WORKER_GH_TOKEN"] == "stub-from-envfile"
        assert log["GH_TOKEN"] == "stub-from-envfile"
        assert log["RPC_URL_4326"] == "https://rpc.example"

    def test_operator_path_preserves_quay_data_dir_override(self, wrapper_env):
        other_user = "not-" + getpass.getuser()
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        override = wrapper_env["tmp"] / "profile-quay"
        _render_wrapper(
            wrapper,
            agent_user=other_user,
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(
            wrapper,
            wrapper_env,
            extra_env={"QUAY_DATA_DIR": str(override)},
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert wrapper_env["sudo_log"].exists()
        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["QUAY_DATA_DIR"] == str(override)
        assert log["HERMES_HOME"] == str(wrapper_env["target"])

    def test_pr_unauthorized_existing_token_mints_replacement(self, wrapper_env):
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(
            wrapper,
            wrapper_env,
            extra_env={
                "GH_FAIL_TOKEN": "stub-from-envfile",
                "GH_PR_LIST_EXIT": "1",
            },
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["QUAY_WORKER_GH_TOKEN"] == "minted-worker-token"
        assert log["GH_TOKEN"] == "minted-worker-token"
        assert "existing GitHub token is invalid; minting replacement" in result.stderr
        assert wrapper_env["helper_calls"].read_text(encoding="utf-8").splitlines() == ["mint"]

    def test_invalid_minted_token_is_not_exported_to_quay(self, wrapper_env):
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(
            wrapper,
            wrapper_env,
            extra_env={
                "GH_PR_LIST_EXIT": "1",
            },
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        log = _parse_quay_log(wrapper_env["quay_log"])
        assert log["QUAY_WORKER_GH_TOKEN"] == ""
        assert log["GH_TOKEN"] == ""
        assert "minted GitHub token is invalid for Quay repos" in result.stderr
        assert wrapper_env["helper_calls"].read_text(encoding="utf-8").splitlines() == ["mint"]

    def test_valid_existing_token_uses_repo_pr_scoped_probe(
        self,
        wrapper_env,
    ):
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(wrapper, wrapper_env)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert not wrapper_env["helper_calls"].exists()
        gh_log = wrapper_env["gh_log"].read_text(encoding="utf-8")
        assert (
            "ARGS: repo view InverterNetwork/hermes-agent --json viewerPermission "
            "GH_TOKEN: stub-from-envfile"
        ) in gh_log
        assert (
            "ARGS: pr list -R InverterNetwork/hermes-agent --limit 1 "
            "GH_TOKEN: stub-from-envfile"
        ) in gh_log
        assert "ARGS: api rate_limit" not in gh_log

    def test_existing_token_uses_installed_repo_origin_fallback(self, wrapper_env):
        installed_repo = wrapper_env["target"] / "hermes-agent"
        subprocess.run(
            ["git", "init", str(installed_repo)],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(installed_repo),
                "remote",
                "add",
                "origin",
                "https://x-access-token:secret@github.com/InverterNetwork/hermes-agent.git",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        wrapper = wrapper_env["tmp"] / "quay-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            quay_bin=wrapper_env["quay_bin"],
        )

        result = _run_wrapper(
            wrapper,
            wrapper_env,
            extra_env={"HERMES_QUAY_GITHUB_AUTH_REPO": ""},
        )
        assert result.returncode == 0, result.stderr + "\n" + result.stdout

        assert not wrapper_env["helper_calls"].exists()
        gh_log = wrapper_env["gh_log"].read_text(encoding="utf-8")
        assert (
            "ARGS: repo view InverterNetwork/hermes-agent --json viewerPermission "
            "GH_TOKEN: stub-from-envfile"
        ) in gh_log
