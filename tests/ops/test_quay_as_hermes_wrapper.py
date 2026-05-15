"""Behavior of the rendered ops/quay-as-hermes wrapper.

The wrapper has two invocation paths:

* operator path: caller is root or fabian-on-host → drop to AGENT_USER
  via `sudo -u`.
* agent path: caller already runs as AGENT_USER (e.g. hermes-gateway
  shelling out to quay from the agent context). The agent user is
  intentionally not in sudoers, so `sudo -u self` would reject — the
  wrapper must skip sudo and exec the preamble + quay directly.

Both branches must apply the same preamble (cd /, load quay.env, mint
$GH_TOKEN). These tests render the wrapper the same way
setup-hermes.sh does (sed-substitute __AGENT_USER__ + __TARGET_DIR__),
stub sudo + quay so we can assert which branch was taken without
actually privilege-escalating, and pre-seed GH_TOKEN in the env file so
we don't depend on `timeout` being installed on dev machines.
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
    one extra substitution so quay points at a tmp stub instead of
    /usr/local/bin/quay."""
    body = WRAPPER_SRC.read_text()
    body = body.replace("__AGENT_USER__", agent_user)
    body = body.replace("__TARGET_DIR__", str(target_dir))
    body = body.replace("/usr/local/bin/quay", str(quay_bin))
    dst.write_text(body)
    dst.chmod(0o755)


@pytest.fixture
def wrapper_env(tmp_path: Path) -> dict:
    target = tmp_path / "target"
    (target / "auth").mkdir(parents=True)
    (target / "quay").mkdir()

    # Pre-seed GH_TOKEN so the mint branch (which would invoke `timeout
    # <secs> python …`) is skipped: avoids a hard dep on coreutils on
    # macOS and isolates the test from the token helper.
    (target / "auth" / "quay.env").write_text(
        "GH_TOKEN=stub-from-envfile\n"
        "LINEAR_API_KEY=lin-stub\n"
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

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
        f'  printf "GH_TOKEN: %s\\n" "${{GH_TOKEN:-}}"\n'
        f'  printf "LINEAR_API_KEY: %s\\n" "${{LINEAR_API_KEY:-}}"\n'
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
        assert log["GH_TOKEN"] == "stub-from-envfile"
        assert log["LINEAR_API_KEY"] == "lin-stub"

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
        assert log["GH_TOKEN"] == "stub-from-envfile"

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
