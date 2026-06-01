"""Behavior of the rendered ops/atlas-as-hermes wrapper."""

from __future__ import annotations

import getpass
import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
WRAPPER_SRC = REPO_ROOT / "ops" / "atlas-as-hermes"


def _render_wrapper(
    dst: Path,
    *,
    agent_user: str,
    target_dir: Path,
    atlas_bin: Path,
    kb_root: Path,
) -> None:
    body = WRAPPER_SRC.read_text()
    body = body.replace("__AGENT_USER__", agent_user)
    body = body.replace("__TARGET_DIR__", str(target_dir))
    body = body.replace("__ATLAS_KB_ROOT__", str(kb_root))
    body = body.replace("__ATLAS_AI_MODE__", "codex-exec")
    body = body.replace("__ATLAS_CODEX_BIN__", "/usr/local/bin/codex")
    body = body.replace("__ATLAS_CODEX_TIMEOUT_MS__", "120000")
    body = body.replace("/usr/local/bin/atlas", str(atlas_bin))
    dst.write_text(body)
    dst.chmod(0o755)


@pytest.fixture
def wrapper_env(tmp_path: Path) -> dict:
    target = tmp_path / "target"
    kb_root = target / "atlas-kb"
    kb_root.mkdir(parents=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    atlas_log = tmp_path / "atlas.log"
    atlas_bin = bin_dir / "atlas"
    atlas_bin.write_text(
        "#!/usr/bin/env bash\n"
        f'{{\n'
        f'  printf "ARGS: %s\\n" "$*"\n'
        f'  printf "PWD: %s\\n" "$PWD"\n'
        f'  printf "HERMES_HOME: %s\\n" "${{HERMES_HOME:-}}"\n'
        f'  printf "ATLAS_KB_ROOT: %s\\n" "${{ATLAS_KB_ROOT:-}}"\n'
        f'  printf "ATLAS_AI_MODE: %s\\n" "${{ATLAS_AI_MODE:-}}"\n'
        f'  printf "ATLAS_CODEX_BIN: %s\\n" "${{ATLAS_CODEX_BIN:-}}"\n'
        f'  printf "ATLAS_CODEX_TIMEOUT_MS: %s\\n" "${{ATLAS_CODEX_TIMEOUT_MS:-}}"\n'
        f'  printf "ATLAS_SESSION_ID: %s\\n" "${{ATLAS_SESSION_ID:-}}"\n'
        f'}} >> {atlas_log}\n'
    )
    atlas_bin.chmod(0o755)

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
        "kb_root": kb_root,
        "bin": bin_dir,
        "atlas_bin": atlas_bin,
        "atlas_log": atlas_log,
        "sudo_log": sudo_log,
    }


def _run_wrapper(wrapper: Path, env: dict) -> subprocess.CompletedProcess:
    proc_env = os.environ.copy()
    proc_env["PATH"] = f"{env['bin']}{os.pathsep}{proc_env['PATH']}"
    return subprocess.run(
        [str(wrapper), "--version"],
        env=proc_env,
        check=False,
        capture_output=True,
        text=True,
    )


def _parse_log(log_path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in log_path.read_text().splitlines():
        if ": " in line:
            k, v = line.split(": ", 1)
            out[k] = v
    return out


class TestAtlasAsHermesWrapper:
    def test_agent_path_runs_from_kb_root_without_sudo(self, wrapper_env):
        wrapper = wrapper_env["tmp"] / "atlas-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=getpass.getuser(),
            target_dir=wrapper_env["target"],
            atlas_bin=wrapper_env["atlas_bin"],
            kb_root=wrapper_env["kb_root"],
        )

        result = _run_wrapper(wrapper, wrapper_env)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert not wrapper_env["sudo_log"].exists()

        log = _parse_log(wrapper_env["atlas_log"])
        assert log["ARGS"] == "--version"
        assert log["PWD"] == str(wrapper_env["kb_root"])
        assert log["HERMES_HOME"] == str(wrapper_env["target"])
        assert log["ATLAS_KB_ROOT"] == str(wrapper_env["kb_root"])
        assert log["ATLAS_AI_MODE"] == "codex-exec"
        assert log["ATLAS_CODEX_BIN"] == "/usr/local/bin/codex"
        assert log["ATLAS_CODEX_TIMEOUT_MS"] == "120000"
        assert log["ATLAS_SESSION_ID"] == "hermes-agent"

    def test_operator_path_drops_to_agent_user(self, wrapper_env):
        other_user = "not-" + getpass.getuser()
        wrapper = wrapper_env["tmp"] / "atlas-as-hermes"
        _render_wrapper(
            wrapper,
            agent_user=other_user,
            target_dir=wrapper_env["target"],
            atlas_bin=wrapper_env["atlas_bin"],
            kb_root=wrapper_env["kb_root"],
        )

        result = _run_wrapper(wrapper, wrapper_env)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert wrapper_env["sudo_log"].exists()
        assert f"-u {other_user}" in wrapper_env["sudo_log"].read_text()

        log = _parse_log(wrapper_env["atlas_log"])
        assert log["PWD"] == str(wrapper_env["kb_root"])
        assert log["ATLAS_KB_ROOT"] == str(wrapper_env["kb_root"])
