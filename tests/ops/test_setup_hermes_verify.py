"""Behavior of `hermes_installer.verify` against a tmp install.

The verify path is a read-only health check, so the cheapest meaningful
coverage is to build a fixture that mirrors a clean install on disk and
assert each drift case yields a named [DRIFT] subject + non-zero exit.

These tests call ``hermes_installer.verify.run()`` directly. One smoke test
per class also exercises the ``setup-hermes.sh --verify`` wrapper to prove
it still forwards correctly.

Production verify expects rails owned by root and state owned by the agent
user. Tests can't actually chown to root, so the script honors two internal
env-var overrides (HERMES_VERIFY_EXPECT_RAILS_OWNER /
HERMES_VERIFY_EXPECT_AGENT_OWNER) — both set to $USER here so the fixture
exercises the real check logic with whatever user the test runner is.
"""

from __future__ import annotations

import getpass
import grp
import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "installer" / "setup-hermes.sh"
INSTALLER_DIR = REPO_ROOT / "installer"

# Make hermes_installer importable for the in-process call path. The bash
# wrapper sets PYTHONPATH itself; here we mutate sys.path once per session.
if str(INSTALLER_DIR) not in sys.path:
    sys.path.insert(0, str(INSTALLER_DIR))

from hermes_installer.verify import VerifyArgs, run as run_verify  # noqa: E402


def _minimal_quay_values(path: Path) -> None:
    path.write_text(
        "quay:\n"
        "  version: \"v0.3.37\"\n"
        "  adapters:\n"
        "    slack:\n"
        "      enabled: false\n",
        encoding="utf-8",
    )


def _run_values_helper(
    values: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(REPO_ROOT / "installer" / "values_helper.py"),
         "--values", str(values), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _run(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(args), cwd=cwd, check=check, capture_output=True, text=True,
    )


def _git(repo: Path, *args: str, **kw) -> subprocess.CompletedProcess:
    return _run(repo, "git", "-C", str(repo), *args, **kw)


def test_render_quay_config_without_runtime_settings_uses_quay_defaults(tmp_path: Path):
    values = tmp_path / "values.yaml"
    out = tmp_path / "quay" / "config.toml"
    code_root = tmp_path / "code"
    _minimal_quay_values(values)

    result = _run_values_helper(
        values,
        "render-quay-config",
        "--out", str(out),
        "--enable-admin-auth",
        "--reference-repos-root", str(code_root),
    )

    assert result.returncode == 0, result.stderr
    rendered = out.read_text(encoding="utf-8")
    assert "[admin]" in rendered
    assert "[context]" in rendered
    assert "agent_invocation" not in rendered
    assert "[agents]" not in rendered
    assert "[agents.invocations" not in rendered
    assert "[reviewer]" not in rendered


def test_render_quay_config_preserves_existing_file_without_force(tmp_path: Path):
    values = tmp_path / "values.yaml"
    out = tmp_path / "quay" / "config.toml"
    code_root = tmp_path / "code"
    _minimal_quay_values(values)
    out.parent.mkdir()
    existing = (
        'agent_invocation = "legacy < {prompt_file}"\n'
        "\n"
        "[agents]\n"
        'worker = "codex"\n'
        'reviewer = "codex"\n'
        'reviewer_model = "gpt-5.4"\n'
        "\n"
        "[agents.invocations.codex]\n"
        'worker = "codex exec --model gpt-5.4 < {prompt_file}"\n'
        'reviewer = "codex exec --model gpt-5.4 < {prompt_file}"\n'
        "\n"
        "[reviewer]\n"
        "enabled = true\n"
        'login = "app/didier-reviewer"\n'
    )
    out.write_text(
        existing,
        encoding="utf-8",
    )

    result = _run_values_helper(
        values,
        "render-quay-config",
        "--out", str(out),
        "--enable-admin-auth",
        "--reference-repos-root", str(code_root),
    )

    assert result.returncode == 0, result.stderr
    assert "preserved" in result.stdout
    assert out.read_text(encoding="utf-8") == existing


@pytest.fixture
def install(tmp_path: Path) -> dict:
    """Build a tmp dir that mirrors a clean install of hermes-agent + state."""
    user = getpass.getuser()

    target = tmp_path / "target"
    fork = tmp_path / "fork"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    py_shim = bin_dir / "python3"
    py_shim.write_text(f'#!/usr/bin/env bash\nexec {sys.executable} "$@"\n')
    py_shim.chmod(0o755)

    # ---- fork (the source git repo we rsynced from at install time) ----
    subprocess.run(["git", "init", "--quiet", "-b", "main", str(fork)], check=True)
    _git(fork, "config", "user.email", "f@h.l")
    _git(fork, "config", "user.name", "f")
    (fork / "README.md").write_text("seed\n")
    _git(fork, "add", "-A")
    _git(fork, "commit", "-q", "-m", "seed")
    _git(fork, "remote", "add", "origin", "https://github.com/example/hermes-agent.git")
    fork_sha = _git(fork, "describe", "--always", "--dirty", "--abbrev=40").stdout.strip()

    # ---- target (the rendered install) ----
    target.mkdir()
    # HERMES_HOME mirror: prod is root:hermes 02775 (setgid). The test runs
    # as the developer so ownership is $USER:$primary_group; mode still
    # carries the setgid bit so verify's mode check stays exercised.
    try:
        os.chown(target, -1, os.getegid())
    except PermissionError:
        pass
    target.chmod(0o2775)

    # rails: a few files mimicking the rsynced hermes-agent tree.
    rails = target / "hermes-agent"
    (rails / "installer").mkdir(parents=True)
    (rails / "venv" / "bin").mkdir(parents=True)
    (rails / "SOUL.md").write_text("seed\n")
    (rails / "SOUL.md").chmod(0o644)
    # Helper + venv python: the helper stub prints a fake token on `mint`
    # (verify's smoke-check + App-scope check both consume `mint` output).
    helper = rails / "installer" / "hermes_github_token.py"
    helper.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "if len(sys.argv) > 1 and sys.argv[1] == 'mint':\n"
        "    print('ghs_stub_token')\n"
        "    sys.exit(0)\n"
        "sys.exit(2)\n"
    )
    helper.chmod(0o755)
    venv_py = rails / "venv" / "bin" / "python"
    os.symlink(sys.executable, venv_py)

    # state: a real clone of fork so .git is valid and we can mutate config.
    state = target / "state"
    subprocess.run(["git", "clone", "--quiet", str(fork), str(state)], check=True)
    _git(state, "config", "user.email", "didier@test")
    _git(state, "config", "user.name", "didier")
    _git(state, "remote", "set-url", "origin", "https://github.com/example/hermes-state.git")

    # Agent dirs (gitignored siblings of state/).
    for d in ("sessions", "logs", "cache", "platforms", "platforms/pairing"):
        (target / d).mkdir()
    # Verify treats a missing code/ root as drift, so the base fixture
    # has to mirror what the installer always provisions.
    (target / "code").mkdir(mode=0o755)

    upstream_workspace = target / "upstream-workspace"
    subprocess.run(
        ["git", "clone", "--quiet", str(fork), str(upstream_workspace)],
        check=True,
    )
    _git(upstream_workspace, "remote", "set-url",
         "origin", "https://github.com/example/hermes-agent.git")
    _git(upstream_workspace, "remote", "add",
         "upstream", "https://github.com/nousresearch/hermes-agent.git")
    _git(upstream_workspace, "config", "user.email", "didier@test")
    _git(upstream_workspace, "config", "user.name", "didier")

    # Symlinks at render-target root pointing into state/ (matches the
    # installer's wiring).
    for d in ("skills", "memories", "cron", "scripts"):
        (state / d).mkdir(exist_ok=True)
        os.symlink(f"state/{d}", target / d)

    # RUNTIME_VERSION matches fork HEAD on a clean install.
    (target / "RUNTIME_VERSION").write_text(fork_sha)

    # The fixture's actual owner is $USER, which the verify-side override
    # treats as agent_owner — so a clean install passes.
    (target / "config.yaml").write_text("model: {}\n")
    (target / "config.yaml").chmod(0o644)

    # Rails-mode 0640; verify expects it present even when empty.
    (target / "gateway-org-defaults.md").write_text("")
    (target / "gateway-org-defaults.md").chmod(0o640)

    # Stub systemctl in PATH bin so verify thinks the timers are loaded.
    # Verify batches `systemctl show -p A -p B -p C <unit>` and parses
    # KEY=VALUE lines — same wire format as real systemctl without --value.
    systemctl_log = tmp_path / "systemctl.log"
    systemctl = bin_dir / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" >> {systemctl_log}\n'
        "if [[ \"$1\" == \"show\" ]]; then\n"
        "  shift\n"
        "  props=()\n"
        "  while [[ $# -gt 0 ]]; do\n"
        "    case \"$1\" in\n"
        "      -p) props+=(\"$2\"); shift 2 ;;\n"
        "      --value) shift ;;\n"
        "      *) shift ;;\n"
        "    esac\n"
        "  done\n"
        "  for prop in \"${props[@]}\"; do\n"
        "    case \"$prop\" in\n"
        "      ActiveState)   echo ActiveState=active ;;\n"
        "      LoadState)     echo LoadState=loaded ;;\n"
        "      UnitFileState) echo UnitFileState=enabled ;;\n"
        "    esac\n"
        "  done\n"
        "  exit 0\n"
        "fi\n"
        "exit 0\n"
    )
    systemctl.chmod(0o755)

    upstream_sync_script = bin_dir / "hermes-upstream-sync"
    upstream_sync_script.write_text("#!/usr/bin/env bash\nexit 0\n")
    upstream_sync_script.chmod(0o755)

    etc_default = tmp_path / "etc-default"
    etc_default.mkdir()
    upstream_sync_env = etc_default / "hermes-upstream-sync"
    upstream_sync_env.write_text(f"FORK_DIR={upstream_workspace}\n")
    upstream_sync_env.chmod(0o644)

    systemd_dir = tmp_path / "systemd"
    systemd_dir.mkdir()
    (systemd_dir / "hermes-upstream-sync.service").write_text(
        "[Unit]\n"
        "Description=Propose an upstream hermes-agent merge as a PR\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"User={user}\n"
        f"Group={user}\n"
        f"EnvironmentFile=-{upstream_sync_env}\n"
        f"ExecStart={upstream_sync_script}\n"
        "Restart=no\n"
        "TimeoutStartSec=300\n",
        encoding="utf-8",
    )
    (systemd_dir / "hermes-upstream-sync.service").chmod(0o644)
    (systemd_dir / "hermes-upstream-sync.timer").write_text(
        "[Unit]\n"
        "Description=Run hermes-upstream-sync weekly\n"
        "[Timer]\n"
        "OnCalendar=Mon *-*-* 09:00:00 UTC\n"
        "RandomizedDelaySec=15min\n"
        "Persistent=true\n"
        "Unit=hermes-upstream-sync.service\n"
        "[Install]\n"
        "WantedBy=timers.target\n",
        encoding="utf-8",
    )
    (systemd_dir / "hermes-upstream-sync.timer").chmod(0o644)

    # On Linux production, useradd creates a primary group named after the
    # user so $AGENT_USER works as both owner and group. In tests, use the
    # group that the setgid target actually carries so child files inherit
    # matching expectations on macOS temp directories too.
    primary_group = grp.getgrgid(target.stat().st_gid).gr_name

    return {
        "tmp": tmp_path,
        "target": target,
        "fork": fork,
        "state": state,
        "rails": rails,
        "fork_sha": fork_sha,
        "user": user,
        "group": primary_group,
        "bin": bin_dir,
        "systemctl_log": systemctl_log,
        "systemd_dir": systemd_dir,
        "upstream_workspace": upstream_workspace,
        "upstream_sync_script": upstream_sync_script,
        "upstream_sync_env": upstream_sync_env,
    }


# ---------------------------------------------------------------------------
# Helpers: scoped env + direct-call + bash-wrapper invokers
# ---------------------------------------------------------------------------


@contextmanager
def _scoped_env(updates: dict[str, str]) -> Iterator[None]:
    """Snapshot/restore os.environ for the duration of a verify run."""
    saved = os.environ.copy()
    try:
        os.environ.update(updates)
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _base_env(install: dict, env_overrides: dict[str, str] | None = None) -> dict[str, str]:
    env = {
        "PATH": f"{install['bin']}{os.pathsep}{os.environ.get('PATH', '')}",
        "HERMES_VERIFY_EXPECT_RAILS_OWNER": install["user"],
        "HERMES_VERIFY_EXPECT_AGENT_OWNER": install["user"],
        "HERMES_VERIFY_EXPECT_AGENT_GROUP": install["group"],
        "HERMES_VERIFY_SYSTEMD_DIR": str(install["systemd_dir"]),
        "HERMES_VERIFY_UPSTREAM_SYNC_SCRIPT": str(install["upstream_sync_script"]),
        "HERMES_VERIFY_UPSTREAM_SYNC_ENV": str(install["upstream_sync_env"]),
    }
    if env_overrides:
        env.update(env_overrides)
    return env


def _run_verify(
    install: dict,
    *,
    auth_method: str = "none",
    quiet: bool = False,
    values: Path | None = None,
    gh_api_base: str | None = None,
    env_overrides: dict[str, str] | None = None,
) -> SimpleNamespace:
    """Direct call into hermes_installer.verify.run() against the fixture.

    Returns a (returncode, stdout, stderr) bag with the same attribute names
    as subprocess.CompletedProcess so existing assertions stay readable.
    """
    out = io.StringIO()
    err = io.StringIO()
    with _scoped_env(_base_env(install, env_overrides)):
        rc = run_verify(
            VerifyArgs(
                fork=install["fork"],
                target=install["target"],
                user=install["user"],
                auth_method=auth_method,
                quiet=quiet,
                values=values,
                gh_api_base=gh_api_base,
            ),
            stdout=out, stderr=err,
        )
    return SimpleNamespace(returncode=rc, stdout=out.getvalue(), stderr=err.getvalue())


def _run_verify_via_wrapper(
    install: dict,
    *extra: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Smoke path — go through bash setup-hermes.sh --verify so we keep
    coverage of the wrapper's argv forwarding."""
    env = os.environ.copy()
    env.update(_base_env(install, env_overrides))
    return subprocess.run(
        [
            "bash", str(SCRIPT),
            "--verify",
            "--target", str(install["target"]),
            "--fork", str(install["fork"]),
            "--user", install["user"],
            *extra,
        ],
        env=env, check=False, capture_output=True, text=True,
    )


def _snapshot(root: Path) -> dict:
    """Capture (mode, mtime_ns, size) for every non-symlink file under root."""
    snap = {}
    for p in root.rglob("*"):
        if p.is_symlink():
            continue
        try:
            s = p.stat()
        except FileNotFoundError:
            continue
        snap[str(p.relative_to(root))] = (s.st_mode, s.st_mtime_ns, s.st_size)
    return snap


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSetupHermesVerify:
    def test_clean_install_passes(self, install):
        result = _run_verify(install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "==> verify:" in result.stdout
        assert "0 drift" in result.stdout
        assert "[DRIFT]" not in result.stderr

    def test_clean_install_passes_via_bash_wrapper(self, install):
        """End-to-end smoke through setup-hermes.sh --verify so we catch
        regressions in the bash-side argv forwarding (PYTHONPATH, flag
        translation, exec)."""
        result = _run_verify_via_wrapper(install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "==> verify:" in result.stdout
        assert "0 drift" in result.stdout
        assert "[DRIFT]" not in result.stderr

    def test_quiet_suppresses_ok_lines(self, install):
        result = _run_verify(install, quiet=True)
        assert result.returncode == 0, result.stderr
        assert "[OK]" not in result.stdout
        assert "verify:" in result.stdout  # closing summary still prints

    def test_verify_is_observably_read_only(self, install):
        before = _snapshot(install["target"])
        result = _run_verify(install)
        assert result.returncode == 0, result.stderr
        after = _snapshot(install["target"])
        assert before == after, "verify mutated files under target"

    def test_rails_world_writable_file_is_drift(self, install):
        (install["rails"] / "SOUL.md").chmod(0o666)
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] rails perms" in result.stderr

    def test_rails_root_group_writable_is_drift(self, install):
        """Bash `find $rails ...` includes $rails itself; the Python port
        must too. A group-writable rails root lets a writable principal
        replace entries under the root-owned tree even if every file
        underneath is mode 0644."""
        install["rails"].chmod(0o775)
        try:
            result = _run_verify(install)
        finally:
            # Restore so other tests in the same fixture aren't affected
            # (pytest tmp_path is per-test, but defensive).
            install["rails"].chmod(0o755)
        assert result.returncode == 1
        assert "[DRIFT] rails perms" in result.stderr

    def test_rails_symlinks_do_not_trip_perms_check(self, install):
        """Symlinks always have mode lrwxrwxrwx — naive `find -perm -g+w`
        flags them as writable. The check must skip symlinks because their
        effective writability comes from the target, not the link itself.

        Real-world repro: a clean install hit this on the venv's
        bin/python3 → python3.12 and lib64 → lib symlinks.
        """
        rails_link = install["rails"] / "venv" / "bin" / "python3-extra"
        rails_link.symlink_to("python")  # 0777 by default on creation
        result = _run_verify(install)
        assert result.returncode == 0, (
            "verify flagged a symlink as group/world-writable:\n" + result.stderr
        )

    def test_state_origin_filesystem_path_is_drift(self, install):
        _git(install["state"], "remote", "set-url", "origin", "/tmp/somewhere")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] state origin" in result.stderr
        assert "filesystem path" in result.stderr

    def test_state_user_email_unset_is_drift(self, install):
        _git(install["state"], "config", "--unset", "user.email")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] state git user.email" in result.stderr

    def test_runtime_version_skew_is_drift(self, install):
        # Advance fork past the recorded RUNTIME_VERSION.
        fork = install["fork"]
        (fork / "FEATURE.md").write_text("x\n")
        _git(fork, "add", "-A")
        _git(fork, "commit", "-q", "-m", "advance")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] RUNTIME_VERSION" in result.stderr

    def test_missing_symlink_is_drift(self, install):
        link = install["target"] / "skills"
        link.unlink()
        link.mkdir()  # replace symlink with a real dir → drift
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] symlink skills" in result.stderr

    def test_scripts_symlink_is_required(self, install):
        link = install["target"] / "scripts"
        link.unlink()
        link.mkdir()
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] symlink scripts" in result.stderr

    def test_state_scripts_dir_is_required(self, install):
        (install["target"] / "scripts").unlink()
        shutil.rmtree(install["state"] / "scripts")
        os.symlink("state/scripts", install["target"] / "scripts")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] state/scripts" in result.stderr

    def test_missing_sessions_dir_is_drift(self, install):
        shutil.rmtree(install["target"] / "sessions")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] sessions" in result.stderr

    def test_missing_config_yaml_is_drift(self, install):
        """config.yaml is seeded by the installer on first run; absence
        means a partial install and the gateway will crash on first start."""
        (install["target"] / "config.yaml").unlink()
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] config.yaml" in result.stderr
        assert "missing" in result.stderr

    def test_root_owned_config_yaml_is_drift(self, install):
        """Tests can't actually chown to root, so flip the verify-side
        agent-owner override to a synthetic user — the fixture file
        (owned by $USER) then mismatches and exercises the same code
        path as a root-owned production install.
        """
        result = _run_verify(
            install, env_overrides={"HERMES_VERIFY_EXPECT_AGENT_OWNER": "nobody"},
        )
        assert result.returncode == 1
        assert "[DRIFT] config.yaml" in result.stderr
        assert f"owner={install['user']}" in result.stderr
        assert "expected 644 nobody" in result.stderr

    def test_wrong_mode_config_yaml_is_drift(self, install):
        (install["target"] / "config.yaml").chmod(0o600)
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] config.yaml" in result.stderr
        assert "mode=600" in result.stderr

    def test_app_auth_clean_passes(self, install):
        """--auth-method app + every artefact in place → verify runs the
        helper smoke and credential-helper check, and both pass."""
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750); auth.chmod(0o750)  # clear setgid inherited from setgid parent on Linux
        (auth / "github-app.pem").write_text(
            "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n"
        )
        (auth / "github-app.pem").chmod(0o640)
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)

        helper_cmd = (
            f"!HERMES_HOME='{target}' "
            f"{install['rails'] / 'venv' / 'bin' / 'python'} "
            f"{install['rails'] / 'installer' / 'hermes_github_token.py'} credential"
        )
        _git(install["state"], "config",
             "credential.https://github.com.helper", helper_cmd)

        result = _run_verify(install, auth_method="app")
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "token helper check passes" in result.stdout
        assert "state credential helper configured" in result.stdout

    def test_app_auth_missing_credential_helper_is_drift(self, install):
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750); auth.chmod(0o750)  # clear setgid inherited from setgid parent on Linux
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)
        (auth / "github-app.pem").write_text("-----BEGIN RSA PRIVATE KEY-----\n")
        (auth / "github-app.pem").chmod(0o640)
        # No credential helper configured on the state repo → drift.
        result = _run_verify(install, auth_method="app")
        assert result.returncode == 1
        assert "[DRIFT] state credential helper" in result.stderr

    def test_app_auth_helper_failure_is_drift(self, install):
        """Replace the token helper with one that exits 2 (mint produces no
        token) — verify must report the drift instead of swallowing the
        failure."""
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750); auth.chmod(0o750)  # clear setgid inherited from setgid parent on Linux
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)
        (auth / "github-app.pem").write_text("-----BEGIN RSA PRIVATE KEY-----\n")
        (auth / "github-app.pem").chmod(0o640)
        helper_cmd = (
            f"!HERMES_HOME='{target}' /bin/true credential"
        )
        _git(install["state"], "config",
             "credential.https://github.com.helper", helper_cmd)

        helper = install["rails"] / "installer" / "hermes_github_token.py"
        helper.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(2)\n"
        )
        helper.chmod(0o755)

        result = _run_verify(install, auth_method="app")
        assert result.returncode == 1
        assert "[DRIFT] token helper check" in result.stderr

    def test_app_auth_missing_auth_dir_is_drift(self, install):
        """--auth-method app + no auth/ dir → drift on every required artefact.

        Prior to the review, the auth, credential-helper, and token-helper
        checks were all gated on `auth/` existing — deleting the directory
        silently passed verify. The contract is now: passing `--auth-method
        app` declares the install as app-auth and every artefact MUST be
        present.
        """
        # No auth/ on the fixture, but pass --auth-method app.
        result = _run_verify(install, auth_method="app")
        assert result.returncode == 1
        # Auth dir, both required files, and credential helper each fire
        # their own [DRIFT] line. The token helper smoke runs against the
        # helper script (which lives in rails, not auth/) — in the test stub
        # it succeeds regardless; in production it'd fail trying to read the
        # missing github-app.env. The contract violation is named explicitly
        # in the four drift lines above.
        assert "[DRIFT] auth dir" in result.stderr
        assert "[DRIFT] auth file github-app.env" in result.stderr
        assert "[DRIFT] auth file github-app.pem" in result.stderr
        assert "[DRIFT] state credential helper" in result.stderr

    def test_app_auth_missing_pem_is_drift(self, install):
        """--auth-method app present but github-app.pem absent → drift named
        explicitly so operators see which file is missing instead of relying
        on a vague mode-glob check that skips when the file isn't there."""
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750); auth.chmod(0o750)  # clear setgid inherited from setgid parent on Linux
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)
        # No github-app.pem.

        helper_cmd = (
            f"!HERMES_HOME='{target}' "
            f"{install['rails'] / 'venv' / 'bin' / 'python'} "
            f"{install['rails'] / 'installer' / 'hermes_github_token.py'} credential"
        )
        _git(install["state"], "config",
             "credential.https://github.com.helper", helper_cmd)

        result = _run_verify(install, auth_method="app")
        assert result.returncode == 1
        assert "[DRIFT] auth file github-app.pem" in result.stderr

    def test_inactive_timer_is_drift(self, install, tmp_path):
        """ActiveState=inactive must report drift. Substring matching
        ('inactive' contains 'active') quietly accepted stopped timers
        before the review fix."""
        # Replace the systemctl stub with one that reports the timer
        # inactive but still loaded+enabled. Substring matching used to let
        # this state through ("inactive" contains "active") — exact-equality
        # parsing now catches it.
        systemctl = install["bin"] / "systemctl"
        systemctl.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$1\" == \"show\" ]]; then\n"
            "  shift\n"
            "  props=()\n"
            "  while [[ $# -gt 0 ]]; do\n"
            "    case \"$1\" in\n"
            "      -p) props+=(\"$2\"); shift 2 ;;\n"
            "      --value) shift ;;\n"
            "      *) shift ;;\n"
            "    esac\n"
            "  done\n"
            "  for prop in \"${props[@]}\"; do\n"
            "    case \"$prop\" in\n"
            "      ActiveState)   echo ActiveState=inactive ;;\n"
            "      LoadState)     echo LoadState=loaded ;;\n"
            "      UnitFileState) echo UnitFileState=enabled ;;\n"
            "    esac\n"
            "  done\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n"
        )
        systemctl.chmod(0o755)

        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] hermes-sync.timer" in result.stderr
        assert "active=inactive" in result.stderr

    def test_upstream_workspace_non_github_origin_is_drift(self, install):
        """When upstream-workspace exists, both remotes must be on github.com."""
        ws = install["upstream_workspace"]
        _git(ws, "remote", "set-url", "origin", str(install["fork"]))
        # origin still points at the local fork path → drift.
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] upstream-workspace origin" in result.stderr

    def test_upstream_sync_missing_script_is_drift(self, install):
        install["upstream_sync_script"].unlink()
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] hermes-upstream-sync script" in result.stderr

    def test_upstream_sync_failed_status_is_drift(self, install):
        status_dir = install["target"] / "state" / "hermes-upstream-sync"
        status_dir.mkdir(parents=True)
        (status_dir / "status.env").write_text(
            "timestamp=2026-05-27T09:00:00Z\n"
            "state=failed\n"
            "detail='fetch upstream failed'\n",
            encoding="utf-8",
        )
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] hermes-upstream-sync status" in result.stderr
        assert "fetch upstream failed" in result.stderr

    def test_upstream_sync_stale_lock_is_drift(self, install):
        lock_dir = install["target"] / "state" / "hermes-upstream-sync" / "lock"
        lock_dir.mkdir(parents=True)
        (lock_dir / "pid").write_text("999999999\n", encoding="utf-8")
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] hermes-upstream-sync lock" in result.stderr

    def test_upstream_sync_service_exec_mismatch_is_drift(self, install):
        unit = install["systemd_dir"] / "hermes-upstream-sync.service"
        text = unit.read_text(encoding="utf-8")
        unit.write_text(
            text.replace(
                f"ExecStart={install['upstream_sync_script']}",
                "ExecStart=/usr/local/bin/old-hermes-upstream-sync",
            ),
            encoding="utf-8",
        )
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] hermes-upstream-sync.service exec" in result.stderr

    def test_quay_artefacts_skipped_when_no_values_file(self, install):
        """The verify-only fixture has no deploy.values.yaml in the fork —
        quay block must silently no-op so non-quay tests stay green."""
        result = _run_verify(install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        # Match check lines specifically — the tmp path itself sometimes
        # contains "quay" (pytest derives it from the test name).
        assert "[OK] quay" not in result.stdout
        assert "[DRIFT] quay" not in result.stderr
        assert "quay-tick.timer" not in result.stdout

    def test_summary_line_counts_match(self, install):
        """The closing line should report the same total/drift counts that
        match the [OK]/[DRIFT] lines emitted above it."""
        # Inject a single drift to make the count predictable.
        _git(install["state"], "remote", "set-url", "origin", "/tmp/somewhere")
        result = _run_verify(install)
        ok_count = result.stdout.count("[OK]")
        drift_count = result.stderr.count("[DRIFT]")
        # Last line of stdout is the summary.
        last = [l for l in result.stdout.splitlines() if l.startswith("==> verify:")][-1]
        # Format: "==> verify: N checks, M drift"
        parts = last.replace(",", "").split()
        n_checks = int(parts[2])
        n_drift = int(parts[4])
        assert n_checks == ok_count + drift_count
        assert n_drift == drift_count
        assert n_drift >= 1


# ---------------------------------------------------------------------------
# Quay verify
# ---------------------------------------------------------------------------


QUAY_VERSION = "v0.3.10"  # tag-shaped, with leading v, as in deploy.values.yaml
BUN_VERSION = "1.3.9"
BUN_LINUX_X64_SHA256 = (
    "4680e80e44e32aa718560ceae85d22ecfbf2efb8f3641782e35e4b7efd65a1aa"
)


def _write_quay_stub(
    path: Path,
    version: str,
    registered_ids: list[str],
    repo_list_override: str | None = None,
    repo_tags: dict[str, dict] | None = None,
    deployment_tags: dict | None = None,
    tags_supported: bool = True,
    serve_supported: bool = True,
    help_probe_side_effect: bool = False,
) -> None:
    """Stub `quay` binary — emits version on `--version`, JSON list on `repo list`.

    `version` is tag-shaped (`v0.1.0`); the stub strips the leading `v` and
    appends a fake build SHA to mirror what older real binaries emitted
    (`${pkg.version}+${shortSHA}`, see scripts/embed.ts in InverterNetwork/quay).
    Verify ignores this output for binary drift and compares SHA256 instead.

    `repo_list_override` lets a caller substitute the `repo list` payload
    directly — useful for testing parse-failure paths with non-JSON output.
    Real binary keys entries by `repo_id` (not `id`); the default branch
    mirrors that shape.

    `repo_tags` maps repo_id → ``{namespaces: {...}}`` JSON for the `repo
    get-tags <id>` reply. Missing keys default to ``{namespaces: {}}``,
    which is what an unconfigured repo emits and matches a fresh
    `apply-tags` clear. `deployment_tags` is the same shape for the
    `tags get-deployment` reply (sans the `repo_id` envelope —
    upstream wraps it as ``{scope: "deployment", namespaces: {...}}``)."""
    import json as _json
    if repo_list_override is None:
        repos_json = ",".join(f'{{"repo_id": "{i}"}}' for i in registered_ids)
        repo_list_payload = f"[{repos_json}]"
    else:
        repo_list_payload = repo_list_override
    semver = version.removeprefix("v")

    # Default unconfigured-vocab payload for repos / deployment that the
    # caller didn't explicitly populate. The shape matches what a clean
    # install emits and lets pre-existing tests round-trip without drift
    # on the new tag-vocab checks.
    #
    # `tags_supported=False` simulates a pre-tag-vocab binary: the `tags`
    # noun isn't registered, so `tags --help` exits non-zero and the
    # capability-gate probe in verify.py / setup-hermes.sh skips the
    # new reconciliation paths. The repo / deployment tag-vocab branches
    # below are still emitted so legacy tests that don't probe stay
    # equivalent in behaviour.
    repo_tags_map = repo_tags or {}
    repo_tags_cases: list[str] = []
    for rid in registered_ids:
        payload = repo_tags_map.get(rid) or {"namespaces": {}}
        envelope = {"repo_id": rid, **payload}
        repo_tags_cases.append(
            f'  "{rid}") echo {_json.dumps(_json.dumps(envelope))}; exit 0 ;;'
        )
    repo_tags_default = (
        '  *) echo \'{"error":"unknown_repo"}\' >&2; exit 1 ;;'
    )
    repo_tags_block = (
        'if [[ "$1" == "repo" && "$2" == "get-tags" ]]; then\n'
        '  case "$3" in\n'
        + "\n".join(repo_tags_cases + [repo_tags_default])
        + "\n  esac\nfi\n"
    )
    deployment_envelope = {
        "scope": "deployment",
        **(deployment_tags or {"namespaces": {}}),
    }
    deployment_block = (
        'if [[ "$1" == "tags" && "$2" == "get-deployment" ]]; then '
        f"echo {_json.dumps(_json.dumps(deployment_envelope))}; exit 0; fi\n"
    )
    tags_side_effect = (
        'mkdir -p "$QUAY_DATA_DIR"; touch "$QUAY_DATA_DIR/tags-help-probe"; '
        if help_probe_side_effect else ""
    )
    serve_side_effect = (
        'mkdir -p "$QUAY_DATA_DIR"; touch "$QUAY_DATA_DIR/serve-help-probe"; '
        if help_probe_side_effect else ""
    )
    tags_help_block = (
        f'if [[ "$1" == "tags" && "$2" == "--help" ]]; then {tags_side_effect}exit 0; fi\n'
        if tags_supported
        else f'if [[ "$1" == "tags" && "$2" == "--help" ]]; then {tags_side_effect}echo "unknown_command" >&2; exit 1; fi\n'
    )
    serve_help_block = (
        f'if [[ "$1" == "serve" && "$2" == "--help" ]]; then {serve_side_effect}exit 0; fi\n'
        if serve_supported
        else f'if [[ "$1" == "serve" && "$2" == "--help" ]]; then {serve_side_effect}echo "unknown command: serve" >&2; exit 1; fi\n'
    )
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{semver}+abc1234"; exit 0; fi\n'
        f'if [[ "$1" == "repo" && "$2" == "list" ]]; then echo \'{repo_list_payload}\'; exit 0; fi\n'
        + tags_help_block
        + serve_help_block
        + repo_tags_block
        + deployment_block
        + "exit 0\n"
    )
    path.chmod(0o755)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _write_expected_quay_sha(expected_file: Path, quay_bin: Path) -> None:
    expected_file.parent.mkdir(parents=True, exist_ok=True)
    expected_file.write_text(f"{_sha256_file(quay_bin)}  /usr/local/bin/quay\n")
    expected_file.chmod(0o644)


def _write_curl_status_stub(
    path: Path,
    *,
    default: str = "200",
    dashboard: str | None = None,
) -> None:
    dashboard_case = ""
    if dashboard is not None:
        dashboard_case = (
            f"if [[ \"$args\" == *\"9119\"* ]]; then printf '{dashboard}'; exit 0; fi\n"
        )
    path.write_text(
        "#!/usr/bin/env bash\n"
        "args=\"$*\"\n"
        "cat >/dev/null || true\n"
        f"{dashboard_case}"
        f"printf '{default}'\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_live_quay_stub(install: dict, version: str, registered_ids: list[str], **kwargs) -> None:
    _write_quay_stub(install["quay_bin"], version, registered_ids, **kwargs)
    _write_expected_quay_sha(install["quay_expected_sha"], install["quay_bin"])


def _quay_expected_sha_path(target: Path) -> Path:
    return (
        target
        / "hermes-agent"
        / "installer"
        / ".state"
        / "quay"
        / "SHA256SUM.expected"
    )


def _app_helper_cmd(install: dict) -> str:
    target = install["target"]
    return (
        f"!HERMES_HOME='{target}' "
        f"{install['rails'] / 'venv' / 'bin' / 'python'} "
        f"{install['rails'] / 'installer' / 'hermes_github_token.py'} credential"
    )


ATLAS_VERSION = "v0.1.0"


def _atlas_app_helper_cmd(install: dict) -> str:
    target = install["target"]
    env_file = target / "auth" / "atlas-manager.env"
    return (
        f"!HERMES_HOME='{target}' HERMES_GH_CONFIG='{env_file}' "
        f"{install['rails'] / 'venv' / 'bin' / 'python'} "
        f"{install['rails'] / 'installer' / 'hermes_github_token.py'} credential"
    )


def _write_atlas_stub(path: Path, version: str = ATLAS_VERSION) -> None:
    semver = version.removeprefix("v")
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{semver}"; exit 0; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


@pytest.fixture
def atlas_install(install: dict) -> dict:
    """Extend the base install fixture with Atlas artefacts and config."""
    fork = install["fork"]
    target = install["target"]
    bin_dir = install["bin"]

    (fork / "installer").mkdir(exist_ok=True)
    repo_helper_src = REPO_ROOT / "installer" / "values_helper.py"
    fork_helper = fork / "installer" / "values_helper.py"
    if not fork_helper.exists():
        os.symlink(repo_helper_src, fork_helper)

    (fork / "deploy.values.yaml").write_text(
        "org:\n"
        "  name: Test\n"
        "  agent_identity_name: didier\n"
        "slack:\n"
        "  app:\n"
        "    display_name: t\n"
        "    description: t\n"
        "    background_color: \"#000\"\n"
        "  runtime:\n"
        "    allowed_channels: []\n"
        "    home_channel: \"\"\n"
        "    require_mention: true\n"
        "    channel_prompts: {}\n"
        "repos: []\n"
        "atlas:\n"
        f"  version: \"{ATLAS_VERSION}\"\n"
        "  release_repo: InverterNetwork/atlas\n"
        "  kb_repo: https://github.com/InverterNetwork/atlas-kb\n"
        "  kb_branch: main\n"
        "  kb_root: \"\"\n"
        "  ai:\n"
        "    mode: api\n"
        "  google_docs:\n"
        "    service_account_file: auth/otto-google-sa.json\n"
        "  hub:\n"
        "    enabled: true\n"
        "    host: 127.0.0.1\n"
        "    port: 8765\n"
        "    public_base_url: https://atlas.example.test\n"
        "    data_dir: \"\"\n"
        "    query_concurrency: 4\n"
        "  github_app:\n"
        "    id: \"3925386\"\n"
        "    installation_id: \"137066070\"\n",
        encoding="utf-8",
    )

    auth = target / "auth"
    auth.mkdir(mode=0o750, exist_ok=True)
    auth.chmod(0o750)
    (auth / "github-app.pem").write_text(
        "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n"
    )
    (auth / "github-app.pem").chmod(0o640)
    (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
    (auth / "github-app.env").chmod(0o640)
    (auth / "atlas-manager.pem").write_text(
        "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n"
    )
    (auth / "atlas-manager.pem").chmod(0o640)
    (auth / "atlas-manager.env").write_text(
        "HERMES_GH_APP_ID=3925386\n"
        "HERMES_GH_INSTALLATION_ID=137066070\n"
        f"HERMES_GH_APP_KEY={auth / 'atlas-manager.pem'}\n"
        f"HERMES_GH_TOKEN_CACHE={target / 'cache' / 'atlas-manager-token-cache.json'}\n",
        encoding="utf-8",
    )
    (auth / "atlas-manager.env").chmod(0o640)
    (auth / "otto-google-sa.json").write_text('{"type":"service_account"}\n', encoding="utf-8")
    (auth / "otto-google-sa.json").chmod(0o640)
    (auth / "atlas-runtime.env").write_text(
        f"ATLAS_GOOGLE_SERVICE_ACCOUNT_FILE={auth / 'otto-google-sa.json'}\n",
        encoding="utf-8",
    )
    (auth / "atlas-runtime.env").chmod(0o640)
    (auth / "atlas.env").write_text("GITBOOK_API_TOKEN=test-token\n", encoding="utf-8")
    (auth / "atlas.env").chmod(0o640)
    (auth / "atlas-hub-auth.json").write_text(
        '{"keys":[{"id":"key_test","hash":"sha256:test","scopes":["v1:*"]}]}\n',
        encoding="utf-8",
    )
    (auth / "atlas-hub-auth.json").chmod(0o640)
    (auth / "atlas-hub-client-api-key").write_text(
        "atlas_hub_sk_test\n",
        encoding="utf-8",
    )
    (auth / "atlas-hub-client-api-key").chmod(0o640)

    _git(install["state"], "config",
         "credential.https://github.com.helper", _app_helper_cmd(install))

    atlas_bin = bin_dir / "atlas"
    _write_atlas_stub(atlas_bin)
    atlas_wrapper = bin_dir / "atlas-as-hermes"
    atlas_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n")
    atlas_wrapper.chmod(0o755)
    atlas_profile = bin_dir / "atlas-env.sh"
    atlas_profile.write_text("# stub\n")
    atlas_profile.chmod(0o644)

    kb_root = target / "atlas-kb"
    subprocess.run(["git", "clone", "--quiet", str(fork), str(kb_root)], check=True)
    _git(kb_root, "remote", "set-url", "origin", "https://github.com/InverterNetwork/atlas-kb")
    _git(kb_root, "config", "user.email", "didier@test")
    _git(kb_root, "config", "user.name", "didier")
    _git(kb_root, "config", "credential.https://github.com.helper", _atlas_app_helper_cmd(install))
    _git(kb_root, "config", "--replace-all",
         "url.https://github.com/.insteadOf", "git@github.com:")
    _git(kb_root, "config", "--add",
         "url.https://github.com/.insteadOf", "ssh://git@github.com/")

    atlas_hub_data_dir = target / "atlas-hub"
    atlas_hub_data_dir.mkdir(mode=0o755)
    atlas_config_dir = target / "config"
    atlas_config_dir.mkdir(mode=0o755, exist_ok=True)
    (atlas_config_dir / "atlas.yaml").write_text(
        "# test Atlas config\n"
        f"kb_root: \"{kb_root}\"\n"
        "kb_branch: \"main\"\n"
        "push: true\n"
        "hub:\n"
        f"  auth_file: \"{auth / 'atlas-hub-auth.json'}\"\n"
        f"  data_dir: \"{atlas_hub_data_dir}\"\n"
        "  query_concurrency: 4\n",
        encoding="utf-8",
    )
    (install["systemd_dir"] / "atlas-hub.service").write_text(
        "[Service]\n"
        f"Environment=ATLAS_CONFIG={atlas_config_dir / 'atlas.yaml'}\n"
        f"Environment=ATLAS_KB_ROOT={kb_root}\n"
        f"EnvironmentFile=-{auth / 'atlas-runtime.env'}\n"
        f"EnvironmentFile=-{auth / 'atlas.env'}\n"
        "ExecStart=/usr/local/bin/atlas --config "
        f"{atlas_config_dir / 'atlas.yaml'} serve --host 127.0.0.1 --port 8765\n",
        encoding="utf-8",
    )

    caddyfile = install["tmp"] / "Caddyfile"
    caddyfile.write_text(
        "atlas.example.test {\n"
        "\t# BEGIN HERMES MANAGED ATLAS HUB ROUTE\n"
        "\thandle /v1/* {\n"
        "\t\treverse_proxy 127.0.0.1:8765\n"
        "\t}\n"
        "\t# END HERMES MANAGED ATLAS HUB ROUTE\n"
        "\thandle {\n"
        "\t\treverse_proxy localhost:3100\n"
        "\t}\n"
        "}\n",
        encoding="utf-8",
    )

    _write_curl_status_stub(bin_dir / "curl")

    # Some git/filesystem operations in the extended fixture can clear the
    # parent setgid bit on macOS; restore the base install invariant.
    target.chmod(0o2775)

    install["values_file"] = fork / "deploy.values.yaml"
    install["atlas_bin"] = atlas_bin
    install["atlas_wrapper"] = atlas_wrapper
    install["atlas_profile"] = atlas_profile
    install["atlas_kb_root"] = kb_root
    install["caddyfile"] = caddyfile
    return install


def _atlas_env(install: dict) -> dict[str, str]:
    return {
        "HERMES_VERIFY_ATLAS_BIN": str(install["atlas_bin"]),
        "HERMES_VERIFY_ATLAS_WRAPPER": str(install["atlas_wrapper"]),
        "HERMES_VERIFY_ATLAS_PROFILE": str(install["atlas_profile"]),
        "HERMES_VERIFY_CADDYFILE": str(install["caddyfile"]),
    }


def _run_verify_atlas(
    install: dict,
    *,
    auth_method: str = "app",
    env_overrides: dict[str, str] | None = None,
) -> SimpleNamespace:
    overrides = _atlas_env(install)
    if env_overrides:
        overrides.update(env_overrides)
    return _run_verify(
        install,
        auth_method=auth_method,
        values=install["values_file"],
        env_overrides=overrides,
    )


class TestAtlasVerify:
    def test_clean_atlas_install_passes(self, atlas_install):
        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 0, result.stderr
        assert "[OK] atlas binary:" in result.stdout
        assert "[OK] atlas binary version:" in result.stdout
        assert "[OK] atlas-as-hermes wrapper:" in result.stdout
        assert "[OK] atlas profile.d drop-in:" in result.stdout
        assert "[OK] Atlas runtime env:" in result.stdout
        assert "[OK] Atlas Google Docs service account env" in result.stdout
        assert "[OK] Atlas Google Docs service account file:" in result.stdout
        assert "[OK] Atlas KB ownership:" in result.stdout
        assert "[OK] Atlas KB credential helper configured" in result.stdout
        assert "[OK] Atlas manager token helper check passes" in result.stdout
        assert "[OK] Atlas manager App scope InverterNetwork/atlas: HTTP 200" in result.stdout
        assert "[OK] Atlas manager App scope InverterNetwork/atlas-kb: HTTP 200" in result.stdout
        assert "[OK] Atlas Hub auth file:" in result.stdout
        assert "[OK] Atlas Hub auth file keys present" in result.stdout
        assert "[OK] Atlas Hub client API key:" in result.stdout
        assert "[OK] Atlas Hub client API key present" in result.stdout
        assert "[OK] Atlas runtime config hub auth_file" in result.stdout
        assert "[OK] Atlas runtime config hub data_dir" in result.stdout
        assert "[OK] Atlas runtime config hub query_concurrency" in result.stdout
        assert "[OK] atlas-hub.service: active loaded enabled" in result.stdout
        assert "[OK] atlas-hub.service ATLAS_CONFIG" in result.stdout
        assert "[OK] atlas-hub.service ATLAS_KB_ROOT" in result.stdout
        assert "[OK] atlas-hub.service secrets env" in result.stdout
        assert "[OK] atlas-hub.service runtime env" in result.stdout
        assert "[OK] atlas-hub.service ExecStart uses atlas serve" in result.stdout
        assert "[OK] atlas-hub.service loopback bind: 127.0.0.1:8765" in result.stdout
        assert "[OK] atlas-hub.service port: 8765" in result.stdout
        assert "[OK] Atlas Hub local health: HTTP 200" in result.stdout
        assert "[OK] Atlas Hub public Caddy route: https://atlas.example.test/v1/* -> 127.0.0.1:8765" in result.stdout
        assert "[OK] Atlas Hub public health: HTTP 200" in result.stdout
        assert "[DRIFT]" not in result.stderr

    def test_missing_atlas_binary_is_drift(self, atlas_install):
        atlas_install["atlas_bin"].unlink()
        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] atlas binary" in result.stderr

    def test_missing_atlas_kb_helper_is_drift(self, atlas_install):
        _git(atlas_install["atlas_kb_root"], "config", "--unset", "credential.https://github.com.helper")
        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] Atlas KB GitHub App credential helper" in result.stderr
        assert "[DRIFT] Atlas KB credential helper" in result.stderr

    def test_atlas_requires_app_auth_method(self, atlas_install):
        result = _run_verify_atlas(atlas_install, auth_method="none")

        assert result.returncode == 1
        assert "[DRIFT] Atlas auth method" in result.stderr

    def test_atlas_repo_scope_failure_is_drift(self, atlas_install):
        curl = atlas_install["bin"] / "curl"
        curl.write_text(
            "#!/usr/bin/env bash\n"
            "args=\"$*\"\n"
            "cat >/dev/null || true\n"
            "if [[ \"$args\" == *\"/repos/InverterNetwork/atlas-kb\"* ]]; then printf '404'; exit 0; fi\n"
            "printf '200'\n",
            encoding="utf-8",
        )
        curl.chmod(0o755)

        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] Atlas manager App scope InverterNetwork/atlas-kb" in result.stderr

    def test_atlas_hub_public_bind_is_drift(self, atlas_install):
        unit = atlas_install["systemd_dir"] / "atlas-hub.service"
        unit.write_text(
            unit.read_text(encoding="utf-8").replace(
                "--host 127.0.0.1", "--host 0.0.0.0"
            ),
            encoding="utf-8",
        )

        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] atlas-hub.service bind address" in result.stderr

    def test_atlas_hub_missing_client_key_is_drift(self, atlas_install):
        (atlas_install["target"] / "auth" / "atlas-hub-client-api-key").unlink()

        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] Atlas Hub client API key" in result.stderr

    def test_atlas_hub_missing_public_caddy_route_is_drift(self, atlas_install):
        atlas_install["caddyfile"].write_text(
            "atlas.example.test {\n"
            "\thandle {\n"
            "\t\treverse_proxy localhost:3100\n"
            "\t}\n"
            "}\n",
            encoding="utf-8",
        )

        result = _run_verify_atlas(atlas_install)

        assert result.returncode == 1
        assert "[DRIFT] Atlas Hub public Caddy route" in result.stderr


@pytest.fixture
def quay_install(install: dict) -> dict:
    """Extend the base install fixture with quay artefacts on disk + a values
    file pinned to QUAY_VERSION + a stubbed `quay` binary on PATH-redirect."""
    fork = install["fork"]
    target = install["target"]
    bin_dir = install["bin"]

    # Values file with one quay repo entry. The values_helper is the real
    # one in the fork — installer/values_helper.py is shipped with the repo,
    # so the test fork needs the helper symlinked into installer/ to mirror
    # `--fork pointing at a working tree`.
    (fork / "installer").mkdir(exist_ok=True)
    repo_helper_src = REPO_ROOT / "installer" / "values_helper.py"
    fork_helper = fork / "installer" / "values_helper.py"
    if not fork_helper.exists():
        os.symlink(repo_helper_src, fork_helper)

    repo_url = "https://github.com/example/test-factory-code"
    repo_id = "test-factory-code"
    (fork / "deploy.values.yaml").write_text(
        "org:\n"
        "  name: Test\n"
        "  agent_identity_name: didier\n"
        "slack:\n"
        "  app:\n"
        "    display_name: t\n"
        "    description: t\n"
        "    background_color: \"#000\"\n"
        "  runtime:\n"
        "    allowed_channels: []\n"
        "    home_channel: \"\"\n"
        "    require_mention: true\n"
        "    channel_prompts: {}\n"
        "repos:\n"
        f"  - id: {repo_id}\n"
        f"    url: {repo_url}\n"
        "    base_branch: main\n"
        "    quay:\n"
        "      package_manager: bun\n"
        "      install_cmd: \"bun install\"\n"
        "quay:\n"
        f"  version: \"{QUAY_VERSION}\"\n"
        "  agent_invocation: \"claude < {prompt_file}\"\n"
        "  runtime_managers:\n"
        "    bun:\n"
        f"      version: \"{BUN_VERSION}\"\n"
        f"      linux_x64_sha256: \"{BUN_LINUX_X64_SHA256}\"\n"
        "  adapters:\n"
        "    linear:\n"
        "      enabled: true\n"
        "      api_key_env: LINEAR_API_KEY\n"
        "    slack:\n"
        "      enabled: false\n"
    )

    # Data dir + config.toml + bare clone. Mode/ownership matches the
    # installer's `install -d -o $AGENT_USER -g $AGENT_USER -m 0755`.
    quay_dir = target / "quay"
    code_root = target / "code"
    quay_dir.mkdir(mode=0o755)
    (quay_dir / "config.toml").write_text(
        "[admin]\n"
        "require_auth = true\n"
        'token_env = "QUAY_ADMIN_TOKEN"\n'
        'forwarded_identity_header = "X-Hermes-User-Id"\n'
        "\n"
        "[context]\n"
        f'reference_repos_root = "{code_root}"\n'
    )
    (quay_dir / "config.toml").chmod(0o644)
    repos_dir = quay_dir / "repos"
    repos_dir.mkdir(mode=0o755)
    bare = repos_dir / f"{repo_id}.git"
    subprocess.run(
        ["git", "clone", "--quiet", "--bare", str(install["fork"]), str(bare)],
        check=True,
    )
    # Point the bare clone's origin at the URL the values file claims, so
    # the verify origin-equality check passes on a clean fixture.
    subprocess.run(
        ["git", "-C", str(bare), "remote", "set-url", "origin", repo_url],
        check=True,
    )

    # Code mirror cloned from the same fork as the bare clone so
    # HEAD == origin/main satisfies the branch-freshness check.
    code_mirror = code_root / repo_id
    code_mirror.parent.mkdir(exist_ok=True, mode=0o755)
    subprocess.run(
        ["git", "clone", "--quiet", str(install["fork"]), str(code_mirror)],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(code_mirror), "remote", "set-url", "origin", repo_url],
        check=True,
    )

    # Stubbed quay binary somewhere on PATH; verify reads
    # HERMES_VERIFY_QUAY_BIN to redirect away from /usr/local/bin/quay.
    quay_stub = bin_dir / "quay"
    _write_quay_stub(quay_stub, QUAY_VERSION, [repo_id])
    quay_expected_sha = _quay_expected_sha_path(target)
    _write_expected_quay_sha(quay_expected_sha, quay_stub)

    auth = target / "auth"
    auth.mkdir(mode=0o750, exist_ok=True)
    auth.chmod(0o750)
    (auth / "quay.env").write_text("QUAY_ADMIN_TOKEN=test-admin-token\n")
    (auth / "quay.env").chmod(0o640)

    systemd_dir = install["systemd_dir"]
    systemd_dir.mkdir(exist_ok=True)
    (systemd_dir / "quay-serve.service").write_text(
        "[Service]\n"
        f"Environment=QUAY_DATA_DIR={target / 'quay'}\n"
        f"EnvironmentFile=-{target / 'auth' / 'gateway-runtime.env'}\n"
        f"EnvironmentFile={target / 'auth' / 'quay.env'}\n"
        "ExecStart=/usr/local/bin/quay serve --host 127.0.0.1 --port 9731\n",
        encoding="utf-8",
    )
    (systemd_dir / "quay-tick.service").write_text(
        "Environment=HERMES_REVIEWER_GH_CONFIG=/etc/hermes/reviewer.env\n"
        "RuntimeDirectory=hermes\n",
        encoding="utf-8",
    )

    # values_helper.py imports pyyaml. The system `python3` on dev macOS
    # often lacks it, so shadow `python3` in PATH with a shim that
    # delegates to whichever interpreter is running pytest (it has pyyaml
    # because the repo's pyproject pulls it in for tests).
    py_shim = bin_dir / "python3"
    if not py_shim.exists():
        py_shim.write_text(f'#!/usr/bin/env bash\nexec {sys.executable} "$@"\n')
        py_shim.chmod(0o755)

    # Operator-invocation glue: wrapper at /usr/local/bin/quay-as-hermes,
    # tick runner at /usr/local/sbin/quay-tick-runner, and login-shell
    # drop-in at /etc/profile.d/quay-data-dir.sh in prod.
    # The test fixture redirects all three via env-var overrides so we can
    # poke at them without touching /usr/local/bin, /usr/local/sbin, or
    # /etc on the dev machine.
    quay_wrapper = bin_dir / "quay-as-hermes"
    quay_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n")
    quay_wrapper.chmod(0o755)
    quay_runner = bin_dir / "quay-tick-runner"
    quay_runner.write_text("#!/usr/bin/env bash\nexit 0\n")
    quay_runner.chmod(0o755)
    quay_profile = bin_dir / "quay-data-dir.sh"
    quay_profile.write_text("# stub\n")
    quay_profile.chmod(0o644)

    # Stubbed bun binary at HERMES_VERIFY_RUNTIME_DIR/bun — verify mirrors
    # the install-time `--version` substring probe against the values pin.
    bun_stub = bin_dir / "bun"
    bun_stub.write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{BUN_VERSION}"; exit 0; fi\n'
        "exit 0\n"
    )
    bun_stub.chmod(0o755)

    codex_stub = bin_dir / "codex"
    codex_stub.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--version" ]]; then echo "codex-cli 0.133.0"; exit 0; fi\n'
        "exit 0\n"
    )
    codex_stub.chmod(0o755)

    agent_home = install["tmp"] / "agent-home"
    agent_home.mkdir(exist_ok=True)
    codex_dir = agent_home / ".codex"
    codex_dir.mkdir(mode=0o700, exist_ok=True)
    codex_dir.chmod(0o700)
    (codex_dir / "auth.json").write_text(
        '{"tokens":{"access_token":"test-access","refresh_token":"test-refresh"}}\n',
        encoding="utf-8",
    )

    _write_curl_status_stub(bin_dir / "curl")

    install["values_file"] = fork / "deploy.values.yaml"
    install["quay_dir"] = quay_dir
    install["quay_bin"] = quay_stub
    install["quay_expected_sha"] = quay_expected_sha
    install["quay_repo_id"] = repo_id
    install["quay_repo_url"] = repo_url
    install["quay_bare"] = bare
    install["code_mirror"] = code_mirror
    install["quay_wrapper"] = quay_wrapper
    install["quay_runner"] = quay_runner
    install["quay_profile"] = quay_profile
    install["bun_bin"] = bun_stub
    install["codex_bin"] = codex_stub
    install["agent_home"] = agent_home
    install["systemd_dir"] = systemd_dir
    return install


def _enable_quay_admin_public_dashboard(install: dict) -> None:
    values = install["values_file"]
    text = values.read_text(encoding="utf-8")
    needle = "quay:\n" f"  version: \"{QUAY_VERSION}\"\n"
    replacement = (
        "quay:\n"
        "  admin:\n"
        "    public_base_url: https://hermes.example.test\n"
        f"  version: \"{QUAY_VERSION}\"\n"
    )
    assert needle in text
    values.write_text(text.replace(needle, replacement, 1), encoding="utf-8")

    target = install["target"]
    (target / "auth" / "gateway-runtime.env").write_text(
        "QUAY_ADMIN_PUBLIC_BASE_URL=https://hermes.example.test\n",
        encoding="utf-8",
    )
    (target / "auth" / "gateway-runtime.env").chmod(0o640)
    (install["systemd_dir"] / "hermes-dashboard.service").write_text(
        "[Unit]\n"
        "After=network-online.target quay-serve.service\n"
        "Wants=network-online.target quay-serve.service\n"
        "[Service]\n"
        f"Environment=HERMES_HOME={target}\n"
        f"Environment=HERMES_WEB_DIST={target / 'hermes-agent' / 'hermes_cli' / 'web_dist'}\n"
        "Environment=QUAY_ADMIN_BASE_URL=http://127.0.0.1:9731\n"
        f"EnvironmentFile={target / 'auth' / 'quay.env'}\n"
        f"EnvironmentFile=-{target / 'auth' / 'gateway-runtime.env'}\n"
        f"ExecStart={target / 'hermes-agent' / 'venv' / 'bin' / 'hermes'} "
        "dashboard --host 127.0.0.1 --port 9119 --no-open\n",
        encoding="utf-8",
    )

    _write_curl_status_stub(install["bin"] / "curl", dashboard="401")


def _configure_quay_app_auth(
    install: dict,
    *,
    include_gitconfig: bool = True,
    include_rewrites: bool = True,
) -> dict[str, str]:
    target = install["target"]
    auth = target / "auth"
    auth.mkdir(mode=0o750, exist_ok=True)
    auth.chmod(0o750)
    (auth / "github-app.pem").write_text(
        "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n"
    )
    (auth / "github-app.pem").chmod(0o640)
    (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
    (auth / "github-app.env").chmod(0o640)

    helper_cmd = _app_helper_cmd(install)
    _git(install["state"], "config",
         "credential.https://github.com.helper", helper_cmd)

    for repo in (install["code_mirror"], install["quay_bare"]):
        _git(repo, "config", "credential.https://github.com.helper", helper_cmd)
        _git(repo, "config", "--replace-all",
             "url.https://github.com/.insteadOf", "git@github.com:")
        _git(repo, "config", "--add",
             "url.https://github.com/.insteadOf", "ssh://git@github.com/")

    agent_home = install.get("agent_home") or install["tmp"] / "agent-home"
    agent_home.mkdir(exist_ok=True)
    if include_gitconfig:
        include_file = auth / "quay-github-app.gitconfig"
        rewrite_lines = ""
        if include_rewrites:
            rewrite_lines = (
                "[url \"https://github.com/\"]\n"
                "\tinsteadOf = git@github.com:\n"
                "\tinsteadOf = ssh://git@github.com/\n"
            )
        include_file.write_text(
            "[credential \"https://github.com\"]\n"
            f"\thelper = {helper_cmd}\n"
            f"{rewrite_lines}"
        )
        include_file.chmod(0o640)
        agent_gitconfig = agent_home / ".gitconfig"
        for pattern in (
            f"gitdir:{target}/quay/worktrees/",
            f"gitdir:{target}/quay/repos/",
        ):
            _run(
                agent_home,
                "git", "config", "--file", str(agent_gitconfig),
                f"includeIf.{pattern}.path", str(include_file),
            )

    _write_curl_status_stub(install["bin"] / "curl")

    return {"HERMES_VERIFY_AGENT_HOME": str(agent_home)}


def _quay_env(install: dict) -> dict[str, str]:
    env = {
        "HERMES_VERIFY_QUAY_BIN": str(install["quay_bin"]),
        "HERMES_VERIFY_QUAY_WRAPPER": str(install["quay_wrapper"]),
        "HERMES_VERIFY_QUAY_RUNNER": str(install["quay_runner"]),
        "HERMES_VERIFY_QUAY_PROFILE": str(install["quay_profile"]),
        "HERMES_VERIFY_RUNTIME_DIR": str(install["bin"]),
        "HERMES_VERIFY_SYSTEMD_DIR": str(install["systemd_dir"]),
    }
    if install.get("agent_home"):
        env["HERMES_VERIFY_AGENT_HOME"] = str(install["agent_home"])
    return env


def _run_verify_quay(
    install: dict,
    *,
    auth_method: str = "none",
    env_overrides: dict[str, str] | None = None,
) -> SimpleNamespace:
    overrides = _quay_env(install)
    if env_overrides:
        overrides.update(env_overrides)
    return _run_verify(
        install,
        auth_method=auth_method,
        values=install["values_file"],
        env_overrides=overrides,
    )


def _run_verify_quay_via_wrapper(install: dict) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.update(_base_env(install, _quay_env(install)))
    return subprocess.run(
        [
            "bash", str(SCRIPT),
            "--verify",
            "--target", str(install["target"]),
            "--fork", str(install["fork"]),
            "--user", install["user"],
            "--values", str(install["values_file"]),
        ],
        env=env, check=False, capture_output=True, text=True,
    )


class TestSetupHermesVerifyQuay:
    def test_clean_quay_install_passes(self, quay_install):
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay binary:" in result.stdout
        assert "[OK] quay SHA256SUM.expected:" in result.stdout
        assert "[OK] quay binary SHA256:" in result.stdout
        assert "quay binary version" not in result.stdout
        assert "[OK] quay data dir ownership:" in result.stdout
        assert "[OK] quay config.toml:" in result.stdout
        assert "[OK] quay admin auth required" in result.stdout
        assert "[OK] quay admin token env: QUAY_ADMIN_TOKEN" in result.stdout
        assert (
            "[OK] quay admin forwarded identity header: X-Hermes-User-Id"
            in result.stdout
        )
        assert (
            f"[OK] quay reference_repos_root: {quay_install['target'] / 'code'}"
            in result.stdout
        )
        assert "[OK] quay reference repos root exists:" in result.stdout
        assert "[OK] quay reference repo mirrors:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} ownership:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} origin:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} registered" in result.stdout
        assert "[OK] quay-as-hermes wrapper:" in result.stdout
        assert "[OK] quay profile.d drop-in:" in result.stdout
        assert "[OK] quay-serve.service: active loaded enabled" in result.stdout
        assert "[OK] quay-serve.service QUAY_DATA_DIR" in result.stdout
        assert "[OK] quay-serve.service auth env" in result.stdout
        assert "[OK] quay-serve.service runtime env" in result.stdout
        assert "[OK] quay-serve.service loopback bind: 127.0.0.1" in result.stdout
        assert "[OK] quay admin token present" in result.stdout
        assert "[OK] quay admin local health: HTTP 200" in result.stdout

    def test_quay_admin_public_dashboard_service_passes(self, quay_install):
        _enable_quay_admin_public_dashboard(quay_install)

        result = _run_verify_quay(quay_install)

        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] hermes-dashboard.service: active loaded enabled" in result.stdout
        assert "[OK] hermes-dashboard.service HERMES_HOME" in result.stdout
        assert "[OK] hermes-dashboard.service HERMES_WEB_DIST" in result.stdout
        assert "[OK] hermes-dashboard.service Quay upstream" in result.stdout
        assert "[OK] hermes-dashboard.service auth env" in result.stdout
        assert "[OK] hermes-dashboard.service runtime env" in result.stdout
        assert (
            "[OK] hermes-dashboard.service loopback bind: 127.0.0.1:9119"
            in result.stdout
        )
        assert "[OK] hermes-dashboard local denial health: HTTP 401" in result.stdout

    def test_quay_admin_public_dashboard_missing_unit_is_drift(self, quay_install):
        _enable_quay_admin_public_dashboard(quay_install)
        (quay_install["systemd_dir"] / "hermes-dashboard.service").unlink()

        result = _run_verify_quay(quay_install)

        assert result.returncode == 1
        assert "[DRIFT] hermes-dashboard.service" in result.stderr

    def test_quay_admin_public_dashboard_public_bind_is_drift(self, quay_install):
        _enable_quay_admin_public_dashboard(quay_install)
        unit = quay_install["systemd_dir"] / "hermes-dashboard.service"
        unit.write_text(
            unit.read_text(encoding="utf-8").replace(
                "--host 127.0.0.1", "--host 0.0.0.0"
            ),
            encoding="utf-8",
        )

        result = _run_verify_quay(quay_install)

        assert result.returncode == 1
        assert "[DRIFT] hermes-dashboard.service bind address" in result.stderr

    def test_quay_admin_public_dashboard_comment_only_ordering_is_drift(
        self, quay_install,
    ):
        _enable_quay_admin_public_dashboard(quay_install)
        unit = quay_install["systemd_dir"] / "hermes-dashboard.service"
        text = unit.read_text(encoding="utf-8")
        text = text.replace(
            "After=network-online.target quay-serve.service\n",
            "After=network-online.target\n",
        )
        text = text.replace(
            "Wants=network-online.target quay-serve.service\n",
            "Wants=network-online.target\n",
        )
        text += "# quay-serve.service mentioned only in a comment\n"
        unit.write_text(text, encoding="utf-8")

        result = _run_verify_quay(quay_install)

        assert result.returncode == 1
        assert "[DRIFT] hermes-dashboard.service quay-serve ordering" in result.stderr
        assert "After=...quay-serve.service" in result.stderr
        assert "Wants=...quay-serve.service" in result.stderr

    def test_quay_admin_public_dashboard_health_failure_is_drift(self, quay_install):
        _enable_quay_admin_public_dashboard(quay_install)
        _write_curl_status_stub(quay_install["bin"] / "curl", dashboard="502")

        result = _run_verify_quay(quay_install)

        assert result.returncode == 1
        assert "[DRIFT] hermes-dashboard local denial health" in result.stderr

    def test_quay_serve_missing_admin_token_is_drift(self, quay_install):
        (quay_install["target"] / "auth" / "quay.env").write_text(
            "LINEAR_API_KEY=lin_test\n",
            encoding="utf-8",
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay admin token" in result.stderr

    def test_quay_serve_public_bind_is_drift(self, quay_install):
        unit = quay_install["systemd_dir"] / "quay-serve.service"
        unit.write_text(
            unit.read_text(encoding="utf-8").replace(
                "--host 127.0.0.1", "--host 0.0.0.0"
            ),
            encoding="utf-8",
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay-serve.service bind address" in result.stderr

    def test_quay_serve_health_failure_is_drift(self, quay_install):
        _write_curl_status_stub(quay_install["bin"] / "curl", default="401")
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay admin local health" in result.stderr

    def test_quay_serve_unit_is_drift_when_binary_lacks_serve(self, quay_install):
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [quay_install["quay_repo_id"]],
            serve_supported=False,
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay-serve.service" in result.stderr
        assert "does not support `serve`" in result.stderr

    def test_admin_config_is_drift_when_binary_lacks_serve(self, quay_install):
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [quay_install["quay_repo_id"]],
            serve_supported=False,
        )
        (quay_install["systemd_dir"] / "quay-serve.service").unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay admin auth config" in result.stderr
        assert "does not support `serve`" in result.stderr

    def test_quay_capability_probes_do_not_touch_live_data_dir(self, quay_install):
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [quay_install["quay_repo_id"]],
            help_probe_side_effect=True,
        )
        before = _snapshot(quay_install["quay_dir"])
        result = _run_verify_quay(quay_install)
        after = _snapshot(quay_install["quay_dir"])
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert before == after
        assert not (quay_install["quay_dir"] / "tags-help-probe").exists()
        assert not (quay_install["quay_dir"] / "serve-help-probe").exists()

    def test_app_auth_quay_gitconfig_passes(self, quay_install):
        env = _configure_quay_app_auth(quay_install)
        result = _run_verify_quay(
            quay_install,
            auth_method="app",
            env_overrides=env,
        )

        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay GitHub App gitconfig:" in result.stdout
        assert "quay GitHub App gitconfig SSH-to-HTTPS rewrites configured" in result.stdout
        assert "agent gitconfig include gitdir:" in result.stdout

    def test_app_auth_missing_quay_gitconfig_is_drift(self, quay_install):
        env = _configure_quay_app_auth(quay_install, include_gitconfig=False)
        result = _run_verify_quay(
            quay_install,
            auth_method="app",
            env_overrides=env,
        )

        assert result.returncode == 1
        assert "[DRIFT] quay GitHub App gitconfig" in result.stderr

    def test_app_auth_quay_gitconfig_missing_rewrite_is_drift(self, quay_install):
        env = _configure_quay_app_auth(quay_install, include_rewrites=False)
        result = _run_verify_quay(
            quay_install,
            auth_method="app",
            env_overrides=env,
        )

        assert result.returncode == 1
        assert "[DRIFT] quay GitHub App gitconfig SSH-to-HTTPS rewrites" in result.stderr

    def test_clean_quay_install_passes_via_bash_wrapper(self, quay_install):
        """Smoke through bash setup-hermes.sh --verify with the values flag
        wired through, so wrapper-side argv forwarding stays covered for the
        quay codepath too."""
        result = _run_verify_quay_via_wrapper(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay binary:" in result.stdout

    def test_verify_codex_required_for_quay_install(self, quay_install):
        # Quay-enabled hosts provision the standard local agent CLI set.
        # Verify should therefore check Codex even when deploy values no longer
        # own agent runtime settings.
        (quay_install["quay_dir"] / "config.toml").write_text(
            'agent_invocation = "codex exec < {prompt_file}"\n'
            "\n"
            "[admin]\n"
            "require_auth = true\n"
            'token_env = "QUAY_ADMIN_TOKEN"\n'
            'forwarded_identity_header = "X-Hermes-User-Id"\n'
            "\n"
            "[context]\n"
            f'reference_repos_root = "{quay_install["target"] / "code"}"\n'
            "\n",
            encoding="utf-8",
        )
        codex = quay_install["bin"] / "codex"
        codex.write_text("#!/usr/bin/env bash\nexit 127\n", encoding="utf-8")
        codex.chmod(0o755)

        result = _run_verify_quay(quay_install)

        assert result.returncode == 1
        assert "[DRIFT] codex binary" in result.stderr

    def test_pre_ast136_quay_pin_skips_reference_repo_check(self, quay_install):
        values = quay_install["values_file"]
        values.write_text(values.read_text().replace(QUAY_VERSION, "v0.3.9"))
        _write_live_quay_stub(quay_install, "v0.3.9", [quay_install["quay_repo_id"]])
        (quay_install["quay_dir"] / "config.toml").write_text(
            "[admin]\n"
            "require_auth = true\n"
            'token_env = "QUAY_ADMIN_TOKEN"\n'
            'forwarded_identity_header = "X-Hermes-User-Id"\n'
            "\n"
            'agent_invocation = "claude < {prompt_file}"\n'
        )

        result = _run_verify_quay(quay_install)

        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "reference_repos_root" not in result.stdout
        assert "reference_repos_root" not in result.stderr

    def test_missing_quay_wrapper_is_drift(self, quay_install):
        quay_install["quay_wrapper"].unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay-as-hermes wrapper" in result.stderr

    def test_missing_quay_profile_dropin_is_drift(self, quay_install):
        quay_install["quay_profile"].unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay profile.d drop-in" in result.stderr

    def test_wrong_mode_quay_profile_dropin_is_drift(self, quay_install):
        # Login shells source profile.d/*; an agent-writable copy would
        # let the agent override the canonical export. Mode drift must
        # be caught, not just absence.
        quay_install["quay_profile"].chmod(0o664)
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay profile.d drop-in" in result.stderr

    def test_missing_quay_binary_is_drift(self, quay_install):
        quay_install["quay_bin"].unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay binary" in result.stderr

    def test_quay_sha_mismatch_is_drift(self, quay_install):
        # Stub now has different bytes than the release SHA recorded at
        # install time, which is the post-install drift signal.
        _write_quay_stub(quay_install["quay_bin"], "v9.9.9", [quay_install["quay_repo_id"]])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay binary SHA256" in result.stderr

    def test_quay_version_string_mismatch_is_not_drift_when_sha_matches(self, quay_install):
        # quay --version can lie independently of the release artifact. If
        # the installed bytes match the recorded SHA, verify must pass.
        _write_live_quay_stub(
            quay_install, "v9.9.9", [quay_install["quay_repo_id"]],
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay binary SHA256:" in result.stdout
        assert "quay binary version" not in result.stdout + result.stderr

    def test_missing_quay_expected_sha_is_drift(self, quay_install):
        quay_install["quay_expected_sha"].unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay binary SHA256" in result.stderr
        assert "missing expected hash" in result.stderr

    def test_missing_quay_data_dir_is_drift(self, quay_install):
        shutil.rmtree(quay_install["quay_dir"])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay data dir" in result.stderr

    def test_missing_quay_config_toml_is_drift(self, quay_install):
        (quay_install["quay_dir"] / "config.toml").unlink()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay config.toml" in result.stderr

    def test_wrong_reference_repos_root_is_drift(self, quay_install):
        wrong = quay_install["target"] / "wrong-code"
        (quay_install["quay_dir"] / "config.toml").write_text(
            "[context]\n"
            f'reference_repos_root = "{wrong}"\n'
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay reference_repos_root" in result.stderr
        assert str(quay_install["target"] / "code") in result.stderr

    def test_reference_repo_mirror_missing_is_drift(self, quay_install):
        shutil.rmtree(quay_install["code_mirror"])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay reference repo mirrors" in result.stderr
        assert quay_install["quay_repo_id"] in result.stderr

    def test_missing_bare_clone_is_drift(self, quay_install):
        shutil.rmtree(quay_install["quay_bare"])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {quay_install['quay_repo_id']}" in result.stderr
        assert "bare clone missing" in result.stderr

    def test_bare_clone_origin_mismatch_is_drift(self, quay_install):
        # Re-point the bare clone's origin at something other than what the
        # values file claims — verify must catch the divergence rather than
        # silently accepting whatever is on disk.
        subprocess.run(
            ["git", "-C", str(quay_install["quay_bare"]), "remote", "set-url",
             "origin", "https://github.com/example/wrong.git"],
            check=True,
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {quay_install['quay_repo_id']} origin" in result.stderr

    def test_origin_check_ignores_insteadof_rewrite(self, quay_install):
        """`url.<base>.insteadOf` rewrites are how an operator can bridge
        an HTTPS values URL to SSH transport when the agent user only
        has SSH credentials. The bare clone's stored URL stays as the
        HTTPS values form; only `git remote get-url` rewrites it. Verify
        must compare the raw stored URL (`git config --get`), not the
        rewritten one — otherwise it fires spurious drift on a healthy
        operator setup. Regression for install-log entry #8."""
        _git(quay_install["quay_bare"], "config",
             "url.git@github.com:.insteadOf", "https://github.com/")
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} origin:" in result.stdout

    def test_unregistered_repo_id_is_drift(self, quay_install):
        # Stub now returns an empty registration list — the bare clone
        # exists but quay never had `repo add` run against it.
        _write_live_quay_stub(quay_install, QUAY_VERSION, [])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {quay_install['quay_repo_id']}" in result.stderr
        assert "not registered" in result.stderr

    def test_quay_repo_list_parse_failure_is_named_drift(self, quay_install):
        """A binary whose `repo list` returns non-JSON should produce ONE
        named drift, not N misleading 'not registered' drifts. Catches
        binary crashes / data-dir corruption / format drift early."""
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [],
            repo_list_override="garbage not json",
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay repo list" in result.stderr
        # Must NOT also produce the per-id "not registered" line — single
        # named drift is the actionable signal.
        assert "not registered with quay" not in result.stderr

    def test_quay_tick_timer_included_in_systemd_loop(self, quay_install):
        """When quay is enabled, the systemd loop must check quay-tick.timer
        too. Repoint the systemctl stub at one that returns inactive for
        quay-tick specifically, and assert verify catches it."""
        systemctl = quay_install["bin"] / "systemctl"
        systemctl.write_text(
            "#!/usr/bin/env bash\n"
            "unit=\"\"\n"
            "if [[ \"$1\" == \"show\" ]]; then\n"
            "  shift\n"
            "  props=()\n"
            "  while [[ $# -gt 0 ]]; do\n"
            "    case \"$1\" in\n"
            "      -p) props+=(\"$2\"); shift 2 ;;\n"
            "      --value) shift ;;\n"
            "      *) unit=\"$1\"; shift ;;\n"
            "    esac\n"
            "  done\n"
            "  for prop in \"${props[@]}\"; do\n"
            "    case \"$prop\" in\n"
            "      ActiveState)\n"
            "        if [[ \"$unit\" == \"quay-tick.timer\" ]]; then echo ActiveState=inactive\n"
            "        else echo ActiveState=active; fi ;;\n"
            "      LoadState)     echo LoadState=loaded ;;\n"
            "      UnitFileState) echo UnitFileState=enabled ;;\n"
            "    esac\n"
            "  done\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n"
        )
        systemctl.chmod(0o755)
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay-tick.timer" in result.stderr

    def test_reviewer_auth_mints_from_config_without_token_file(self, quay_install):
        reviewer_env = quay_install["tmp"] / "reviewer.env"
        reviewer_key = quay_install["tmp"] / "reviewer.pem"
        reviewer_env.write_text("HERMES_GH_APP_ID=1\n", encoding="utf-8")
        reviewer_key.write_text(
            "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
            encoding="utf-8",
        )
        try:
            gid = grp.getgrnam(quay_install["group"]).gr_gid
            os.chown(reviewer_env, -1, gid)
            os.chown(reviewer_key, -1, gid)
        except PermissionError:
            pass
        reviewer_env.chmod(0o640)
        reviewer_key.chmod(0o640)

        _write_curl_status_stub(quay_install["bin"] / "curl")

        systemd_dir = quay_install["tmp"] / "systemd"
        systemd_dir.mkdir(exist_ok=True)
        (systemd_dir / "quay-tick.service").write_text(
            "Environment=HERMES_REVIEWER_GH_CONFIG=/etc/hermes/reviewer.env\n"
            "RuntimeDirectory=hermes\n",
            encoding="utf-8",
        )

        result = _run_verify_quay(
            quay_install,
            env_overrides={
                "HERMES_VERIFY_REVIEWER_ENV": str(reviewer_env),
                "HERMES_VERIFY_REVIEWER_KEY": str(reviewer_key),
                "HERMES_VERIFY_SYSTEMD_DIR": str(systemd_dir),
            },
        )

        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] reviewer token helper check passes" in result.stdout
        assert "[OK] reviewer App installation scope: HTTP 200" in result.stdout
        assert "[OK] quay-tick.service reviewer config env" in result.stdout
        assert "reviewer-gh-token" not in result.stdout
        assert "reviewer-gh-token" not in result.stderr

    def test_legacy_reviewer_token_timer_is_drift(self, quay_install):
        systemd_dir = quay_install["tmp"] / "systemd"
        systemd_dir.mkdir(exist_ok=True)
        (systemd_dir / "hermes-reviewer-token.timer").write_text(
            "[Timer]\nOnUnitActiveSec=30min\n",
            encoding="utf-8",
        )

        result = _run_verify_quay(
            quay_install,
            env_overrides={"HERMES_VERIFY_SYSTEMD_DIR": str(systemd_dir)},
        )

        assert result.returncode == 1
        assert "[DRIFT] hermes-reviewer-token.timer" in result.stderr
        assert "QUAY_REVIEWER_GH_TOKEN" in result.stderr


class TestSetupHermesVerifyTagVocab:
    """Drift detection for `repos[].quay.tags`.

    The strict-reconciliation contract is the load-bearing piece: values
    is the source of truth for per-repo vocab, anything in quay not in
    values is drift. Each test starts from `quay_install` (clean install,
    no per-repo tag vocab on either side) and pokes one side of the
    comparison."""

    def _patch_repo_tags(self, install: dict, body: str) -> None:
        """Insert ``body`` (already 6-space indented) into the repo's
        `quay:` sub-block. Anchored on the fixture's `install_cmd:` line
        so the new block lands at the right depth — naive append at EOF
        would attach to the deployment-level `quay:` instead."""
        anchor = "      install_cmd: \"bun install\"\n"
        text = install["values_file"].read_text()
        assert anchor in text, "fixture shape changed; update _patch_repo_tags anchor"
        install["values_file"].write_text(text.replace(anchor, anchor + body))

    def test_clean_install_no_tag_vocab_is_ok(self, quay_install):
        # Baseline: with no repo tags configured anywhere, the per-repo
        # check must pass — empty live state matches empty desired state.
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        repo_id = quay_install["quay_repo_id"]
        assert f"[OK] quay repo {repo_id} tag vocab" in result.stdout

    def test_per_repo_drift_add_in_values_surfaces(self, quay_install):
        # values.yaml gains a tag namespace; live quay still empty → drift.
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [bonding-curve]\n",
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        repo_id = quay_install["quay_repo_id"]
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr

    def test_per_repo_drift_remove_from_values_surfaces(self, quay_install):
        # Inverse: values has no tags, live quay does → drift. Strict
        # reconciliation MUST surface what setup-hermes.sh would clear
        # on the next install run, otherwise removal silently doesn't
        # happen and the live vocab keeps growing.
        repo_id = quay_install["quay_repo_id"]
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            repo_tags={
                repo_id: {
                    "namespaces": {
                        "area": {"values": ["bonding-curve"], "required": False},
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr

    def test_per_repo_drift_value_mismatch_surfaces(self, quay_install):
        # Both sides have the namespace but with different values → drift.
        repo_id = quay_install["quay_repo_id"]
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [bonding-curve]\n",
        )
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            repo_tags={
                repo_id: {
                    "namespaces": {
                        "area": {"values": ["vesting"], "required": False},
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr
        # The drift line carries both sides so the operator can diff
        # them at a glance.
        assert "vesting" in result.stderr
        assert "bonding-curve" in result.stderr

    def test_per_repo_match_is_ok(self, quay_install):
        # End-to-end positive: values + live state agree, including
        # value sort order independence.
        repo_id = quay_install["quay_repo_id"]
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [vesting, bonding-curve]\n",  # unsorted in values
        )
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            repo_tags={
                repo_id: {
                    # Live state sorted (matches what real quay emits) —
                    # normalisation must compare equal regardless.
                    "namespaces": {
                        "area": {
                            "values": ["bonding-curve", "vesting"],
                            "required": False,
                        },
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert f"[OK] quay repo {repo_id} tag vocab" in result.stdout

    def test_unregistered_repo_skips_tag_vocab_check(self, quay_install):
        # `quay repo get-tags` on an unregistered id exits with
        # `unknown_repo`; running the tag-vocab check there would surface
        # as spurious drift. The "not registered" line is the actionable
        # signal — tag-vocab follow-up must be gated.
        repo_id = quay_install["quay_repo_id"]
        _write_live_quay_stub(quay_install, QUAY_VERSION, [])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "not registered" in result.stderr
        assert f"quay repo {repo_id} tag vocab" not in result.stderr

    def test_pre_ast87_binary_skips_tag_vocab_check(self, quay_install):
        # A quay binary without the `tags` noun must skip both
        # per-repo tag-vocab drift checks; otherwise a fork with
        # quay.version still pinned at a pre-tag-vocab release would
        # surface spurious drift on every verify run. Even WITH
        # non-empty tag vocab in values, the check must skip — the
        # install-time path also skips, so checking here would just
        # amplify the same upgrade-not-yet-taken signal.
        repo_id = quay_install["quay_repo_id"]
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [bonding-curve]\n",
        )
        _write_live_quay_stub(
            quay_install, QUAY_VERSION, [repo_id],
            tags_supported=False,
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "tag vocab" not in result.stdout
        assert "tag vocab" not in result.stderr

    def test_drift_detail_lists_per_namespace_changes(self, quay_install):
        # The `live={…} expected={…}` JSON-blob form was unreadable
        # past two namespaces; verify must emit one line per change so
        # the operator can read the full diff inline. Uses single-word
        # namespaces (`[a-z0-9]+`, no dashes — the schema validator
        # would reject dashed namespaces upstream of this drift check).
        repo_id = quay_install["quay_repo_id"]
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [vesting]\n"
            "        layer: [foo]\n",  # only in values
        )
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            repo_tags={
                repo_id: {
                    "namespaces": {
                        "area": {"values": ["vesting"], "required": True},  # required-flip
                        "risk": {"values": ["x"], "required": False},  # only in live
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        # Subject line + per-change extras both visible.
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr
        assert "+ namespace `layer`" in result.stderr
        assert "- namespace `risk`" in result.stderr
        assert "area.required" in result.stderr

    def test_malformed_live_required_flag_surfaces(self, quay_install):
        # `"required": "false"` (string instead of bool) is a real
        # failure mode if upstream's JSON contract drifts. The verifier
        # MUST surface it as drift rather than silently coercing
        # truthy-string to True (and showing [OK]).
        repo_id = quay_install["quay_repo_id"]
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            repo_tags={
                repo_id: {
                    "namespaces": {
                        # Type lie: required as string. Helpfully the
                        # JSON value would normally be true/false; this
                        # is the regression we're guarding.
                        "area": {"values": ["bonding-curve"], "required": "false"},
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr
        assert "required" in result.stderr

    def test_drift_detail_names_failing_source(self, quay_install):
        # The four failure modes (subprocess error, JSON-decode error,
        # non-dict top level, namespaces shape) used to collapse into
        # one "could not read live state" message. The tagged-failure
        # return path must name the actual source, otherwise an operator
        # chasing a quay crash and an operator chasing JSON-shape drift
        # have to start from the same useless string.
        repo_id = quay_install["quay_repo_id"]
        # Make `quay repo get-tags <id>` exit non-zero with stderr.
        quay_stub = quay_install["quay_bin"]
        quay_stub.write_text(
            "#!/usr/bin/env bash\n"
            f'if [[ "$1" == "--version" ]]; then echo "{QUAY_VERSION.removeprefix("v")}+abc"; exit 0; fi\n'
            f'if [[ "$1" == "repo" && "$2" == "list" ]]; then echo \'[{{"repo_id":"{repo_id}"}}]\'; exit 0; fi\n'
            'if [[ "$1" == "tags" && "$2" == "--help" ]]; then exit 0; fi\n'
            'if [[ "$1" == "tags" && "$2" == "get-deployment" ]]; then echo \'{"scope":"deployment","namespaces":{}}\'; exit 0; fi\n'
            'if [[ "$1" == "repo" && "$2" == "get-tags" ]]; then echo "boom: db locked" >&2; exit 7; fi\n'
            "exit 0\n"
        )
        quay_stub.chmod(0o755)
        _write_expected_quay_sha(quay_install["quay_expected_sha"], quay_stub)
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {repo_id} tag vocab" in result.stderr
        # The drift line names quay AND the stderr-head: the operator
        # immediately knows it's a binary failure, not a JSON-shape issue.
        assert "quay repo get-tags" in result.stderr
        assert "boom: db locked" in result.stderr
