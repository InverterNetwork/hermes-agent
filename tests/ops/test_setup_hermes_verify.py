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


def _run(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        list(args), cwd=cwd, check=check, capture_output=True, text=True,
    )


def _git(repo: Path, *args: str, **kw) -> subprocess.CompletedProcess:
    return _run(repo, "git", "-C", str(repo), *args, **kw)


@pytest.fixture
def install(tmp_path: Path) -> dict:
    """Build a tmp dir that mirrors a clean install of hermes-agent + state."""
    user = getpass.getuser()

    target = tmp_path / "target"
    fork = tmp_path / "fork"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    # ---- fork (the source git repo we rsynced from at install time) ----
    subprocess.run(["git", "init", "--quiet", "-b", "main", str(fork)], check=True)
    _git(fork, "config", "user.email", "f@h.l")
    _git(fork, "config", "user.name", "f")
    (fork / "README.md").write_text("seed\n")
    _git(fork, "add", "-A")
    _git(fork, "commit", "-q", "-m", "seed")
    fork_sha = _git(fork, "describe", "--always", "--dirty", "--abbrev=40").stdout.strip()

    # ---- target (the rendered install) ----
    target.mkdir()
    # HERMES_HOME mirror: prod is root:hermes 02775 (setgid). The test runs
    # as the developer so ownership is $USER:$primary_group; mode still
    # carries the setgid bit so verify's mode check stays exercised.
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

    # Symlinks at render-target root pointing into state/ (matches the
    # installer's wiring).
    for d in ("skills", "memories", "cron"):
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

    # On Linux production, useradd creates a primary group named after the
    # user so $AGENT_USER works as both owner and group. On macOS test, the
    # user's primary group is "staff" — feed the actual gid name through
    # the override so the auth-dir owner check has matching expectations.
    primary_group = grp.getgrgid(os.getegid()).gr_name

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
        target = install["target"]
        ws = target / "upstream-workspace"
        subprocess.run(["git", "clone", "--quiet", str(install["fork"]), str(ws)], check=True)
        _git(ws, "remote", "add", "upstream",
             "https://github.com/nousresearch/hermes-agent.git")
        # origin still points at the local fork path → drift.
        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] upstream-workspace origin" in result.stderr

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


QUAY_VERSION = "v0.1.0"  # tag-shaped, with leading v, as in deploy.values.yaml
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
) -> None:
    """Stub `quay` binary — emits version on `--version`, JSON list on `repo list`.

    `version` is tag-shaped (`v0.1.0`); the stub strips the leading `v` and
    appends a fake build SHA to mirror what older real binaries emitted
    (`${pkg.version}+${shortSHA}`, see scripts/embed.ts in lafawnduh1966/quay).
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
    tags_help_block = (
        'if [[ "$1" == "tags" && "$2" == "--help" ]]; then exit 0; fi\n'
        if tags_supported
        else 'if [[ "$1" == "tags" && "$2" == "--help" ]]; then echo "unknown_command" >&2; exit 1; fi\n'
    )
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{semver}+abc1234"; exit 0; fi\n'
        f'if [[ "$1" == "repo" && "$2" == "list" ]]; then echo \'{repo_list_payload}\'; exit 0; fi\n'
        + tags_help_block
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
    quay_dir.mkdir(mode=0o755)
    (quay_dir / "config.toml").write_text("# stub\n")
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
    code_mirror = target / "code" / repo_id
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

    install["values_file"] = fork / "deploy.values.yaml"
    install["quay_dir"] = quay_dir
    install["quay_bin"] = quay_stub
    install["quay_expected_sha"] = quay_expected_sha
    install["quay_repo_id"] = repo_id
    install["quay_repo_url"] = repo_url
    install["quay_bare"] = bare
    install["quay_wrapper"] = quay_wrapper
    install["quay_runner"] = quay_runner
    install["quay_profile"] = quay_profile
    install["bun_bin"] = bun_stub
    return install


def _quay_env(install: dict) -> dict[str, str]:
    return {
        "HERMES_VERIFY_QUAY_BIN": str(install["quay_bin"]),
        "HERMES_VERIFY_QUAY_WRAPPER": str(install["quay_wrapper"]),
        "HERMES_VERIFY_QUAY_RUNNER": str(install["quay_runner"]),
        "HERMES_VERIFY_QUAY_PROFILE": str(install["quay_profile"]),
        "HERMES_VERIFY_RUNTIME_DIR": str(install["bin"]),
    }


def _run_verify_quay(install: dict, *, env_overrides: dict[str, str] | None = None) -> SimpleNamespace:
    overrides = _quay_env(install)
    if env_overrides:
        overrides.update(env_overrides)
    return _run_verify(install, values=install["values_file"], env_overrides=overrides)


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
        assert f"[OK] quay repo {quay_install['quay_repo_id']} ownership:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} origin:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} registered" in result.stdout
        assert "[OK] quay-as-hermes wrapper:" in result.stdout
        assert "[OK] quay profile.d drop-in:" in result.stdout

    def test_clean_quay_install_passes_via_bash_wrapper(self, quay_install):
        """Smoke through bash setup-hermes.sh --verify with the values flag
        wired through, so wrapper-side argv forwarding stays covered for the
        quay codepath too."""
        result = _run_verify_quay_via_wrapper(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay binary:" in result.stdout

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
        reviewer_env.chmod(0o640)
        reviewer_key.chmod(0o640)

        curl = quay_install["bin"] / "curl"
        curl.write_text("#!/usr/bin/env bash\nprintf '200'\n", encoding="utf-8")
        curl.chmod(0o755)

        systemd_dir = quay_install["tmp"] / "systemd"
        systemd_dir.mkdir()
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
        systemd_dir.mkdir()
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
    """Drift detection for `repos[].quay.tags` and `quay.tag_namespaces`.

    The strict-reconciliation contract is the load-bearing piece: values
    is the source of truth, anything in quay not in values is drift.
    Each test starts from `quay_install` (clean install, no tag vocab on
    either side) and pokes one side of the comparison."""

    def _patch_repo_tags(self, install: dict, body: str) -> None:
        """Insert ``body`` (already 6-space indented) into the repo's
        `quay:` sub-block. Anchored on the fixture's `install_cmd:` line
        so the new block lands at the right depth — naive append at EOF
        would attach to the deployment-level `quay:` instead."""
        anchor = "      install_cmd: \"bun install\"\n"
        text = install["values_file"].read_text()
        assert anchor in text, "fixture shape changed; update _patch_repo_tags anchor"
        install["values_file"].write_text(text.replace(anchor, anchor + body))

    def _patch_deployment_tags(self, install: dict, body: str) -> None:
        """Insert ``body`` (already 2-space indented) into the top-level
        `quay:` block. Anchored on the `version:` line for the same
        reason as `_patch_repo_tags`."""
        anchor_re = re.compile(r"^  version: \"v[0-9.]+\"\n", re.MULTILINE)
        text = install["values_file"].read_text()
        new, n = anchor_re.subn(lambda m: m.group(0) + body, text, count=1)
        assert n == 1, "fixture shape changed; update _patch_deployment_tags anchor"
        install["values_file"].write_text(new)

    def test_clean_install_no_tag_vocab_is_ok(self, quay_install):
        # Baseline: with no tags configured anywhere, both checks must
        # pass — empty live state matches empty desired state.
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        repo_id = quay_install["quay_repo_id"]
        assert f"[OK] quay repo {repo_id} tag vocab" in result.stdout
        assert "[OK] quay deployment tag vocab" in result.stdout

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

    def test_deployment_drift_add_in_values_surfaces(self, quay_install):
        self._patch_deployment_tags(
            quay_install,
            "  tag_namespaces:\n"
            "    risk:\n"
            "      values: [pii]\n",
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay deployment tag vocab" in result.stderr

    def test_deployment_drift_remove_from_values_surfaces(self, quay_install):
        repo_id = quay_install["quay_repo_id"]
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            deployment_tags={
                "namespaces": {
                    "tasktype": {
                        "values": ["bugfix"],
                        "required": True,
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay deployment tag vocab" in result.stderr

    def test_deployment_required_flag_drift_surfaces(self, quay_install):
        # Same values, same namespace; only the `required` flag differs.
        # Strict reconciliation must catch this — `required: true` vs
        # `false` is the difference between every ticket needing a
        # tasktype tag and none needing one.
        repo_id = quay_install["quay_repo_id"]
        self._patch_deployment_tags(
            quay_install,
            "  tag_namespaces:\n"
            "    tasktype:\n"
            "      required: true\n"
            "      values: [bugfix]\n",
        )
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            deployment_tags={
                "namespaces": {
                    "tasktype": {
                        "values": ["bugfix"],
                        "required": False,
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay deployment tag vocab" in result.stderr

    def test_deployment_match_is_ok(self, quay_install):
        repo_id = quay_install["quay_repo_id"]
        self._patch_deployment_tags(
            quay_install,
            "  tag_namespaces:\n"
            "    tasktype:\n"
            "      required: true\n"
            "      values: [bugfix, refactor]\n",
        )
        _write_live_quay_stub(
            quay_install,
            QUAY_VERSION,
            [repo_id],
            deployment_tags={
                "namespaces": {
                    "tasktype": {
                        "values": ["bugfix", "refactor"],
                        "required": True,
                    },
                },
            },
        )
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay deployment tag vocab" in result.stdout

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

    def test_pre_ast87_binary_skips_both_tag_vocab_checks(self, quay_install):
        # A quay binary without the `tags` noun must skip both
        # per-repo and deployment tag-vocab drift checks; otherwise a
        # fork with quay.version still pinned at a pre-tag-vocab
        # release would surface spurious drift on every verify run.
        # Even WITH non-empty tag vocab in values, the check must skip
        # — the install-time path also skips, so checking here would
        # just amplify the same upgrade-not-yet-taken signal.
        repo_id = quay_install["quay_repo_id"]
        self._patch_repo_tags(
            quay_install,
            "      tags:\n"
            "        area: [bonding-curve]\n",
        )
        self._patch_deployment_tags(
            quay_install,
            "  tag_namespaces:\n"
            "    risk:\n"
            "      values: [pii]\n",
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
