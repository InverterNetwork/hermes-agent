"""End-to-end behavior of `setup-hermes.sh --verify` against a tmp install.

The verify path is a read-only health check, so the cheapest meaningful
coverage is to build a fixture that mirrors a clean install on disk and
assert each drift case yields a named [DRIFT] subject + non-zero exit.

Production verify expects rails owned by root and state owned by the agent
user. Tests can't actually chown to root, so the script honors two internal
env-var overrides (HERMES_VERIFY_EXPECT_RAILS_OWNER /
HERMES_VERIFY_EXPECT_AGENT_OWNER) — both set to $USER here so the fixture
exercises the real check logic with whatever user the test runner is.
"""

from __future__ import annotations

import getpass
import grp
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "installer" / "setup-hermes.sh"


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
    # Helper + venv python: the helper is a stub that succeeds on `check`.
    helper = rails / "installer" / "hermes_github_token.py"
    helper.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        "if len(sys.argv) > 1 and sys.argv[1] == 'check':\n"
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

    # Symlinks at render-target root pointing into state/ (matches the
    # installer's wiring).
    for d in ("skills", "memories", "cron"):
        (state / d).mkdir(exist_ok=True)
        os.symlink(f"state/{d}", target / d)

    # RUNTIME_VERSION matches fork HEAD on a clean install.
    (target / "RUNTIME_VERSION").write_text(fork_sha)

    # Stub systemctl in PATH bin so verify thinks the timers are loaded.
    # Verify invokes `systemctl show -p <Prop> --value <unit>` once per
    # property, so the stub returns a single value matching the requested
    # prop — same wire format as real systemctl.
    systemctl_log = tmp_path / "systemctl.log"
    systemctl = bin_dir / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" >> {systemctl_log}\n'
        "if [[ \"$1\" == \"show\" ]]; then\n"
        "  prop=\"\"\n"
        "  shift\n"
        "  while [[ $# -gt 0 ]]; do\n"
        "    case \"$1\" in\n"
        "      -p) prop=\"$2\"; shift 2 ;;\n"
        "      --value) shift ;;\n"
        "      *) shift ;;\n"
        "    esac\n"
        "  done\n"
        "  case \"$prop\" in\n"
        "    ActiveState)   echo active ;;\n"
        "    LoadState)     echo loaded ;;\n"
        "    UnitFileState) echo enabled ;;\n"
        "  esac\n"
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


def _run_verify(install: dict, *extra: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PATH"] = f"{install['bin']}{os.pathsep}{env['PATH']}"
    # Test fixture can't chown to root, so tell verify to expect $USER on both
    # sides. The check logic still runs end-to-end.
    env["HERMES_VERIFY_EXPECT_RAILS_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_GROUP"] = install["group"]
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

    def test_quiet_suppresses_ok_lines(self, install):
        result = _run_verify(install, "--quiet")
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

    def test_app_auth_clean_passes(self, install):
        """--auth-method app + every artefact in place → verify runs the
        helper smoke and credential-helper check, and both pass."""
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750)
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

        result = _run_verify(install, "--auth-method", "app")
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "token helper check passes" in result.stdout
        assert "state credential helper configured" in result.stdout

    def test_app_auth_missing_credential_helper_is_drift(self, install):
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750)
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)
        (auth / "github-app.pem").write_text("-----BEGIN RSA PRIVATE KEY-----\n")
        (auth / "github-app.pem").chmod(0o640)
        # No credential helper configured on the state repo → drift.
        result = _run_verify(install, "--auth-method", "app")
        assert result.returncode == 1
        assert "[DRIFT] state credential helper" in result.stderr

    def test_app_auth_helper_failure_is_drift(self, install):
        """Replace the token helper with one that exits 2 on `check` — verify
        must report the drift instead of swallowing the failure."""
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750)
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

        result = _run_verify(install, "--auth-method", "app")
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
        result = _run_verify(install, "--auth-method", "app")
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
        auth.mkdir(mode=0o750)
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

        result = _run_verify(install, "--auth-method", "app")
        assert result.returncode == 1
        assert "[DRIFT] auth file github-app.pem" in result.stderr

    def test_inactive_timer_is_drift(self, install, tmp_path):
        """ActiveState=inactive must report drift. Substring matching
        ('inactive' contains 'active') quietly accepted stopped timers
        before the review fix."""
        # Replace the systemctl stub with one that reports the timer
        # inactive but still loaded+enabled. Substring matching used to let
        # this state through ("inactive" contains "active") — exact-equality
        # parsing in the script now catches it.
        systemctl = install["bin"] / "systemctl"
        systemctl.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$1\" == \"show\" ]]; then\n"
            "  prop=\"\"\n"
            "  shift\n"
            "  while [[ $# -gt 0 ]]; do\n"
            "    case \"$1\" in\n"
            "      -p) prop=\"$2\"; shift 2 ;;\n"
            "      --value) shift ;;\n"
            "      *) shift ;;\n"
            "    esac\n"
            "  done\n"
            "  case \"$prop\" in\n"
            "    ActiveState)   echo inactive ;;\n"
            "    LoadState)     echo loaded ;;\n"
            "    UnitFileState) echo enabled ;;\n"
            "  esac\n"
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
