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
    for d in ("sessions", "logs", "cache"):
        (target / d).mkdir()

    # Symlinks at render-target root pointing into state/ (matches the
    # installer's wiring).
    for d in ("skills", "memories", "cron"):
        (state / d).mkdir(exist_ok=True)
        os.symlink(f"state/{d}", target / d)

    # RUNTIME_VERSION matches fork HEAD on a clean install.
    (target / "RUNTIME_VERSION").write_text(fork_sha)

    # Stub systemctl in PATH bin so verify thinks the timers are loaded.
    # Also captures argv for assertions if a future test needs them.
    systemctl_log = tmp_path / "systemctl.log"
    systemctl = bin_dir / "systemctl"
    systemctl.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$*" >> {systemctl_log}\n'
        "if [[ \"$1\" == \"show\" ]]; then\n"
        "  printf 'active\\nloaded\\nenabled\\n'\n"
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
        """auth/ + github-app.env present → verify also runs the helper smoke
        and the credential-helper config check, and both pass on a clean
        install."""
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

        result = _run_verify(install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "token helper check passes" in result.stdout
        assert "state credential helper configured" in result.stdout

    def test_app_auth_missing_credential_helper_is_drift(self, install):
        target = install["target"]
        auth = target / "auth"
        auth.mkdir(mode=0o750)
        (auth / "github-app.env").write_text("HERMES_GH_APP_ID=1\n")
        (auth / "github-app.env").chmod(0o640)
        # No credential helper configured on the state repo → drift.
        result = _run_verify(install)
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

        result = _run_verify(install)
        assert result.returncode == 1
        assert "[DRIFT] token helper check" in result.stderr

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
