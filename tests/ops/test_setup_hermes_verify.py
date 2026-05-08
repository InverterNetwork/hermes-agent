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


def _run_verify(
    install: dict,
    *extra: str,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PATH"] = f"{install['bin']}{os.pathsep}{env['PATH']}"
    # Test fixture can't chown to root, so tell verify to expect $USER on both
    # sides. The check logic still runs end-to-end.
    env["HERMES_VERIFY_EXPECT_RAILS_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_GROUP"] = install["group"]
    if env_overrides:
        env.update(env_overrides)
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

        result = _run_verify(install, "--auth-method", "app")
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
        result = _run_verify(install, "--auth-method", "app")
        assert result.returncode == 1
        assert "[DRIFT] state credential helper" in result.stderr

    def test_app_auth_helper_failure_is_drift(self, install):
        """Replace the token helper with one that exits 2 on `check` — verify
        must report the drift instead of swallowing the failure."""
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


def _write_quay_stub(
    path: Path,
    version: str,
    registered_ids: list[str],
    repo_list_override: str | None = None,
) -> None:
    """Stub `quay` binary — emits version on `--version`, JSON list on `repo list`.

    `version` is the tag-shaped pin (`v0.1.0`); the stub strips the leading
    `v` and appends a fake build SHA to mirror what the real binary emits
    (`${pkg.version}+${shortSHA}`, see scripts/embed.ts in lafawnduh1966/quay).
    The verify path strips the `v` from the pin before comparing, so the
    test exercises that real format end-to-end.

    `repo_list_override` lets a caller substitute the `repo list` payload
    directly — useful for testing parse-failure paths with non-JSON output.
    Real binary keys entries by `repo_id` (not `id`); the default branch
    mirrors that shape."""
    if repo_list_override is None:
        repos_json = ",".join(f'{{"repo_id": "{i}"}}' for i in registered_ids)
        repo_list_payload = f"[{repos_json}]"
    else:
        repo_list_payload = repo_list_override
    semver = version.removeprefix("v")
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{semver}+abc1234"; exit 0; fi\n'
        f'if [[ "$1" == "repo" && "$2" == "list" ]]; then echo \'{repo_list_payload}\'; exit 0; fi\n'
        "exit 0\n"
    )
    path.chmod(0o755)


@pytest.fixture
def quay_install(install: dict, tmp_path: Path) -> dict:
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
        "    slash_command_name: t\n"
        "    slash_command_description: t\n"
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

    # values_helper.py imports pyyaml. The system `python3` on dev macOS
    # often lacks it, so shadow `python3` in PATH with a shim that
    # delegates to whichever interpreter is running pytest (it has pyyaml
    # because the repo's pyproject pulls it in for tests).
    py_shim = bin_dir / "python3"
    if not py_shim.exists():
        py_shim.write_text(f'#!/usr/bin/env bash\nexec {sys.executable} "$@"\n')
        py_shim.chmod(0o755)

    # Operator-invocation glue: wrapper at /usr/local/bin/quay-as-hermes
    # and login-shell drop-in at /etc/profile.d/quay-data-dir.sh in prod.
    # The test fixture redirects both via HERMES_VERIFY_QUAY_WRAPPER /
    # HERMES_VERIFY_QUAY_PROFILE so we can poke at them without touching
    # /usr/local/bin or /etc on the dev machine.
    quay_wrapper = bin_dir / "quay-as-hermes"
    quay_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n")
    quay_wrapper.chmod(0o755)
    quay_profile = bin_dir / "quay-data-dir.sh"
    quay_profile.write_text("# stub\n")
    quay_profile.chmod(0o644)

    # Stale ~/.quay/ probe: redirect to a tmp path so we never touch the
    # developer's real home dir. Default state is "no stale dir" — tests
    # that assert the drift path mkdir() it explicitly.
    stale_quay_dir = tmp_path / "stale-quay"

    install["values_file"] = fork / "deploy.values.yaml"
    install["quay_dir"] = quay_dir
    install["quay_bin"] = quay_stub
    install["quay_repo_id"] = repo_id
    install["quay_repo_url"] = repo_url
    install["quay_bare"] = bare
    install["quay_wrapper"] = quay_wrapper
    install["quay_profile"] = quay_profile
    install["stale_quay_dir"] = stale_quay_dir
    return install


def _run_verify_quay(install: dict, *extra: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PATH"] = f"{install['bin']}{os.pathsep}{env['PATH']}"
    env["HERMES_VERIFY_EXPECT_RAILS_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_OWNER"] = install["user"]
    env["HERMES_VERIFY_EXPECT_AGENT_GROUP"] = install["group"]
    env["HERMES_VERIFY_QUAY_BIN"] = str(install["quay_bin"])
    env["HERMES_VERIFY_QUAY_WRAPPER"] = str(install["quay_wrapper"])
    env["HERMES_VERIFY_QUAY_PROFILE"] = str(install["quay_profile"])
    env["HERMES_VERIFY_STALE_QUAY_DIR"] = str(install["stale_quay_dir"])
    return subprocess.run(
        [
            "bash", str(SCRIPT),
            "--verify",
            "--target", str(install["target"]),
            "--fork", str(install["fork"]),
            "--user", install["user"],
            "--values", str(install["values_file"]),
            *extra,
        ],
        env=env, check=False, capture_output=True, text=True,
    )


class TestSetupHermesVerifyQuay:
    def test_clean_quay_install_passes(self, quay_install):
        result = _run_verify_quay(quay_install)
        assert result.returncode == 0, result.stderr + "\n" + result.stdout
        assert "[OK] quay binary:" in result.stdout
        assert "[OK] quay binary version:" in result.stdout
        assert "[OK] quay data dir ownership:" in result.stdout
        assert "[OK] quay config.toml:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} ownership:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} origin:" in result.stdout
        assert f"[OK] quay repo {quay_install['quay_repo_id']} registered" in result.stdout
        assert "[OK] no stale quay data dir at" in result.stdout
        assert "[OK] quay-as-hermes wrapper:" in result.stdout
        assert "[OK] quay profile.d drop-in:" in result.stdout

    def test_stale_quay_data_dir_is_drift(self, quay_install):
        # Mimic the krustentier failure mode: a sudo-without-env invocation
        # left an empty ~/.quay/ behind. Verify must surface it so the
        # operator re-runs the installer to reconcile.
        quay_install["stale_quay_dir"].mkdir()
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] stale quay data dir" in result.stderr

    def test_stale_quay_dir_symlink_is_also_drift(self, quay_install):
        # Symlink-shaped workarounds (operator pointed ~/.quay at the
        # canonical dir to make naked `quay …` invocations work) used to
        # be exempted on the install side and would never reconcile,
        # leaving verify in permanent drift. The contract is now
        # consistent: any presence at all — symlink included — is drift,
        # and the install reconciler removes the link.
        quay_install["stale_quay_dir"].symlink_to(quay_install["quay_dir"])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] stale quay data dir" in result.stderr

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

    def test_quay_version_mismatch_is_drift(self, quay_install):
        # Stub now reports a different tag than the values file claims —
        # the binary on disk is lying about its tag, which is exactly the
        # drift this check exists to catch.
        _write_quay_stub(quay_install["quay_bin"], "v9.9.9", [quay_install["quay_repo_id"]])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay binary version" in result.stderr

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
        _write_quay_stub(quay_install["quay_bin"], QUAY_VERSION, [])
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert f"[DRIFT] quay repo {quay_install['quay_repo_id']}" in result.stderr
        assert "not registered" in result.stderr

    def test_quay_repo_list_parse_failure_is_named_drift(self, quay_install):
        """A binary whose `repo list` returns non-JSON should produce ONE
        named drift, not N misleading 'not registered' drifts. Catches
        binary crashes / data-dir corruption / format drift early."""
        _write_quay_stub(
            quay_install["quay_bin"],
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
            "  prop=\"\"\n"
            "  shift\n"
            "  while [[ $# -gt 0 ]]; do\n"
            "    case \"$1\" in\n"
            "      -p) prop=\"$2\"; shift 2 ;;\n"
            "      --value) shift ;;\n"
            "      *) unit=\"$1\"; shift ;;\n"
            "    esac\n"
            "  done\n"
            "  case \"$prop\" in\n"
            "    ActiveState)\n"
            "      if [[ \"$unit\" == \"quay-tick.timer\" ]]; then echo inactive\n"
            "      else echo active; fi ;;\n"
            "    LoadState)     echo loaded ;;\n"
            "    UnitFileState) echo enabled ;;\n"
            "  esac\n"
            "  exit 0\n"
            "fi\n"
            "exit 0\n"
        )
        systemctl.chmod(0o755)
        result = _run_verify_quay(quay_install)
        assert result.returncode == 1
        assert "[DRIFT] quay-tick.timer" in result.stderr
