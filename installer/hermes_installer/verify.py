"""Read-only health check for a rendered hermes-agent install.

Pure port of ``setup-hermes.sh --verify``. Inspects the live install for
drift, prints ``[OK]``/``[DRIFT]`` lines, exits 0 (no drift) or 1 (drift
detected). Never writes — operators run it before/after deploys to catch
ownership, perms, symlink, git-config, RUNTIME_VERSION, systemd-unit, and
auth-helper drift in one shot.

Each check is independent (no early exit); the closing summary always prints.

values-file queries shell out to ``python3 $values_helper`` for byte-for-byte
parity with the bash original — verify never imports the helper in-process.
"""

from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import IO
from urllib.parse import urlparse

from .caddy import caddyfile_has_atlas_hub_route


_REFERENCE_REPOS_MIN_QUAY_VERSION = (0, 3, 10)


# ---------------------------------------------------------------------------
# Args + counters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VerifyArgs:
    fork: Path
    target: Path
    user: str
    auth_method: str = "none"
    quiet: bool = False
    values: Path | None = None
    gh_api_base: str | None = None


@dataclass
class _State:
    args: VerifyArgs
    out: IO[str]
    err: IO[str]
    rails_owner: str
    agent_owner: str
    agent_group: str
    quay_bin: str
    quay_wrapper: str
    quay_runner: str
    quay_profile: str
    atlas_bin: str
    atlas_wrapper: str
    atlas_profile: str
    runtime_dir: str
    systemd_dir: str
    upstream_sync_script: str
    upstream_sync_env: str
    reviewer_env: str
    reviewer_key: str
    caddyfile: str
    values_file: Path
    values_helper: Path
    quay_version: str
    atlas_version: str
    atlas_ai_mode: str
    atlas_hub_enabled: bool = False
    codex_required: bool = False
    quay_tags_supported: bool = False
    quay_serve_supported: bool = False
    total: int = 0
    drift: int = 0

    def v_ok(self, msg: str) -> None:
        self.total += 1
        if not self.args.quiet:
            print(f"[OK] {msg}", file=self.out, flush=True)

    def v_drift(self, subject: str, detail: str) -> None:
        self.total += 1
        self.drift += 1
        print(f"[DRIFT] {subject}: {detail}", file=self.err, flush=True)


# ---------------------------------------------------------------------------
# Stat / git helpers — verify-only counterparts to setup-hermes.sh's helpers
# ---------------------------------------------------------------------------


def _run(
    cmd: list[str], *, timeout: float = 30, input: str | None = None,
) -> tuple[int, str, str]:
    """Subprocess shim with the verify-wide error contract: capture text,
    never raise on non-zero, swallow OSError/SubprocessError into
    ``(1, "", "")`` so callers can branch on rc without try/except."""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False,
            timeout=timeout, input=input,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except (OSError, subprocess.SubprocessError):
        return 1, "", ""


def _info_from_st(st: os.stat_result) -> tuple[str, str, str]:
    """Format ``(mode, owner, group)`` from a fresh stat result. Mode matches
    bash's ``_mode`` helper (octal, leading zero stripped: ``"750"`` /
    ``"2775"``); missing pwd/grp entries become empty strings (preserves the
    bash behavior of treating an unknown UID as no-name)."""
    mode = format(st.st_mode & 0o7777, "o")
    try:
        owner = pwd.getpwuid(st.st_uid).pw_name
    except KeyError:
        owner = ""
    try:
        group = grp.getgrgid(st.st_gid).gr_name
    except KeyError:
        group = ""
    return mode, owner, group


def _stat_info(path: Path) -> tuple[str, str, str] | None:
    """One stat → ``(mode, owner, group)``, or ``None`` on stat failure."""
    try:
        st = path.stat()
    except OSError:
        return None
    return _info_from_st(st)


def _owner(path: Path) -> str:
    info = _stat_info(path)
    return info[1] if info else ""


def _group(path: Path) -> str:
    info = _stat_info(path)
    return info[2] if info else ""


def _mode(path: Path) -> str:
    info = _stat_info(path)
    return info[0] if info else ""


def _current_user_name() -> str:
    try:
        return pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        return ""


def _sudo_prefix_for(target_user: str) -> list[str]:
    """``["sudo", "-u", target_user]`` when running as root and not already
    that user; ``[]`` otherwise. Mirrors the bash drop-privs guard."""
    if target_user and os.geteuid() == 0 and _current_user_name() != target_user:
        return ["sudo", "-u", target_user]
    return []


def _git_as_owner(repo: Path, *args: str) -> tuple[int, str, str]:
    """Run ``git -C repo args``; when running as root and the repo is owned by
    a different user, drop privileges via ``sudo -u <owner>`` to sidestep
    git's ``safe.directory`` guard.

    Returns (returncode, stdout, stderr)."""
    owner = _owner(repo / ".git") or _owner(repo)
    return _run([*_sudo_prefix_for(owner), "git", "-C", str(repo), *args])


def _git_str(repo: Path, *args: str) -> str:
    """``_git_as_owner`` + ``out.strip() if rc == 0 else ""`` — the
    rc-then-strip-or-empty pattern that wraps every stored-value git probe."""
    rc, out, _ = _git_as_owner(repo, *args)
    return out.strip() if rc == 0 else ""


def _first_stdout_line(cmd: list[str], timeout: float = 10) -> str:
    """Run ``cmd``, return its first stdout line — regardless of exit code.

    Mirrors the bash ``$bin --version 2>/dev/null | head -n1 || true`` shape
    used for ``--version`` probes: a binary that prints its version then exits
    non-zero (rare, but legal) still surfaces the version string to the
    substring compare."""
    _rc, out, _ = _run(cmd, timeout=timeout)
    return (out.splitlines() or [""])[0]


def _read_shell_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return values
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        try:
            tokens = shlex.split(line, comments=True, posix=True)
        except ValueError:
            tokens = []
        token = tokens[0] if tokens else line
        key, sep, value = token.partition("=")
        if sep and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            values[key] = value
    return values


def _pid_is_running(pid_text: str) -> bool:
    try:
        pid = int(pid_text.strip())
    except ValueError:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except RuntimeError:
        # tests/conftest.py guards real signal delivery; verify only needs a
        # liveness probe, so a blocked probe is treated as not running.
        return False
    except OSError:
        return False
    return True


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
    except OSError:
        return ""
    return h.hexdigest()


def _read_expected_sha256(path: Path) -> str:
    try:
        first = (path.read_text(encoding="utf-8").split() or [""])[0].lower()
    except (OSError, UnicodeDecodeError):
        return ""
    return first if re.fullmatch(r"[0-9a-f]{64}", first) else ""


def _git_config_get(repo: Path, *args: str) -> str:
    """Read a stored git config value (literal — sidesteps insteadOf rewrites)
    via ``_git_as_owner``. Returns '' on missing key or git failure."""
    return _git_str(repo, "config", "--get", *args)


def _git_config_get_all(repo: Path, *args: str) -> list[str]:
    """Read all stored git config values for a key. Returns [] on failure."""
    out = _git_str(repo, "config", "--get-all", *args)
    return [line.strip() for line in out.splitlines() if line.strip()]


def _agent_home(s: _State) -> Path:
    override = os.environ.get("HERMES_VERIFY_AGENT_HOME")
    if override:
        return Path(override)
    try:
        return Path(pwd.getpwnam(s.agent_owner).pw_dir)
    except KeyError:
        return s.args.target.parent


def _git_config_file_get_as_agent(s: _State, config_file: Path, key: str) -> str:
    rc, out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "git", "config", "--file", str(config_file), "--get", key,
    ])
    return out.strip() if rc == 0 else ""


def _git_config_file_get_all_as_agent(s: _State, config_file: Path, key: str) -> list[str]:
    rc, out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "git", "config", "--file", str(config_file), "--get-all", key,
    ])
    if rc != 0:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def _v_check_clone_basics(
    s: _State, label: str, clone_path: Path, expected_url: str, expected_owner: str,
) -> None:
    """Verify-side counterpart to bash ``_v_check_clone_basics`` — emits
    v_ok/v_drift for owner + literal stored origin URL on a clone."""
    owner = _owner(clone_path / ".git") or _owner(clone_path)
    if owner == expected_owner:
        s.v_ok(f"{label} ownership: {owner}")
    else:
        s.v_drift(f"{label} ownership", f"expected {expected_owner}, got {owner}")
    origin = _git_config_get(clone_path, "remote.origin.url")
    if origin == expected_url:
        s.v_ok(f"{label} origin: {origin}")
    else:
        s.v_drift(
            f"{label} origin",
            f"got '{origin or '?'}' (expected '{expected_url}')",
        )


def _v_check_github_app_clone_auth(
    s: _State,
    label: str,
    clone_path: Path,
    *,
    required: bool,
) -> None:
    """Check GitHub App auth config on a GitHub clone.

    The credential helper handles HTTPS remotes. The url rewrites are just as
    important for Quay worktrees: install commands can run `git submodule
    update`, and .gitmodules may still contain SSH GitHub URLs.
    """
    if not required:
        return

    helper = _git_config_get(
        clone_path, "--local", "credential.https://github.com.helper",
    )
    if helper:
        s.v_ok(f"{label} GitHub App credential helper configured")
    else:
        s.v_drift(
            f"{label} GitHub App credential helper",
            "missing credential.https://github.com.helper",
        )

    expected = ("git@github.com:", "ssh://git@github.com/")
    actual = set(_git_config_get_all(
        clone_path, "--local", "url.https://github.com/.insteadOf",
    ))
    missing = [v for v in expected if v not in actual]
    if not missing:
        s.v_ok(f"{label} GitHub SSH-to-HTTPS rewrites configured")
    else:
        s.v_drift(
            f"{label} GitHub SSH-to-HTTPS rewrites",
            f"missing {missing!r}",
        )


def _check_quay_github_app_gitconfig(s: _State, *, required: bool) -> None:
    """Check the Git include used by fresh Quay worktrees.

    Quay task bootstrap runs in disposable worktrees, and GitHub submodules can
    still be declared with SSH URLs. The setup-time code therefore writes a
    root-owned include file with HTTPS App auth + SSH-to-HTTPS rewrites, then
    wires that file into the agent user's global gitconfig for Quay gitdirs.
    """
    if not required:
        return

    include_file = s.args.target / "auth" / "quay-github-app.gitconfig"
    agent_gitconfig = _agent_home(s) / ".gitconfig"

    if include_file.is_file():
        _check_mode_owner(
            s,
            "quay GitHub App gitconfig",
            _stat_info(include_file),
            "640",
            s.rails_owner,
            s.agent_group,
        )
    else:
        s.v_drift(
            "quay GitHub App gitconfig",
            f"missing: {include_file}",
        )
        return

    helper = _git_config_file_get_as_agent(
        s, include_file, "credential.https://github.com.helper",
    )
    if helper:
        s.v_ok("quay GitHub App gitconfig credential helper configured")
    else:
        s.v_drift(
            "quay GitHub App gitconfig credential helper",
            "missing credential.https://github.com.helper",
        )

    expected_rewrites = ("git@github.com:", "ssh://git@github.com/")
    actual_rewrites = set(_git_config_file_get_all_as_agent(
        s, include_file, "url.https://github.com/.insteadOf",
    ))
    missing_rewrites = [v for v in expected_rewrites if v not in actual_rewrites]
    if not missing_rewrites:
        s.v_ok("quay GitHub App gitconfig SSH-to-HTTPS rewrites configured")
    else:
        s.v_drift(
            "quay GitHub App gitconfig SSH-to-HTTPS rewrites",
            f"missing {missing_rewrites!r}",
        )

    expected_includes = (
        f"gitdir:{s.args.target}/quay/worktrees/",
        f"gitdir:{s.args.target}/quay/repos/",
    )
    for pattern in expected_includes:
        key = f"includeIf.{pattern}.path"
        actual = _git_config_file_get_as_agent(s, agent_gitconfig, key)
        if actual == str(include_file):
            s.v_ok(f"agent gitconfig include {pattern}")
        else:
            s.v_drift(
                f"agent gitconfig include {pattern}",
                f"got '{actual or '?'}' (expected '{include_file}')",
            )


def _check_mode_owner(
    s: _State,
    label: str,
    info: tuple[str, str, str] | None,
    expected_mode: str,
    expected_owner: str,
    expected_group: str | None = None,
) -> None:
    """Emit ``[OK] {label}: {mode} {owner[:group]}`` or the matching drift.

    Pass the result of ``_stat_info(path)`` as ``info``; ``None`` means the
    path can't be stat'd and is reported as drift (callers that need a more
    specific message — "missing or non-executable" — should branch before
    calling this)."""
    if info is None:
        s.v_drift(label, "stat failed")
        return
    mode, owner, group = info
    if expected_group is not None:
        owner_disp = f"{owner}:{group}"
        expected_disp = f"{expected_owner}:{expected_group}"
    else:
        owner_disp = owner
        expected_disp = expected_owner
    if mode == expected_mode and owner_disp == expected_disp:
        s.v_ok(f"{label}: {mode} {owner_disp}")
    else:
        s.v_drift(
            label,
            f"mode={mode} owner={owner_disp} (expected {expected_mode} {expected_disp})",
        )


# ---------------------------------------------------------------------------
# values_helper subprocess wrappers — keep python3 (system) interpreter to
# match the bash, which deliberately avoids depending on the rails venv.
# ---------------------------------------------------------------------------


def _values_get(values_file: Path, values_helper: Path, dotted: str) -> str:
    if not values_file.is_file() or not values_helper.is_file():
        return ""
    rc, out, _ = _run(
        ["python3", str(values_helper), "--values", str(values_file), "get", dotted],
    )
    return out.strip() if rc == 0 else ""


def _values_helper_run(
    values_helper: Path, *args: str, values_file: Path | None = None,
) -> tuple[int, str, str]:
    """Run any values_helper subcommand; ``values_file`` flag is prepended
    when provided (matches bash invocation shape)."""
    cmd = ["python3", str(values_helper)]
    if values_file is not None:
        cmd += ["--values", str(values_file)]
    cmd += list(args)
    return _run(cmd, timeout=60)


# ---------------------------------------------------------------------------
# Per-section checks
# ---------------------------------------------------------------------------


def _check_target(s: _State) -> bool:
    """Returns False when target is missing — caller short-circuits the rest."""
    target = s.args.target
    if not target.is_dir():
        s.v_drift("target dir", f"missing: {target}")
        return False
    s.v_ok(f"target dir present: {target}")

    # HERMES_HOME must be root:agent 02775 (setgid). Drift here silently
    # breaks the gateway: without g+w the runtime can't create
    # gateway.lock / gateway.pid / platforms/, and without setgid+group
    # those files won't inherit the hermes group.
    _check_mode_owner(
        s, "target dir", _stat_info(target),
        "2775", s.rails_owner, s.agent_group,
    )
    return True


def _check_rails(s: _State) -> None:
    rails = s.args.target / "hermes-agent"
    if not rails.is_dir():
        s.v_drift("rails", f"missing: {rails}")
        return
    owner = _owner(rails)
    if owner == s.rails_owner:
        s.v_ok(f"rails ownership: {owner}")
    else:
        s.v_drift("rails ownership", f"expected {s.rails_owner}, got {owner}")

    # Rails must not be writable by group or world — the protection boundary
    # that keeps the agent from rewriting its own code paths. Skip symlinks:
    # they're always lrwxrwxrwx and would trip a naive perm check even though
    # their effective writability comes from the target. The venv ships
    # several (lib64 → lib, bin/python → python3.12).
    #
    # Includes the rails root itself — `os.walk` does not yield the start
    # path in dirnames/filenames, but `find $rails ...` does, so a g+w on
    # $rails would slip through verify even though it lets a writable
    # principal replace entries under the root-owned tree.
    bad: list[str] = []
    try:
        root_st = os.lstat(rails)
        if root_st.st_mode & 0o022:
            bad.append(str(rails))
    except OSError:
        pass
    for dirpath, dirnames, filenames in os.walk(rails, followlinks=False):
        for name in dirnames + filenames:
            if len(bad) >= 3:
                break
            p = os.path.join(dirpath, name)
            try:
                st = os.lstat(p)
            except OSError:
                continue
            if stat.S_ISLNK(st.st_mode):
                continue
            if st.st_mode & 0o022:
                bad.append(p)
        if len(bad) >= 3:
            break
    if not bad:
        s.v_ok("rails perms: no group/world-writable files")
    else:
        s.v_drift("rails perms", f"writable: {' '.join(bad)}")


def _check_auth(s: _State, app_auth_expected: bool) -> None:
    target = s.args.target
    auth_dir = target / "auth"
    auth_present = auth_dir.is_dir()

    if app_auth_expected and not auth_present:
        s.v_drift("auth dir", f"missing: {auth_dir} (required by --auth-method app)")

    if auth_present:
        _check_mode_owner(
            s, "auth dir", _stat_info(auth_dir),
            "750", s.rails_owner, s.agent_group,
        )

    # Required artefacts under app auth — list explicitly so a missing file is
    # named in the drift line, instead of relying on the glob below to find it.
    if app_auth_expected:
        for required in ("github-app.env", "github-app.pem"):
            if not (auth_dir / required).is_file():
                s.v_drift(
                    f"auth file {required}",
                    "missing (required by --auth-method app)",
                )

    if auth_present:
        # Sorted so iteration order is deterministic across runs.
        for f in sorted([*auth_dir.glob("*.pem"), *auth_dir.glob("*.env")]):
            fm = _mode(f)
            if fm == "640":
                s.v_ok(f"auth file {f.name}: {fm}")
            else:
                s.v_drift(f"auth file {f.name}", f"mode={fm} (expected 640)")


def _check_agent_dirs(s: _State) -> None:
    target = s.args.target
    for d in ("sessions", "logs", "cache", "platforms", "platforms/pairing"):
        p = target / d
        if p.is_dir():
            own = _owner(p)
            if own == s.agent_owner:
                s.v_ok(f"{d} ownership: {own}")
            else:
                s.v_drift(f"{d} ownership", f"expected {s.agent_owner}, got {own}")
        else:
            s.v_drift(d, f"missing: {p}")


def _check_config_yaml(s: _State) -> None:
    cfg = s.args.target / "config.yaml"
    if not cfg.is_file():
        s.v_drift("config.yaml", f"missing: {cfg}")
        return
    _check_mode_owner(s, "config.yaml", _stat_info(cfg), "644", s.agent_owner)


def _check_gateway_org_defaults(s: _State) -> None:
    # setup-hermes.sh always renders this (empty file when repos[] is
    # empty); a missing file means the install step didn't run.
    path = s.args.target / "gateway-org-defaults.md"
    if not path.is_file():
        s.v_drift("gateway-org-defaults.md", f"missing: {path}")
        return
    _check_mode_owner(
        s, "gateway-org-defaults.md", _stat_info(path),
        "640", s.rails_owner, s.agent_group,
    )


def _check_state_symlinks(s: _State) -> None:
    target = s.args.target
    for d in ("skills", "memories", "cron"):
        link = target / d
        if link.is_symlink():
            tgt = os.readlink(link)
            if tgt == f"state/{d}":
                s.v_ok(f"symlink {d} -> {tgt}")
            else:
                s.v_drift(f"symlink {d}", f"points to {tgt} (expected state/{d})")
        else:
            s.v_drift(f"symlink {d}", f"not a symlink: {link}")


def _check_state_repo(s: _State, app_auth_expected: bool) -> None:
    state = s.args.target / "state"
    if not (state / ".git").is_dir():
        s.v_drift("state repo", f"missing or not a git repo: {state}")
        return
    sown = _owner(state / ".git")
    if sown == s.agent_owner:
        s.v_ok(f"state .git ownership: {sown}")
    else:
        s.v_drift("state ownership", f"expected {s.agent_owner}, got {sown}")
    for k in ("user.name", "user.email"):
        # `--local` so we only read this repo's own config — accepting global
        # values would mask drift after a `git config --unset`.
        val = _git_config_get(state, "--local", k)
        if val:
            s.v_ok(f"state git {k}: {val}")
        else:
            s.v_drift(f"state git {k}", "not configured")
    origin_url = _git_str(state, "remote", "get-url", "origin")
    if not origin_url:
        s.v_drift("state origin", "not configured")
    elif re.match(r"^(https?://|ssh://|git://|git@)", origin_url):
        s.v_ok(f"state origin: {origin_url}")
    else:
        s.v_drift("state origin", f"filesystem path: {origin_url}")
    # Under --auth-method=app the credential helper must be persisted into
    # state/.git/config — otherwise hermes-sync pushes silently lose
    # authentication on re-clone or config drift.
    if app_auth_expected:
        helper = _git_config_get(
            state, "--local", "credential.https://github.com.helper",
        )
        if helper:
            s.v_ok("state credential helper configured")
        else:
            s.v_drift(
                "state credential helper",
                "missing (required by --auth-method app)",
            )


def _check_runtime_version(s: _State) -> None:
    rv_path = s.args.target / "RUNTIME_VERSION"
    fork = s.args.fork
    if not rv_path.is_file():
        s.v_drift("RUNTIME_VERSION", f"missing at {rv_path}")
        return
    rv = rv_path.read_text().strip()
    if (fork / ".git").is_dir():
        fork_sha = _git_str(fork, "describe", "--always", "--dirty", "--abbrev=40")
        if fork_sha and rv == fork_sha:
            s.v_ok(f"RUNTIME_VERSION matches fork HEAD: {rv}")
        else:
            s.v_drift(
                "RUNTIME_VERSION",
                f"{rv} != fork HEAD {fork_sha or '<unavailable>'}",
            )
    else:
        s.v_ok(f"RUNTIME_VERSION: {rv} (fork unavailable for comparison)")


def _check_repos_schema(s: _State) -> None:
    if not (s.values_file.is_file() and s.values_helper.is_file()):
        return
    # Pass the synced skills tree so `slack_triggers[].skill` is checked
    # against installed skills, not just shape. Missing directory →
    # validate-schema shape-validates only (first install before sync).
    helper_args: list[str] = ["validate-schema"]
    skills_root = s.args.target / "skills"
    if skills_root.is_dir():
        helper_args += ["--skills-root", str(skills_root)]
    rc, out, errout = _values_helper_run(
        s.values_helper, *helper_args, values_file=s.values_file,
    )
    if rc != 0:
        # Capture both streams so the drift detail names the actual problem.
        combined = (out + errout).strip()
        first3 = " ".join(combined.splitlines()[:3])
        s.v_drift("repos[] schema", first3)
    else:
        s.v_ok("repos[] schema valid")


def _check_code_root(s: _State) -> None:
    code_root = s.args.target / "code"
    if not code_root.is_dir():
        s.v_drift("code mirrors root", f"missing: {code_root}")
        return
    crowner = _owner(code_root)
    if crowner == s.agent_owner:
        s.v_ok(f"code mirrors root ownership: {crowner}")
    else:
        s.v_drift(
            "code mirrors root ownership",
            f"expected {s.agent_owner}, got {crowner}",
        )


def _list_repos_tsv(s: _State) -> tuple[str, bool]:
    """Returns (tsv_payload, list_failed)."""
    if not (s.values_file.is_file() and s.values_helper.is_file()):
        return "", False
    rc, out, _ = _values_helper_run(
        s.values_helper, "list-repos", values_file=s.values_file,
    )
    if rc != 0:
        return "", True
    return out, False


def _parse_repos_tsv(tsv: str) -> list[tuple[str, str, str, str, str]]:
    """Yield (id, url, base_branch, package_manager, install_cmd) tuples.

    Bash ``read -r a b c d e`` assigns missing trailing fields to ''; we
    pad to keep the same arity."""
    rows = []
    for line in tsv.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        parts += [""] * (5 - len(parts))
        rows.append(tuple(parts[:5]))  # type: ignore[arg-type]
    return rows


def _parse_quay_version(value: str) -> tuple[int, int, int] | None:
    m = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", value.strip())
    if m is None:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _quay_supports_reference_repos(version: str) -> bool:
    parsed = _parse_quay_version(version)
    return parsed is not None and parsed >= _REFERENCE_REPOS_MIN_QUAY_VERSION


def _read_quay_config(s: _State) -> dict | None:
    quay_cfg = s.args.target / "quay" / "config.toml"
    try:
        return tomllib.loads(quay_cfg.read_text(encoding="utf-8"))
    except OSError:
        # _check_quay_artefacts owns the missing-file drift line.
        return None
    except tomllib.TOMLDecodeError as exc:
        s.v_drift("quay config.toml parse", str(exc))
        return None


def _discover_git_children(root: Path) -> set[str]:
    try:
        entries = list(root.iterdir())
    except OSError:
        return set()
    names: set[str] = set()
    for entry in entries:
        if not entry.is_dir():
            continue
        git_path = entry / ".git"
        if git_path.is_dir() or git_path.is_file():
            names.add(entry.name)
    return names


def _check_quay_reference_repos(s: _State, repos_tsv: str) -> None:
    cfg = _read_quay_config(s)
    if cfg is None:
        return

    context = cfg.get("context")
    got = context.get("reference_repos_root") if isinstance(context, dict) else None
    expected_root = s.args.target / "code"
    expected = str(expected_root)
    if got == expected:
        s.v_ok(f"quay reference_repos_root: {got}")
    else:
        s.v_drift(
            "quay reference_repos_root",
            f"expected {expected}, got {got!r}" if got is not None else "missing",
        )

    if not isinstance(got, str) or not got:
        return

    root = Path(got)
    if root.is_dir():
        s.v_ok(f"quay reference repos root exists: {root}")
    else:
        s.v_drift("quay reference repos root", f"missing: {root}")
        return

    expected_ids = {
        repo_id
        for repo_id, _repo_url, _repo_base, _repo_pkg, _install in _parse_repos_tsv(
            repos_tsv,
        )
        if repo_id
    }
    if not expected_ids:
        return
    discovered = _discover_git_children(root)
    missing = sorted(expected_ids - discovered)
    if missing:
        s.v_drift(
            "quay reference repo mirrors",
            f"missing under {root}: {', '.join(missing)}",
        )
    else:
        s.v_ok(f"quay reference repo mirrors: {len(expected_ids)} under {root}")


def _read_env_value(path: Path, key: str) -> str | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    prefix = f"{key}="
    for line in lines:
        if line.startswith(prefix):
            return line[len(prefix):]
    return None


def _check_quay_admin_config(s: _State) -> None:
    cfg = _read_quay_config(s)
    if cfg is None:
        return
    admin = cfg.get("admin")
    if not s.quay_serve_supported:
        if isinstance(admin, dict):
            s.v_drift(
                "quay admin auth config",
                "present but installed quay binary does not support `serve`",
            )
        return
    if not isinstance(admin, dict):
        s.v_drift("quay admin auth config", "missing [admin] block")
        return
    if admin.get("require_auth") is True:
        s.v_ok("quay admin auth required")
    else:
        s.v_drift(
            "quay admin auth required",
            f"expected true, got {admin.get('require_auth')!r}",
        )
    if admin.get("token_env") == "QUAY_ADMIN_TOKEN":
        s.v_ok("quay admin token env: QUAY_ADMIN_TOKEN")
    else:
        s.v_drift(
            "quay admin token env",
            f"expected QUAY_ADMIN_TOKEN, got {admin.get('token_env')!r}",
        )
    if admin.get("forwarded_identity_header") == "X-Hermes-User-Id":
        s.v_ok("quay admin forwarded identity header: X-Hermes-User-Id")
    else:
        s.v_drift(
            "quay admin forwarded identity header",
            f"expected X-Hermes-User-Id, got {admin.get('forwarded_identity_header')!r}",
        )


def _executable_info(path: Path) -> tuple[str, str, str] | None:
    """Like ``_stat_info`` but returns ``None`` for non-regular or
    non-executable files — caller treats that as a "missing or
    non-executable" drift, distinct from a stat-failed missing-file drift."""
    try:
        st = path.stat()
    except OSError:
        return None
    if not stat.S_ISREG(st.st_mode) or not (st.st_mode & 0o111):
        return None
    return _info_from_st(st)


def _check_version_pin(
    s: _State, label: str, bin_path: Path, pin: str,
) -> None:
    """Substring-match ``<bin> --version`` against ``pin``. Used by
    runtime-manager checks; binaries may emit build-suffixed versions like
    ``1.3.9+commit``."""
    actual = _first_stdout_line([str(bin_path), "--version"])
    if actual and pin in actual:
        s.v_ok(f"{label}: {actual}")
    else:
        s.v_drift(label, f"got '{actual or '?'}' (expected to contain '{pin}')")


def _check_quay_binary_sha256(s: _State, quay_bin: Path, expected_file: Path) -> None:
    if not expected_file.is_file():
        s.v_drift("quay binary SHA256", f"missing expected hash: {expected_file}")
        return

    _check_mode_owner(
        s, "quay SHA256SUM.expected", _stat_info(expected_file), "644", s.rails_owner,
    )
    expected = _read_expected_sha256(expected_file)
    if not expected:
        s.v_drift("quay binary SHA256", f"invalid expected hash in {expected_file}")
        return

    actual = _sha256(quay_bin)
    if not actual:
        s.v_drift("quay binary SHA256", f"could not read {quay_bin}")
    elif actual == expected:
        s.v_ok(f"quay binary SHA256: {actual}")
    else:
        s.v_drift(
            "quay binary SHA256",
            f"got {actual} (expected {expected})",
        )


def _quay_expected_sha_file(target: Path) -> Path:
    return (
        target
        / "hermes-agent"
        / "installer"
        / ".state"
        / "quay"
        / "SHA256SUM.expected"
    )


def _check_quay_artefacts(s: _State, repos_tsv: str) -> None:
    """All quay-binary, data-dir, operator-glue, and runtime-manager checks
    that gate on quay.version being non-empty."""
    target = s.args.target
    quay_dir = target / "quay"

    quay_bin = Path(s.quay_bin)
    quay_bin_info = _executable_info(quay_bin)
    if quay_bin_info is None:
        s.v_drift("quay binary", f"missing or non-executable: {quay_bin}")
    else:
        _check_mode_owner(s, "quay binary", quay_bin_info, "755", s.rails_owner)
        _check_quay_binary_sha256(s, quay_bin, _quay_expected_sha_file(target))

    if not quay_dir.is_dir():
        s.v_drift("quay data dir", f"missing: {quay_dir}")
    else:
        # Owner-only — the data dir inherits a setgid bit from the 02775
        # parent on Linux, so exact-mode matching against 755 is broken.
        downer = _owner(quay_dir)
        if downer == s.agent_owner:
            s.v_ok(f"quay data dir ownership: {downer}")
        else:
            s.v_drift(
                "quay data dir ownership",
                f"expected {s.agent_owner}, got {downer}",
            )
        quay_cfg = quay_dir / "config.toml"
        if not quay_cfg.is_file():
            s.v_drift("quay config.toml", f"missing: {quay_cfg}")
        else:
            _check_mode_owner(
                s, "quay config.toml", _stat_info(quay_cfg), "644", s.agent_owner,
            )
            _check_quay_admin_config(s)

    for label, raw_path in (
        ("quay-as-hermes wrapper", s.quay_wrapper),
        ("quay-tick-runner", s.quay_runner),
    ):
        p = Path(raw_path)
        info = _executable_info(p)
        if info is None:
            s.v_drift(label, f"missing or non-executable: {p}")
        else:
            _check_mode_owner(s, label, info, "755", s.rails_owner)

    quay_profile = Path(s.quay_profile)
    if not quay_profile.is_file():
        s.v_drift("quay profile.d drop-in", f"missing: {quay_profile}")
    else:
        _check_mode_owner(
            s, "quay profile.d drop-in", _stat_info(quay_profile),
            "644", s.rails_owner,
        )

    declared: list[str] = []
    for _id, _url, _base, rt_pkg, _install in _parse_repos_tsv(repos_tsv):
        if rt_pkg and rt_pkg not in declared:
            declared.append(rt_pkg)
    rt_install_dir = Path(s.runtime_dir)
    for rt_name in declared:
        rt_path = rt_install_dir / rt_name
        label = f"runtime manager {rt_name}"
        rt_pin = _values_get(
            s.values_file,
            s.values_helper,
            f"quay.runtime_managers.{rt_name}.version",
        )
        if not rt_pin:
            s.v_drift(
                label,
                f"no quay.runtime_managers.{rt_name}.version pin in {s.values_file}",
            )
            continue
        rt_info = _executable_info(rt_path)
        if rt_info is None:
            s.v_drift(label, f"missing or non-executable: {rt_path}")
            continue
        _check_mode_owner(s, label, rt_info, "755", s.rails_owner)
        _check_version_pin(s, f"{label} version", rt_path, rt_pin)


def _atlas_kb_root(s: _State) -> Path:
    raw = _values_get(s.values_file, s.values_helper, "atlas.kb_root")
    if not raw:
        return s.args.target / "atlas-kb"
    path = Path(raw)
    if path.is_absolute():
        return path
    return s.args.target / path


def _atlas_expected_credential_helper(s: _State) -> str:
    target = s.args.target
    rails = target / "hermes-agent"
    env_file = target / "auth" / "atlas-manager.env"
    venv_py = rails / "venv" / "bin" / "python"
    helper_py = rails / "installer" / "hermes_github_token.py"
    return (
        f"!HERMES_HOME='{target}' HERMES_GH_CONFIG='{env_file}' "
        f"{venv_py} {helper_py} credential"
    )


def _check_atlas_version_pin(s: _State, atlas_bin: Path) -> None:
    actual = _first_stdout_line([str(atlas_bin), "--version"])
    pin = s.atlas_version
    normalized_pin = pin[1:] if pin.startswith("v") else pin
    if actual and (pin in actual or normalized_pin in actual):
        s.v_ok(f"atlas binary version: {actual}")
    else:
        expected = f"{pin} / {normalized_pin}" if pin != normalized_pin else pin
        s.v_drift(
            "atlas binary version",
            f"got '{actual or '?'}' (expected to contain '{expected}')",
        )


def _github_repo_slug_from_url(url: str) -> str | None:
    m = re.match(r"^https://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$", url)
    if not m:
        return None
    return f"{m.group(1)}/{m.group(2)}"


def _atlas_required_repo_slugs(s: _State) -> list[str]:
    slugs: list[str] = []

    release_repo = _values_get(s.values_file, s.values_helper, "atlas.release_repo")
    release_repo = release_repo or "InverterNetwork/atlas"
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", release_repo):
        slugs.append(release_repo)

    kb_repo = _values_get(s.values_file, s.values_helper, "atlas.kb_repo")
    kb_repo = kb_repo or "https://github.com/InverterNetwork/atlas-kb"
    kb_slug = _github_repo_slug_from_url(kb_repo)
    if kb_slug:
        slugs.append(kb_slug)

    deduped: list[str] = []
    for slug in slugs:
        if slug not in deduped:
            deduped.append(slug)
    return deduped


def _check_atlas_manager_token(s: _State) -> None:
    """Atlas uses its own GitHub App identity. Verify must prove that the
    staged App env/key can mint a token without falling back to the generic
    Hermes worker App."""
    target = s.args.target
    rails = target / "hermes-agent"
    helper_py = rails / "installer" / "hermes_github_token.py"
    venv_py = rails / "venv" / "bin" / "python"
    env_file = target / "auth" / "atlas-manager.env"
    key_file = target / "auth" / "atlas-manager.pem"

    _check_mode_owner(
        s, "auth file atlas-manager.env", _stat_info(env_file),
        "640", s.rails_owner, s.agent_group,
    )
    _check_mode_owner(
        s, "auth file atlas-manager.pem", _stat_info(key_file),
        "640", s.rails_owner, s.agent_group,
    )

    if not env_file.is_file() or not key_file.is_file():
        return
    if not helper_py.is_file():
        s.v_drift("Atlas manager token helper", f"missing helper at {helper_py}")
        return
    if not (venv_py.exists() and os.access(venv_py, os.X_OK)):
        s.v_drift("Atlas manager token helper", f"missing venv python at {venv_py}")
        return

    rc, mint_out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "env",
        "-u", "HERMES_GH_APP_ID",
        "-u", "HERMES_GH_INSTALLATION_ID",
        "-u", "HERMES_GH_APP_KEY",
        "-u", "HERMES_GH_API",
        "-u", "HERMES_GH_TOKEN_CACHE",
        "-u", "HERMES_GH_TOKEN_OVERRIDE",
        f"HERMES_HOME={target}",
        f"HERMES_GH_CONFIG={env_file}",
        str(venv_py),
        str(helper_py),
        "mint",
        "--no-cache",
    ])
    token = mint_out.strip() if rc == 0 else ""
    if token:
        s.v_ok("Atlas manager token helper check passes")
    else:
        s.v_drift("Atlas manager token helper check", "mint failed")
        return

    gh_api_base = s.args.gh_api_base or "https://api.github.com"
    required_slugs = _atlas_required_repo_slugs(s)
    if not required_slugs:
        s.v_drift(
            "Atlas manager App scope",
            "could not derive atlas.release_repo or atlas.kb_repo repo slugs",
        )
        return
    for slug in required_slugs:
        api_url = f"{gh_api_base}/repos/{slug}"
        http_code = _gh_api_http_code(token, api_url)
        if http_code == "200":
            s.v_ok(f"Atlas manager App scope {slug}: HTTP 200")
        else:
            s.v_drift(
                f"Atlas manager App scope {slug}",
                f"{api_url} returned HTTP {http_code}",
            )


def _check_atlas_artefacts(s: _State, *, app_auth_expected: bool) -> None:
    if not app_auth_expected:
        s.v_drift(
            "Atlas auth method",
            "atlas.version is set, so verify must run with --auth-method app",
        )

    atlas_bin = Path(s.atlas_bin)
    atlas_bin_info = _executable_info(atlas_bin)
    if atlas_bin_info is None:
        s.v_drift("atlas binary", f"missing or non-executable: {atlas_bin}")
    else:
        _check_mode_owner(s, "atlas binary", atlas_bin_info, "755", s.rails_owner)
        _check_atlas_version_pin(s, atlas_bin)

    atlas_wrapper = Path(s.atlas_wrapper)
    wrapper_info = _executable_info(atlas_wrapper)
    if wrapper_info is None:
        s.v_drift(
            "atlas-as-hermes wrapper",
            f"missing or non-executable: {atlas_wrapper}",
        )
    else:
        _check_mode_owner(s, "atlas-as-hermes wrapper", wrapper_info, "755", s.rails_owner)

    atlas_profile = Path(s.atlas_profile)
    if not atlas_profile.is_file():
        s.v_drift("atlas profile.d drop-in", f"missing: {atlas_profile}")
    else:
        _check_mode_owner(
            s, "atlas profile.d drop-in", _stat_info(atlas_profile),
            "644", s.rails_owner,
        )

    kb_root = _atlas_kb_root(s)
    kb_repo = _values_get(s.values_file, s.values_helper, "atlas.kb_repo")
    kb_repo = kb_repo or "https://github.com/InverterNetwork/atlas-kb"
    if not (kb_root / ".git").is_dir():
        s.v_drift("Atlas KB clone", f"missing .git at {kb_root}")
    else:
        _v_check_clone_basics(s, "Atlas KB", kb_root, kb_repo, s.agent_owner)
        _v_check_github_app_clone_auth(
            s, "Atlas KB", kb_root, required=app_auth_expected,
        )
        helper = _git_config_get(kb_root, "credential.https://github.com.helper")
        expected_helper = _atlas_expected_credential_helper(s)
        if helper == expected_helper:
            s.v_ok("Atlas KB credential helper configured")
        else:
            s.v_drift(
                "Atlas KB credential helper",
                f"got '{helper or '?'}' (expected Atlas manager helper)",
            )
        user_name = _git_config_get(kb_root, "user.name")
        user_email = _git_config_get(kb_root, "user.email")
        if user_name:
            s.v_ok(f"Atlas KB git user.name: {user_name}")
        else:
            s.v_drift("Atlas KB git user.name", "missing")
        if user_email:
            s.v_ok(f"Atlas KB git user.email: {user_email}")
        else:
            s.v_drift("Atlas KB git user.email", "missing")

    _check_atlas_manager_token(s)


def _check_atlas_hub_service(s: _State) -> None:
    auth_file = s.args.target / "auth" / "atlas-hub-auth.json"
    auth_info = _stat_info(auth_file)
    if auth_info is None:
        s.v_drift("Atlas Hub auth file", f"missing: {auth_file}")
    else:
        _check_mode_owner(s, "Atlas Hub auth file", auth_info, "640", s.rails_owner)
        try:
            auth_payload = json.loads(auth_file.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            s.v_drift("Atlas Hub auth file", f"unreadable or invalid JSON: {exc}")
        else:
            keys = auth_payload.get("keys") if isinstance(auth_payload, dict) else None
            if isinstance(keys, list) and keys:
                s.v_ok("Atlas Hub auth file keys present")
            else:
                s.v_drift("Atlas Hub auth file keys", "missing non-empty keys list")

    client_key_file = s.args.target / "auth" / "atlas-hub-client-api-key"
    client_key_info = _stat_info(client_key_file)
    if client_key_info is None:
        s.v_drift("Atlas Hub client API key", f"missing: {client_key_file}")
        client_key = ""
    else:
        _check_mode_owner(
            s, "Atlas Hub client API key", client_key_info, "640", s.rails_owner,
        )
        try:
            client_key = client_key_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            s.v_drift("Atlas Hub client API key", f"unreadable: {exc}")
            client_key = ""
        if client_key.startswith("atlas_hub_sk_"):
            s.v_ok("Atlas Hub client API key present")
        else:
            s.v_drift("Atlas Hub client API key", "missing atlas_hub_sk_ value")

    host = _values_get(s.values_file, s.values_helper, "atlas.hub.host") or "127.0.0.1"
    port = _values_get(s.values_file, s.values_helper, "atlas.hub.port") or "8765"
    public_base_url = (
        _values_get(s.values_file, s.values_helper, "atlas.hub.public_base_url")
        .strip()
        .rstrip("/")
    )
    data_dir_raw = _values_get(s.values_file, s.values_helper, "atlas.hub.data_dir")
    data_dir = (
        s.args.target / data_dir_raw
        if data_dir_raw and not data_dir_raw.startswith("/")
        else Path(data_dir_raw or (s.args.target / "atlas-hub"))
    )
    query_concurrency = (
        _values_get(s.values_file, s.values_helper, "atlas.hub.query_concurrency")
        or "4"
    )
    atlas_config = s.args.target / "config" / "atlas.yaml"
    config_info = _stat_info(atlas_config)
    if config_info is None:
        s.v_drift("Atlas runtime config", f"missing: {atlas_config}")
    else:
        try:
            config_text = atlas_config.read_text(encoding="utf-8")
        except OSError as exc:
            s.v_drift("Atlas runtime config", f"unreadable: {exc}")
            config_text = ""
        expectations = {
            "hub auth_file": f'auth_file: "{auth_file}"',
            "hub data_dir": f'data_dir: "{data_dir}"',
            "hub query_concurrency": f"query_concurrency: {query_concurrency}",
        }
        for label, needle in expectations.items():
            if config_text and needle in config_text:
                s.v_ok(f"Atlas runtime config {label}")
            elif config_text:
                s.v_drift(f"Atlas runtime config {label}", f"missing {needle}")

    if not shutil.which("systemctl"):
        return

    unit = "atlas-hub.service"
    active, load, unitfile = _systemd_unit_state(s, unit)
    if active == "active" and load == "loaded" and unitfile == "enabled":
        s.v_ok(f"{unit}: active loaded enabled")
    else:
        s.v_drift(
            unit,
            f"active={active or '?'} load={load or '?'} unitfile={unitfile or '?'}",
        )

    unit_file = Path(s.systemd_dir) / unit
    if not unit_file.is_file():
        s.v_drift(unit, f"missing unit file: {unit_file}")
        return
    _check_mode_owner(s, unit, _stat_info(unit_file), "644", s.rails_owner)
    try:
        text = unit_file.read_text(encoding="utf-8")
    except OSError as exc:
        s.v_drift(unit, f"unreadable: {exc}")
        return

    expected_config = f"Environment=ATLAS_CONFIG={atlas_config}"
    if expected_config in text:
        s.v_ok("atlas-hub.service ATLAS_CONFIG")
    else:
        s.v_drift("atlas-hub.service ATLAS_CONFIG", f"missing {expected_config}")

    expected_kb_root = f"Environment=ATLAS_KB_ROOT={_atlas_kb_root(s)}"
    if expected_kb_root in text:
        s.v_ok("atlas-hub.service ATLAS_KB_ROOT")
    else:
        s.v_drift("atlas-hub.service ATLAS_KB_ROOT", f"missing {expected_kb_root}")

    auth_env = f"EnvironmentFile=-{s.args.target / 'auth' / 'atlas.env'}"
    if auth_env in text:
        s.v_ok("atlas-hub.service secrets env")
    else:
        s.v_drift("atlas-hub.service secrets env", f"missing {auth_env}")

    if re.search(r"ExecStart=.*\batlas\s+--config\s+\S+\s+serve\b", text):
        s.v_ok("atlas-hub.service ExecStart uses atlas serve")
    else:
        s.v_drift("atlas-hub.service ExecStart", "missing `atlas --config ... serve`")

    if re.search(r"ExecStart=.*--host\s+(127\.0\.0\.1|localhost)\b", text):
        s.v_ok(f"atlas-hub.service loopback bind: {host}:{port}")
    else:
        s.v_drift(
            "atlas-hub.service bind address",
            "expected ExecStart to include --host 127.0.0.1 or localhost",
        )

    if re.search(rf"ExecStart=.*--port\s+{re.escape(str(port))}\b", text):
        s.v_ok(f"atlas-hub.service port: {port}")
    else:
        s.v_drift("atlas-hub.service port", f"expected --port {port}")

    if client_key:
        health_url = (
            os.environ.get("HERMES_VERIFY_ATLAS_HUB_HEALTH_URL")
            or f"http://{host}:{port}/v1/health"
        )
        code = _bearer_http_code(health_url, client_key)
        if code == "200":
            s.v_ok(f"Atlas Hub local health: HTTP {code}")
        else:
            s.v_drift("Atlas Hub local health", f"{health_url} returned HTTP {code}")

    if public_base_url:
        _check_atlas_hub_public_route(
            s,
            public_base_url=public_base_url,
            hub_host=host,
            hub_port=str(port),
            client_key=client_key,
        )


def _check_atlas_hub_public_route(
    s: _State,
    *,
    public_base_url: str,
    hub_host: str,
    hub_port: str,
    client_key: str,
) -> None:
    parsed = urlparse(public_base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        s.v_drift(
            "Atlas Hub public base URL",
            "atlas.hub.public_base_url must be an http(s) origin without path, "
            "query, fragment, or credentials",
        )
        return

    caddyfile = Path(s.caddyfile)
    try:
        caddy_text = caddyfile.read_text(encoding="utf-8")
    except OSError as exc:
        s.v_drift("Atlas Hub public Caddy route", f"cannot read {caddyfile}: {exc}")
    else:
        try:
            ok = caddyfile_has_atlas_hub_route(
                caddy_text,
                public_base_url=public_base_url,
                hub_host=hub_host,
                hub_port=hub_port,
            )
        except ValueError as exc:
            s.v_drift("Atlas Hub public Caddy route", str(exc))
        else:
            if ok:
                s.v_ok(
                    "Atlas Hub public Caddy route: "
                    f"{public_base_url}/v1/* -> {hub_host}:{hub_port}"
                )
            else:
                s.v_drift(
                    "Atlas Hub public Caddy route",
                    f"missing managed /v1/* reverse_proxy to {hub_host}:{hub_port}",
                )

    if client_key:
        health_url = (
            os.environ.get("HERMES_VERIFY_ATLAS_HUB_PUBLIC_HEALTH_URL")
            or f"{public_base_url}/v1/health"
        )
        code = _bearer_http_code(health_url, client_key)
        if code == "200":
            s.v_ok(f"Atlas Hub public health: HTTP {code}")
        else:
            s.v_drift("Atlas Hub public health", f"{health_url} returned HTTP {code}")


def _check_codex_prereqs(s: _State) -> None:
    """Codex CLI host-prep checks — fire only when the active quay agent
    invocation path references `codex`. setup-hermes.sh provisions the binary
    in a root-owned managed path; verify confirms the binary is runnable and the
    operator completed `codex login` with usable token material. ChatGPT
    subscription auth — never OPENAI_API_KEY."""
    rc, out, _ = _run([
        *_sudo_prefix_for(s.agent_owner), "bash", "-c", "codex --version",
    ])
    if rc != 0:
        s.v_drift(
            "codex binary",
            f"`codex --version` failed for {s.agent_owner}; "
            "re-run setup-hermes.sh to restore the pinned binary",
        )
        return
    s.v_ok(f"codex binary: {(out.splitlines() or [''])[0] or '?'}")

    if os.environ.get("HERMES_VERIFY_AGENT_HOME"):
        agent_home = _agent_home(s)
    else:
        try:
            agent_home = Path(pwd.getpwnam(s.agent_owner).pw_dir)
        except KeyError:
            s.v_drift("codex auth dir", f"agent user not found: {s.agent_owner}")
            return
    codex_dir = agent_home / ".codex"
    info = _stat_info(codex_dir)
    if info is None:
        s.v_drift(
            "codex auth",
            f"missing {codex_dir} — run `sudo -u {s.agent_owner} -H codex login`",
        )
        return
    mode, owner, _grp = info
    if owner != s.agent_owner:
        s.v_drift(
            "codex auth dir ownership",
            f"{codex_dir}: owner={owner} (expected {s.agent_owner})",
        )
    elif mode not in ("700", "750"):
        # Holds OAuth tokens; world-readable would leak the ChatGPT session.
        s.v_drift(
            "codex auth dir perms",
            f"{codex_dir}: mode={mode} (expected 700)",
        )
    else:
        s.v_ok(f"codex auth dir: mode={mode} owner={owner}")

    # auth.json schema: {"tokens": {"access_token": "...", "refresh_token":
    # "...", ...}}. Both tokens must be present and non-empty — that's what
    # hermes_cli/auth.py:_import_codex_cli_tokens reads on every Codex API
    # call. A dir holding only config.toml / stale files would otherwise
    # pass verify while workers fail at runtime.
    auth_path = codex_dir / "auth.json"
    if not auth_path.is_file():
        s.v_drift(
            "codex auth",
            f"missing {auth_path} — run `sudo -u {s.agent_owner} -H codex login`",
        )
        return
    try:
        payload = json.loads(auth_path.read_text())
    except PermissionError:
        s.v_drift(
            "codex auth",
            f"{auth_path}: permission denied (run verify as root or {s.agent_owner})",
        )
        return
    except (OSError, ValueError) as exc:
        s.v_drift("codex auth", f"{auth_path}: unreadable or malformed ({exc})")
        return
    tokens = payload.get("tokens") if isinstance(payload, dict) else None
    if not isinstance(tokens, dict):
        s.v_drift("codex auth", f"{auth_path}: missing `tokens` object")
        return
    access = tokens.get("access_token")
    refresh = tokens.get("refresh_token")
    if not isinstance(access, str) or not access:
        s.v_drift("codex auth", f"{auth_path}: tokens.access_token empty/missing")
        return
    if not isinstance(refresh, str) or not refresh:
        s.v_drift("codex auth", f"{auth_path}: tokens.refresh_token empty/missing")
        return
    s.v_ok(f"codex auth: {auth_path} carries access+refresh tokens")


def _quay_registered_ids(s: _State) -> tuple[str, bool]:
    """Returns (newline-joined ids, list_failed). Empty + False means quay not
    enabled / not present, no list attempted."""
    target = s.args.target
    quay_bin = Path(s.quay_bin)
    if not (
        s.quay_version
        and _executable_info(quay_bin) is not None
        and (target / "quay").is_dir()
    ):
        return "", False
    rc1, repo_list_out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "env", f"QUAY_DATA_DIR={target / 'quay'}", str(quay_bin), "repo", "list",
    ])
    if rc1 != 0:
        return "", True
    rc2, ids, _ = _run(
        ["python3", str(s.values_helper), "parse-repo-list-ids"],
        input=repo_list_out,
    )
    if rc2 != 0:
        return "", True
    return ids.strip(), False


def _normalize_namespaces(raw: object) -> tuple[dict | None, str | None]:
    """Canonical shape for tag-vocab drift compare.

    Accepts the ``namespaces`` field from either ``quay … get-tags`` (live)
    or ``values_helper.py get-…-tags`` (desired). Returns
    ``({ns: {"values": sorted([...]), "required": bool}}, None)`` on
    success so the two sides string-compare equal even if upstream
    enumeration order drifts. Returns ``(None, reason)`` on shape
    failure — the verifier MUST surface malformed live state as drift,
    so coercing strings/bools (a 'false' string silently → True) would
    mask exactly the kind of regression this check exists to catch.
    """
    if not isinstance(raw, dict):
        return None, f"top level is not a mapping ({type(raw).__name__})"
    out: dict[str, dict] = {}
    for k_raw, spec in raw.items():
        if not isinstance(k_raw, str):
            return None, f"namespace key {k_raw!r} is not a string"
        if not isinstance(spec, dict):
            return None, f"namespace {k_raw!r} entry is not a mapping"
        values_raw = spec.get("values")
        if not isinstance(values_raw, list):
            return None, f"namespace {k_raw!r}.values is not a list"
        for j, v in enumerate(values_raw):
            if not isinstance(v, str):
                return None, (
                    f"namespace {k_raw!r}.values[{j}] is not a string "
                    f"({type(v).__name__})"
                )
        required_raw = spec.get("required", False)
        if not isinstance(required_raw, bool):
            return None, (
                f"namespace {k_raw!r}.required is not a bool "
                f"({type(required_raw).__name__})"
            )
        out[k_raw] = {"values": sorted(values_raw), "required": required_raw}
    return dict(sorted(out.items())), None


def _quay_get_namespaces(
    s: _State, *args: str,
) -> tuple[dict | None, str | None]:
    """Run ``quay …`` and extract a normalised ``namespaces`` dict.

    ``args`` is the verb tail (``"repo", "get-tags", repo_id`` or
    ``"tags", "get-deployment"``). Returns ``(namespaces, None)`` on
    success or ``(None, reason)`` distinguishing the four failure
    modes (subprocess error, JSON-decode error, non-dict top level,
    namespaces shape mismatch) so the drift line names the actual
    cause — operators chasing a missing binary, a quay crash, and a
    JSON-shape regression all need different signals.
    """
    rc, out, errs = _run([
        *_sudo_prefix_for(s.agent_owner),
        "env", f"QUAY_DATA_DIR={s.args.target / 'quay'}",
        s.quay_bin, *args,
    ])
    if rc != 0:
        # Trim to one line — multi-line stderr would derail the [DRIFT] format.
        head = (errs or "").strip().splitlines()
        detail = head[0] if head else f"exit {rc}"
        return None, f"quay {' '.join(args)} failed: {detail}"
    try:
        data = json.loads(out)
    except json.JSONDecodeError as exc:
        return None, f"quay {' '.join(args)}: invalid JSON ({exc.msg})"
    if not isinstance(data, dict):
        return None, (
            f"quay {' '.join(args)}: top level is not a JSON object "
            f"({type(data).__name__})"
        )
    namespaces, err = _normalize_namespaces(data.get("namespaces"))
    if err is not None:
        return None, f"quay {' '.join(args)}: {err}"
    return namespaces, None


def _values_get_namespaces(
    s: _State, *args: str,
) -> tuple[dict | None, str | None]:
    """Run ``values_helper.py …`` and extract a normalised ``namespaces``
    dict. ``args`` is the subcommand and any positional flags
    (currently ``"get-repo-tags", repo_id``)."""
    rc, out, errs = _values_helper_run(
        s.values_helper, *args, values_file=s.values_file,
    )
    if rc != 0:
        head = (errs or "").strip().splitlines()
        detail = head[0] if head else f"exit {rc}"
        return None, f"values_helper.py {' '.join(args)} failed: {detail}"
    try:
        data = json.loads(out)
    except json.JSONDecodeError as exc:
        return None, f"values_helper.py {' '.join(args)}: invalid JSON ({exc.msg})"
    if not isinstance(data, dict):
        return None, (
            f"values_helper.py {' '.join(args)}: top level is not a JSON "
            f"object ({type(data).__name__})"
        )
    namespaces, err = _normalize_namespaces(data.get("namespaces"))
    if err is not None:
        return None, f"values_helper.py {' '.join(args)}: {err}"
    return namespaces, None


def _diff_namespaces(live: dict, desired: dict) -> list[str]:
    """One line per drift-add / drift-remove / value-add / value-remove /
    required-flip. The `live={…} expected={…}` JSON-blob form is
    unreadable past two namespaces; this lets an operator see at a
    glance which knob is wrong without mentally diffing two payloads.
    Returns ``[]`` when the two sides agree."""
    lines: list[str] = []
    live_keys = set(live.keys())
    desired_keys = set(desired.keys())
    for ns in sorted(desired_keys - live_keys):
        lines.append(f"+ namespace `{ns}` in values, missing in live")
    for ns in sorted(live_keys - desired_keys):
        lines.append(f"- namespace `{ns}` in live, missing in values")
    for ns in sorted(live_keys & desired_keys):
        lv = set(live[ns]["values"])
        dv = set(desired[ns]["values"])
        added = sorted(dv - lv)
        removed = sorted(lv - dv)
        if added:
            lines.append(f"~ {ns}: values added in values: {added}")
        if removed:
            lines.append(f"~ {ns}: values present in live, missing in values: {removed}")
        if live[ns]["required"] != desired[ns]["required"]:
            lines.append(
                f"~ {ns}.required: live={live[ns]['required']} "
                f"expected={desired[ns]['required']}"
            )
    return lines


def _check_repo_tag_vocab(s: _State, repo_id: str) -> None:
    """Drift check for per-repo tag vocab. Compares the live state from
    ``quay repo get-tags <id>`` against the desired state emitted by
    ``values_helper get-repo-tags <id>``. Either side unreadable → drift
    line that names the failing source so the operator knows which
    surface to look at."""
    label = f"quay repo {repo_id} tag vocab"
    live, live_err = _quay_get_namespaces(s, "repo", "get-tags", repo_id)
    desired, desired_err = _values_get_namespaces(s, "get-repo-tags", repo_id)
    if live_err is not None:
        s.v_drift(label, live_err)
        return
    if desired_err is not None:
        s.v_drift(label, desired_err)
        return
    assert live is not None and desired is not None  # narrow for mypy
    if live == desired:
        s.v_ok(label)
        return
    diffs = _diff_namespaces(live, desired)
    s.v_drift(label, diffs[0] if diffs else "shapes differ")
    for extra in diffs[1:]:
        # Subsequent lines under the same drift subject — surface them so the
        # operator can read the full picture without re-running the helpers.
        # Each extra line counts as part of the same drift, not a new one.
        print(f"        {extra}", file=s.err, flush=True)


def _check_per_entry_repos(
    s: _State,
    repos_tsv: str,
    *,
    app_auth_expected: bool,
) -> None:
    """Per-entry code mirror checks + optional bare clone + registration.

    Iterates list-repos[]; the loop runs even when quay is disabled (code
    mirrors are the always-on subsystem). Bare-clone and registration
    sub-checks are conditional on (a) the entry carrying a quay: block
    (non-empty repo_pkg) and (b) quay being enabled on this host."""
    if not repos_tsv.strip():
        return
    target = s.args.target
    code_root = target / "code"
    registered_ids, list_failed = _quay_registered_ids(s)
    if list_failed:
        s.v_drift(
            "quay repo list",
            "non-list/non-JSON output (binary crash, data dir corruption, or format drift)",
        )
    registered_set = set(registered_ids.splitlines()) if registered_ids else set()

    for repo_id, repo_url, repo_base, repo_pkg, _install in _parse_repos_tsv(repos_tsv):
        if not repo_id:
            continue
        code_dir = code_root / repo_id
        if not (code_dir / ".git").is_dir():
            s.v_drift(
                f"code mirror {repo_id}",
                f"missing or not a git repo: {code_dir}",
            )
        else:
            _v_check_clone_basics(
                s, f"code mirror {repo_id}", code_dir, repo_url, s.agent_owner,
            )
            if repo_pkg and repo_url.startswith("https://github.com/"):
                _v_check_github_app_clone_auth(
                    s,
                    f"code mirror {repo_id}",
                    code_dir,
                    required=app_auth_expected,
                )
            # HEAD must be on origin/<base_branch>. hermes-code-sync hard-resets
            # to origin/<base> every tick; mismatch means the timer hasn't run
            # since install, itself worth surfacing. `git rev-parse` accepts
            # both refs in one invocation and emits them newline-separated.
            sha_lines = _git_str(
                code_dir, "rev-parse", "HEAD", f"origin/{repo_base}",
            ).splitlines()
            mhead = sha_lines[0].strip() if len(sha_lines) >= 1 else ""
            expected_head = sha_lines[1].strip() if len(sha_lines) >= 2 else ""
            if mhead and expected_head and mhead == expected_head:
                s.v_ok(f"code mirror {repo_id} at origin/{repo_base}")
            else:
                s.v_drift(
                    f"code mirror {repo_id} branch",
                    f"HEAD={mhead or '?'} expected origin/{repo_base}={expected_head or '?'}",
                )

        if repo_pkg and s.quay_version:
            bare = target / "quay" / "repos" / f"{repo_id}.git"
            if not bare.is_dir():
                s.v_drift(f"quay repo {repo_id}", f"bare clone missing: {bare}")
            else:
                _v_check_clone_basics(
                    s, f"quay repo {repo_id}", bare, repo_url, s.agent_owner,
                )
                if repo_url.startswith("https://github.com/"):
                    _v_check_github_app_clone_auth(
                        s,
                        f"quay repo {repo_id}",
                        bare,
                        required=app_auth_expected,
                    )
            if not list_failed:
                if repo_id in registered_set:
                    s.v_ok(f"quay repo {repo_id} registered")
                    # Tag-vocab drift only makes sense for an actually-
                    # registered repo AND a binary that supports the
                    # `tags` noun; `quay repo get-tags` on a missing id
                    # exits with `unknown_repo`, and on a pre-tag-vocab
                    # binary it'd be `unknown_command`. Either would
                    # surface as a spurious "could not read live state"
                    # line, so gate at the call site.
                    if s.quay_tags_supported:
                        _check_repo_tag_vocab(s, repo_id)
                else:
                    s.v_drift(f"quay repo {repo_id}", "not registered with quay")
        elif repo_pkg and not s.quay_version:
            s.v_drift(
                f"quay repo {repo_id}",
                "values file carries quay: block but quay.version is unset",
            )


def _check_systemd(s: _State) -> None:
    if not shutil.which("systemctl"):
        return
    timers = [
        "hermes-sync.timer",
        "hermes-code-sync.timer",
        "hermes-upstream-sync.timer",
    ]
    if s.quay_version:
        timers.append("quay-tick.timer")
        if _values_get(
            s.values_file,
            s.values_helper,
            "quay.orchestrator.enabled",
        ) == "true":
            timers.append("quay-orchestrator.timer")
    for u in timers:
        # Batch the three property reads into a single `systemctl show` —
        # systemctl emits properties in its own (alphabetical) order, not
        # the `-p` flag order, so we parse `KEY=VALUE` lines and look up
        # by key rather than relying on positional output. (`--value`
        # would drop the keys and reintroduce the ordering bug.)
        rc, show_out, _ = _run(
            [
                "systemctl", "show",
                "-p", "ActiveState",
                "-p", "LoadState",
                "-p", "UnitFileState",
                u,
            ],
            timeout=10,
        )
        props: dict[str, str] = {}
        if rc == 0:
            for line in show_out.splitlines():
                key, sep, value = line.partition("=")
                if sep:
                    props[key.strip()] = value.strip()
        active = props.get("ActiveState", "")
        load = props.get("LoadState", "")
        unitfile = props.get("UnitFileState", "")
        if active == "active" and load == "loaded" and unitfile == "enabled":
            s.v_ok(f"{u}: active loaded enabled")
        else:
            s.v_drift(
                u,
                f"active={active or '?'} load={load or '?'} unitfile={unitfile or '?'}",
            )
        # Both .service and .timer files ship together; check both for
        # ownership drift.
        base = u.removesuffix(".timer")
        for unit_file in (
            Path(s.systemd_dir) / f"{base}.service",
            Path(s.systemd_dir) / f"{base}.timer",
        ):
            if unit_file.is_file():
                so = _owner(unit_file)
                if so == s.rails_owner:
                    s.v_ok(f"{unit_file} ownership: {so}")
                else:
                    s.v_drift(
                        f"{unit_file} ownership",
                        f"expected {s.rails_owner}, got {so}",
                    )

    if s.quay_version:
        _check_quay_tick_unit_architecture(s)
        for legacy_unit in (
            Path(s.systemd_dir) / "hermes-reviewer-token.service",
            Path(s.systemd_dir) / "hermes-reviewer-token.timer",
        ):
            if legacy_unit.exists():
                s.v_drift(
                    legacy_unit.name,
                    "legacy reviewer-token unit should be removed; "
                    "quay-tick-runner now mints QUAY_REVIEWER_GH_TOKEN",
                )


def _systemd_unit_state(s: _State, unit: str) -> tuple[str, str, str]:
    rc, show_out, _ = _run(
        [
            "systemctl", "show",
            "-p", "ActiveState",
            "-p", "LoadState",
            "-p", "UnitFileState",
            unit,
        ],
        timeout=10,
    )
    props: dict[str, str] = {}
    if rc == 0:
        for line in show_out.splitlines():
            key, sep, value = line.partition("=")
            if sep:
                props[key.strip()] = value.strip()
    return (
        props.get("ActiveState", ""),
        props.get("LoadState", ""),
        props.get("UnitFileState", ""),
    )


def _bearer_http_code(url: str, token: str) -> str:
    _rc, code_out, _ = _run(
        [
            "curl", "-sS", "-o", "/dev/null",
            "-w", "%{http_code}",
            "--config", "-",
            url,
        ],
        input=f'header = "Authorization: Bearer {token}"\n',
        timeout=5,
    )
    return code_out.strip() or "000"


def _http_code(url: str) -> str:
    _rc, code_out, _ = _run(
        ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", url],
        timeout=5,
    )
    return code_out.strip() or "000"


def _check_quay_serve_service(s: _State) -> None:
    if not shutil.which("systemctl"):
        return

    unit = "quay-serve.service"
    active, load, unitfile = _systemd_unit_state(s, unit)
    if active == "active" and load == "loaded" and unitfile == "enabled":
        s.v_ok(f"{unit}: active loaded enabled")
    else:
        s.v_drift(
            unit,
            f"active={active or '?'} load={load or '?'} unitfile={unitfile or '?'}",
        )

    unit_file = Path(s.systemd_dir) / unit
    if not unit_file.is_file():
        s.v_drift(unit, f"missing unit file: {unit_file}")
        return
    owner = _owner(unit_file)
    if owner == s.rails_owner:
        s.v_ok(f"{unit_file} ownership: {owner}")
    else:
        s.v_drift(
            f"{unit_file} ownership",
            f"expected {s.rails_owner}, got {owner}",
        )
    try:
        text = unit_file.read_text(encoding="utf-8")
    except OSError as exc:
        s.v_drift(unit, f"unreadable: {exc}")
        return

    expected_data_dir = f"Environment=QUAY_DATA_DIR={s.args.target / 'quay'}"
    if expected_data_dir in text:
        s.v_ok("quay-serve.service QUAY_DATA_DIR")
    else:
        s.v_drift(
            "quay-serve.service QUAY_DATA_DIR",
            f"missing {expected_data_dir}",
        )

    auth_env = f"EnvironmentFile={s.args.target / 'auth' / 'quay.env'}"
    if auth_env in text:
        s.v_ok("quay-serve.service auth env")
    else:
        s.v_drift("quay-serve.service auth env", f"missing {auth_env}")

    runtime_env = f"EnvironmentFile=-{s.args.target / 'auth' / 'gateway-runtime.env'}"
    if runtime_env in text:
        s.v_ok("quay-serve.service runtime env")
    else:
        s.v_drift("quay-serve.service runtime env", f"missing {runtime_env}")

    if re.search(r"ExecStart=.*\bquay\s+serve\b", text):
        s.v_ok("quay-serve.service ExecStart uses quay serve")
    else:
        s.v_drift("quay-serve.service ExecStart", "missing `quay serve`")

    if re.search(r"ExecStart=.*--host\s+127\.0\.0\.1\b", text):
        s.v_ok("quay-serve.service loopback bind: 127.0.0.1")
    else:
        s.v_drift(
            "quay-serve.service bind address",
            "expected ExecStart to include --host 127.0.0.1",
        )

    token = _read_env_value(s.args.target / "auth" / "quay.env", "QUAY_ADMIN_TOKEN")
    if token:
        s.v_ok("quay admin token present")
    else:
        s.v_drift(
            "quay admin token",
            f"missing QUAY_ADMIN_TOKEN in {s.args.target / 'auth' / 'quay.env'}",
        )
        return

    health_url = (
        os.environ.get("HERMES_VERIFY_QUAY_HEALTH_URL")
        or "http://127.0.0.1:9731/v1/meta"
    )
    code = _bearer_http_code(health_url, token)
    if code == "200":
        s.v_ok(f"quay admin local health: HTTP {code}")
    else:
        s.v_drift("quay admin local health", f"{health_url} returned HTTP {code}")


def _check_quay_serve_not_installed(s: _State) -> None:
    unit_file = Path(s.systemd_dir) / "quay-serve.service"
    if unit_file.exists():
        s.v_drift(
            "quay-serve.service",
            "installed but the pinned quay binary does not support `serve`",
        )


def _check_hermes_dashboard_service(s: _State) -> None:
    if not shutil.which("systemctl"):
        return

    unit = "hermes-dashboard.service"
    active, load, unitfile = _systemd_unit_state(s, unit)
    if active == "active" and load == "loaded" and unitfile == "enabled":
        s.v_ok(f"{unit}: active loaded enabled")
    else:
        s.v_drift(
            unit,
            f"active={active or '?'} load={load or '?'} unitfile={unitfile or '?'}",
        )

    unit_file = Path(s.systemd_dir) / unit
    if not unit_file.is_file():
        s.v_drift(unit, f"missing unit file: {unit_file}")
        return
    owner = _owner(unit_file)
    if owner == s.rails_owner:
        s.v_ok(f"{unit_file} ownership: {owner}")
    else:
        s.v_drift(
            f"{unit_file} ownership",
            f"expected {s.rails_owner}, got {owner}",
        )
    try:
        text = unit_file.read_text(encoding="utf-8")
    except OSError as exc:
        s.v_drift(unit, f"unreadable: {exc}")
        return

    expected_home = f"Environment=HERMES_HOME={s.args.target}"
    if expected_home in text:
        s.v_ok("hermes-dashboard.service HERMES_HOME")
    else:
        s.v_drift("hermes-dashboard.service HERMES_HOME", f"missing {expected_home}")

    expected_web_dist = f"Environment=HERMES_WEB_DIST={s.args.target / 'hermes-agent' / 'hermes_cli' / 'web_dist'}"
    if expected_web_dist in text:
        s.v_ok("hermes-dashboard.service HERMES_WEB_DIST")
    else:
        s.v_drift(
            "hermes-dashboard.service HERMES_WEB_DIST",
            f"missing {expected_web_dist}",
        )

    expected_base = "Environment=QUAY_ADMIN_BASE_URL=http://127.0.0.1:9731"
    if expected_base in text:
        s.v_ok("hermes-dashboard.service Quay upstream")
    else:
        s.v_drift("hermes-dashboard.service Quay upstream", f"missing {expected_base}")

    auth_env = f"EnvironmentFile={s.args.target / 'auth' / 'quay.env'}"
    if auth_env in text:
        s.v_ok("hermes-dashboard.service auth env")
    else:
        s.v_drift("hermes-dashboard.service auth env", f"missing {auth_env}")

    runtime_env = f"EnvironmentFile=-{s.args.target / 'auth' / 'gateway-runtime.env'}"
    if runtime_env in text:
        s.v_ok("hermes-dashboard.service runtime env")
    else:
        s.v_drift("hermes-dashboard.service runtime env", f"missing {runtime_env}")

    if re.search(r"ExecStart=.*\bdashboard\b.*--host\s+127\.0\.0\.1\b.*--port\s+9119\b", text):
        s.v_ok("hermes-dashboard.service loopback bind: 127.0.0.1:9119")
    else:
        s.v_drift(
            "hermes-dashboard.service bind address",
            "expected ExecStart to include dashboard --host 127.0.0.1 --port 9119",
        )

    after_quay = re.search(
        r"^After=.*\bquay-serve\.service\b", text, re.MULTILINE,
    )
    wants_quay = re.search(
        r"^Wants=.*\bquay-serve\.service\b", text, re.MULTILINE,
    )
    if after_quay and wants_quay:
        s.v_ok("hermes-dashboard.service requires quay-serve ordering")
    else:
        missing = []
        if not after_quay:
            missing.append("After=...quay-serve.service")
        if not wants_quay:
            missing.append("Wants=...quay-serve.service")
        s.v_drift(
            "hermes-dashboard.service quay-serve ordering",
            "missing " + ", ".join(missing),
        )

    health_url = (
        os.environ.get("HERMES_VERIFY_DASHBOARD_HEALTH_URL")
        or "http://127.0.0.1:9119/quay/admin/"
    )
    code = _http_code(health_url)
    if code == "401":
        s.v_ok(f"hermes-dashboard local denial health: HTTP {code}")
    else:
        s.v_drift(
            "hermes-dashboard local denial health",
            f"{health_url} returned HTTP {code}",
        )


def _check_quay_tick_unit_architecture(s: _State) -> None:
    unit_file = Path(s.systemd_dir) / "quay-tick.service"
    if not unit_file.is_file():
        return
    try:
        text = unit_file.read_text(encoding="utf-8")
    except OSError as exc:
        s.v_drift("quay-tick.service", f"unreadable: {exc}")
        return
    if "HERMES_REVIEWER_GH_CONFIG=" in text:
        s.v_ok("quay-tick.service reviewer config env")
    else:
        s.v_drift(
            "quay-tick.service reviewer config env",
            "missing HERMES_REVIEWER_GH_CONFIG for reviewer token minting",
        )
    if "RuntimeDirectory=hermes" in text:
        s.v_ok("quay-tick.service reviewer cache runtime dir")
    else:
        s.v_drift(
            "quay-tick.service reviewer cache runtime dir",
            "missing RuntimeDirectory=hermes for /run/hermes reviewer cache",
        )


def _gh_api_http_code(token: str, url: str) -> str:
    """GET ``url`` with ``Authorization: Bearer <token>``; return the HTTP
    status code as a string (``"000"`` on local failure).

    Token flows via ``--config -`` on stdin so it never lands in argv,
    where ``/proc/<pid>/cmdline`` (any user) would surface it.
    """
    _rc, code_out, _ = _run(
        [
            "curl", "-sS", "-o", "/dev/null",
            "-w", "%{http_code}",
            "--config", "-",
            "-H", "Accept: application/vnd.github+json",
            "-H", "X-GitHub-Api-Version: 2022-11-28",
            url,
        ],
        input=f'header = "Authorization: Bearer {token}"\n',
    )
    return code_out.strip() or "000"


def _check_token_helper(s: _State, repos_tsv: str) -> None:
    """Under --auth-method=app, the helper must be runnable end-to-end.
    Names missing-helper vs missing-venv-python separately so the drift line
    points at the actual broken artefact."""
    target = s.args.target
    rails = target / "hermes-agent"
    helper_py = rails / "installer" / "hermes_github_token.py"
    venv_py = rails / "venv" / "bin" / "python"
    if not helper_py.is_file():
        s.v_drift("token helper", f"missing helper at {helper_py}")
        return
    if not (venv_py.exists() and os.access(venv_py, os.X_OK)):
        s.v_drift("token helper", f"missing venv python at {venv_py}")
        return

    # `mint` doubles as the smoke check (succeeds iff a token can be obtained)
    # AND produces the token for the App-scope check below — one subprocess
    # instead of `check` then `mint`.
    rc, mint_out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "env",
        f"HERMES_HOME={target}",
        str(venv_py),
        str(helper_py),
        "mint",
        "--no-cache",
    ])
    app_token = mint_out.strip() if rc == 0 else ""
    if app_token:
        s.v_ok("token helper check passes")
    else:
        s.v_drift("token helper check", "mint failed")

    if app_token and repos_tsv.strip():
        gh_api_base = s.args.gh_api_base or "https://api.github.com"
        for scope_id, scope_url, _scope_base, scope_pkg, _scope_install in _parse_repos_tsv(
            repos_tsv,
        ):
            if not scope_id or not scope_pkg:
                continue
            m = re.match(r"^https://github\.com/([^/]+)/([^/]+)$", scope_url)
            if not m:
                continue
            api_url = f"{gh_api_base}/repos/{m.group(1)}/{m.group(2)}"
            http_code = _gh_api_http_code(app_token, api_url)
            if http_code == "200":
                s.v_ok(f"App scope {scope_id}: HTTP 200")
            else:
                s.v_drift(
                    f"App scope {scope_id}",
                    f"{api_url} returned HTTP {http_code}",
                )


def _check_reviewer_token(s: _State) -> None:
    """Reviewer App auth: /etc/hermes/reviewer.{pem,env} on-disk perms
    plus an App-installation API probe with a token minted from that env.

    Gated on presence of reviewer.env — that's what setup-hermes.sh writes
    when the reviewer auth block runs. Hosts that never staged a reviewer
    key skip every check here silently."""
    env_file = Path(s.reviewer_env)
    if not env_file.is_file():
        return

    key_file = Path(s.reviewer_key)
    _check_mode_owner(
        s, "reviewer.pem", _stat_info(key_file), "640", s.rails_owner, s.agent_group,
    )
    _check_mode_owner(
        s, "reviewer.env", _stat_info(env_file), "640", s.rails_owner, s.agent_group,
    )

    target = s.args.target
    rails = target / "hermes-agent"
    helper_py = rails / "installer" / "hermes_github_token.py"
    venv_py = rails / "venv" / "bin" / "python"
    if not helper_py.is_file():
        s.v_drift("reviewer token helper", f"missing helper at {helper_py}")
        return
    if not (venv_py.exists() and os.access(venv_py, os.X_OK)):
        s.v_drift("reviewer token helper", f"missing venv python at {venv_py}")
        return

    # Avoid the runtime cache so this read-only health check does not depend
    # on /run/hermes existing outside the systemd tick.
    rc, mint_out, _ = _run([
        *_sudo_prefix_for(s.agent_owner),
        "env",
        "-u", "HERMES_GH_CONFIG",
        "-u", "HERMES_GH_APP_ID",
        "-u", "HERMES_GH_INSTALLATION_ID",
        "-u", "HERMES_GH_APP_KEY",
        "-u", "HERMES_GH_API",
        "-u", "HERMES_GH_TOKEN_CACHE",
        "-u", "HERMES_GH_TOKEN_OVERRIDE",
        f"HERMES_HOME={target}",
        f"HERMES_GH_CONFIG={env_file}",
        str(venv_py),
        str(helper_py),
        "mint",
        "--no-cache",
    ])
    token = mint_out.strip() if rc == 0 else ""
    if token:
        s.v_ok("reviewer token helper check passes")
    else:
        s.v_drift("reviewer token helper check", "mint failed")
        return

    # Per BRIX-1390 acceptance: token must answer `gh api /installation/
    # repositories` — proves the App identity is recognized for *some*
    # installation, without coupling to the specific repos[] entries
    # (which the reviewer App's scope may be a strict subset of whatever
    # deployment.values currently declares as quay-managed repos).
    gh_api_base = s.args.gh_api_base or "https://api.github.com"
    api_url = f"{gh_api_base}/installation/repositories"
    http_code = _gh_api_http_code(token, api_url)
    if http_code == "200":
        s.v_ok("reviewer App installation scope: HTTP 200")
    else:
        s.v_drift(
            "reviewer App installation scope",
            f"{api_url} returned HTTP {http_code}",
        )


def _check_upstream_workspace(s: _State) -> None:
    ws = s.args.target / "upstream-workspace"
    if not (ws / ".git").is_dir():
        return
    for rname in ("origin", "upstream"):
        url = _git_str(ws, "remote", "get-url", rname)
        if not url:
            s.v_drift(f"upstream-workspace {rname}", "not configured")
        elif (
            url.startswith("https://github.com/")
            or url.startswith("git@github.com:")
            or url.startswith("ssh://git@github.com/")
        ):
            s.v_ok(f"upstream-workspace {rname}: {url}")
        else:
            s.v_drift(
                f"upstream-workspace {rname}", f"not a github.com URL: {url}",
            )


def _check_upstream_sync_service(s: _State) -> None:
    script = Path(s.upstream_sync_script)
    if not script.is_file():
        s.v_drift("hermes-upstream-sync script", f"missing: {script}")
    elif os.access(script, os.X_OK):
        _check_mode_owner(
            s,
            "hermes-upstream-sync script",
            _stat_info(script),
            "755",
            s.rails_owner,
        )
    else:
        s.v_drift("hermes-upstream-sync script", f"not executable: {script}")

    env_file = Path(s.upstream_sync_env)
    env_values: dict[str, str] = {}
    if not env_file.is_file():
        s.v_drift("hermes-upstream-sync env", f"missing: {env_file}")
    else:
        _check_mode_owner(
            s,
            "hermes-upstream-sync env",
            _stat_info(env_file),
            "644",
            s.rails_owner,
        )
        env_values = _read_shell_env(env_file)

    fork_dir = Path(env_values.get("FORK_DIR") or s.args.target / "upstream-workspace")
    if not (fork_dir / ".git").is_dir():
        # A fork with no origin remote intentionally skips provisioning, but
        # installed production forks should have one and therefore need a
        # runnable workspace before the timer is enabled.
        fork_origin = _git_str(s.args.fork, "remote", "get-url", "origin")
        if fork_origin:
            s.v_drift(
                "hermes-upstream-sync workspace",
                f"missing or not a git repo: {fork_dir}",
            )
    else:
        owner = _owner(fork_dir / ".git") or _owner(fork_dir)
        if owner == s.agent_owner:
            s.v_ok(f"hermes-upstream-sync workspace ownership: {owner}")
        else:
            s.v_drift(
                "hermes-upstream-sync workspace ownership",
                f"expected {s.agent_owner}, got {owner}",
            )
        for remote_name in ("origin", "upstream"):
            remote_url = _git_str(fork_dir, "remote", "get-url", remote_name)
            if remote_url:
                s.v_ok(f"hermes-upstream-sync workspace {remote_name}: {remote_url}")
            else:
                s.v_drift(
                    f"hermes-upstream-sync workspace {remote_name}",
                    "not configured",
                )
        for key in ("user.name", "user.email"):
            val = _git_config_get(fork_dir, "--local", key)
            if val:
                s.v_ok(f"hermes-upstream-sync workspace git {key}: {val}")
            else:
                s.v_drift(
                    f"hermes-upstream-sync workspace git {key}",
                    "not configured",
                )

    state_dir = Path(
        env_values.get("HERMES_UPSTREAM_SYNC_STATE_DIR")
        or s.args.target / "state" / "hermes-upstream-sync"
    )
    lock_dir = Path(env_values.get("HERMES_UPSTREAM_SYNC_LOCK_DIR") or state_dir / "lock")
    status_file = state_dir / "status.env"

    if lock_dir.exists():
        pid_file = lock_dir / "pid"
        pid_text = ""
        try:
            pid_text = pid_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError):
            pass
        if pid_text and _pid_is_running(pid_text):
            s.v_ok(f"hermes-upstream-sync lock active: pid {pid_text}")
        else:
            s.v_drift(
                "hermes-upstream-sync lock",
                f"stale lock: {lock_dir}",
            )

    if status_file.exists():
        status_values = _read_shell_env(status_file)
        state = status_values.get("state", "")
        detail = status_values.get("detail", "")
        if state == "failed":
            s.v_drift(
                "hermes-upstream-sync status",
                f"last run failed: {detail or '?'}",
            )
        elif state == "running" and not lock_dir.exists():
            s.v_drift(
                "hermes-upstream-sync status",
                "last run recorded running without an active lock",
            )
        elif state:
            s.v_ok(f"hermes-upstream-sync status: {state}")
        else:
            s.v_drift("hermes-upstream-sync status", f"malformed: {status_file}")

    if not shutil.which("systemctl"):
        return

    service_file = Path(s.systemd_dir) / "hermes-upstream-sync.service"
    timer_file = Path(s.systemd_dir) / "hermes-upstream-sync.timer"
    if not service_file.is_file():
        s.v_drift(
            "hermes-upstream-sync.service",
            f"missing unit file: {service_file}",
        )
        service_text = ""
    else:
        _check_mode_owner(
            s,
            "hermes-upstream-sync.service",
            _stat_info(service_file),
            "644",
            s.rails_owner,
        )
        try:
            service_text = service_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            s.v_drift("hermes-upstream-sync.service", f"unreadable: {exc}")
            service_text = ""

    service_expectations = {
        "user": f"User={s.agent_owner}",
        "group": f"Group={s.agent_owner}",
        "environment file": f"EnvironmentFile=-{env_file}",
        "exec": f"ExecStart={script}",
    }
    for label, needle in service_expectations.items():
        if service_text and needle in service_text:
            s.v_ok(f"hermes-upstream-sync.service {label}")
        elif service_text:
            s.v_drift(
                f"hermes-upstream-sync.service {label}",
                f"missing {needle}",
            )

    if not timer_file.is_file():
        s.v_drift(
            "hermes-upstream-sync.timer",
            f"missing unit file: {timer_file}",
        )
        timer_text = ""
    else:
        _check_mode_owner(
            s,
            "hermes-upstream-sync.timer",
            _stat_info(timer_file),
            "644",
            s.rails_owner,
        )
        try:
            timer_text = timer_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            s.v_drift("hermes-upstream-sync.timer", f"unreadable: {exc}")
            timer_text = ""

    timer_expectations = {
        "persistent": "Persistent=true",
        "service target": "Unit=hermes-upstream-sync.service",
    }
    for label, needle in timer_expectations.items():
        if timer_text and needle in timer_text:
            s.v_ok(f"hermes-upstream-sync.timer {label}")
        elif timer_text:
            s.v_drift(
                f"hermes-upstream-sync.timer {label}",
                f"missing {needle}",
            )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    args: VerifyArgs,
    *,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
) -> int:
    """Execute the verify pipeline. Returns 0 (no drift) or 1 (drift)."""
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr

    if args.auth_method not in ("none", "app"):
        print(
            f"--auth-method must be 'none' or 'app' (got: {args.auth_method})",
            file=err,
        )
        return 2

    # Test-fixture overrides match the bash internal env-vars.
    rails_owner = os.environ.get("HERMES_VERIFY_EXPECT_RAILS_OWNER") or "root"
    agent_owner = os.environ.get("HERMES_VERIFY_EXPECT_AGENT_OWNER") or args.user
    agent_group = os.environ.get("HERMES_VERIFY_EXPECT_AGENT_GROUP") or agent_owner

    values_file = args.values or Path(
        os.environ.get("VALUES_FILE") or (args.fork / "deploy.values.yaml")
    )
    values_helper = Path(
        os.environ.get("VALUES_HELPER") or (args.fork / "installer" / "values_helper.py")
    )
    quay_version = ""
    atlas_version = ""
    atlas_ai_mode = ""
    codex_required = False
    if values_file.is_file() and values_helper.is_file():
        quay_version = _values_get(values_file, values_helper, "quay.version")
        if quay_version:
            codex_required = True
        atlas_version = _values_get(values_file, values_helper, "atlas.version")
        if atlas_version:
            atlas_ai_mode = _values_get(values_file, values_helper, "atlas.ai.mode")
            atlas_ai_mode = atlas_ai_mode or "codex-exec"
            if atlas_ai_mode == "codex-exec":
                codex_required = True
            atlas_hub_enabled = (
                _values_get(values_file, values_helper, "atlas.hub.enabled") == "true"
            )
        else:
            atlas_hub_enabled = False
    else:
        atlas_hub_enabled = False

    quay_bin = os.environ.get("HERMES_VERIFY_QUAY_BIN") or "/usr/local/bin/quay"

    # Capability probe — mirrors the install-time gate in setup-hermes.sh.
    # `quay tags --help` returns 0 only when the tag-vocab noun is
    # registered; older quay binaries return non-zero,
    # and the per-repo tag-vocab drift checks would otherwise fire spurious
    # "could not read live state" lines on a perfectly valid pre-tag-vocab
    # install. Only probes when quay is enabled and the binary exists.
    quay_tags_supported = False
    quay_serve_supported = False
    if quay_version and Path(quay_bin).is_file():
        with tempfile.TemporaryDirectory(prefix="hermes-quay-probe-") as probe_dir:
            probe_path = Path(probe_dir)
            try:
                probe_path.chmod(0o777)
            except OSError:
                pass
            rc, _, _ = _run([
                *_sudo_prefix_for(agent_owner),
                "env", f"QUAY_DATA_DIR={probe_path}",
                quay_bin, "tags", "--help",
            ])
            quay_tags_supported = (rc == 0)
            rc, _, _ = _run([
                *_sudo_prefix_for(agent_owner),
                "env", f"QUAY_DATA_DIR={probe_path}",
                quay_bin, "serve", "--help",
            ])
            quay_serve_supported = (rc == 0)

    s = _State(
        args=args,
        out=out,
        err=err,
        rails_owner=rails_owner,
        agent_owner=agent_owner,
        agent_group=agent_group,
        quay_bin=quay_bin,
        quay_wrapper=os.environ.get("HERMES_VERIFY_QUAY_WRAPPER") or "/usr/local/bin/quay-as-hermes",
        quay_runner=os.environ.get("HERMES_VERIFY_QUAY_RUNNER") or "/usr/local/sbin/quay-tick-runner",
        quay_profile=os.environ.get("HERMES_VERIFY_QUAY_PROFILE") or "/etc/profile.d/quay-data-dir.sh",
        atlas_bin=os.environ.get("HERMES_VERIFY_ATLAS_BIN") or "/usr/local/bin/atlas",
        atlas_wrapper=os.environ.get("HERMES_VERIFY_ATLAS_WRAPPER") or "/usr/local/bin/atlas-as-hermes",
        atlas_profile=os.environ.get("HERMES_VERIFY_ATLAS_PROFILE") or "/etc/profile.d/atlas-env.sh",
        runtime_dir=os.environ.get("HERMES_VERIFY_RUNTIME_DIR") or "/usr/local/bin",
        systemd_dir=os.environ.get("HERMES_VERIFY_SYSTEMD_DIR") or "/etc/systemd/system",
        upstream_sync_script=os.environ.get("HERMES_VERIFY_UPSTREAM_SYNC_SCRIPT") or "/usr/local/sbin/hermes-upstream-sync",
        upstream_sync_env=os.environ.get("HERMES_VERIFY_UPSTREAM_SYNC_ENV") or "/etc/default/hermes-upstream-sync",
        reviewer_env=os.environ.get("HERMES_VERIFY_REVIEWER_ENV") or "/etc/hermes/reviewer.env",
        reviewer_key=os.environ.get("HERMES_VERIFY_REVIEWER_KEY") or "/etc/hermes/reviewer.pem",
        caddyfile=os.environ.get("HERMES_VERIFY_CADDYFILE") or "/etc/caddy/Caddyfile",
        values_file=values_file,
        values_helper=values_helper,
        quay_version=quay_version,
        atlas_version=atlas_version,
        atlas_ai_mode=atlas_ai_mode,
        atlas_hub_enabled=atlas_hub_enabled,
        codex_required=codex_required,
        quay_tags_supported=quay_tags_supported,
        quay_serve_supported=quay_serve_supported,
    )

    print(
        f"==> verify: {args.target} (rails={s.rails_owner} agent={s.agent_owner})",
        file=out, flush=True,
    )

    if not _check_target(s):
        print(
            f"==> verify: {s.total} checks, {s.drift} drift",
            file=out, flush=True,
        )
        return 1

    app_auth_expected = args.auth_method == "app"

    _check_rails(s)
    _check_auth(s, app_auth_expected)
    _check_agent_dirs(s)
    _check_config_yaml(s)
    _check_gateway_org_defaults(s)
    _check_state_symlinks(s)
    _check_state_repo(s, app_auth_expected)
    _check_runtime_version(s)
    _check_repos_schema(s)
    _check_code_root(s)
    repos_tsv, repos_failed = _list_repos_tsv(s)
    if repos_failed:
        s.v_drift(
            "repos[] schema",
            "values_helper.py list-repos exited non-zero (run it manually for details)",
        )
    if quay_version:
        _check_quay_artefacts(s, repos_tsv)
        _check_quay_github_app_gitconfig(s, required=app_auth_expected)
        if _quay_supports_reference_repos(quay_version):
            _check_quay_reference_repos(s, repos_tsv)
    if atlas_version:
        _check_atlas_artefacts(s, app_auth_expected=app_auth_expected)
        if s.atlas_hub_enabled:
            _check_atlas_hub_service(s)
    if s.codex_required:
        _check_codex_prereqs(s)
    _check_per_entry_repos(s, repos_tsv, app_auth_expected=app_auth_expected)
    _check_systemd(s)
    if quay_version:
        if s.quay_serve_supported:
            _check_quay_serve_service(s)
            if _values_get(
                s.values_file,
                s.values_helper,
                "quay.admin.public_base_url",
            ):
                _check_hermes_dashboard_service(s)
        else:
            _check_quay_serve_not_installed(s)
    if app_auth_expected:
        _check_token_helper(s, repos_tsv)
    # Reviewer auth is gated by /etc/hermes/reviewer.env presence inside
    # the check itself; safe to invoke unconditionally.
    _check_reviewer_token(s)
    _check_upstream_workspace(s)
    _check_upstream_sync_service(s)

    print(
        f"==> verify: {s.total} checks, {s.drift} drift",
        file=out, flush=True,
    )
    return 0 if s.drift == 0 else 1
