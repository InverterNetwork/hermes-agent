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
import json
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import IO


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
    runtime_dir: str
    values_file: Path
    values_helper: Path
    quay_version: str
    agent_invocation: str
    quay_tags_supported: bool = False
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


def _git_config_get(repo: Path, *args: str) -> str:
    """Read a stored git config value (literal — sidesteps insteadOf rewrites)
    via ``_git_as_owner``. Returns '' on missing key or git failure."""
    return _git_str(repo, "config", "--get", *args)


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
    """Substring-match ``<bin> --version`` against ``pin``. Used by the quay
    binary and runtime-manager checks; both binaries may emit build-suffixed
    versions like ``0.1.0+abc1234`` or ``1.3.9+commit``."""
    actual = _first_stdout_line([str(bin_path), "--version"])
    if actual and pin in actual:
        s.v_ok(f"{label}: {actual}")
    else:
        s.v_drift(label, f"got '{actual or '?'}' (expected to contain '{pin}')")


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
        # quay.version is a git tag (`v0.1.0`); the binary embeds
        # `${pkg.version}+${shortSHA}` (`0.1.0+abc1234`) — no `v` prefix.
        # Strip the leading `v` from the pin for the substring compare so a
        # clean release-built binary doesn't fire false drift.
        _check_version_pin(
            s, "quay binary version", quay_bin, s.quay_version.removeprefix("v"),
        )

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


def _check_codex_prereqs(s: _State) -> None:
    """Codex CLI host-prep checks — fire only when quay.agent_invocation
    references `codex`. Codex is operator-installed under the agent user
    (mirrors the claude precedent, not rails-class), so verify confirms
    the operator completed install + `codex login` rather than provisioning
    the binary itself. ChatGPT subscription auth — never OPENAI_API_KEY."""
    rc, out, _ = _run([
        *_sudo_prefix_for(s.agent_owner), "bash", "-c", "codex --version",
    ])
    if rc != 0:
        s.v_drift(
            "codex binary",
            f"`codex --version` failed for {s.agent_owner}; "
            "install per ops/README.md → 'Pre-install: codex CLI'",
        )
        return
    s.v_ok(f"codex binary: {(out.splitlines() or [''])[0] or '?'}")

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
    (``"get-repo-tags", repo_id`` / ``"get-deployment-tags"``)."""
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


def _check_deployment_tag_vocab(s: _State) -> None:
    """Drift check for deployment tag vocab. Gated on quay.version being
    set AND the binary supporting the `tags` noun (matches the
    install-time capability probe in setup-hermes.sh — otherwise
    `tags get-deployment` exits non-zero and would surface as
    spurious drift on a perfectly valid pre-tag-vocab install)."""
    if not s.quay_version or not s.quay_tags_supported:
        return
    label = "quay deployment tag vocab"
    live, live_err = _quay_get_namespaces(s, "tags", "get-deployment")
    desired, desired_err = _values_get_namespaces(s, "get-deployment-tags")
    if live_err is not None:
        s.v_drift(label, live_err)
        return
    if desired_err is not None:
        s.v_drift(label, desired_err)
        return
    assert live is not None and desired is not None
    if live == desired:
        s.v_ok(label)
        return
    diffs = _diff_namespaces(live, desired)
    s.v_drift(label, diffs[0] if diffs else "shapes differ")
    for extra in diffs[1:]:
        print(f"        {extra}", file=s.err, flush=True)


def _check_per_entry_repos(s: _State, repos_tsv: str) -> None:
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
            Path(f"/etc/systemd/system/{base}.service"),
            Path(f"/etc/systemd/system/{base}.timer"),
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
        "env", f"HERMES_HOME={target}", str(venv_py), str(helper_py), "mint",
    ])
    app_token = mint_out.strip() if rc == 0 else ""
    if app_token:
        s.v_ok("token helper check passes")
    else:
        s.v_drift("token helper check", "mint failed")

    # App scope: curl with --config - fed via stdin so the token never lands
    # in any argv visible through /proc/<pid>/cmdline.
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
            _rc, code_out, _ = _run(
                [
                    "curl", "-sS", "-o", "/dev/null",
                    "-w", "%{http_code}",
                    "--config", "-",
                    "-H", "Accept: application/vnd.github+json",
                    "-H", "X-GitHub-Api-Version: 2022-11-28",
                    api_url,
                ],
                input=f'header = "Authorization: Bearer {app_token}"\n',
            )
            http_code = code_out.strip() or "000"
            if http_code == "200":
                s.v_ok(f"App scope {scope_id}: HTTP 200")
            else:
                s.v_drift(
                    f"App scope {scope_id}",
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
    agent_invocation = ""
    if values_file.is_file() and values_helper.is_file():
        quay_version = _values_get(values_file, values_helper, "quay.version")
        if quay_version:
            agent_invocation = _values_get(
                values_file, values_helper, "quay.agent_invocation",
            )

    quay_bin = os.environ.get("HERMES_VERIFY_QUAY_BIN") or "/usr/local/bin/quay"

    # Capability probe — mirrors the install-time gate in setup-hermes.sh.
    # `quay tags --help` returns 0 only when the tag-vocab noun is
    # registered; older quay binaries return non-zero,
    # and the tag-vocab drift checks would otherwise fire spurious
    # "could not read live state" lines on a perfectly valid pre-tag-vocab
    # install. Only probes when quay is enabled and the binary exists.
    quay_tags_supported = False
    if quay_version and Path(quay_bin).is_file():
        target_quay = args.target / "quay"
        rc, _, _ = _run([
            *_sudo_prefix_for(agent_owner),
            "env", f"QUAY_DATA_DIR={target_quay}",
            quay_bin, "tags", "--help",
        ])
        quay_tags_supported = (rc == 0)

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
        runtime_dir=os.environ.get("HERMES_VERIFY_RUNTIME_DIR") or "/usr/local/bin",
        values_file=values_file,
        values_helper=values_helper,
        quay_version=quay_version,
        agent_invocation=agent_invocation,
        quay_tags_supported=quay_tags_supported,
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
    if "codex" in s.agent_invocation:
        _check_codex_prereqs(s)
    _check_per_entry_repos(s, repos_tsv)
    _check_deployment_tag_vocab(s)
    _check_systemd(s)
    if app_auth_expected:
        _check_token_helper(s, repos_tsv)
    _check_upstream_workspace(s)

    print(
        f"==> verify: {s.total} checks, {s.drift} drift",
        file=out, flush=True,
    )
    return 0 if s.drift == 0 else 1
