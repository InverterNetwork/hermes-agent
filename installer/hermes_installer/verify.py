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
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Iterable


# ---------------------------------------------------------------------------
# Args + counters
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VerifyArgs:
    fork: Path
    user: str
    target: Path | None = None
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


def _owner(path: Path) -> str:
    """Return the username owning ``path``, or '' if path can't be stat'd."""
    try:
        return pwd.getpwuid(path.stat().st_uid).pw_name
    except (FileNotFoundError, KeyError, PermissionError, OSError):
        return ""


def _group(path: Path) -> str:
    try:
        return grp.getgrgid(path.stat().st_gid).gr_name
    except (FileNotFoundError, KeyError, PermissionError, OSError):
        return ""


def _mode(path: Path) -> str:
    """Return mode as the bash ``_mode`` helper does — octal string with the
    setuid/setgid/sticky high bits and the leading zero stripped (so callers
    compare against ``"750"``/``"2775"`` directly)."""
    try:
        m = path.stat().st_mode & 0o7777
    except (FileNotFoundError, PermissionError, OSError):
        return ""
    return format(m, "o")


def _git_as_owner(repo: Path, *args: str) -> tuple[int, str, str]:
    """Run ``git -C repo args``; when running as root and the repo is owned by
    a different user, drop privileges via ``sudo -u <owner>`` to sidestep
    git's ``safe.directory`` guard. Mirrors the bash helper.

    Returns (returncode, stdout, stderr)."""
    git_dir = repo / ".git"
    owner = _owner(git_dir) or _owner(repo)
    try:
        cur = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        cur = ""
    if not owner or cur == owner:
        cmd = ["git", "-C", str(repo), *args]
    elif os.geteuid() == 0:
        cmd = ["sudo", "-u", owner, "git", "-C", str(repo), *args]
    else:
        cmd = ["git", "-C", str(repo), *args]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=30,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except (OSError, subprocess.SubprocessError):
        return 1, "", ""


def _git_config_get(repo: Path, *args: str) -> str:
    """Read a stored git config value (literal — sidesteps insteadOf rewrites)
    via ``_git_as_owner``. Returns '' on missing key or git failure."""
    rc, out, _ = _git_as_owner(repo, "config", "--get", *args)
    return out.strip() if rc == 0 else ""


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


# ---------------------------------------------------------------------------
# values_helper subprocess wrappers — keep python3 (system) interpreter to
# match the bash, which deliberately avoids depending on the rails venv.
# ---------------------------------------------------------------------------


def _values_get(values_file: Path, values_helper: Path, dotted: str) -> str:
    if not values_file.is_file() or not values_helper.is_file():
        return ""
    try:
        proc = subprocess.run(
            ["python3", str(values_helper), "--values", str(values_file), "get", dotted],
            capture_output=True, text=True, check=False, timeout=30,
        )
        return proc.stdout.strip() if proc.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def _values_helper_run(
    values_helper: Path, *args: str, values_file: Path | None = None,
) -> tuple[int, str, str]:
    """Run any values_helper subcommand; ``values_file`` flag is prepended
    when provided (matches bash invocation shape)."""
    cmd = ["python3", str(values_helper)]
    if values_file is not None:
        cmd += ["--values", str(values_file)]
    cmd += list(args)
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=60,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except (OSError, subprocess.SubprocessError):
        return 1, "", ""


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

    # Target dir mode + group ownership: HERMES_HOME must be root:agent 02775
    # (setgid). Drift here silently breaks the gateway: without g+w the
    # runtime can't create gateway.lock / gateway.pid / platforms/, and
    # without setgid+group those files won't inherit the hermes group.
    tmode = _mode(target)
    towner_g = f"{_owner(target)}:{_group(target)}"
    expected_owner_group = f"{s.rails_owner}:{s.agent_group}"
    if tmode == "2775" and towner_g == expected_owner_group:
        s.v_ok(f"target dir: {tmode} {towner_g}")
    else:
        s.v_drift(
            "target dir",
            f"mode={tmode} owner={towner_g} (expected 2775 {expected_owner_group})",
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
    bad: list[str] = []
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
        mode = _mode(auth_dir)
        owner_g = f"{_owner(auth_dir)}:{_group(auth_dir)}"
        expected = f"{s.rails_owner}:{s.agent_group}"
        if mode == "750" and owner_g == expected:
            s.v_ok(f"auth dir: {mode} {owner_g}")
        else:
            s.v_drift(
                "auth dir",
                f"mode={mode} owner={owner_g} (expected 750 {expected})",
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
        # Sorted glob so iteration order is deterministic across runs.
        for f in sorted(list(auth_dir.glob("*.pem")) + list(auth_dir.glob("*.env"))):
            if not f.exists():
                continue
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
    cmode = _mode(cfg)
    cowner = _owner(cfg)
    if cmode == "644" and cowner == s.agent_owner:
        s.v_ok(f"config.yaml: {cmode} {cowner}")
    else:
        s.v_drift(
            "config.yaml",
            f"mode={cmode} owner={cowner} (expected 644 {s.agent_owner})",
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
        rc, out, _ = _git_as_owner(state, "config", "--local", k)
        val = out.strip() if rc == 0 else ""
        if val:
            s.v_ok(f"state git {k}: {val}")
        else:
            s.v_drift(f"state git {k}", "not configured")
    rc, out, _ = _git_as_owner(state, "remote", "get-url", "origin")
    origin_url = out.strip() if rc == 0 else ""
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
        rc, out, _ = _git_as_owner(
            fork, "describe", "--always", "--dirty", "--abbrev=40",
        )
        fork_sha = out.strip() if rc == 0 else ""
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
    rc, out, errout = _values_helper_run(
        s.values_helper, "validate-schema", values_file=s.values_file,
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


def _check_quay_artefacts(s: _State, repos_tsv: str) -> None:
    """All quay-binary, data-dir, operator-glue, and runtime-manager checks
    that gate on quay.version being non-empty."""
    target = s.args.target
    quay_dir = target / "quay"

    quay_bin = Path(s.quay_bin)
    if not (quay_bin.is_file() and os.access(quay_bin, os.X_OK)):
        s.v_drift("quay binary", f"missing or non-executable: {quay_bin}")
    else:
        qmode = _mode(quay_bin)
        qowner = _owner(quay_bin)
        if qmode == "755" and qowner == s.rails_owner:
            s.v_ok(f"quay binary: {qmode} {qowner}")
        else:
            s.v_drift(
                "quay binary",
                f"mode={qmode} owner={qowner} (expected 755 {s.rails_owner})",
            )
        try:
            proc = subprocess.run(
                [str(quay_bin), "--version"],
                capture_output=True, text=True, check=False, timeout=10,
            )
            actual_version = (proc.stdout.splitlines() or [""])[0] if proc.returncode == 0 else ""
        except (OSError, subprocess.SubprocessError):
            actual_version = ""
        # quay.version is a git tag (`v0.1.0`); the binary embeds
        # `${pkg.version}+${shortSHA}` (`0.1.0+abc1234`) — no `v` prefix.
        # Strip the leading `v` from the pin for the substring compare so a
        # clean release-built binary doesn't fire false drift.
        pin_semver = s.quay_version.removeprefix("v")
        if actual_version and pin_semver in actual_version:
            s.v_ok(f"quay binary version: {actual_version}")
        else:
            s.v_drift(
                "quay binary version",
                f"got '{actual_version or '?'}' (expected to contain '{pin_semver}')",
            )

    # Data dir + config.toml
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
            cmode = _mode(quay_cfg)
            cowner = _owner(quay_cfg)
            if cmode == "644" and cowner == s.agent_owner:
                s.v_ok(f"quay config.toml: {cmode} {cowner}")
            else:
                s.v_drift(
                    "quay config.toml",
                    f"mode={cmode} owner={cowner} (expected 644 {s.agent_owner})",
                )

    # Operator-invocation glue
    quay_wrapper = Path(s.quay_wrapper)
    if quay_wrapper.is_file() and os.access(quay_wrapper, os.X_OK):
        wmode = _mode(quay_wrapper)
        wowner = _owner(quay_wrapper)
        if wmode == "755" and wowner == s.rails_owner:
            s.v_ok(f"quay-as-hermes wrapper: {wmode} {wowner}")
        else:
            s.v_drift(
                "quay-as-hermes wrapper",
                f"mode={wmode} owner={wowner} (expected 755 {s.rails_owner})",
            )
    else:
        s.v_drift(
            "quay-as-hermes wrapper", f"missing or non-executable: {quay_wrapper}",
        )

    quay_runner = Path(s.quay_runner)
    if quay_runner.is_file() and os.access(quay_runner, os.X_OK):
        rmode = _mode(quay_runner)
        rowner = _owner(quay_runner)
        if rmode == "755" and rowner == s.rails_owner:
            s.v_ok(f"quay-tick-runner: {rmode} {rowner}")
        else:
            s.v_drift(
                "quay-tick-runner",
                f"mode={rmode} owner={rowner} (expected 755 {s.rails_owner})",
            )
    else:
        s.v_drift("quay-tick-runner", f"missing or non-executable: {quay_runner}")

    quay_profile = Path(s.quay_profile)
    if quay_profile.is_file():
        pmode = _mode(quay_profile)
        powner = _owner(quay_profile)
        if pmode == "644" and powner == s.rails_owner:
            s.v_ok(f"quay profile.d drop-in: {pmode} {powner}")
        else:
            s.v_drift(
                "quay profile.d drop-in",
                f"mode={pmode} owner={powner} (expected 644 {s.rails_owner})",
            )
    else:
        s.v_drift("quay profile.d drop-in", f"missing: {quay_profile}")

    # Runtime managers (bun, ...) — every distinct repos[].quay.package_manager.
    declared: list[str] = []
    for _id, _url, _base, rt_pkg, _install in _parse_repos_tsv(repos_tsv):
        if rt_pkg and rt_pkg not in declared:
            declared.append(rt_pkg)
    rt_install_dir = Path(s.runtime_dir)
    for rt_name in declared:
        rt_path = rt_install_dir / rt_name
        rt_pin = _values_get(
            s.values_file,
            s.values_helper,
            f"quay.runtime_managers.{rt_name}.version",
        )
        if not rt_pin:
            s.v_drift(
                f"runtime manager {rt_name}",
                f"no quay.runtime_managers.{rt_name}.version pin in {s.values_file}",
            )
            continue
        if not (rt_path.is_file() and os.access(rt_path, os.X_OK)):
            s.v_drift(
                f"runtime manager {rt_name}",
                f"missing or non-executable: {rt_path}",
            )
            continue
        rt_mode = _mode(rt_path)
        rt_owner = _owner(rt_path)
        if rt_mode == "755" and rt_owner == s.rails_owner:
            s.v_ok(f"runtime manager {rt_name}: {rt_mode} {rt_owner}")
        else:
            s.v_drift(
                f"runtime manager {rt_name}",
                f"mode={rt_mode} owner={rt_owner} (expected 755 {s.rails_owner})",
            )
        try:
            proc = subprocess.run(
                [str(rt_path), "--version"],
                capture_output=True, text=True, check=False, timeout=10,
            )
            rt_actual = (proc.stdout.splitlines() or [""])[0] if proc.returncode == 0 else ""
        except (OSError, subprocess.SubprocessError):
            rt_actual = ""
        if rt_actual and rt_pin in rt_actual:
            s.v_ok(f"runtime manager {rt_name} version: {rt_actual}")
        else:
            s.v_drift(
                f"runtime manager {rt_name} version",
                f"got '{rt_actual or '?'}' (expected to contain '{rt_pin}')",
            )


def _quay_registered_ids(s: _State) -> tuple[str, bool]:
    """Returns (newline-joined ids, list_failed). Empty + False means quay not
    enabled / not present, no list attempted."""
    target = s.args.target
    quay_bin = Path(s.quay_bin)
    if not (
        s.quay_version
        and quay_bin.is_file() and os.access(quay_bin, os.X_OK)
        and (target / "quay").is_dir()
    ):
        return "", False
    cmd_prefix: list[str] = []
    try:
        cur = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        cur = ""
    if os.geteuid() == 0 and cur != s.agent_owner:
        cmd_prefix = ["sudo", "-u", s.agent_owner]
    cmd = [
        *cmd_prefix,
        "env", f"QUAY_DATA_DIR={target / 'quay'}", str(quay_bin), "repo", "list",
    ]
    try:
        proc1 = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=30,
        )
        if proc1.returncode != 0:
            return "", True
        proc2 = subprocess.run(
            ["python3", str(s.values_helper), "parse-repo-list-ids"],
            input=proc1.stdout, capture_output=True, text=True,
            check=False, timeout=30,
        )
        if proc2.returncode != 0:
            return "", True
        return proc2.stdout.strip(), False
    except (OSError, subprocess.SubprocessError):
        return "", True


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
        # Code mirror checks (every entry)
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
            # since install, itself worth surfacing.
            rc1, out1, _ = _git_as_owner(code_dir, "rev-parse", "HEAD")
            mhead = out1.strip() if rc1 == 0 else ""
            rc2, out2, _ = _git_as_owner(code_dir, "rev-parse", f"origin/{repo_base}")
            expected_head = out2.strip() if rc2 == 0 else ""
            if mhead and expected_head and mhead == expected_head:
                s.v_ok(f"code mirror {repo_id} at origin/{repo_base}")
            else:
                s.v_drift(
                    f"code mirror {repo_id} branch",
                    f"HEAD={mhead or '?'} expected origin/{repo_base}={expected_head or '?'}",
                )

        # Quay-side checks
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
        def show(prop: str) -> str:
            try:
                proc = subprocess.run(
                    ["systemctl", "show", "-p", prop, "--value", u],
                    capture_output=True, text=True, check=False, timeout=10,
                )
                return proc.stdout.strip() if proc.returncode == 0 else ""
            except (OSError, subprocess.SubprocessError):
                return ""
        active = show("ActiveState")
        load = show("LoadState")
        unitfile = show("UnitFileState")
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
    cmd_prefix: list[str] = []
    try:
        cur = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        cur = ""
    if os.geteuid() == 0 and cur != s.agent_owner:
        cmd_prefix = ["sudo", "-u", s.agent_owner]
    cmd = [
        *cmd_prefix,
        "env", f"HERMES_HOME={target}", str(venv_py), str(helper_py), "mint",
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=30,
        )
        app_token = proc.stdout.strip() if proc.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        app_token = ""
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
            config_input = f'header = "Authorization: Bearer {app_token}"\n'
            try:
                proc = subprocess.run(
                    [
                        "curl", "-sS", "-o", "/dev/null",
                        "-w", "%{http_code}",
                        "--config", "-",
                        "-H", "Accept: application/vnd.github+json",
                        "-H", "X-GitHub-Api-Version: 2022-11-28",
                        api_url,
                    ],
                    input=config_input,
                    capture_output=True, text=True, check=False, timeout=30,
                )
                http_code = (proc.stdout or "").strip() or "000"
            except (OSError, subprocess.SubprocessError):
                http_code = "000"
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
        rc, out, _ = _git_as_owner(ws, "remote", "get-url", rname)
        url = out.strip() if rc == 0 else ""
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

    # Agent user must exist (verify needs the home for default --target).
    try:
        agent_pw = pwd.getpwnam(args.user)
    except KeyError:
        print(f"agent user '{args.user}' does not exist", file=err)
        return 1
    target = args.target if args.target is not None else Path(agent_pw.pw_dir) / ".hermes"
    args = VerifyArgs(
        fork=args.fork,
        target=target,
        user=args.user,
        auth_method=args.auth_method,
        quiet=args.quiet,
        values=args.values,
        gh_api_base=args.gh_api_base,
    )

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
    if values_file.is_file() and values_helper.is_file():
        quay_version = _values_get(values_file, values_helper, "quay.version")

    s = _State(
        args=args,
        out=out,
        err=err,
        rails_owner=rails_owner,
        agent_owner=agent_owner,
        agent_group=agent_group,
        quay_bin=os.environ.get("HERMES_VERIFY_QUAY_BIN") or "/usr/local/bin/quay",
        quay_wrapper=os.environ.get("HERMES_VERIFY_QUAY_WRAPPER") or "/usr/local/bin/quay-as-hermes",
        quay_runner=os.environ.get("HERMES_VERIFY_QUAY_RUNNER") or "/usr/local/sbin/quay-tick-runner",
        quay_profile=os.environ.get("HERMES_VERIFY_QUAY_PROFILE") or "/etc/profile.d/quay-data-dir.sh",
        runtime_dir=os.environ.get("HERMES_VERIFY_RUNTIME_DIR") or "/usr/local/bin",
        values_file=values_file,
        values_helper=values_helper,
        quay_version=quay_version,
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
    _check_per_entry_repos(s, repos_tsv)
    _check_systemd(s)
    if app_auth_expected:
        _check_token_helper(s, repos_tsv)
    _check_upstream_workspace(s)

    print(
        f"==> verify: {s.total} checks, {s.drift} drift",
        file=out, flush=True,
    )
    return 0 if s.drift == 0 else 1
