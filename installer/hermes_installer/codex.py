"""Install the pinned Codex CLI for quay agent workers.

Codex is not a runtime manager for repository bootstrap; it is the worker
binary quay shells out to. That means the executable belongs under the
agent user's home (so the user can own its CLI state), with a root-owned
system symlink for root-owned wrappers and PATH probes.
"""

from __future__ import annotations

import hashlib
import os
import pwd
import shutil
import stat
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from .config import CodexPin
from .util import fail, info


_ASSET_NAME = "codex-x86_64-unknown-linux-musl"
_ASSET_ARCHIVE = f"{_ASSET_NAME}.tar.gz"
_URL_TEMPLATE = (
    "https://github.com/openai/codex/releases/download/{version}/"
    + _ASSET_ARCHIVE
)


def ensure_codex(
    pin: CodexPin | None,
    *,
    agent_user: str,
    symlink_path: Path = Path("/usr/local/bin/codex"),
    agent_home: Path | None = None,
) -> None:
    if pin is None:
        return

    pw = _agent_user(agent_user)
    home = agent_home or Path(pw.pw_dir)
    install_path = home / ".local" / "bin" / "codex"

    if _codex_at_pinned_version(install_path, pin.version, pw.pw_uid, pw.pw_gid):
        _ensure_symlink(install_path, symlink_path)
        info(f"codex {pin.version} already at {install_path}; no-op")
        return

    arch = _host_arch()
    if arch != "x86_64":
        fail(
            f"codex {pin.version}: only linux-x86_64 is supported today "
            f"(host reports {arch!r}). Add a linux_<arch>_sha256 pin in "
            "deploy.values.yaml and an arch-aware recipe in codex.py to extend."
        )

    url = _URL_TEMPLATE.format(version=pin.version)
    info(f"installing codex {pin.version} -> {install_path}")
    info(f"  fetching {url}")

    with tempfile.TemporaryDirectory(prefix="hermes-codex-") as tmp_str:
        tmp = Path(tmp_str)
        archive_path = tmp / _ASSET_ARCHIVE
        try:
            urllib.request.urlretrieve(url, archive_path)
        except (OSError, urllib.error.URLError) as exc:
            fail(f"download failed for codex {pin.version} ({url}): {exc}")

        actual_sha = _sha256(archive_path)
        if actual_sha != pin.linux_x64_sha256:
            fail(
                f"SHA256 mismatch for codex {pin.version}\n"
                f"       expected: {pin.linux_x64_sha256}\n"
                f"       actual:   {actual_sha}\n"
                f"       url:      {url}"
            )

        source_path = _extract_codex(archive_path, tmp, pin.version)
        _ensure_agent_bin_dir(install_path.parent, pw.pw_uid, pw.pw_gid)
        _atomic_install_for_user(source_path, install_path, pw.pw_uid, pw.pw_gid)

    _ensure_symlink(install_path, symlink_path)
    info(f"codex {pin.version} installed at {install_path}")


def _agent_user(agent_user: str) -> pwd.struct_passwd:
    try:
        return pwd.getpwnam(agent_user)
    except KeyError:
        fail(f"agent user not found: {agent_user}")


def _extract_codex(archive_path: Path, tmp: Path, version: str) -> Path:
    extract_dir = tmp / "extract"
    extract_dir.mkdir()
    try:
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(extract_dir)
    except tarfile.TarError as exc:
        fail(f"codex {version}: not a valid tar.gz ({exc})")

    member_path = extract_dir / _ASSET_NAME
    if not member_path.is_file():
        fail(f"codex {version}: archive member {_ASSET_NAME!r} missing after extract")
    return member_path


def _atomic_install_for_user(src: Path, dst: Path, uid: int, gid: int) -> None:
    staged = dst.with_name(dst.name + ".hermes-staging")
    try:
        shutil.copy2(src, staged)
        os.chmod(staged, 0o755)
        try:
            os.chown(staged, uid, gid)
        except PermissionError:
            pass
        os.replace(staged, dst)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise


def _ensure_agent_bin_dir(bin_dir: Path, uid: int, gid: int) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    for path in (bin_dir.parent, bin_dir):
        try:
            os.chown(path, uid, gid)
        except PermissionError:
            pass
        os.chmod(path, 0o755)


def _ensure_symlink(target: Path, link: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    current = None
    if link.is_symlink():
        current = Path(os.readlink(link))
    elif link.exists():
        link.unlink()
    if current != target:
        tmp = link.with_name(link.name + ".hermes-staging")
        tmp.unlink(missing_ok=True)
        os.symlink(target, tmp)
        os.replace(tmp, link)
    try:
        os.lchown(link, 0, 0)
    except PermissionError:
        pass


def _codex_at_pinned_version(
    install_path: Path,
    pinned_version: str,
    uid: int,
    gid: int,
) -> bool:
    if not install_path.exists():
        return False
    st = install_path.stat()
    if not (st.st_mode & stat.S_IXUSR):
        return False
    if os.geteuid() == 0:
        if st.st_uid != uid or st.st_gid != gid or (st.st_mode & 0o777) != 0o755:
            return False
    try:
        out = subprocess.run(
            [str(install_path), "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if out.returncode != 0:
        return False
    want = _version_number(pinned_version)
    return want in (out.stdout + out.stderr)


def _version_number(version: str) -> str:
    if version.startswith("rust-v"):
        return version.removeprefix("rust-v")
    return version.removeprefix("v")


def _host_arch() -> str:
    return os.uname().machine


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
