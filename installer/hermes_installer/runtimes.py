"""Pinned-binary install machinery for runtime managers (bun, ...).

Each entry in ``_RECIPES`` knows how to turn a version pin into a download
URL and where in the extracted archive its executable lives.
``ensure_runtime`` fetches the upstream zip, verifies the SHA256 against
the values-file pin, extracts the binary, and atomically installs it
root:root 0755 at the target path. Idempotent — a binary already at the
pinned version is a no-op.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import tempfile
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .config import RuntimeManagerPin
from .util import fail, info


@dataclass(frozen=True)
class _Recipe:
    url_template: str  # uses {version}; pinned to the linux x86_64 asset
    archive_member: str  # path inside the zip to the executable to install


_RECIPES: dict[str, _Recipe] = {
    "bun": _Recipe(
        url_template=(
            "https://github.com/oven-sh/bun/releases/download/"
            "bun-v{version}/bun-linux-x64.zip"
        ),
        archive_member="bun-linux-x64/bun",
    ),
}


def ensure_runtimes(
    pins: dict[str, RuntimeManagerPin],
    install_dir: Path = Path("/usr/local/bin"),
) -> None:
    if not pins:
        return
    install_dir.mkdir(parents=True, exist_ok=True)
    for name in sorted(pins):
        ensure_runtime(pins[name], install_dir / name)


def ensure_runtime(pin: RuntimeManagerPin, install_path: Path) -> None:
    recipe = _RECIPES.get(pin.name)
    if recipe is None:
        fail(
            f"no install recipe for runtime manager {pin.name!r}; "
            f"add a _Recipe entry to installer/hermes_installer/runtimes.py"
        )

    if _binary_at_pinned_version(install_path, pin.version):
        info(f"{pin.name} {pin.version} already at {install_path}; no-op")
        return

    arch = _host_arch()
    if arch != "x86_64":
        fail(
            f"{pin.name} {pin.version}: only linux-x86_64 is supported today "
            f"(host reports {arch!r}). Add a linux_<arch>_sha256 pin in "
            "deploy.values.yaml and an arch-aware recipe in runtimes.py to extend."
        )

    url = recipe.url_template.format(version=pin.version)
    info(f"installing {pin.name} {pin.version} → {install_path}")
    info(f"  fetching {url}")

    with tempfile.TemporaryDirectory(prefix=f"hermes-{pin.name}-") as tmp_str:
        tmp = Path(tmp_str)
        zip_path = tmp / f"{pin.name}.zip"
        try:
            urllib.request.urlretrieve(url, zip_path)
        except (OSError, urllib.error.URLError) as exc:
            fail(f"download failed for {pin.name} {pin.version} ({url}): {exc}")

        actual_sha = _sha256(zip_path)
        if actual_sha != pin.linux_x64_sha256:
            fail(
                f"SHA256 mismatch for {pin.name} {pin.version}\n"
                f"       expected: {pin.linux_x64_sha256}\n"
                f"       actual:   {actual_sha}\n"
                f"       url:      {url}"
            )

        extract_dir = tmp / "extract"
        extract_dir.mkdir()
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(extract_dir)
        except zipfile.BadZipFile as exc:
            fail(f"{pin.name} {pin.version}: not a valid zip ({exc})")

        member_path = extract_dir / recipe.archive_member
        if not member_path.is_file():
            fail(
                f"{pin.name} {pin.version}: archive member "
                f"{recipe.archive_member!r} missing after extract"
            )

        _atomic_install(member_path, install_path)

    info(f"{pin.name} {pin.version} installed at {install_path}")


def _atomic_install(src: Path, dst: Path) -> None:
    """Stage next to ``dst`` then ``os.replace`` so a torn write never leaves
    a half-installed binary. ``os.replace`` is atomic on POSIX when source
    and destination share a filesystem; staging in the same dir guarantees that.
    """
    staged = dst.with_name(dst.name + ".hermes-staging")
    try:
        shutil.copy2(src, staged)
        os.chmod(staged, 0o755)
        try:
            os.chown(staged, 0, 0)
        except PermissionError:
            # Tests run non-root with --skip-root-check; chown only matters in prod.
            pass
        os.replace(staged, dst)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise


def _host_arch() -> str:
    return os.uname().machine


def _binary_at_pinned_version(install_path: Path, pinned_version: str) -> bool:
    """Substring-match ``<binary> --version`` against the pin — absorbs
    `+commit-sha` build suffixes that the upstream binary may emit.

    When running as root, also require the safe-install contract
    (root:root 0755). A binary at the pinned version with the wrong owner
    or mode might have been swapped by an agent-writable predecessor; its
    bytes are no longer trustworthy, so a chmod-back is unsafe — force a
    re-download + SHA verify by returning False here. The strict check is
    bypassed when not running as root because tests can't make files
    root-owned.
    """
    if not install_path.exists():
        return False
    st = install_path.stat()
    if not (st.st_mode & stat.S_IXUSR):
        return False
    if os.geteuid() == 0:
        if st.st_uid != 0 or (st.st_mode & 0o777) != 0o755:
            return False
    try:
        out = subprocess.run(
            [str(install_path), "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if out.returncode != 0:
        return False
    return pinned_version in (out.stdout + out.stderr)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
