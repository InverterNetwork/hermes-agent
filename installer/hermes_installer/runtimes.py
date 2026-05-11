"""Pinned-binary install machinery for runtime managers (bun, pnpm, ...).

Each entry in ``_RECIPES`` knows how to turn a version pin into a download
URL and how to extract the executable from the downloaded asset. Two
archive kinds are supported:

* ``"zip"`` — upstream ships a zip (bun); ``archive_member`` is the path
  inside the zip to the executable to install.
* ``"binary"`` — upstream ships the executable directly (pnpm); the
  downloaded file IS the install target.

``ensure_runtime`` fetches the upstream asset, verifies the SHA256
against the values-file pin, extracts the binary (zip path) or uses the
downloaded file as-is (binary path), and atomically installs it
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
from typing import Literal

from .config import RuntimeManagerPin
from .util import fail, info


ArchiveKind = Literal["zip", "binary"]


@dataclass(frozen=True)
class _Recipe:
    url_template: str  # uses {version}; pinned to the linux x86_64 asset
    archive_kind: ArchiveKind = "zip"
    # Path inside the zip to the executable; required for ``archive_kind="zip"``
    # and must be ``None`` for ``archive_kind="binary"`` (the downloaded file
    # IS the install target).
    archive_member: str | None = None

    def __post_init__(self) -> None:
        if self.archive_kind == "zip" and not self.archive_member:
            raise ValueError(
                "zip recipe requires `archive_member` (path inside the zip)"
            )
        if self.archive_kind == "binary" and self.archive_member is not None:
            raise ValueError(
                "binary recipe must not set `archive_member` "
                "(the downloaded file is the install target)"
            )


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
        suffix = ".zip" if recipe.archive_kind == "zip" else ""
        download_path = tmp / f"{pin.name}{suffix}"
        try:
            urllib.request.urlretrieve(url, download_path)
        except (OSError, urllib.error.URLError) as exc:
            fail(f"download failed for {pin.name} {pin.version} ({url}): {exc}")

        actual_sha = _sha256(download_path)
        if actual_sha != pin.linux_x64_sha256:
            fail(
                f"SHA256 mismatch for {pin.name} {pin.version}\n"
                f"       expected: {pin.linux_x64_sha256}\n"
                f"       actual:   {actual_sha}\n"
                f"       url:      {url}"
            )

        if recipe.archive_kind == "zip":
            source_path = _extract_zip_member(
                pin=pin, recipe=recipe, zip_path=download_path, tmp=tmp,
            )
        else:
            source_path = download_path

        _atomic_install(source_path, install_path)

    info(f"{pin.name} {pin.version} installed at {install_path}")


def _extract_zip_member(
    *,
    pin: RuntimeManagerPin,
    recipe: _Recipe,
    zip_path: Path,
    tmp: Path,
) -> Path:
    extract_dir = tmp / "extract"
    extract_dir.mkdir()
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)
    except zipfile.BadZipFile as exc:
        fail(f"{pin.name} {pin.version}: not a valid zip ({exc})")

    assert recipe.archive_member is not None  # validated in _Recipe.__post_init__
    member_path = extract_dir / recipe.archive_member
    if not member_path.is_file():
        fail(
            f"{pin.name} {pin.version}: archive member "
            f"{recipe.archive_member!r} missing after extract"
        )
    return member_path


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
        if st.st_uid != 0 or st.st_gid != 0 or (st.st_mode & 0o777) != 0o755:
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
