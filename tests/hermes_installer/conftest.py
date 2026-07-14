"""Shared fixtures for hermes_installer tests.

A "fake bun" exercises the zip-archive recipe path; a "fake binary"
exercises the plain-binary recipe path. The zip's member layout
(``bun-linux-x64/bun``) matches the recipe in
``installer/hermes_installer/runtimes.py:_RECIPES['bun']``; the
plain-binary helper builds a single executable file with no archive
wrapper — the same shape that a pnpm-style ``pnpm-linux-x64`` release
asset has.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from installer.hermes_installer import runtimes


def fake_bun_script(version: str) -> str:
    return (
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{version}"; exit 0; fi\n'
        "exit 1\n"
    )


def build_bun_zip(out_dir: Path, version: str) -> tuple[Path, str]:
    """Create a bun-shaped zip in ``out_dir``. Returns (zip_path, sha256)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "bun.zip"
    body = fake_bun_script(version).encode()
    with zipfile.ZipFile(zip_path, "w") as zf:
        info = zipfile.ZipInfo("bun-linux-x64/bun")
        info.external_attr = (0o755 << 16) | 0x8000  # regular file, mode 0755
        zf.writestr(info, body)
    sha = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    return zip_path, sha


def build_anvil_tar(out_dir: Path, version: str) -> tuple[Path, str]:
    """Create a Foundry-shaped tarball containing ``anvil``.

    Returns (tar_path, sha256). The member layout matches the recipe in
    ``installer/hermes_installer/runtimes.py:_RECIPES['anvil']``.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tar_path = out_dir / "foundry.tar.gz"
    body = fake_bun_script(version).encode()
    with tarfile.open(tar_path, "w:gz") as tf:
        info = tarfile.TarInfo("anvil")
        info.mode = 0o755
        info.size = len(body)
        tf.addfile(info, fileobj=io.BytesIO(body))
    sha = hashlib.sha256(tar_path.read_bytes()).hexdigest()
    return tar_path, sha


def build_fake_binary(out_dir: Path, name: str, version: str) -> tuple[Path, str]:
    """Create a fake plain-binary asset (bash script reporting ``--version``)
    in ``out_dir``. Returns (binary_path, sha256). Mirrors the upstream
    pnpm-linux-x64 distribution shape: a single executable file, no archive.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    bin_path = out_dir / name
    bin_path.write_bytes(fake_bun_script(version).encode())
    sha = hashlib.sha256(bin_path.read_bytes()).hexdigest()
    return bin_path, sha


@pytest.fixture(autouse=True)
def _force_x86_64_arch(monkeypatch):
    """Default to ``x86_64`` so tests pass on Apple Silicon dev hosts.
    Tests that exercise the arch-rejection path re-monkeypatch."""
    monkeypatch.setattr(runtimes, "_host_arch", lambda: "x86_64")


@pytest.fixture
def fake_bun_zip(tmp_path: Path) -> tuple[Path, str]:
    """Pre-built bun-shaped zip + its SHA256."""
    return build_bun_zip(tmp_path / "src", "1.3.9")


@pytest.fixture
def fake_anvil_tar(tmp_path: Path) -> tuple[Path, str]:
    """Pre-built Foundry-shaped tarball + its SHA256."""
    return build_anvil_tar(tmp_path / "src", "1.7.1")


@pytest.fixture
def patch_urlretrieve(monkeypatch, fake_bun_zip, fake_anvil_tar):
    """Stub ``urllib.request.urlretrieve`` to copy fake release asset bytes
    instead of going to the network."""
    src_zip, _sha = fake_bun_zip
    src_tar, _anvil_sha = fake_anvil_tar

    def _fake(url: str, dst):
        src = src_tar if "foundry" in url else src_zip
        Path(dst).write_bytes(src.read_bytes())

    monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)
    return _fake
