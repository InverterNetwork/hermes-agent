"""Shared fixtures for hermes_installer tests.

A "fake bun" is the only runtime we have a recipe for today, so the helpers
build a zip whose member layout (``bun-linux-x64/bun``) matches the recipe
in ``installer/hermes_installer/runtimes.py:_RECIPES['bun']``.
"""

from __future__ import annotations

import hashlib
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
def patch_urlretrieve(monkeypatch, fake_bun_zip):
    """Stub ``urllib.request.urlretrieve`` to copy the fake zip's bytes
    instead of going to the network."""
    src_zip, _sha = fake_bun_zip

    def _fake(url: str, dst):  # noqa: ARG001 — url not asserted
        Path(dst).write_bytes(src_zip.read_bytes())

    monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)
    return _fake
