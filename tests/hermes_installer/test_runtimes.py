"""Pinned-binary install machinery tests.

Covers:
* SHA-mismatch refusal — installer must exit non-zero on bad SHA.
* Idempotent re-install — binary at pinned version is a no-op (no fetch).
* Wrong-version replacement — pinned version mismatch triggers replace.
* Missing archive member — extracted zip without the recipe member fails loud.
* Atomic install — half-installed binaries don't survive failed extracts.
* Permission/root failure — require_root() exits when euid != 0.

Tests fabricate a local zip, monkeypatch ``urllib.request.urlretrieve`` to
serve that zip in place of a network fetch, and run ``ensure_runtime`` /
``ensure_runtimes`` end-to-end.
"""

from __future__ import annotations

import hashlib
import os
import stat
import zipfile
from pathlib import Path

import pytest

from installer.hermes_installer.config import RuntimeManagerPin
from installer.hermes_installer import runtimes
from installer.hermes_installer.runtimes import (
    _RECIPES,
    ensure_runtime,
    ensure_runtimes,
)
from installer.hermes_installer.util import require_root


def _fake_bun_script(version: str) -> str:
    return (
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{version}"; exit 0; fi\n'
        "exit 1\n"
    )


def _build_bun_zip(tmp_path: Path, version: str) -> tuple[Path, str]:
    """Create a bun-shaped zip at ``tmp_path/bun.zip``. Returns (zip_path, sha256)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    zip_path = tmp_path / "bun.zip"
    member_data = _fake_bun_script(version).encode()
    with zipfile.ZipFile(zip_path, "w") as zf:
        info = zipfile.ZipInfo("bun-linux-x64/bun")
        info.external_attr = (0o755 << 16) | 0x8000  # regular file, mode 0755
        zf.writestr(info, member_data)
    sha = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    return zip_path, sha


@pytest.fixture
def bun_recipe():
    return _RECIPES["bun"]


@pytest.fixture
def fake_zip(tmp_path: Path) -> tuple[Path, str]:
    return _build_bun_zip(tmp_path / "src", "1.3.9")


@pytest.fixture
def patched_urlretrieve(monkeypatch, fake_zip):
    src_zip, _sha = fake_zip

    def _fake(url: str, dst):  # noqa: ARG001 — url not asserted
        Path(dst).write_bytes(src_zip.read_bytes())

    monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)
    return _fake


class TestEnsureRuntime:
    def test_happy_path_installs_executable(
        self, tmp_path: Path, bun_recipe, fake_zip, patched_urlretrieve
    ):
        _, sha = fake_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        ensure_runtime(pin, bun_recipe, install_path)

        assert install_path.is_file()
        assert install_path.stat().st_mode & stat.S_IXUSR
        # Mode bits are 0755 (within the lower 12 bits).
        assert (install_path.stat().st_mode & 0o777) == 0o755

    def test_sha_mismatch_refuses(
        self, tmp_path: Path, bun_recipe, fake_zip, monkeypatch, capsys
    ):
        src_zip, _ = fake_zip

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(src_zip.read_bytes())

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        bad_pin = RuntimeManagerPin(
            name="bun",
            version="1.3.9",
            linux_x64_sha256="0" * 64,  # never matches a real zip
        )
        install_path = tmp_path / "bin" / "bun"
        with pytest.raises(SystemExit) as excinfo:
            ensure_runtime(bad_pin, bun_recipe, install_path)
        assert excinfo.value.code == 1
        err = capsys.readouterr().err
        assert "SHA256 mismatch" in err
        assert not install_path.exists(), "no half-install on SHA refusal"

    def test_idempotent_when_already_at_pinned_version(
        self, tmp_path: Path, bun_recipe, fake_zip, monkeypatch
    ):
        _, sha = fake_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir(parents=True)
        install_path.write_text(_fake_bun_script("1.3.9"))
        os.chmod(install_path, 0o755)
        before_mtime = install_path.stat().st_mtime_ns

        # If urlretrieve is invoked, the test fails — idempotent path must
        # short-circuit before touching the network.
        def _explode(url, dst):  # noqa: ARG001
            raise AssertionError("urlretrieve called on idempotent re-install")

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _explode)
        ensure_runtime(pin, bun_recipe, install_path)
        assert install_path.stat().st_mtime_ns == before_mtime, "binary should not be replaced"

    def test_wrong_version_present_triggers_replace(
        self, tmp_path: Path, bun_recipe, fake_zip, patched_urlretrieve
    ):
        _, sha = fake_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir(parents=True)
        install_path.write_text(_fake_bun_script("0.0.1"))  # stale
        os.chmod(install_path, 0o755)

        ensure_runtime(pin, bun_recipe, install_path)

        # The new contents come from the fresh download — assert the version
        # probe now reports the pin.
        import subprocess

        out = subprocess.run(
            [str(install_path), "--version"], capture_output=True, text=True
        )
        assert out.returncode == 0
        assert "1.3.9" in out.stdout

    def test_missing_archive_member_fails_loud(
        self, tmp_path: Path, bun_recipe, monkeypatch, capsys
    ):
        # Build a zip whose only member is at the wrong path.
        bad_zip = tmp_path / "bad.zip"
        with zipfile.ZipFile(bad_zip, "w") as zf:
            zf.writestr("not-bun/elsewhere", b"x")
        sha = hashlib.sha256(bad_zip.read_bytes()).hexdigest()

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(bad_zip.read_bytes())

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        with pytest.raises(SystemExit):
            ensure_runtime(pin, bun_recipe, install_path)
        assert "archive member" in capsys.readouterr().err


class TestEnsureRuntimes:
    def test_empty_pins_is_noop(self, tmp_path: Path):
        # Passing an empty pins dict must not even create the install_dir.
        ensure_runtimes({}, install_dir=tmp_path / "bin")
        assert not (tmp_path / "bin").exists()

    def test_unknown_recipe_fails_loud(self, tmp_path: Path, capsys):
        pin = RuntimeManagerPin(
            name="pnpm",
            version="9.0.0",
            linux_x64_sha256="0" * 64,
        )
        with pytest.raises(SystemExit):
            ensure_runtimes({"pnpm": pin}, install_dir=tmp_path / "bin")
        # The diagnostic must point at the file an operator would edit to add
        # the missing recipe.
        assert "runtimes.py" in capsys.readouterr().err


class TestRequireRoot:
    def test_non_root_exits(self, monkeypatch, capsys):
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        with pytest.raises(SystemExit) as excinfo:
            require_root()
        # Exit code 2 — the CLI uses 1 for value/diagnostic failures and 2 for
        # invariant violations like "must be root".
        assert excinfo.value.code == 2
        assert "root" in capsys.readouterr().err

    def test_root_passes(self, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        require_root()  # must not raise
