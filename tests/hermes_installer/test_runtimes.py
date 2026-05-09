"""Pinned-binary install machinery tests.

Covers:
* SHA-mismatch refusal — installer must exit non-zero on bad SHA.
* Idempotent re-install — binary at pinned version is a no-op (no fetch).
* Wrong-version replacement — pinned version mismatch triggers replace.
* Missing archive member — extracted zip without the recipe member fails loud.
* Atomic install — half-installed binaries don't survive failed extracts.
* Permission/root failure — require_root() exits when euid != 0.
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
from installer.hermes_installer.runtimes import ensure_runtime, ensure_runtimes
from installer.hermes_installer.util import require_root

from tests.hermes_installer.conftest import fake_bun_script


class TestEnsureRuntime:
    def test_happy_path_installs_executable(
        self, tmp_path: Path, fake_bun_zip, patch_urlretrieve
    ):
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        ensure_runtime(pin, install_path)

        assert install_path.is_file()
        assert install_path.stat().st_mode & stat.S_IXUSR
        assert (install_path.stat().st_mode & 0o777) == 0o755

    def test_sha_mismatch_refuses(
        self, tmp_path: Path, patch_urlretrieve, capsys
    ):
        bad_pin = RuntimeManagerPin(
            name="bun", version="1.3.9", linux_x64_sha256="0" * 64
        )
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        with pytest.raises(SystemExit) as excinfo:
            ensure_runtime(bad_pin, install_path)
        assert excinfo.value.code == 1
        err = capsys.readouterr().err
        assert "SHA256 mismatch" in err
        assert not install_path.exists(), "no half-install on SHA refusal"

    def test_idempotent_when_already_at_pinned_version(
        self, tmp_path: Path, fake_bun_zip, monkeypatch
    ):
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        install_path.write_text(fake_bun_script("1.3.9"))
        os.chmod(install_path, 0o755)
        before_mtime = install_path.stat().st_mtime_ns

        def _explode(url, dst):  # noqa: ARG001
            raise AssertionError("urlretrieve called on idempotent re-install")

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _explode)
        ensure_runtime(pin, install_path)
        assert install_path.stat().st_mtime_ns == before_mtime

    def test_wrong_version_present_triggers_replace(
        self, tmp_path: Path, fake_bun_zip, patch_urlretrieve
    ):
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        install_path.write_text(fake_bun_script("0.0.1"))
        os.chmod(install_path, 0o755)

        ensure_runtime(pin, install_path)

        import subprocess

        out = subprocess.run(
            [str(install_path), "--version"], capture_output=True, text=True
        )
        assert out.returncode == 0
        assert "1.3.9" in out.stdout

    def test_missing_archive_member_fails_loud(
        self, tmp_path: Path, monkeypatch, capsys
    ):
        bad_zip = tmp_path / "bad.zip"
        with zipfile.ZipFile(bad_zip, "w") as zf:
            zf.writestr("not-bun/elsewhere", b"x")
        sha = hashlib.sha256(bad_zip.read_bytes()).hexdigest()

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(bad_zip.read_bytes())

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        with pytest.raises(SystemExit):
            ensure_runtime(pin, install_path)
        assert "archive member" in capsys.readouterr().err

    def test_unknown_recipe_fails_loud(self, tmp_path: Path, capsys):
        pin = RuntimeManagerPin(
            name="pnpm", version="9.0.0", linux_x64_sha256="0" * 64
        )
        install_path = tmp_path / "bin" / "pnpm"
        install_path.parent.mkdir()
        with pytest.raises(SystemExit):
            ensure_runtime(pin, install_path)
        assert "runtimes.py" in capsys.readouterr().err

    def test_arch_unsupported_fails_loud(
        self, tmp_path: Path, fake_bun_zip, monkeypatch, capsys
    ):
        # Override the conftest autouse fixture to simulate an arm64 host.
        monkeypatch.setattr(runtimes, "_host_arch", lambda: "aarch64")

        def _explode(url, dst):  # noqa: ARG001
            raise AssertionError("urlretrieve called on unsupported-arch host")

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _explode)

        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        with pytest.raises(SystemExit):
            ensure_runtime(pin, install_path)
        err = capsys.readouterr().err
        assert "aarch64" in err
        assert "linux-x86_64" in err
        assert not install_path.exists(), "no half-install on unsupported arch"

    def test_root_with_wrong_owner_triggers_reinstall(
        self, tmp_path: Path, fake_bun_zip, patch_urlretrieve, monkeypatch
    ):
        # Even when the existing binary reports the pinned version, a
        # non-root owner means the bytes are no longer trusted — re-install
        # forces a fresh SHA-verified download instead of chmod-back.
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        # Pre-existing "1.3.9" binary, mode 0755 — but owned by the test
        # user (uid != 0), which is the failure mode we're guarding against.
        install_path.write_text(fake_bun_script("1.3.9"))
        os.chmod(install_path, 0o755)
        assert install_path.stat().st_uid != 0

        called: dict[str, bool] = {"urlretrieve": False}
        original = patch_urlretrieve

        def _track(url, dst):
            called["urlretrieve"] = True
            original(url, dst)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _track)
        ensure_runtime(pin, install_path)
        assert called["urlretrieve"], "wrong-owner binary should trigger re-install"

    def test_root_with_wrong_group_triggers_reinstall(
        self, tmp_path: Path, fake_bun_zip, patch_urlretrieve, monkeypatch
    ):
        # Tests can't make a file root:non-root in the filesystem, so
        # synthesize the stat result. Forging the gid alone (uid=0, mode=0755)
        # exercises the gid limb of the strict-state check in isolation.
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        install_path.write_text(fake_bun_script("1.3.9"))
        os.chmod(install_path, 0o755)
        real_stat = install_path.stat()
        forged = os.stat_result(
            (real_stat.st_mode, real_stat.st_ino, real_stat.st_dev,
             real_stat.st_nlink, 0, 1000, real_stat.st_size,
             real_stat.st_atime, real_stat.st_mtime, real_stat.st_ctime)
        )
        target = str(install_path)
        original_stat = Path.stat
        monkeypatch.setattr(
            Path, "stat",
            lambda self, *a, **kw: forged if str(self) == target else original_stat(self, *a, **kw),
        )

        called: dict[str, bool] = {"urlretrieve": False}

        def _track(url, dst):
            called["urlretrieve"] = True
            patch_urlretrieve(url, dst)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _track)
        ensure_runtime(pin, install_path)
        assert called["urlretrieve"], "non-root group should trigger re-install"

    def test_root_with_wrong_mode_triggers_reinstall(
        self, tmp_path: Path, fake_bun_zip, patch_urlretrieve, monkeypatch
    ):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        _, sha = fake_bun_zip
        pin = RuntimeManagerPin(name="bun", version="1.3.9", linux_x64_sha256=sha)
        install_path = tmp_path / "bin" / "bun"
        install_path.parent.mkdir()
        install_path.write_text(fake_bun_script("1.3.9"))
        os.chmod(install_path, 0o775)  # group-writable — outside the contract

        called: dict[str, bool] = {"urlretrieve": False}

        def _track(url, dst):
            called["urlretrieve"] = True
            patch_urlretrieve(url, dst)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _track)
        ensure_runtime(pin, install_path)
        assert called["urlretrieve"], "wrong-mode binary should trigger re-install"


class TestEnsureRuntimes:
    def test_empty_pins_is_noop(self, tmp_path: Path):
        ensure_runtimes({}, install_dir=tmp_path / "bin")
        assert not (tmp_path / "bin").exists()


class TestRequireRoot:
    def test_non_root_exits(self, monkeypatch, capsys):
        monkeypatch.setattr(os, "geteuid", lambda: 1000)
        with pytest.raises(SystemExit) as excinfo:
            require_root()
        assert excinfo.value.code == 2
        assert "root" in capsys.readouterr().err

    def test_root_passes(self, monkeypatch):
        monkeypatch.setattr(os, "geteuid", lambda: 0)
        require_root()
