"""Pinned-binary install machinery tests.

Covers:
* SHA-mismatch refusal — installer must exit non-zero on bad SHA.
* Idempotent re-install — binary at pinned version is a no-op (no fetch).
* Wrong-version replacement — pinned version mismatch triggers replace.
* Missing archive member — extracted zip without the recipe member fails loud.
* Plain-binary recipe — downloads, SHA-verifies, installs without extracting.
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

from tests.hermes_installer.conftest import build_fake_binary, fake_bun_script


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
        # Use a synthetic name not in _RECIPES — bun and pnpm are both wired.
        pin = RuntimeManagerPin(
            name="never-shipped", version="9.0.0", linux_x64_sha256="0" * 64
        )
        install_path = tmp_path / "bin" / "never-shipped"
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


class TestBinaryRecipe:
    """Plain-binary recipes (e.g. pnpm) ship the executable directly — no
    zip wrapper. The installer must skip the extract step and install the
    downloaded file as-is, while still SHA-verifying it against the pin."""

    @staticmethod
    def _register_fake(monkeypatch, name: str, url_template: str) -> None:
        recipes = dict(runtimes._RECIPES)
        recipes[name] = runtimes._Recipe(
            url_template=url_template, archive_kind="binary",
        )
        monkeypatch.setattr(runtimes, "_RECIPES", recipes)

    def test_happy_path_installs_executable(
        self, tmp_path: Path, monkeypatch
    ):
        self._register_fake(monkeypatch, "fakepm", "https://example/{version}")
        src_bin, sha = build_fake_binary(tmp_path / "src", "fakepm", "10.0.0")
        src_bytes = src_bin.read_bytes()

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(src_bytes)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        pin = RuntimeManagerPin(
            name="fakepm", version="10.0.0", linux_x64_sha256=sha
        )
        install_path = tmp_path / "bin" / "fakepm"
        install_path.parent.mkdir()
        ensure_runtime(pin, install_path)

        assert install_path.is_file()
        assert install_path.stat().st_mode & stat.S_IXUSR
        assert (install_path.stat().st_mode & 0o777) == 0o755
        assert install_path.read_bytes() == src_bytes

    def test_no_extract_step_for_binary(
        self, tmp_path: Path, monkeypatch
    ):
        # An invalid zip would crash the zip-path extractor; binary recipes
        # must never reach that code, so a non-zip payload installs cleanly.
        self._register_fake(monkeypatch, "fakepm", "https://example/{version}")
        src_bin, sha = build_fake_binary(tmp_path / "src", "fakepm", "10.0.0")
        src_bytes = src_bin.read_bytes()
        assert not src_bytes.startswith(b"PK"), "test asset must not be a zip"

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(src_bytes)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        # Sentinel: if anyone reintroduces an unconditional extract, this fires.
        def _explode(*args, **kwargs):
            raise AssertionError("zip extract reached on binary recipe")

        monkeypatch.setattr(runtimes.zipfile, "ZipFile", _explode)

        pin = RuntimeManagerPin(
            name="fakepm", version="10.0.0", linux_x64_sha256=sha
        )
        install_path = tmp_path / "bin" / "fakepm"
        install_path.parent.mkdir()
        ensure_runtime(pin, install_path)
        assert install_path.is_file()

    def test_sha_mismatch_refuses(
        self, tmp_path: Path, monkeypatch, capsys
    ):
        self._register_fake(monkeypatch, "fakepm", "https://example/{version}")
        src_bin, _real_sha = build_fake_binary(
            tmp_path / "src", "fakepm", "10.0.0"
        )
        src_bytes = src_bin.read_bytes()

        def _fake(url, dst):  # noqa: ARG001
            Path(dst).write_bytes(src_bytes)

        monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake)

        bad_pin = RuntimeManagerPin(
            name="fakepm", version="10.0.0", linux_x64_sha256="0" * 64
        )
        install_path = tmp_path / "bin" / "fakepm"
        install_path.parent.mkdir()
        with pytest.raises(SystemExit) as excinfo:
            ensure_runtime(bad_pin, install_path)
        assert excinfo.value.code == 1
        err = capsys.readouterr().err
        assert "SHA256 mismatch" in err
        assert "expected:" in err and "actual:" in err and "url:" in err
        assert not install_path.exists(), "no half-install on SHA refusal"


class TestRecipeShape:
    """The recipe data model enforces archive_kind/archive_member coherence
    at construction time, so a mistyped _RECIPES entry fails loud at import."""

    def test_zip_recipe_requires_archive_member(self):
        with pytest.raises(ValueError, match="archive_member"):
            runtimes._Recipe(url_template="https://x/{version}", archive_kind="zip")

    def test_binary_recipe_rejects_archive_member(self):
        with pytest.raises(ValueError, match="archive_member"):
            runtimes._Recipe(
                url_template="https://x/{version}",
                archive_kind="binary",
                archive_member="something/else",
            )

    def test_unknown_archive_kind_rejected_at_construction(self):
        # A typo like "bniary" must fail at import rather than silently
        # falling through to a non-zip install path.
        with pytest.raises(ValueError, match="archive_kind"):
            runtimes._Recipe(
                url_template="https://x/{version}",
                archive_kind="bniary",  # type: ignore[arg-type]
            )

    def test_unknown_archive_kind_rejected_with_archive_member(self):
        # Same guard, even when archive_member is set — kind check runs
        # first so the operator's actionable error is the unknown kind.
        with pytest.raises(ValueError, match="archive_kind"):
            runtimes._Recipe(
                url_template="https://x/{version}",
                archive_kind="tar",  # type: ignore[arg-type]
                archive_member="some/path",
            )

    def test_pnpm_wired_as_binary_recipe(self):
        # pnpm-linux-x64 ships as a single executable, not a zip — the
        # recipe must take the plain-binary path so the install matches
        # upstream's distribution shape.
        recipe = runtimes._RECIPES["pnpm"]
        assert recipe.archive_kind == "binary"
        assert recipe.archive_member is None
        assert "pnpm-linux-x64" in recipe.url_template
        assert "{version}" in recipe.url_template

    def test_bun_wired_as_zip_recipe(self):
        recipe = runtimes._RECIPES["bun"]
        assert recipe.archive_kind == "zip"
        assert recipe.archive_member == "bun-linux-x64/bun"


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
