"""Codex worker CLI installer tests."""

from __future__ import annotations

import hashlib
import os
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from installer.hermes_installer import codex
from installer.hermes_installer.codex import ensure_codex
from installer.hermes_installer.config import CodexPin
from tests.hermes_installer.conftest import fake_bun_script


def _build_codex_tar(out_dir: Path, version: str) -> tuple[Path, str, bytes]:
    out_dir.mkdir(parents=True, exist_ok=True)
    body = fake_bun_script(f"codex {version}").encode()
    member = out_dir / codex._ASSET_NAME
    member.write_bytes(body)
    os.chmod(member, 0o755)
    tar_path = out_dir / codex._ASSET_ARCHIVE
    with tarfile.open(tar_path, "w:gz") as tf:
        tf.add(member, arcname=codex._ASSET_NAME)
    return tar_path, hashlib.sha256(tar_path.read_bytes()).hexdigest(), body


@pytest.fixture
def fake_agent(tmp_path: Path, monkeypatch):
    home = tmp_path / "agent-home"
    home.mkdir()
    pw = SimpleNamespace(pw_dir=str(home), pw_uid=os.getuid(), pw_gid=os.getgid())
    monkeypatch.setattr(codex, "_agent_user", lambda user: pw)
    return home


@pytest.fixture(autouse=True)
def _force_x86_64_arch(monkeypatch):
    monkeypatch.setattr(codex, "_host_arch", lambda: "x86_64")


@pytest.fixture
def fake_codex_tar(tmp_path: Path):
    return _build_codex_tar(tmp_path / "src", "0.130.0")


@pytest.fixture
def patch_urlretrieve(monkeypatch, fake_codex_tar):
    src_tar, _sha, _body = fake_codex_tar

    def _fake(url: str, dst):  # noqa: ARG001
        Path(dst).write_bytes(src_tar.read_bytes())

    monkeypatch.setattr(codex.urllib.request, "urlretrieve", _fake)
    return _fake


class TestEnsureCodex:
    def test_none_pin_is_noop(self, tmp_path: Path, monkeypatch):
        def _explode(user):  # noqa: ARG001
            raise AssertionError("agent lookup should not run without codex pin")

        monkeypatch.setattr(codex, "_agent_user", _explode)
        ensure_codex(None, agent_user="agent", symlink_path=tmp_path / "codex")
        assert not (tmp_path / "codex").exists()

    def test_happy_path_installs_agent_binary_and_system_symlink(
        self,
        tmp_path: Path,
        fake_agent: Path,
        fake_codex_tar,
        patch_urlretrieve,
    ):
        _tar, sha, body = fake_codex_tar
        pin = CodexPin(version="rust-v0.130.0", linux_x64_sha256=sha)
        symlink = tmp_path / "usr-local-bin" / "codex"

        ensure_codex(pin, agent_user="agent", symlink_path=symlink)

        install_path = fake_agent / ".local" / "bin" / "codex"
        assert install_path.is_file()
        assert install_path.read_bytes() == body
        assert (install_path.stat().st_mode & 0o777) == 0o755
        assert ((fake_agent / ".local").stat().st_mode & 0o777) == 0o755
        assert (install_path.parent.stat().st_mode & 0o777) == 0o755
        assert symlink.is_symlink()
        assert Path(os.readlink(symlink)) == install_path

    def test_sha_mismatch_refuses_without_half_install(
        self,
        tmp_path: Path,
        fake_agent: Path,
        patch_urlretrieve,
        capsys,
    ):
        pin = CodexPin(version="rust-v0.130.0", linux_x64_sha256="0" * 64)
        with pytest.raises(SystemExit) as excinfo:
            ensure_codex(pin, agent_user="agent", symlink_path=tmp_path / "codex")
        assert excinfo.value.code == 1
        assert "SHA256 mismatch" in capsys.readouterr().err
        assert not (fake_agent / ".local" / "bin" / "codex").exists()
        assert not (tmp_path / "codex").exists()

    def test_rerun_is_idempotent_and_repairs_symlink(
        self,
        tmp_path: Path,
        fake_agent: Path,
        fake_codex_tar,
        patch_urlretrieve,
        monkeypatch,
    ):
        _tar, sha, _body = fake_codex_tar
        pin = CodexPin(version="rust-v0.130.0", linux_x64_sha256=sha)
        symlink = tmp_path / "usr-local-bin" / "codex"
        ensure_codex(pin, agent_user="agent", symlink_path=symlink)
        install_path = fake_agent / ".local" / "bin" / "codex"
        before_mtime = install_path.stat().st_mtime_ns
        symlink.unlink()

        def _explode(url, dst):  # noqa: ARG001
            raise AssertionError("urlretrieve called on idempotent re-run")

        monkeypatch.setattr(codex.urllib.request, "urlretrieve", _explode)
        ensure_codex(pin, agent_user="agent", symlink_path=symlink)

        assert install_path.stat().st_mtime_ns == before_mtime
        assert symlink.is_symlink()
        assert Path(os.readlink(symlink)) == install_path

    def test_unsupported_arch_fails_before_fetch(
        self,
        tmp_path: Path,
        fake_agent: Path,
        fake_codex_tar,
        monkeypatch,
        capsys,
    ):
        _tar, sha, _body = fake_codex_tar
        monkeypatch.setattr(codex, "_host_arch", lambda: "aarch64")
        monkeypatch.setattr(
            codex.urllib.request,
            "urlretrieve",
            lambda url, dst: (_ for _ in ()).throw(AssertionError("fetch")),
        )
        with pytest.raises(SystemExit):
            ensure_codex(
                CodexPin(version="rust-v0.130.0", linux_x64_sha256=sha),
                agent_user="agent",
                symlink_path=tmp_path / "codex",
            )
        assert "linux-x86_64" in capsys.readouterr().err
