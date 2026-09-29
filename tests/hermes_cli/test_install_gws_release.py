import hashlib
import os
from pathlib import Path
import subprocess
import tarfile


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALLER = REPO_ROOT / "installer" / "install-gws-release"


def _archive(tmp_path: Path, version: str = "0.22.5") -> Path:
    payload = tmp_path / "payload"
    payload.mkdir()
    gws = payload / "gws"
    gws.write_text(
        f"#!/usr/bin/env bash\nprintf 'gws {version}\\n'\n",
        encoding="utf-8",
    )
    gws.chmod(0o755)
    archive = tmp_path / "gws.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(gws, arcname="gws")
    return archive


def _fake_curl(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    curl = fake_bin / "curl"
    curl.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        "printf '%s\\n' \"$*\" > \"$FAKE_CURL_ARGS\"\n"
        "out=''\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "  if [ \"$1\" = -o ]; then out=\"$2\"; shift 2; else shift; fi\n"
        "done\n"
        "cp \"$FAKE_GWS_ARCHIVE\" \"$out\"\n",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    return fake_bin


def _run(
    tmp_path: Path,
    archive: Path,
    expected_sha: str,
    *,
    arch: str = "x86_64",
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    binary = tmp_path / "installed" / "gws"
    binary.parent.mkdir()
    expected_file = tmp_path / "state" / "SHA256SUM.expected"
    env = os.environ.copy()
    env["FAKE_GWS_ARCHIVE"] = str(archive)
    env["FAKE_CURL_ARGS"] = str(tmp_path / "curl-args")
    env["PATH"] = f"{_fake_curl(tmp_path)}:{env['PATH']}"
    result = subprocess.run(
        [
            "bash",
            str(INSTALLER),
            "--version",
            "0.22.5",
            "--release-repo",
            "googleworkspace/cli",
            "--arch",
            arch,
            "--expected-sha",
            expected_sha,
            "--bin-dst",
            str(binary),
            "--expected-sha-dst",
            str(expected_file),
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    return result, binary, expected_file


def test_checksum_mismatch_fails_before_install(tmp_path):
    archive = _archive(tmp_path)
    result, binary, expected_file = _run(tmp_path, archive, "0" * 64)

    assert result.returncode == 1
    assert "pinned SHA256 mismatch" in result.stderr
    assert not binary.exists()
    assert not expected_file.exists()


def test_version_mismatch_fails_before_install(tmp_path):
    archive = _archive(tmp_path, version="0.22.4")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    result, binary, expected_file = _run(tmp_path, archive, digest)

    assert result.returncode == 1
    assert "downloaded gws version mismatch" in result.stderr
    assert not binary.exists()
    assert not expected_file.exists()


def test_valid_archive_installs_binary_and_verifier_digest(tmp_path):
    archive = _archive(tmp_path)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    result, binary, expected_file = _run(tmp_path, archive, digest)

    assert result.returncode == 0, result.stderr
    assert binary.stat().st_mode & 0o777 == 0o755
    assert expected_file.stat().st_mode & 0o777 == 0o644
    assert expected_file.read_text(encoding="utf-8").strip() == hashlib.sha256(
        binary.read_bytes()
    ).hexdigest()


def test_aarch64_selects_pinned_aarch64_asset(tmp_path):
    archive = _archive(tmp_path)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    result, binary, _ = _run(tmp_path, archive, digest, arch="aarch64")

    assert result.returncode == 0, result.stderr
    assert binary.exists()
    assert (
        "google-workspace-cli-aarch64-unknown-linux-gnu.tar.gz"
        in (tmp_path / "curl-args").read_text(encoding="utf-8")
    )
