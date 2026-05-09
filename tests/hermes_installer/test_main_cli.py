"""End-to-end tests for ``python3 -m hermes_installer``.

Exercises the argparse surface + boundary failure modes:
* missing ``--values`` flag → argparse error.
* non-root invocation without ``--skip-root-check`` → exit 2 (the production
  invariant; setup-hermes.sh is sudo-driven).
* full ensure-runtimes happy path with a stubbed urlretrieve.
"""

from __future__ import annotations

import hashlib
import os
import zipfile
from pathlib import Path

import pytest
import yaml

from installer.hermes_installer import __main__ as cli_main
from installer.hermes_installer import runtimes


def _build_bun_zip(out: Path, version: str) -> str:
    body = (
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == "--version" ]]; then echo "{version}"; exit 0; fi\n'
        "exit 1\n"
    ).encode()
    with zipfile.ZipFile(out, "w") as zf:
        info = zipfile.ZipInfo("bun-linux-x64/bun")
        info.external_attr = (0o755 << 16) | 0x8000
        zf.writestr(info, body)
    return hashlib.sha256(out.read_bytes()).hexdigest()


@pytest.fixture
def values_file(tmp_path: Path) -> tuple[Path, str]:
    src_zip = tmp_path / "src.zip"
    sha = _build_bun_zip(src_zip, "1.3.9")
    p = tmp_path / "deploy.values.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "repos": [
                    {
                        "id": "test-factory-code",
                        "url": "https://github.com/InverterNetwork/test-factory-code",
                        "base_branch": "main",
                        "quay": {
                            "package_manager": "bun",
                            "install_cmd": "bun install",
                        },
                    }
                ],
                "quay": {
                    "version": "v0.1.2",
                    "runtime_managers": {
                        "bun": {
                            "version": "1.3.9",
                            "linux_x64_sha256": sha,
                        }
                    },
                },
            },
            sort_keys=False,
        )
    )
    return p, str(src_zip)


def test_missing_values_arg_errors(monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(SystemExit):
        cli_main.main(["ensure-runtimes"])


def test_non_root_fails_without_skip(monkeypatch, capsys, tmp_path: Path):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    # Empty values file is enough — root check happens before file load.
    p = tmp_path / "v.yaml"
    p.write_text("repos: []\n")
    with pytest.raises(SystemExit) as excinfo:
        cli_main.main(["ensure-runtimes", "--values", str(p)])
    assert excinfo.value.code == 2
    assert "root" in capsys.readouterr().err


def test_skip_root_check_lets_tests_run(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    p = tmp_path / "v.yaml"
    p.write_text("repos: []\n")
    rc = cli_main.main(
        ["ensure-runtimes", "--values", str(p), "--skip-root-check"]
    )
    assert rc == 0


def test_ensure_runtimes_end_to_end(
    monkeypatch, tmp_path: Path, values_file
):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    values_path, src_zip = values_file

    def _fake_urlretrieve(url, dst):  # noqa: ARG001
        Path(dst).write_bytes(Path(src_zip).read_bytes())

    monkeypatch.setattr(runtimes.urllib.request, "urlretrieve", _fake_urlretrieve)

    install_dir = tmp_path / "bin"
    rc = cli_main.main(
        [
            "ensure-runtimes",
            "--values",
            str(values_path),
            "--install-dir",
            str(install_dir),
            "--skip-root-check",
        ]
    )
    assert rc == 0
    assert (install_dir / "bun").is_file()
    assert (install_dir / "bun").stat().st_mode & 0o100  # owner exec bit


def test_ensure_runtimes_missing_pin_diagnostic(
    monkeypatch, tmp_path: Path, capsys
):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    p = tmp_path / "v.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "repos": [{"id": "a", "quay": {"package_manager": "bun"}}],
                "quay": {"version": "v0.1.2", "runtime_managers": {}},
            }
        )
    )
    with pytest.raises(SystemExit) as excinfo:
        cli_main.main(
            [
                "ensure-runtimes",
                "--values",
                str(p),
                "--install-dir",
                str(tmp_path / "bin"),
                "--skip-root-check",
            ]
        )
    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    assert "quay.runtime_managers.bun" in err
