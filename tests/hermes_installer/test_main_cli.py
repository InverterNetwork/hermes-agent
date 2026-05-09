"""End-to-end tests for ``python3 -m hermes_installer``."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from installer.hermes_installer import __main__ as cli_main


@pytest.fixture
def values_file(tmp_path: Path, fake_bun_zip) -> Path:
    _src_zip, sha = fake_bun_zip
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
                        "bun": {"version": "1.3.9", "linux_x64_sha256": sha}
                    },
                },
            },
            sort_keys=False,
        )
    )
    return p


def test_missing_values_arg_errors(monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    with pytest.raises(SystemExit):
        cli_main.main(["ensure-runtimes"])


def test_non_root_fails_without_skip(monkeypatch, capsys, tmp_path: Path):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
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
    monkeypatch, tmp_path: Path, values_file, patch_urlretrieve
):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    install_dir = tmp_path / "bin"
    rc = cli_main.main(
        [
            "ensure-runtimes",
            "--values", str(values_file),
            "--install-dir", str(install_dir),
            "--skip-root-check",
        ]
    )
    assert rc == 0
    assert (install_dir / "bun").is_file()
    assert (install_dir / "bun").stat().st_mode & 0o100


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
                "--values", str(p),
                "--install-dir", str(tmp_path / "bin"),
                "--skip-root-check",
            ]
        )
    assert excinfo.value.code == 1
    assert "quay.runtime_managers.bun" in capsys.readouterr().err
