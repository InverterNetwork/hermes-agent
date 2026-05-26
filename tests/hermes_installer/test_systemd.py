from __future__ import annotations

import os
from pathlib import Path

import pytest

from installer.hermes_installer import __main__ as cli_main
from installer.hermes_installer.systemd import (
    render_unit_template,
    seed_default_env,
)


def test_render_unit_template_replaces_explicit_tokens(tmp_path: Path):
    template = tmp_path / "demo.service.in"
    template.write_text(
        "User=__AGENT_USER__\n"
        "Environment=HERMES_HOME=__TARGET_DIR__\n",
        encoding="utf-8",
    )

    rendered = render_unit_template(
        template,
        {"AGENT_USER": "hermes", "TARGET_DIR": "/srv/hermes"},
    )

    assert "User=hermes" in rendered
    assert "Environment=HERMES_HOME=/srv/hermes" in rendered
    assert "__" not in rendered


def test_render_unit_template_fails_on_unresolved_token(tmp_path: Path, capsys):
    template = tmp_path / "demo.service.in"
    template.write_text("User=__AGENT_USER__\n", encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        render_unit_template(template, {})

    assert excinfo.value.code == 1
    assert "__AGENT_USER__" in capsys.readouterr().err


def test_seed_default_env_preserves_existing_file(tmp_path: Path, capsys):
    out = tmp_path / "default" / "quay-serve"
    out.parent.mkdir()
    out.write_text("OPERATOR_OVERRIDE=1\n", encoding="utf-8")

    wrote = seed_default_env(out, "quay-serve", chown_root=False)

    assert wrote is False
    assert out.read_text(encoding="utf-8") == "OPERATOR_OVERRIDE=1\n"
    assert "already present (preserving)" in capsys.readouterr().out


def test_cli_seeds_default_env_and_renders_unit(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    template = tmp_path / "hermes-dashboard.service.in"
    template.write_text(
        "User=__AGENT_USER__\n"
        "Environment=HOME=__AGENT_HOME__\n"
        "Environment=HERMES_HOME=__TARGET_DIR__\n",
        encoding="utf-8",
    )
    default_env = tmp_path / "etc" / "default" / "hermes-dashboard"
    unit = tmp_path / "etc" / "systemd" / "system" / "hermes-dashboard.service"

    assert cli_main.main(
        [
            "seed-systemd-default-env",
            "--service-name",
            "hermes-dashboard",
            "--out",
            str(default_env),
            "--skip-root-check",
        ]
    ) == 0
    assert "extra env for hermes-dashboard.service" in default_env.read_text(
        encoding="utf-8"
    )

    assert cli_main.main(
        [
            "render-systemd-unit",
            "--template",
            str(template),
            "--out",
            str(unit),
            "--set",
            "AGENT_USER=hermes",
            "--set",
            "AGENT_HOME=/home/hermes",
            "--set",
            "TARGET_DIR=/home/hermes/.hermes",
            "--skip-root-check",
        ]
    ) == 0
    rendered = unit.read_text(encoding="utf-8")
    assert "User=hermes" in rendered
    assert "Environment=HOME=/home/hermes" in rendered
    assert "Environment=HERMES_HOME=/home/hermes/.hermes" in rendered
