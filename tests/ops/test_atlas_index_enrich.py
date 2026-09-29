"""Exercise rendered enrichment units and the local CLI command boundary."""
from __future__ import annotations

import configparser
import os
from pathlib import Path
import shlex
import subprocess

from installer.hermes_installer.systemd import render_unit_template

ROOT = Path(__file__).resolve().parents[2]


def test_rendered_enrichment_unit_executes_local_index_command(tmp_path):
    rendered = render_unit_template(
        ROOT / "ops/atlas-index-enrich.service",
        {"AGENT_USER": "test-agent", "TARGET_DIR": str(tmp_path)},
    )
    config = configparser.ConfigParser(interpolation=None)
    config.read_string(rendered)
    args = shlex.split(config["Service"]["ExecStart"])
    wrapper = tmp_path / "atlas-as-hermes"
    wrapper.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\nexit 7\n')
    wrapper.chmod(0o755)
    assert args[0] == "/usr/local/bin/atlas-as-hermes"
    result = subprocess.run([str(wrapper), *args[1:]], capture_output=True, text=True)
    assert result.stdout.splitlines() == ["--local", "index", "enrich"]
    assert result.returncode == 7  # no shell wrapper masks Atlas failures
    assert config["Service"]["User"] == "test-agent"
    assert config["Service"]["WorkingDirectory"] == str(tmp_path)
    assert config["Service"]["TimeoutStartSec"] == "3600"
    assert config["Service"]["MemoryMax"] == "4G"
    assert config["Service"]["TasksMax"] == "128"


def test_enrichment_timer_has_valid_recurring_calendar():
    rendered = render_unit_template(ROOT / "ops/atlas-index-enrich.timer", {})
    config = configparser.ConfigParser(interpolation=None)
    config.read_string(rendered)
    assert config["Timer"]["Unit"] == "atlas-index-enrich.service"
    assert config["Timer"]["Persistent"] == "true"
    result = subprocess.run(
        ["systemd-analyze", "calendar", "--iterations=2", config["Timer"]["OnCalendar"]],
        capture_output=True, text=True, env={**os.environ, "TZ": "UTC"},
    )
    assert result.returncode == 0, result.stderr
    assert "Iteration #2" in result.stdout
