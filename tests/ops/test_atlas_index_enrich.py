"""Exercise rendered enrichment units and the local CLI command boundary."""
from __future__ import annotations

import configparser
import os
from pathlib import Path
import subprocess
import shutil

import pytest

from installer.hermes_installer.systemd import render_unit_template

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.linux_only
def test_enrichment_timer_has_valid_recurring_calendar():
    systemd_analyze = shutil.which("systemd-analyze")
    if systemd_analyze is None:
        pytest.skip("systemd-analyze not available")
    rendered = render_unit_template(ROOT / "ops/atlas-index-enrich.timer", {})
    config = configparser.ConfigParser(interpolation=None)
    config.read_string(rendered)
    assert config["Timer"]["Unit"] == "atlas-index-enrich.service"
    assert config["Timer"]["Persistent"] == "true"
    result = subprocess.run(
        [systemd_analyze, "calendar", "--iterations=2", config["Timer"]["OnCalendar"]],
        capture_output=True, text=True, env={**os.environ, "TZ": "UTC"},
    )
    assert result.returncode == 0, result.stderr
