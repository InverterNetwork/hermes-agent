from __future__ import annotations

import importlib.util
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALLER_PATH = REPO_ROOT / "scripts" / "install_dev_nav_cron.py"


def _load_installer():
    spec = importlib.util.spec_from_file_location("install_dev_nav_cron", INSTALLER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_installer_writes_no_agent_dev_nav_job(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    import hermes_constants
    import cron.jobs

    importlib.reload(hermes_constants)
    importlib.reload(cron.jobs)

    installer = _load_installer()
    installer.install()

    script = home / "scripts" / "publish-dev-nav-sepolia.sh"
    assert script.is_file()
    assert script.stat().st_mode & 0o777 == 0o700

    payload = script.read_text()
    assert "0x3dBe8d456198B63d46703bbf8f46778B2922c825" in payload
    assert "setNav(uint256,uint256)" in payload
    assert "lastNavUpdate()(uint256)" in payload
    assert "latestReportedPrice()(uint256)" in payload
    assert "--private-key \"$private_key\"" in payload

    jobs_data = json.loads((home / "cron" / "jobs.json").read_text())
    jobs = jobs_data["jobs"]
    assert len(jobs) == 1
    job = jobs[0]
    assert job["name"] == "dev-nav-sepolia-daily-publish"
    assert job["schedule"]["kind"] == "cron"
    assert job["schedule"]["expr"] == "30 6 * * *"
    assert job["script"] == "publish-dev-nav-sepolia.sh"
    assert job["no_agent"] is True
    assert job["prompt"] in {None, ""}


def test_installer_upserts_existing_job(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    import hermes_constants
    import cron.jobs

    importlib.reload(hermes_constants)
    importlib.reload(cron.jobs)

    installer = _load_installer()
    installer.install()
    installer.install()

    jobs_data = json.loads((home / "cron" / "jobs.json").read_text())
    jobs = jobs_data["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["name"] == "dev-nav-sepolia-daily-publish"
