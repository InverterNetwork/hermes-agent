from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest


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
    assert 'signer_args+=(--account "$DEV_NAV_PUBLISH_ACCOUNT")' in payload
    assert 'signer_args+=(--keystore "$DEV_NAV_PUBLISH_KEYSTORE")' in payload
    assert 'signer_args+=(--password-file "$DEV_NAV_PUBLISH_PASSWORD_FILE")' in payload
    assert "set DEV_NAV_PUBLISH_PASSWORD_FILE so cast cannot prompt interactively" in payload
    assert "--private-key" not in payload
    assert "private_key=" not in payload
    assert "PRIVATE_KEY:-" not in payload
    assert "raw private-key env vars are not accepted" in payload

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

    verified = installer.verify_installed()
    assert verified["job"]["id"] == job["id"]
    assert verified["script"] == script


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


def test_installer_removes_duplicate_owned_jobs(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    import hermes_constants
    import cron.jobs

    importlib.reload(hermes_constants)
    importlib.reload(cron.jobs)

    from cron.jobs import create_job

    create_job(
        prompt="legacy duplicate 1",
        schedule="every 1d",
        name="dev-nav-sepolia-daily-publish",
    )
    create_job(
        prompt="legacy duplicate 2",
        schedule="every 1d",
        name="dev-nav-sepolia-daily-publish",
    )

    installer = _load_installer()
    installer.install()

    jobs_data = json.loads((home / "cron" / "jobs.json").read_text())
    jobs = jobs_data["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["name"] == "dev-nav-sepolia-daily-publish"
    assert jobs[0]["schedule"]["expr"] == "30 6 * * *"
    assert jobs[0]["script"] == "publish-dev-nav-sepolia.sh"
    assert jobs[0]["no_agent"] is True


def test_verify_installed_fails_on_host_state_drift(tmp_path, monkeypatch):
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
    script.write_text("#!/usr/bin/env bash\necho drifted\n", encoding="utf-8")

    with pytest.raises(installer.VerificationError, match="script payload drifted"):
        installer.verify_installed()


def test_run_now_executes_materialized_script(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    import hermes_constants
    import cron.jobs

    importlib.reload(hermes_constants)
    importlib.reload(cron.jobs)

    installer = _load_installer()
    installer.install()

    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="ok\n", stderr="")

    monkeypatch.setattr(installer.subprocess, "run", fake_run)

    installer.run_now()

    assert calls
    args, kwargs = calls[0]
    assert args[0] == ["bash", str(home / "scripts" / "publish-dev-nav-sepolia.sh")]
    assert kwargs["cwd"] == str(home)
    assert kwargs["env"]["HERMES_HOME"] == str(home)
