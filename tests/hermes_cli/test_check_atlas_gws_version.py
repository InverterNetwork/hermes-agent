from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK = REPO_ROOT / "installer" / "check_atlas_gws_version.py"


def _run(actual: str, minimum: str = "0.1.16") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(CHECK), actual, minimum],
        text=True,
        capture_output=True,
        check=False,
    )


def test_accepts_pinned_and_later_stable_versions():
    assert _run("v0.1.16").returncode == 0
    assert _run("0.2.0").returncode == 0


def test_rejects_older_atlas_release():
    result = _run("v0.1.15")
    assert result.returncode == 1
    assert "older than the Google Docs gws minimum" in result.stderr


def test_rejects_non_stable_version():
    result = _run("v0.1.16-rc.1")
    assert result.returncode == 1
    assert "requires a stable semver tag" in result.stderr


def test_rejects_drifted_minimum():
    result = _run("v0.1.16", "0.1.15")
    assert result.returncode == 1
    assert "minimum_atlas_version must be exactly 0.1.16" in result.stderr
