"""Static wiring checks for the Atlas source-sync systemd units.

Guards the failure-visibility and full-vs-incremental contract that the runner
change relies on: a genuine source failure must surface (non-zero exit →
OnFailure alert), and the daily unit must pass ``--full`` while the hourly one
must not.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OPS_DIR = REPO_ROOT / "ops"

INCREMENTAL = OPS_DIR / "atlas-source-sync.service"
FULL = OPS_DIR / "atlas-source-sync-full.service"
FAILURE = OPS_DIR / "atlas-source-sync-failure.service"
RUNNER = OPS_DIR / "atlas-source-sync-runner"


def _directive(text: str, key: str) -> list[str]:
    out = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            out.append(value.strip())
    return out


@pytest.mark.parametrize("unit", [INCREMENTAL, FULL])
def test_services_alert_on_failure(unit: Path) -> None:
    # A non-zero runner exit (a genuine source failure survives isolation) must
    # trigger the alert, so a real failure is never silent.
    assert _directive(unit.read_text(encoding="utf-8"), "OnFailure") == [
        "atlas-source-sync-failure.service"
    ]


def test_failure_service_emits_atlas_alert_marker() -> None:
    text = FAILURE.read_text(encoding="utf-8")
    assert "systemd-cat -t atlas-alert -p err" in text, (
        "the failure unit must emit an ERR-priority atlas-alert journal marker"
    )


def test_full_unit_passes_full_flag_incremental_does_not() -> None:
    full_exec = _directive(FULL.read_text(encoding="utf-8"), "ExecStart")
    inc_exec = _directive(INCREMENTAL.read_text(encoding="utf-8"), "ExecStart")
    assert full_exec and full_exec[0].endswith("atlas-source-sync-runner --full")
    assert inc_exec and inc_exec[0].endswith("atlas-source-sync-runner")
    assert "--full" not in inc_exec[0]


@pytest.mark.parametrize("unit", [INCREMENTAL, FULL])
def test_services_invoke_the_runner(unit: Path) -> None:
    exec_start = _directive(unit.read_text(encoding="utf-8"), "ExecStart")
    assert exec_start and "/usr/local/sbin/atlas-source-sync-runner" in exec_start[0]


def test_runner_isolates_per_source_not_blanket_errexit() -> None:
    # The runner must keep set -e for setup but drop it for the source loop, so a
    # source failure cannot abort later sources. Rollback = restoring the old
    # blanket-errexit runner + the daily timer (pure ops-file revert).
    text = RUNNER.read_text(encoding="utf-8")
    assert "set -euo pipefail" in text, "setup phase must still hard-fail"
    assert "set +e" in text, "the per-source loop must not run under blanket errexit"
