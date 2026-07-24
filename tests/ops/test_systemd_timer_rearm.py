"""Regression tests for BRIX-1874.

Every recurring systemd timer shipped under ``ops/`` must carry a wall-clock
``OnCalendar`` trigger so it always re-arms after a stop or a
restart-past-boot.

Background: a monotonic-only schedule (``OnBootSec`` + ``OnUnitActiveSec`` with
no ``OnCalendar``) can silently deadlock. ``OnBootSec`` elapses only once per
boot, and ``OnUnitActiveSec`` re-anchors off the *previous* service activation,
so a stop followed by a start past boot — with no prior activation to anchor on
— computes NO next elapse: the timer sits ``active`` with ``Trigger: n/a`` and
never fires again. That is exactly how the Quay delivery-outbox drain went
silently dead for ~3 days. An ``OnCalendar`` trigger is anchored on the wall
clock and cannot deadlock.

Each entry below maps a hardened timer to the ``OnCalendar`` cadence that
preserves its prior ``OnUnitActiveSec`` frequency exactly.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OPS_DIR = REPO_ROOT / "ops"

# timer stem -> expected OnCalendar cadence (equivalent to the prior
# OnUnitActiveSec cadence noted in the comment).
HARDENED_TIMERS = {
    "quay-orchestrator": "minutely",  # was OnUnitActiveSec=1min
    "quay-tick": "minutely",  # was OnUnitActiveSec=1min
    "hermes-sync": "*:0/2",  # was OnUnitActiveSec=2min
    "hermes-code-sync": "*:0/5",  # was OnUnitActiveSec=5min
    "atlas-source-sync": "hourly",  # was OnUnitActiveSec=1h
    # Daily full reconciliation, deliberately OFF the top of the hour so it never
    # coincides with the hourly incremental (which fires at every HH:00) on the
    # shared atlas-kb write lock. See test_atlas_source_sync_schedule.py.
    "atlas-source-sync-full": "*-*-* 04:20:00 UTC",
}


def _directive_values(text: str, key: str) -> list[str]:
    """Values of every ``Key=...`` directive in a unit file, ignoring comments
    and surrounding whitespace."""
    values = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            values.append(value.strip())
    return values


@pytest.mark.parametrize(
    ("stem", "cadence"), sorted(HARDENED_TIMERS.items())
)
def test_timer_has_wall_clock_trigger(stem: str, cadence: str) -> None:
    text = (OPS_DIR / f"{stem}.timer").read_text(encoding="utf-8")

    on_calendar = _directive_values(text, "OnCalendar")
    assert on_calendar, (
        f"{stem}.timer must define OnCalendar so it always has a wall-clock "
        "next elapse and cannot deadlock with no next trigger (BRIX-1874)."
    )
    assert on_calendar == [cadence], (
        f"{stem}.timer OnCalendar drifted from its intended cadence: expected "
        f"{cadence!r}, got {on_calendar!r}"
    )

    # A single wall-clock schedule source: the monotonic anchors that caused
    # the deadlock must be gone, not kept alongside OnCalendar.
    assert not _directive_values(text, "OnBootSec"), (
        f"{stem}.timer still sets OnBootSec; drop it in favour of OnCalendar."
    )
    assert not _directive_values(text, "OnUnitActiveSec"), (
        f"{stem}.timer still sets OnUnitActiveSec; drop it in favour of "
        "OnCalendar."
    )


@pytest.mark.parametrize("stem", sorted(HARDENED_TIMERS))
def test_timer_passes_systemd_analyze_verify(stem: str, tmp_path: Path) -> None:
    """Validate each timer as real systemd would (this also confirms the
    OnCalendar expression parses). Skipped where systemd tooling is absent
    (e.g. CI runners / macOS)."""
    systemd_analyze = shutil.which("systemd-analyze")
    if systemd_analyze is None:
        pytest.skip("systemd-analyze not available")

    timer_dst = tmp_path / f"{stem}.timer"
    timer_dst.write_text(
        (OPS_DIR / f"{stem}.timer").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    # Provide the referenced service so `verify` can resolve Unit=. The
    # services are installer templates (__PLACEHOLDER__ tokens); fill them so
    # the unit parses. Non-absolute EnvironmentFile= warnings are non-fatal.
    service_src = OPS_DIR / f"{stem}.service"
    if service_src.exists():
        (tmp_path / f"{stem}.service").write_text(
            re.sub(r"__[A-Z0-9_]+__", "x", service_src.read_text(encoding="utf-8")),
            encoding="utf-8",
        )

    result = subprocess.run(
        [systemd_analyze, "verify", str(timer_dst)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"systemd-analyze verify rejected {stem}.timer:\n"
        f"{result.stdout}\n{result.stderr}"
    )
