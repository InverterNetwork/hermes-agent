"""Schedule-separation regression for the Atlas source-sync timers.

The hourly incremental (``atlas-source-sync.timer``, ``OnCalendar=hourly``) and
the daily full reconciliation (``atlas-source-sync-full.timer``) write the SAME
``atlas-kb`` through a single KB write lock. They must NEVER be scheduled to fire
at the same wall-clock instant: when the full run was ``OnCalendar=daily`` it
fired at 00:00 alongside the 00:00 hourly incremental, lost the lock race, and
(under the old runner) aborted the entire full pass — so the Slack FULL
reconciliation silently never ran.

Invariant: the full timer fires OFF the top of the hour (a minute other than
:00), so it can never coincide with an ``OnCalendar=hourly`` fire (which is
always at HH:00:00). Kept deterministic — a fixed minute, not RandomizedDelaySec
— so the separation is reproducible.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
OPS_DIR = REPO_ROOT / "ops"

INCREMENTAL_TIMER = OPS_DIR / "atlas-source-sync.timer"
FULL_TIMER = OPS_DIR / "atlas-source-sync-full.timer"

# Calendar shortcuts that resolve to the top of an hour (HH:00:00) and would
# therefore collide with the hourly incremental.
_TOP_OF_HOUR_SHORTCUTS = {"minutely", "hourly", "daily", "weekly", "monthly", "yearly"}


def _on_calendar(path: Path) -> str:
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == "OnCalendar":
            return value.strip()
    raise AssertionError(f"{path.name} has no OnCalendar directive")


def test_incremental_is_hourly_on_the_hour() -> None:
    # Baseline: the incremental fires at every HH:00 — the fire instant the full
    # run must avoid.
    assert _on_calendar(INCREMENTAL_TIMER) == "hourly"


def test_full_timer_fires_off_the_top_of_the_hour() -> None:
    expr = _on_calendar(FULL_TIMER)
    assert expr not in _TOP_OF_HOUR_SHORTCUTS, (
        f"atlas-source-sync-full.timer OnCalendar={expr!r} resolves to the top of "
        "the hour and would collide with the hourly incremental on the KB lock."
    )
    # An explicit HH:MM:SS expression must carry a non-:00 minute.
    m = re.search(r"(\d{1,2}):(\d{2}):(\d{2})\s*$", expr)
    assert m is not None, (
        f"expected a concrete HH:MM:SS time-of-day, got OnCalendar={expr!r}"
    )
    minute = m.group(2)
    assert minute != "00", (
        f"atlas-source-sync-full.timer fires at minute :{minute} — it must be off "
        ":00 so it never coincides with the hourly incremental at HH:00."
    )


def test_full_timer_uses_no_randomized_jitter() -> None:
    # Separation is a fixed offset, not random jitter, so it is deterministic and
    # testable (and can never randomly land back on :00). Check the directive, not
    # the word (the file comment explains why RandomizedDelaySec is avoided).
    directives = [
        raw.strip().partition("=")[0].strip()
        for raw in FULL_TIMER.read_text(encoding="utf-8").splitlines()
        if not raw.strip().startswith("#") and "=" in raw
    ]
    assert "RandomizedDelaySec" not in directives, (
        "the schedule separation must be a deterministic fixed minute, not a "
        "RandomizedDelaySec directive (which could resolve onto the top of the hour)."
    )


def test_systemd_analyze_next_elapse_is_off_the_hour() -> None:
    """Validate against real systemd: the full timer's next elapse is at :20, not
    :00. Skipped where systemd-analyze is absent (CI runners / macOS)."""
    systemd_analyze = shutil.which("systemd-analyze")
    if systemd_analyze is None:
        pytest.skip("systemd-analyze not available")
    expr = _on_calendar(FULL_TIMER)
    result = subprocess.run(
        [systemd_analyze, "calendar", expr],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"systemd-analyze rejected OnCalendar={expr!r}:\n{result.stdout}\n{result.stderr}"
    )
    # The "Next elapse:" line must land on a non-:00 minute.
    next_line = next(
        (ln for ln in result.stdout.splitlines() if "Next elapse:" in ln), ""
    )
    assert next_line, f"no 'Next elapse:' in systemd-analyze output:\n{result.stdout}"
    assert re.search(r"\b\d{2}:00:\d{2}\b", next_line) is None, (
        f"full timer next elapse lands on the top of the hour: {next_line!r}"
    )
