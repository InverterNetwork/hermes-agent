"""Regression test for `_check_systemd` parsing.

`systemctl show -p ActiveState -p LoadState -p UnitFileState <unit>` returns
properties in the binary's own (alphabetical) order, not the `-p` flag order.
A previous version of this code used `--value` and assumed positional output,
which silently swapped fields and reported every healthy timer as drift.
"""

from __future__ import annotations

import shutil
from typing import Any

import pytest

from installer.hermes_installer import verify


@pytest.fixture(autouse=True)
def _stub_systemctl_present(monkeypatch):
    monkeypatch.setattr(verify.shutil, "which", lambda x: "/bin/" + x)
    monkeypatch.setattr(verify, "_values_get", lambda *_args, **_kwargs: "false")


class _FakeState:
    """Minimal stand-in for `_State`. Captures v_ok / v_drift calls."""

    rails_owner = "root"
    quay_version = "v0.3.7"
    values_file = "/tmp/deploy.values.yaml"
    values_helper = "/tmp/values_helper.py"
    systemd_dir = "/tmp/systemd"

    def __init__(self):
        self.ok: list[str] = []
        self.drifts: list[tuple[str, str]] = []

    def v_ok(self, msg: str) -> None:
        self.ok.append(msg)

    def v_drift(self, label: str, detail: str) -> None:
        self.drifts.append((label, detail))


def _run_factory(systemctl_output: str):
    """Return a `_run` replacement that emits `systemctl_output` for the
    `systemctl show ... <unit>` call and a stable owner-style answer for
    everything else."""

    def _run(argv: list[str], **_: Any) -> tuple[int, str, str]:
        if argv and argv[0] == "systemctl":
            return 0, systemctl_output, ""
        return 0, "", ""

    return _run


def test_parses_alphabetical_output_correctly(monkeypatch, tmp_path):
    # systemctl emits properties in alphabetical order (ActiveState,
    # LoadState, UnitFileState), regardless of the `-p` flag order.
    out = "ActiveState=active\nLoadState=loaded\nUnitFileState=enabled\n"
    monkeypatch.setattr(verify, "_run", _run_factory(out))
    monkeypatch.setattr(verify, "_owner", lambda p: "root")
    monkeypatch.setattr(verify.Path, "is_file", lambda self: False)

    s = _FakeState()
    verify._check_systemd(s)

    timer_drifts = [d for d in s.drifts if "timer" in d[0]]
    assert timer_drifts == [], f"expected no timer drift, got {timer_drifts}"
    assert any("active loaded enabled" in m for m in s.ok)


def test_parses_reverse_order_output_correctly(monkeypatch, tmp_path):
    # If a future systemctl emits in reverse order, the parser still works
    # because it keys on KEY=VALUE rather than line position.
    out = "UnitFileState=enabled\nLoadState=loaded\nActiveState=active\n"
    monkeypatch.setattr(verify, "_run", _run_factory(out))
    monkeypatch.setattr(verify, "_owner", lambda p: "root")
    monkeypatch.setattr(verify.Path, "is_file", lambda self: False)

    s = _FakeState()
    verify._check_systemd(s)

    timer_drifts = [d for d in s.drifts if "timer" in d[0]]
    assert timer_drifts == []


def test_inactive_timer_reports_drift(monkeypatch, tmp_path):
    out = "ActiveState=inactive\nLoadState=loaded\nUnitFileState=enabled\n"
    monkeypatch.setattr(verify, "_run", _run_factory(out))
    monkeypatch.setattr(verify, "_owner", lambda p: "root")
    monkeypatch.setattr(verify.Path, "is_file", lambda self: False)

    s = _FakeState()
    verify._check_systemd(s)

    timer_drifts = [d for d in s.drifts if "timer" in d[0]]
    assert len(timer_drifts) >= 1
    assert "active=inactive" in timer_drifts[0][1]
