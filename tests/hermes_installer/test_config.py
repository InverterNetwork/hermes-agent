"""Schema-validation tests for ``installer/hermes_installer/config.py``.

The contract:
* ``required_runtime_managers`` returns the pin set when repos[] declares
  package_manager(s) that all have matching ``quay.runtime_managers.<name>``
  entries.
* Missing pins exit non-zero with a diagnostic naming the offending entries.
* Invalid pins (wrong SHA shape, missing version) also exit non-zero.
* No declared package_managers → empty result, no diagnostic.
"""

from __future__ import annotations

import pytest

from installer.hermes_installer.config import (
    RuntimeManagerPin,
    required_runtime_managers,
)


def _values(repos: list[dict], runtime_managers: dict | None = None) -> dict:
    return {
        "repos": repos,
        "quay": {
            "version": "v0.1.2",
            "runtime_managers": runtime_managers or {},
        },
    }


VALID_BUN_PIN = {
    "version": "1.3.9",
    "linux_x64_sha256": "4680e80e44e32aa718560ceae85d22ecfbf2efb8f3641782e35e4b7efd65a1aa",
}


class TestRequiredRuntimeManagers:
    def test_no_package_managers_declared(self):
        values = _values(
            repos=[{"id": "code-only", "url": "https://x", "base_branch": "main"}]
        )
        assert required_runtime_managers(values) == {}

    def test_happy_path_bun(self):
        values = _values(
            repos=[
                {
                    "id": "test-factory-code",
                    "url": "https://x",
                    "base_branch": "main",
                    "quay": {"package_manager": "bun", "install_cmd": "bun install"},
                }
            ],
            runtime_managers={"bun": VALID_BUN_PIN},
        )
        pins = required_runtime_managers(values)
        assert "bun" in pins
        assert pins["bun"] == RuntimeManagerPin(
            name="bun",
            version="1.3.9",
            linux_x64_sha256=VALID_BUN_PIN["linux_x64_sha256"],
        )

    def test_dedupes_across_repos(self):
        values = _values(
            repos=[
                {"id": "a", "quay": {"package_manager": "bun"}},
                {"id": "b", "quay": {"package_manager": "bun"}},
            ],
            runtime_managers={"bun": VALID_BUN_PIN},
        )
        assert list(required_runtime_managers(values)) == ["bun"]

    def test_missing_pin_exits_with_diagnostic(self, capsys):
        values = _values(
            repos=[{"id": "a", "quay": {"package_manager": "bun"}}],
            runtime_managers={},
        )
        with pytest.raises(SystemExit) as excinfo:
            required_runtime_managers(values)
        assert excinfo.value.code == 1
        err = capsys.readouterr().err
        assert "bun" in err
        # Operator-actionable message — names the values-file path the
        # operator must edit, not just "missing".
        assert "quay.runtime_managers.bun" in err

    def test_invalid_sha_shape_exits(self, capsys):
        values = _values(
            repos=[{"id": "a", "quay": {"package_manager": "bun"}}],
            runtime_managers={
                "bun": {"version": "1.3.9", "linux_x64_sha256": "deadbeef"}
            },
        )
        with pytest.raises(SystemExit):
            required_runtime_managers(values)
        assert "linux_x64_sha256" in capsys.readouterr().err

    def test_missing_version_exits(self, capsys):
        values = _values(
            repos=[{"id": "a", "quay": {"package_manager": "bun"}}],
            runtime_managers={
                "bun": {
                    "linux_x64_sha256": VALID_BUN_PIN["linux_x64_sha256"],
                }
            },
        )
        with pytest.raises(SystemExit):
            required_runtime_managers(values)
        assert "version" in capsys.readouterr().err

    def test_repos_without_quay_block_ignored(self):
        values = _values(
            repos=[
                {"id": "code-only", "url": "https://x", "base_branch": "main"},
                {
                    "id": "managed",
                    "quay": {"package_manager": "bun", "install_cmd": "bun i"},
                },
            ],
            runtime_managers={"bun": VALID_BUN_PIN},
        )
        assert list(required_runtime_managers(values)) == ["bun"]
