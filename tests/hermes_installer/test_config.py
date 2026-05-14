"""Tests for ``required_runtime_managers``: walks repos[].quay.package_manager
and looks each up in quay.runtime_managers.<name>, exiting non-zero with an
operator-actionable diagnostic on missing/invalid pins."""

from __future__ import annotations

import pytest

from installer.hermes_installer.config import (
    CodexPin,
    RuntimeManagerPin,
    required_codex_pin,
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

VALID_PNPM_PIN = {
    "version": "10.33.4",
    "linux_x64_sha256": "ff1795595535a10d0dfe327303f3dd02377be141190b1f5756de68edde2cf813",
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

    def test_bun_and_pnpm_pins_returned_together(self):
        # When repos[] declares both bun- and pnpm-managed entries,
        # ensure_runtimes must see both pins so neither manager goes
        # unpinned at install time.
        values = _values(
            repos=[
                {
                    "id": "test-factory-code",
                    "quay": {"package_manager": "bun"},
                },
                {
                    "id": "brix-indexer",
                    "quay": {"package_manager": "pnpm"},
                },
            ],
            runtime_managers={"bun": VALID_BUN_PIN, "pnpm": VALID_PNPM_PIN},
        )
        pins = required_runtime_managers(values)
        assert set(pins) == {"bun", "pnpm"}
        assert pins["bun"].version == "1.3.9"
        assert pins["pnpm"].version == "10.33.4"
        assert pins["pnpm"].linux_x64_sha256 == VALID_PNPM_PIN["linux_x64_sha256"]

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


VALID_CODEX_PIN = {
    "version": "rust-v0.130.0",
    "linux_x64_sha256": "16779e7b7857508a768a36d7d4e084eec336ec23946ed70a9b09489b8f861190",
}


def _codex_values(*, worker: str = "codex", invocations: dict | None = None) -> dict:
    return {
        "quay": {
            "agent_invocation": "claude < {prompt_file}",
            "codex": VALID_CODEX_PIN,
            "agents": {
                "worker": worker,
                "invocations": invocations or {
                    "codex": {
                        "worker": "codex exec --json < {prompt_file}",
                    },
                    "claude": {
                        "worker": "claude --print < {prompt_file}",
                    },
                },
            },
        },
    }


class TestRequiredCodexPin:
    def test_active_codex_invocation_returns_pin(self):
        assert required_codex_pin(_codex_values()) == CodexPin(
            version=VALID_CODEX_PIN["version"],
            linux_x64_sha256=VALID_CODEX_PIN["linux_x64_sha256"],
        )

    def test_claude_only_invocation_is_noop(self):
        values = _codex_values(worker="claude")
        assert required_codex_pin(values) is None

    def test_legacy_agent_invocation_still_triggers(self):
        values = {
            "quay": {
                "agent_invocation": "codex exec < {prompt_file}",
                "codex": VALID_CODEX_PIN,
            }
        }
        assert required_codex_pin(values) is not None

    def test_unused_codex_invocation_does_not_trigger(self):
        values = _codex_values(worker="claude")
        assert required_codex_pin(values) is None

    def test_missing_pin_fails_when_codex_active(self, capsys):
        values = _codex_values()
        del values["quay"]["codex"]
        with pytest.raises(SystemExit) as excinfo:
            required_codex_pin(values)
        assert excinfo.value.code == 1
        assert "quay.codex" in capsys.readouterr().err

    def test_invalid_sha_fails_when_codex_active(self, capsys):
        values = _codex_values()
        values["quay"]["codex"]["linux_x64_sha256"] = "deadbeef"
        with pytest.raises(SystemExit):
            required_codex_pin(values)
        assert "linux_x64_sha256" in capsys.readouterr().err
