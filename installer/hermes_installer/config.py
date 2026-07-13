"""Schema-validated loader for ``deploy.values.yaml``.

``required_runtime_managers`` walks ``repos[].quay.package_manager`` and
returns the matching ``quay.runtime_managers.<name>`` pins, failing loud
with an actionable diagnostic when a declared manager has no pin block.
``required_quay_toolchain`` returns Quay host-level worker tools (anvil, ...).
``required_codex_pin`` does the same for the Codex CLI, but only when setup is
provisioning Quay's standard local agent CLI set, or Atlas is enabled in
``codex-exec`` mode.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .util import fail


@dataclass(frozen=True)
class RuntimeManagerPin:
    name: str
    version: str
    linux_x64_sha256: str


@dataclass(frozen=True)
class CodexPin:
    version: str
    linux_x64_sha256: str


def load_values(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        fail(f"values file not found: {path}")
    if not isinstance(data, dict):
        fail(f"top-level of {path} must be a mapping")
    return data


def required_runtime_managers(values: dict[str, Any]) -> dict[str, RuntimeManagerPin]:
    """Compute the runtime-manager pins required by ``repos[]``.

    A declared ``package_manager`` without a matching ``quay.runtime_managers.<name>``
    block is fail-loud — the installer can't invent a version or SHA, and silently
    skipping would let quay's bootstrap fail with the same ``command not found``
    we're trying to fix.
    """
    requested: set[str] = set()
    for entry in values.get("repos", []) or []:
        if not isinstance(entry, dict):
            continue
        quay = entry.get("quay")
        if not isinstance(quay, dict):
            continue
        pm = quay.get("package_manager")
        if isinstance(pm, str) and pm:
            requested.add(pm)

    if not requested:
        return {}

    quay_block = values.get("quay") or {}
    rms_raw = quay_block.get("runtime_managers") if isinstance(quay_block, dict) else None
    rms = rms_raw if isinstance(rms_raw, dict) else {}

    out: dict[str, RuntimeManagerPin] = {}
    missing: list[str] = []
    invalid: list[str] = []
    for name in sorted(requested):
        spec = rms.get(name)
        if not isinstance(spec, dict):
            missing.append(name)
            continue
        version = spec.get("version")
        sha = spec.get("linux_x64_sha256")
        if not isinstance(version, str) or not version:
            invalid.append(f"{name}: missing or non-string `version`")
            continue
        if not isinstance(sha, str) or len(sha) != 64:
            invalid.append(f"{name}: `linux_x64_sha256` must be a 64-char hex string")
            continue
        try:
            int(sha, 16)
        except ValueError:
            invalid.append(f"{name}: `linux_x64_sha256` must be a 64-char hex string")
            continue
        out[name] = RuntimeManagerPin(name=name, version=version, linux_x64_sha256=sha.lower())

    if missing or invalid:
        lines = [
            "repos[] declares package_manager(s) without a usable quay.runtime_managers pin:"
        ]
        for n in missing:
            lines.append(
                f"  - {n}: add quay.runtime_managers.{n}.{{version, linux_x64_sha256}} "
                "to deploy.values.yaml"
            )
        for d in invalid:
            lines.append(f"  - {d}")
        fail("\n".join(lines))

    return out


def required_quay_toolchain(values: dict[str, Any]) -> dict[str, RuntimeManagerPin]:
    """Return host-level tools Quay workers need when Quay is enabled.

    These are not package managers declared by a repo's bootstrap command.
    They are substrate tools the worker runtime itself must make available on
    ``PATH`` for task tests, such as Foundry's ``anvil`` for fork tests.
    """
    quay_block = values.get("quay") or {}
    if not isinstance(quay_block, dict):
        return {}
    version = quay_block.get("version")
    if not isinstance(version, str) or not version:
        return {}

    toolchain_raw = quay_block.get("toolchain")
    toolchain = toolchain_raw if isinstance(toolchain_raw, dict) else {}
    return _pins_from_block(
        toolchain,
        requested={"anvil"},
        path="quay.toolchain",
        missing_message=(
            "quay.version is set, but the Quay worker toolchain is missing "
            "usable pins:"
        ),
    )


def required_codex_pin(values: dict[str, Any], *, force: bool = False) -> CodexPin | None:
    """Return the Codex CLI pin when Atlas or live Quay config requires Codex.

    Quay-owned agent runtime settings are intentionally not sourced from
    deploy values. Atlas requires Codex when ``atlas.version`` is non-empty and
    ``atlas.ai.mode == codex-exec``. ``force`` is used by setup-hermes.sh when
    Quay is enabled and the standard local agent CLI set should be provisioned.
    """
    if not (force or _atlas_codex_active(values)):
        return None

    quay_block = values.get("quay") or {}
    spec = quay_block.get("codex") if isinstance(quay_block, dict) else None
    if not isinstance(spec, dict):
        fail(
            "Codex is required, but no usable "
            "quay.codex.{version, linux_x64_sha256} pin exists in deploy.values.yaml"
        )
    version = spec.get("version")
    sha = spec.get("linux_x64_sha256")
    invalid: list[str] = []
    if not isinstance(version, str) or not version:
        invalid.append("missing or non-string `version`")
    if not isinstance(sha, str) or len(sha) != 64:
        invalid.append("`linux_x64_sha256` must be a 64-char hex string")
    else:
        try:
            int(sha, 16)
        except ValueError:
            invalid.append("`linux_x64_sha256` must be a 64-char hex string")
    if invalid:
        fail(
            "Codex is required, but quay.codex is invalid:\n"
            + "\n".join(f"  - {item}" for item in invalid)
        )
    return CodexPin(version=version, linux_x64_sha256=sha.lower())


def _pins_from_block(
    block: dict[str, Any],
    *,
    requested: set[str],
    path: str,
    missing_message: str,
) -> dict[str, RuntimeManagerPin]:
    out: dict[str, RuntimeManagerPin] = {}
    missing: list[str] = []
    invalid: list[str] = []
    for name in sorted(requested):
        spec = block.get(name)
        if not isinstance(spec, dict):
            missing.append(name)
            continue
        version = spec.get("version")
        sha = spec.get("linux_x64_sha256")
        if not isinstance(version, str) or not version:
            invalid.append(f"{name}: missing or non-string `version`")
            continue
        if not isinstance(sha, str) or len(sha) != 64:
            invalid.append(f"{name}: `linux_x64_sha256` must be a 64-char hex string")
            continue
        try:
            int(sha, 16)
        except ValueError:
            invalid.append(f"{name}: `linux_x64_sha256` must be a 64-char hex string")
            continue
        out[name] = RuntimeManagerPin(name=name, version=version, linux_x64_sha256=sha.lower())

    if missing or invalid:
        lines = [missing_message]
        for n in missing:
            lines.append(
                f"  - {n}: add {path}.{n}.{{version, linux_x64_sha256}} "
                "to deploy.values.yaml"
            )
        for d in invalid:
            lines.append(f"  - {d}")
        fail("\n".join(lines))

    return out


def _atlas_codex_active(values: dict[str, Any]) -> bool:
    atlas = values.get("atlas") or {}
    if not isinstance(atlas, dict):
        return False
    version = atlas.get("version")
    if not isinstance(version, str) or not version:
        return False
    ai = atlas.get("ai") or {}
    if not isinstance(ai, dict):
        return False
    return ai.get("mode") == "codex-exec"
