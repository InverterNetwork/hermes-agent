"""Schema-validated loader for ``deploy.values.yaml``.

``required_runtime_managers`` walks ``repos[].quay.package_manager`` and
returns the matching ``quay.runtime_managers.<name>`` pins, failing loud
with an actionable diagnostic when a declared manager has no pin block.
``required_codex_pin`` does the same for the Codex CLI, but only when the
active quay agent invocation path references the ``codex`` binary.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shlex
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


def required_codex_pin(values: dict[str, Any]) -> CodexPin | None:
    """Return the Codex CLI pin when the active quay agent path uses Codex.

    The legacy ``quay.agent_invocation`` field remains a valid trigger for
    old config. When the newer ``quay.agents`` block is present, only the
    selected worker/reviewer invocation entries are active; an unused
    ``invocations.codex`` table should not force a Codex install.
    """
    if not _codex_invocation_active(values):
        return None

    quay_block = values.get("quay") or {}
    spec = quay_block.get("codex") if isinstance(quay_block, dict) else None
    if not isinstance(spec, dict):
        fail(
            "quay agent invocation references codex, but no usable "
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
            "quay agent invocation references codex, but quay.codex is invalid:\n"
            + "\n".join(f"  - {item}" for item in invalid)
        )
    return CodexPin(version=version, linux_x64_sha256=sha.lower())


def _codex_invocation_active(values: dict[str, Any]) -> bool:
    quay = values.get("quay") or {}
    if not isinstance(quay, dict):
        return False

    active_cmds: list[str] = []
    agents = quay.get("agents")
    if isinstance(agents, dict):
        invocations = agents.get("invocations")
        invocations = invocations if isinstance(invocations, dict) else {}
        for role in ("worker", "reviewer"):
            agent_id = agents.get(role)
            if not isinstance(agent_id, str) or not agent_id:
                continue
            agent_spec = invocations.get(agent_id)
            if not isinstance(agent_spec, dict):
                continue
            cmd = agent_spec.get(role)
            if isinstance(cmd, str):
                active_cmds.append(cmd)
    else:
        legacy = quay.get("agent_invocation")
        if isinstance(legacy, str):
            active_cmds.append(legacy)

    return any(_command_references_binary(cmd, "codex") for cmd in active_cmds)


def _command_references_binary(command: str, binary: str) -> bool:
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()
    return any(Path(tok).name == binary for tok in tokens)
