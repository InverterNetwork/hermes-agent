"""Schema-validated loader for ``deploy.values.yaml``.

``required_runtime_managers`` walks ``repos[].quay.package_manager`` and
returns the matching ``quay.runtime_managers.<name>`` pins, failing loud
with an actionable diagnostic when a declared manager has no pin block.
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
