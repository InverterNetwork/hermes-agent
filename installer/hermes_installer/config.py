"""Schema-validated loaders for the install-side configuration files.

Two config files are in scope today, both bash → Python migration targets:

* ``deploy.values.yaml`` — fork-level values (org identity, repos[], quay
  block). YAML, loaded with PyYAML.
* ``config.toml`` — quay's runtime config (per-host, lives at
  ``$TARGET/quay/config.toml``). TOML via stdlib ``tomllib``. The loader
  is here so future install-side features (config probes, drift checks)
  share the schema-error path with the YAML loader.

``required_runtime_managers`` is the v0 schema check the rest of the
package depends on: it walks ``repos[].quay.package_manager`` and
returns the matching ``quay.runtime_managers.<name>`` pins, failing
loud (with an actionable diagnostic) on any mismatch.
"""

from __future__ import annotations

import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


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
        sys.stderr.write(f"hermes_installer: values file not found: {path}\n")
        sys.exit(1)
    if not isinstance(data, dict):
        sys.stderr.write(f"hermes_installer: top-level of {path} must be a mapping\n")
        sys.exit(1)
    return data


def load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        sys.stderr.write(f"hermes_installer: toml file not found: {path}\n")
        sys.exit(1)
    if not isinstance(data, dict):
        sys.stderr.write(f"hermes_installer: top-level of {path} must be a mapping\n")
        sys.exit(1)
    return data


def required_runtime_managers(values: dict[str, Any]) -> dict[str, RuntimeManagerPin]:
    """Compute the runtime-manager pins required by ``repos[]``.

    Walks every ``repos[].quay.package_manager`` and looks each name up in
    ``quay.runtime_managers.<name>``. A declared package_manager without a
    matching pin block is fail-loud — the installer can't invent a version
    or SHA, and silently skipping would let quay's bootstrap fail with the
    same ``command not found`` we're trying to fix.
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
        if not isinstance(sha, str) or len(sha) != 64 or not all(c in "0123456789abcdef" for c in sha.lower()):
            invalid.append(f"{name}: `linux_x64_sha256` must be a 64-char lowercase hex string")
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
        sys.stderr.write("\n".join(lines) + "\n")
        sys.exit(1)

    return out
