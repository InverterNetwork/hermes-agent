"""Caddyfile helpers for Hermes-managed edge routes."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse


BEGIN_ATLAS_HUB = "# BEGIN HERMES MANAGED ATLAS HUB ROUTE"
END_ATLAS_HUB = "# END HERMES MANAGED ATLAS HUB ROUTE"


def public_origin_site_label(public_base_url: str) -> tuple[str, str]:
    """Return ``(normalized_origin, caddy_site_label)`` for a public URL."""
    raw = public_base_url.strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "atlas.hub.public_base_url must be an http(s) origin like "
            "https://didier.brix.fyi"
        )
    if parsed.username or parsed.password:
        raise ValueError("atlas.hub.public_base_url must not include credentials")
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(
            "atlas.hub.public_base_url must be an origin without path, query, "
            "or fragment"
        )
    return f"{parsed.scheme}://{parsed.netloc}", parsed.netloc


def ensure_atlas_hub_route(
    caddyfile: Path,
    *,
    public_base_url: str,
    hub_host: str,
    hub_port: str,
) -> bool:
    """Ensure Caddy proxies ``/v1/*`` on the public origin to Atlas Hub.

    Returns ``True`` when the file changed. The route is deliberately managed
    as a small marked block so operator-owned routes remain untouched.
    """
    _, site_label = public_origin_site_label(public_base_url)
    block = _route_block(hub_host=hub_host, hub_port=hub_port, indent="\t")

    text = caddyfile.read_text(encoding="utf-8") if caddyfile.exists() else ""
    updated = _replace_marked_block(text, block)
    if updated is None:
        updated = _insert_site_block(text, site_label=site_label, block=block)

    if updated == text:
        return False
    caddyfile.parent.mkdir(parents=True, exist_ok=True)
    caddyfile.write_text(updated, encoding="utf-8")
    return True


def caddyfile_has_atlas_hub_route(
    text: str,
    *,
    public_base_url: str,
    hub_host: str,
    hub_port: str,
) -> bool:
    """Best-effort verifier for the managed Atlas Hub route."""
    _, site_label = public_origin_site_label(public_base_url)
    if BEGIN_ATLAS_HUB not in text or END_ATLAS_HUB not in text:
        return False
    if f"reverse_proxy {hub_host}:{hub_port}" not in text:
        return False
    if not re.search(r"(?m)^\s*handle\s+/v1/\*\s*\{", text):
        return False
    return _find_site_declaration(text.splitlines(), site_label) is not None


def _route_block(*, hub_host: str, hub_port: str, indent: str) -> str:
    inner = f"{indent}\t"
    return "\n".join(
        [
            f"{indent}{BEGIN_ATLAS_HUB}",
            f"{indent}handle /v1/* {{",
            f"{inner}reverse_proxy {hub_host}:{hub_port}",
            f"{indent}}}",
            f"{indent}{END_ATLAS_HUB}",
        ]
    )


def _replace_marked_block(text: str, block: str) -> str | None:
    begin = text.find(BEGIN_ATLAS_HUB)
    end = text.find(END_ATLAS_HUB)
    if begin == -1 and end == -1:
        return None
    if begin == -1 or end == -1 or end < begin:
        raise ValueError("Caddyfile has a partial Hermes Atlas Hub managed block")

    line_start = text.rfind("\n", 0, begin) + 1
    line_end = text.find("\n", end)
    if line_end == -1:
        line_end = len(text)
        suffix = ""
    else:
        suffix = "\n"
    return f"{text[:line_start]}{block}{suffix}{text[line_end + len(suffix):]}"


def _insert_site_block(text: str, *, site_label: str, block: str) -> str:
    lines = text.splitlines()
    site_index = _find_site_declaration(lines, site_label)
    if site_index is None:
        prefix = f"{text.rstrip()}\n\n" if text.strip() else ""
        return f"{prefix}{site_label} {{\n{block}\n}}\n"

    updated_lines = lines[: site_index + 1] + block.splitlines() + lines[site_index + 1 :]
    trailing = "\n" if text.endswith("\n") else ""
    return "\n".join(updated_lines) + trailing


def _find_site_declaration(lines: list[str], site_label: str) -> int | None:
    wanted = {site_label, f"http://{site_label}", f"https://{site_label}"}
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "{" not in stripped:
            continue
        prefix = stripped.split("{", 1)[0].strip()
        labels = {label.strip().rstrip(",") for label in re.split(r"[\s,]+", prefix)}
        if labels & wanted:
            return idx
    return None
