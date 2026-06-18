import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALLER_DIR = REPO_ROOT / "installer"
if str(INSTALLER_DIR) not in sys.path:
    sys.path.insert(0, str(INSTALLER_DIR))

from hermes_installer.caddy import ensure_atlas_hub_route


def test_ensure_atlas_hub_route_inserts_before_existing_catchall(tmp_path: Path):
    caddyfile = tmp_path / "Caddyfile"
    caddyfile.write_text(
        "didier.brix.fyi {\n"
        "\thandle /quay/admin* {\n"
        "\t\treverse_proxy 127.0.0.1:9119\n"
        "\t}\n"
        "\n"
        "\thandle {\n"
        "\t\treverse_proxy localhost:3100\n"
        "\t}\n"
        "}\n",
        encoding="utf-8",
    )

    changed = ensure_atlas_hub_route(
        caddyfile,
        public_base_url="https://didier.brix.fyi",
        hub_host="127.0.0.1",
        hub_port="8765",
    )

    text = caddyfile.read_text(encoding="utf-8")
    assert changed is True
    assert "# BEGIN HERMES MANAGED ATLAS HUB ROUTE" in text
    assert "handle /v1/*" in text
    assert "reverse_proxy 127.0.0.1:8765" in text
    assert text.index("handle /v1/*") < text.index("handle {")


def test_ensure_atlas_hub_route_updates_existing_managed_block(tmp_path: Path):
    caddyfile = tmp_path / "Caddyfile"
    caddyfile.write_text(
        "didier.brix.fyi {\n"
        "\t# BEGIN HERMES MANAGED ATLAS HUB ROUTE\n"
        "\thandle /v1/* {\n"
        "\t\treverse_proxy 127.0.0.1:1111\n"
        "\t}\n"
        "\t# END HERMES MANAGED ATLAS HUB ROUTE\n"
        "}\n",
        encoding="utf-8",
    )

    changed = ensure_atlas_hub_route(
        caddyfile,
        public_base_url="https://didier.brix.fyi",
        hub_host="127.0.0.1",
        hub_port="8765",
    )

    text = caddyfile.read_text(encoding="utf-8")
    assert changed is True
    assert "reverse_proxy 127.0.0.1:8765" in text
    assert "1111" not in text


def test_ensure_atlas_hub_route_rejects_path_base_url(tmp_path: Path):
    with pytest.raises(ValueError):
        ensure_atlas_hub_route(
            tmp_path / "Caddyfile",
            public_base_url="https://didier.brix.fyi/atlas",
            hub_host="127.0.0.1",
            hub_port="8765",
        )
