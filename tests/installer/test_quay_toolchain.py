import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALLER_DIR = REPO_ROOT / "installer"
if str(INSTALLER_DIR) not in sys.path:
    sys.path.insert(0, str(INSTALLER_DIR))

from hermes_installer.config import required_quay_toolchain
from hermes_installer.runtimes import _RECIPES


ANVIL_SHA = "cf7e688ed0c4c48adffca788b496076e31060b67ac5afe1e43dbb5499c20c88b"


def test_quay_toolchain_requires_anvil_when_quay_enabled():
    values = {
        "quay": {
            "version": "v0.1.0",
            "toolchain": {
                "anvil": {
                    "version": "1.7.1",
                    "linux_x64_sha256": ANVIL_SHA,
                },
            },
        },
    }

    pins = required_quay_toolchain(values)

    assert list(pins) == ["anvil"]
    assert pins["anvil"].version == "1.7.1"
    assert pins["anvil"].linux_x64_sha256 == ANVIL_SHA


def test_quay_toolchain_noops_when_quay_disabled():
    assert required_quay_toolchain({"quay": {"toolchain": {}}}) == {}


def test_quay_toolchain_fails_loud_when_anvil_pin_missing():
    with pytest.raises(SystemExit):
        required_quay_toolchain({"quay": {"version": "v0.1.0"}})


def test_anvil_recipe_uses_foundry_linux_tarball():
    recipe = _RECIPES["anvil"]

    assert recipe.archive_kind == "tar.gz"
    assert recipe.archive_member == "anvil"
    assert (
        recipe.url_template.format(version="1.7.1")
        == "https://github.com/foundry-rs/foundry/releases/download/"
        "v1.7.1/foundry_v1.7.1_linux_amd64.tar.gz"
    )
