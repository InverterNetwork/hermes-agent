#!/usr/bin/env python3
"""Fail closed unless the Atlas pin supports the no-fallback gws adapter."""

from __future__ import annotations

import re
import sys


PINNED_MINIMUM = "0.1.16"


def _semver(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        raise SystemExit(
            "FAIL: Atlas Google Docs gws migration requires a stable semver tag"
        )
    return tuple(map(int, match.groups()))


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: check_atlas_gws_version.py ACTUAL MINIMUM")
    actual, minimum = sys.argv[1:]
    if minimum != PINNED_MINIMUM:
        raise SystemExit(
            "FAIL: atlas.google_docs.minimum_atlas_version must be exactly "
            f"{PINNED_MINIMUM}"
        )
    if _semver(actual) < _semver(minimum):
        raise SystemExit(
            f"FAIL: atlas.version {actual} is older than the Google Docs gws "
            f"minimum {minimum}; release Atlas first and update the Hermes pin"
        )


if __name__ == "__main__":
    main()
