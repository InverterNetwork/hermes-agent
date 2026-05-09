"""Stderr-friendly fail, root-check, and ``==>`` headers that match
``setup-hermes.sh``'s output convention."""

from __future__ import annotations

import os
import sys
from typing import NoReturn


def info(msg: str) -> None:
    print(f"==> {msg}", flush=True)


def fail(msg: str, code: int = 1) -> NoReturn:
    sys.stderr.write(f"FAIL: {msg}\n")
    sys.exit(code)


def require_root() -> None:
    if os.geteuid() != 0:
        fail(
            "hermes_installer must be invoked as root (it writes /usr/local/bin); "
            "run from setup-hermes.sh under sudo.",
            code=2,
        )
