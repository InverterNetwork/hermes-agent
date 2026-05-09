"""Shared helpers: stderr-friendly fail, root-check, subprocess wrapper, and
``==>`` headers that match the bash conventions for the bash → Python
transition period."""

from __future__ import annotations

import os
import subprocess
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


def run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    """``subprocess.run`` wrapper with ``check=True`` and a stderr surface on failure.

    Errors print the offending command + captured stderr before re-raising,
    so a ``CalledProcessError`` propagating out of the installer is legible
    rather than a bare ``returncode=1``.
    """
    try:
        return subprocess.run(  # type: ignore[call-overload]
            cmd, check=True, capture_output=True, text=True, **kwargs
        )
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(f"FAIL: {' '.join(cmd)} (exit {exc.returncode})\n")
        if exc.stderr:
            sys.stderr.write(exc.stderr)
        raise
