"""``python3 -m hermes_installer`` entry point.

Subcommands:

* ``ensure-runtimes`` — provision pinned runtime managers (bun, ...) the
  ``repos[].quay.package_manager`` entries declare. Idempotent. Refuses
  on SHA mismatch or missing pin. Requires euid 0 unless
  ``--skip-root-check`` is passed (tests only).

Invoked from ``installer/setup-hermes.sh`` on every install run that has
at least one ``repos[]`` entry declaring a package_manager. Direct
invocation is supported for tests and ad-hoc operator use.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_values, required_runtime_managers
from .runtimes import ensure_runtimes
from .util import require_root


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="hermes_installer")
    sub = p.add_subparsers(dest="cmd", required=True)

    er = sub.add_parser(
        "ensure-runtimes",
        help="provision pinned runtime managers (bun, ...) declared by repos[]",
    )
    er.add_argument(
        "--values",
        required=True,
        type=Path,
        help="path to deploy.values.yaml",
    )
    er.add_argument(
        "--install-dir",
        type=Path,
        default=Path("/usr/local/bin"),
        help="directory to install runtime binaries into (default: /usr/local/bin)",
    )
    er.add_argument(
        "--skip-root-check",
        action="store_true",
        help="bypass the euid==0 assertion (tests only)",
    )

    args = p.parse_args(argv)

    if args.cmd == "ensure-runtimes":
        if not args.skip_root_check:
            require_root()
        values = load_values(args.values)
        pins = required_runtime_managers(values)
        ensure_runtimes(pins, install_dir=args.install_dir)
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
