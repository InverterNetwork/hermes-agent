"""``python3 -m hermes_installer`` entry point.

* ``ensure-runtimes`` — provision pinned runtime managers (bun, ...) the
  ``repos[].quay.package_manager`` entries declare. Idempotent. Refuses
  on SHA mismatch or missing pin. Requires euid 0 unless
  ``--skip-root-check`` is passed (tests only).
* ``verify`` — read-only health check. Inspects the live install for drift
  and exits 0 (no drift) or 1 (drift detected). No root required.
"""

from __future__ import annotations

import argparse
import pwd
import sys
from pathlib import Path

from .util import require_root
from .verify import VerifyArgs, run as run_verify


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

    vr = sub.add_parser("verify", help="read-only health check of a rendered install")
    vr.add_argument("--fork", required=True, type=Path, help="fork repo path")
    vr.add_argument(
        "--target",
        type=Path,
        default=None,
        help="render target (HERMES_HOME); defaults to <user>'s ~/.hermes",
    )
    vr.add_argument("--user", required=True, help="agent user (used for ownership checks)")
    vr.add_argument(
        "--auth-method",
        choices=("none", "app"),
        default="none",
        help="auth method declared at install time",
    )
    vr.add_argument("--quiet", action="store_true", help="suppress [OK] lines")
    vr.add_argument(
        "--values",
        type=Path,
        default=None,
        help="path to deploy.values.yaml (defaults to <fork>/deploy.values.yaml)",
    )
    vr.add_argument(
        "--gh-api-base",
        default=None,
        help="override GitHub API base URL (CI only; production uses api.github.com)",
    )

    args = p.parse_args(argv)

    if args.cmd == "ensure-runtimes":
        # Lazy-import: ensure-runtimes pulls in PyYAML via .config, which we
        # don't want as a hard dependency of the verify path (verify shells
        # out to values_helper.py instead).
        from .config import load_values, required_runtime_managers
        from .runtimes import ensure_runtimes

        if not args.skip_root_check:
            require_root()
        values = load_values(args.values)
        pins = required_runtime_managers(values)
        ensure_runtimes(pins, install_dir=args.install_dir)
        return 0

    if args.cmd == "verify":
        try:
            agent_pw = pwd.getpwnam(args.user)
        except KeyError:
            print(f"agent user '{args.user}' does not exist", file=sys.stderr)
            return 1
        target = args.target or Path(agent_pw.pw_dir) / ".hermes"
        return run_verify(
            VerifyArgs(
                fork=args.fork,
                target=target,
                user=args.user,
                auth_method=args.auth_method,
                quiet=args.quiet,
                values=args.values,
                gh_api_base=args.gh_api_base,
            )
        )

    return 2


if __name__ == "__main__":
    sys.exit(main())
