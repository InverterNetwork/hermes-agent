"""``python3 -m hermes_installer`` entry point.

* ``ensure-runtimes`` — provision pinned runtime managers (bun, ...) the
  ``repos[].quay.package_manager`` entries declare. Idempotent. Refuses
  on SHA mismatch or missing pin. Requires euid 0 unless
  ``--skip-root-check`` is passed (tests only).
* ``ensure-codex`` — provision the pinned Codex CLI as a root-managed
  binary when Atlas or the caller say Codex is required.
* ``ensure-caddy-atlas-hub-route`` — install the public Caddy route that
  forwards Atlas CLI traffic to the loopback-only Hub service.
* ``seed-systemd-default-env`` — create a preserved /etc/default override
  file for an installer-managed service.
* ``render-systemd-unit`` — render a systemd unit template with explicit
  ``__PLACEHOLDER__`` values.
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

    ec = sub.add_parser(
        "ensure-codex",
        help="provision pinned Codex CLI when active quay agents reference codex",
    )
    ec.add_argument("--values", required=True, type=Path, help="path to deploy.values.yaml")
    ec.add_argument("--agent-user", required=True, help="agent user that owns ~/.codex state")
    ec.add_argument(
        "--symlink-path",
        type=Path,
        default=Path("/usr/local/bin/codex"),
        help="system symlink to create (default: /usr/local/bin/codex)",
    )
    ec.add_argument(
        "--required",
        action="store_true",
        help="force provisioning from quay.codex even when deploy values do not reference codex",
    )
    ec.add_argument(
        "--skip-root-check",
        action="store_true",
        help="bypass the euid==0 assertion (tests only)",
    )

    sde = sub.add_parser(
        "seed-systemd-default-env",
        help="create a preserved /etc/default override file for a service",
    )
    sde.add_argument(
        "--service-name",
        required=True,
        help="service name without .service",
    )
    sde.add_argument("--out", required=True, type=Path, help="output env file path")
    sde.add_argument(
        "--skip-root-check",
        action="store_true",
        help="bypass the euid==0 assertion (tests only)",
    )

    rsu = sub.add_parser(
        "render-systemd-unit",
        help="render a systemd unit template with __PLACEHOLDER__ values",
    )
    rsu.add_argument(
        "--template",
        required=True,
        type=Path,
        help="unit template path",
    )
    rsu.add_argument("--out", required=True, type=Path, help="rendered unit path")
    rsu.add_argument(
        "--set",
        dest="replacements",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="template replacement; may be passed more than once",
    )
    rsu.add_argument(
        "--skip-root-check",
        action="store_true",
        help="bypass the euid==0 assertion (tests only)",
    )

    car = sub.add_parser(
        "ensure-caddy-atlas-hub-route",
        help="install the Caddy route that exposes Atlas Hub through HTTPS",
    )
    car.add_argument(
        "--caddyfile",
        type=Path,
        default=Path("/etc/caddy/Caddyfile"),
        help="Caddyfile path to update (default: /etc/caddy/Caddyfile)",
    )
    car.add_argument("--public-base-url", required=True, help="public Hub origin")
    car.add_argument("--hub-host", required=True, help="loopback Atlas Hub host")
    car.add_argument("--hub-port", required=True, help="loopback Atlas Hub port")
    car.add_argument(
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

    if args.cmd == "ensure-codex":
        from .codex import ensure_codex
        from .config import load_values, required_codex_pin

        if not args.skip_root_check:
            require_root()
        values = load_values(args.values)
        pin = required_codex_pin(values, force=args.required)
        ensure_codex(pin, agent_user=args.agent_user, symlink_path=args.symlink_path)
        return 0

    if args.cmd == "seed-systemd-default-env":
        from .systemd import seed_default_env

        if not args.skip_root_check:
            require_root()
        seed_default_env(
            args.out,
            args.service_name,
            chown_root=not args.skip_root_check,
        )
        return 0

    if args.cmd == "render-systemd-unit":
        from .systemd import parse_replacement, render_systemd_unit

        if not args.skip_root_check:
            require_root()
        replacements = dict(parse_replacement(raw) for raw in args.replacements)
        render_systemd_unit(
            args.template,
            args.out,
            replacements,
            chown_root=not args.skip_root_check,
        )
        return 0

    if args.cmd == "ensure-caddy-atlas-hub-route":
        from .caddy import ensure_atlas_hub_route

        if not args.skip_root_check:
            require_root()
        try:
            changed = ensure_atlas_hub_route(
                args.caddyfile,
                public_base_url=args.public_base_url,
                hub_host=args.hub_host,
                hub_port=args.hub_port,
            )
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if changed:
            print(f"updated {args.caddyfile}")
        else:
            print(f"{args.caddyfile} already had the Atlas Hub route")
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

    raise AssertionError(f"unreachable: argparse should have rejected cmd={args.cmd!r}")


if __name__ == "__main__":
    sys.exit(main())
