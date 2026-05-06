#!/usr/bin/env python3
"""Read deploy.values.yaml and emit shell-friendly outputs.

setup-hermes.sh shells out to this helper rather than parsing YAML in bash.
Four subcommands:

  get <key>                    — print scalar at dotted path (org.name) or a
                                  list joined by `--sep` (default ",").
  render-manifest <in> <out>   — substitute ${slack.app.*} placeholders into
                                  installer/slack-manifest.json.tmpl.
  render-runtime-config <out>  — write ~/.hermes/config.yaml from
                                  slack.runtime.*. Skips if <out> already
                                  exists, so operator hand-edits survive.
  render-quay-config <out>     — write ~/.hermes/quay/config.toml from the
                                  quay.* block. Skips if <out> already exists.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import string
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    sys.stderr.write(
        "values_helper.py: PyYAML missing. On Debian/Ubuntu install with:\n"
        "  apt-get install -y python3-yaml\n"
    )
    sys.exit(1)


_MISSING = object()


def _load(values_path: Path) -> dict:
    try:
        with values_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        sys.stderr.write(f"values_helper.py: not found: {values_path}\n")
        sys.exit(1)
    if not isinstance(data, dict):
        sys.stderr.write(
            f"values_helper.py: top-level of {values_path} must be a mapping\n"
        )
        sys.exit(1)
    return data


def _lookup(data: dict, dotted: str) -> Any:
    cur: Any = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return _MISSING
        cur = cur[part]
    return cur


def cmd_get(args: argparse.Namespace) -> int:
    data = _load(Path(args.values))
    val = _lookup(data, args.key)
    if val is _MISSING:
        sys.stderr.write(f"values_helper.py: key not found: {args.key}\n")
        return 1
    if isinstance(val, list):
        sys.stdout.write(args.sep.join(str(v) for v in val))
    elif isinstance(val, bool):
        sys.stdout.write("true" if val else "false")
    elif val is None:
        sys.stdout.write("")
    else:
        sys.stdout.write(str(val))
    return 0


class _DottedTemplate(string.Template):
    """``string.Template`` with dots allowed in identifiers (e.g. ${slack.app.name})."""

    idpattern = r"(?a:[_a-z][._a-z0-9]*)"


def _flatten_scalars(data: dict, prefix: str) -> dict[str, str]:
    """Flatten nested mappings under ``data`` to dotted keys, scalars only.

    Lists and mappings are skipped — Slack's manifest schema is all scalars,
    and the alternative (JSON-encoding nested values into a JSON string field)
    would be a footgun.
    """
    out: dict[str, str] = {}
    for k, v in data.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flatten_scalars(v, prefix=f"{key}."))
        elif isinstance(v, (str, int, float, bool)):
            out[key] = str(v)
    return out


_PLACEHOLDER_RE = re.compile(r"\$\{" + _DottedTemplate.idpattern + r"\}")


def cmd_render_manifest(args: argparse.Namespace) -> int:
    data = _load(Path(args.values))
    in_path = Path(args.in_path)
    out_path = Path(args.out)

    try:
        tmpl = in_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        sys.stderr.write(f"values_helper.py: template not found: {in_path}\n")
        return 1

    # Restrict the substitution scope to slack.app.* — the manifest template
    # documents that contract, and walking the whole values dict would let an
    # unrelated key (e.g. org.name) silently become a usable placeholder.
    slack_app = _lookup(data, "slack.app")
    if not isinstance(slack_app, dict):
        sys.stderr.write("values_helper.py: slack.app missing or not a mapping\n")
        return 1
    flat = _flatten_scalars(slack_app, prefix="slack.app.")

    # JSON-escape every value before substitution so a description containing
    # a quote / backslash doesn't break the manifest's JSON syntax.
    escaped = {k: json.dumps(v)[1:-1] for k, v in flat.items()}
    rendered = _DottedTemplate(tmpl).safe_substitute(escaped)

    leftover = _PLACEHOLDER_RE.findall(rendered)
    if leftover:
        sys.stderr.write(
            "values_helper.py: unresolved placeholders in rendered manifest: "
            + ", ".join(sorted(set(leftover)))
            + "\n"
        )
        return 1

    try:
        json.loads(rendered)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"values_helper.py: rendered manifest is not valid JSON: {exc}\n")
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(rendered, encoding="utf-8")
    return 0


def cmd_render_runtime_config(args: argparse.Namespace) -> int:
    """Seed <target>/config.yaml from slack.runtime.

    Idempotent: refuses to overwrite an existing file unless --force is
    passed. The installer relies on this so operator hand-edits to the live
    config survive subsequent re-runs.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    if out_path.exists() and not args.force:
        sys.stdout.write(f"preserved: {out_path}\n")
        return 0

    runtime = (data.get("slack") or {}).get("runtime") or {}
    if not isinstance(runtime, dict):
        sys.stderr.write("values_helper.py: slack.runtime must be a mapping\n")
        return 1

    slack_block: dict[str, Any] = {}
    if "allowed_channels" in runtime:
        ac = runtime["allowed_channels"] or []
        slack_block["allowed_channels"] = list(ac) if isinstance(ac, list) else [ac]
    if runtime.get("require_mention") is not None:
        slack_block["require_mention"] = bool(runtime["require_mention"])
    if runtime.get("home_channel"):
        slack_block["home_channel"] = str(runtime["home_channel"])
    cp = runtime.get("channel_prompts")
    if isinstance(cp, dict) and cp:
        slack_block["channel_prompts"] = {str(k): str(v) for k, v in cp.items()}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# Seeded by setup-hermes.sh from deploy.values.yaml on first install.\n"
        "# Subsequent installer runs preserve this file — edit freely.\n"
    )
    # Always write a file (even with an empty body) so setup-hermes.sh can
    # rely on the path existing for chown/chmod.
    if slack_block:
        body = yaml.safe_dump({"slack": slack_block}, sort_keys=False, default_flow_style=False)
    else:
        body = "# No recognized slack.runtime keys in deploy.values.yaml.\n"
    out_path.write_text(header + body, encoding="utf-8")
    sys.stdout.write(f"wrote: {out_path}\n")
    return 0


def _toml_basic_string(s: str) -> str:
    """Encode ``s`` as a TOML basic string (double-quoted, with escapes).

    json.dumps yields the same escape set TOML basic strings accept for the
    control characters that matter here (``\\"``, ``\\\\``, ``\\n``, ``\\r``,
    ``\\t``, ``\\b``, ``\\f``, ``\\uXXXX``). Non-ASCII characters are allowed
    literally in TOML basic strings, which matches ``ensure_ascii=False``.
    """
    return json.dumps(s, ensure_ascii=False)


def cmd_render_quay_config(args: argparse.Namespace) -> int:
    """Seed <target>/quay/config.toml from the quay.* block.

    Idempotent: refuses to overwrite an existing file unless --force is passed,
    so operator hand-edits to the live config survive subsequent installer
    runs (matches render-runtime-config). Always creates a file when the
    inputs validate, so setup-hermes.sh can chown/chmod the path without
    racing the helper.

    Deliberately omitted from the rendered TOML:
      * data_dir   — set via QUAY_DATA_DIR in the systemd unit env, so the
                     runtime path doesn't get duplicated in two places.
      * repos_root — quay defaults this to ${data_dir}/repos, which lands
                     where we want for free.
      * version    — consumed by setup-hermes.sh to fetch the binary, not by
                     quay at runtime; lives in deploy.values.yaml only.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    if out_path.exists() and not args.force:
        sys.stdout.write(f"preserved: {out_path}\n")
        return 0

    quay = data.get("quay") or {}
    if not isinstance(quay, dict):
        sys.stderr.write("values_helper.py: quay must be a mapping\n")
        return 1

    agent_invocation = quay.get("agent_invocation")
    if not isinstance(agent_invocation, str) or not agent_invocation:
        sys.stderr.write(
            "values_helper.py: quay.agent_invocation is required (non-empty string)\n"
        )
        return 1

    adapters = quay.get("adapters") or {}
    if not isinstance(adapters, dict):
        sys.stderr.write("values_helper.py: quay.adapters must be a mapping\n")
        return 1

    lines = [
        "# Seeded by setup-hermes.sh from deploy.values.yaml on first install.",
        "# Subsequent installer runs preserve this file — edit freely.",
        "#",
        "# data_dir comes from QUAY_DATA_DIR set in the systemd unit; repos_root",
        "# defaults to ${data_dir}/repos. Set them here only to override.",
        "",
        f"agent_invocation = {_toml_basic_string(agent_invocation)}",
    ]

    linear = adapters.get("linear") or {}
    if isinstance(linear, dict) and linear.get("enabled"):
        api_key_env = linear.get("api_key_env") or "LINEAR_API_KEY"
        lines.append("")
        lines.append("[adapters.linear]")
        lines.append("enabled = true")
        lines.append(f"api_key_env = {_toml_basic_string(str(api_key_env))}")

    slack_adapter = adapters.get("slack") or {}
    if isinstance(slack_adapter, dict) and slack_adapter.get("enabled"):
        bot_token_env = slack_adapter.get("bot_token_env") or "SLACK_TOKEN"
        lines.append("")
        lines.append("[adapters.slack]")
        lines.append("enabled = true")
        lines.append(f"bot_token_env = {_toml_basic_string(str(bot_token_env))}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    sys.stdout.write(f"wrote: {out_path}\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--values",
        default=os.environ.get("HERMES_VALUES_FILE", "deploy.values.yaml"),
        help="path to deploy.values.yaml (default: ./deploy.values.yaml or $HERMES_VALUES_FILE)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get", help="print scalar/list value at dotted key")
    p_get.add_argument("key")
    p_get.add_argument("--sep", default=",", help="join separator for lists")
    p_get.set_defaults(func=cmd_get)

    p_man = sub.add_parser("render-manifest", help="render Slack manifest template")
    p_man.add_argument("--in", dest="in_path", required=True, help="path to .tmpl")
    p_man.add_argument("--out", required=True, help="output JSON path")
    p_man.set_defaults(func=cmd_render_manifest)

    p_cfg = sub.add_parser("render-runtime-config",
                           help="seed <target>/config.yaml from slack.runtime")
    p_cfg.add_argument("--out", required=True, help="output config.yaml path")
    p_cfg.add_argument("--force", action="store_true",
                       help="overwrite an existing file")
    p_cfg.set_defaults(func=cmd_render_runtime_config)

    p_quay = sub.add_parser("render-quay-config",
                            help="seed <target>/quay/config.toml from quay.*")
    p_quay.add_argument("--out", required=True, help="output config.toml path")
    p_quay.add_argument("--force", action="store_true",
                        help="overwrite an existing file")
    p_quay.set_defaults(func=cmd_render_quay_config)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
