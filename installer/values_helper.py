#!/usr/bin/env python3
"""Read deploy.values.yaml and emit shell-friendly outputs.

setup-hermes.sh shells out to this helper rather than parsing YAML in bash.
Seven subcommands:

  get <key>                    — print scalar at dotted path (org.name) or a
                                  list joined by `--sep` (default ",").
  render-manifest <in> <out>   — substitute ${slack.app.*} placeholders into
                                  installer/slack-manifest.json.tmpl.
  render-runtime-config <out>  — write ~/.hermes/config.yaml from
                                  slack.runtime.*. Skips if <out> already
                                  exists, so operator hand-edits survive.
  render-quay-config <out>     — write ~/.hermes/quay/config.toml from the
                                  quay.* block. Skips if <out> already exists.
  list-repos                   — emit one TSV line per quay.repos entry
                                  (id, url, base_branch, package_manager,
                                  install_cmd) for setup-hermes.sh to iterate.
  parse-repo-list-ids          — read `quay repo list` JSON from stdin, emit
                                  one repo_id per line. Single source of truth
                                  for the field name on the consumer side.
                                  Exits 2 with a stderr message on parse
                                  failure (non-JSON / non-list shape).
  list-repo-orgs               — emit one distinct GitHub org per line
                                  (first-seen order) from quay.repos[].url.
                                  Accepts only HTTPS GitHub URLs; rejects
                                  anything else with a clear stderr message.
                                  Empty quay.repos → exit 0, no output.
  list-repo-rewrites           — emit one TSV line per quay.repos entry
                                  (<org>\\t<repo>) for narrow per-repo
                                  url.insteadOf rewrites in
                                  stage-quay-repo-auth.sh. Uses the same
                                  HTTPS-GitHub validation as list-repo-orgs;
                                  strips a trailing `.git` from the repo name.
                                  Distinct (org, repo) pairs, first-seen order.
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
from urllib.parse import urlparse

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

    Adequate for our inputs (CLI command templates, env var names): both
    formats accept the same escapes for the control characters that
    realistically appear (``\\"``, ``\\\\``, ``\\n``, ``\\r``, ``\\t``,
    ``\\b``, ``\\f``), and TOML basic strings allow non-ASCII literals,
    matching ``ensure_ascii=False``. Not a fully general TOML encoder —
    raw U+007F (DEL) would round-trip through ``json.dumps`` unescaped.
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


_REPO_FIELDS = ("id", "url", "base_branch", "package_manager", "install_cmd")

# `id` is interpolated into the bare-clone path
# ($QUAY_DATA_DIR/repos/<id>.git) by setup-hermes.sh. Reject anything
# that could escape that directory or shape-confuse `quay repo add --id`:
# disallow path separators, leading dots (no `..` / `.hidden`), leading
# dashes (would be parsed as a flag), and any character outside the
# alnum/dot/dash/underscore set.
_REPO_ID_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*$")


def cmd_list_repos(args: argparse.Namespace) -> int:
    """Emit one TSV line per quay.repos entry: <id>\\t<url>\\t<base_branch>\\t<package_manager>\\t<install_cmd>.

    setup-hermes.sh iterates this output to provision a bare clone per
    repo and register each with `quay repo add`. Required-field absence is
    a hard fail (don't silently skip a repo). Tabs/newlines inside any
    field also fail — TSV parsing on the shell side would split them
    incorrectly. The `id` field is shape-checked because it lands in a
    filesystem path on the bash side.
    """
    data = _load(Path(args.values))
    quay = data.get("quay") or {}
    if not isinstance(quay, dict):
        sys.stderr.write("values_helper.py: quay must be a mapping\n")
        return 1
    repos = quay.get("repos")
    if repos is None:
        return 0
    if not isinstance(repos, list):
        sys.stderr.write("values_helper.py: quay.repos must be a list\n")
        return 1

    for i, entry in enumerate(repos):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: quay.repos[{i}] must be a mapping\n")
            return 1
        row: list[str] = []
        for field in _REPO_FIELDS:
            v = entry.get(field)
            if v is None or v == "":
                sys.stderr.write(
                    f"values_helper.py: quay.repos[{i}].{field} is required\n"
                )
                return 1
            s = str(v)
            if "\t" in s or "\n" in s:
                sys.stderr.write(
                    f"values_helper.py: quay.repos[{i}].{field} contains tab or newline; "
                    "refusing to emit ambiguous TSV\n"
                )
                return 1
            row.append(s)
        if not _REPO_ID_RE.match(row[0]):
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].id={row[0]!r} must match "
                f"{_REPO_ID_RE.pattern} (alnum/./-/_, no path separators, no leading dot)\n"
            )
            return 1
        sys.stdout.write("\t".join(row) + "\n")
    return 0


def cmd_parse_repo_list_ids(args: argparse.Namespace) -> int:
    """Read `quay repo list` JSON from stdin, emit one repo_id per line.

    Centralises the JSON shape contract — the real binary keys entries by
    `repo_id` (not `id`), and that detail used to live duplicated across
    the install-time and verify-time bash heredocs. Bringing it here means
    one place to update if the format ever drifts, and direct unit-test
    coverage of the parser.

    Exit codes:
      0  parsed cleanly (output may be empty if no repos are registered)
      2  parse failure — non-JSON, or top level isn't a list
    """
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"values_helper.py: quay repo list: not valid JSON: {exc}\n")
        return 2
    if not isinstance(data, list):
        sys.stderr.write(
            f"values_helper.py: quay repo list: expected JSON array, "
            f"got {type(data).__name__}\n"
        )
        return 2
    for r in data:
        if isinstance(r, dict) and r.get("repo_id"):
            sys.stdout.write(str(r["repo_id"]) + "\n")
    return 0


def cmd_list_repo_orgs(args: argparse.Namespace) -> int:
    """Emit one distinct GitHub org per line from quay.repos[].url (first-seen order).

    Only HTTPS GitHub URLs (https://github.com/<org>/<repo>[.git]) are accepted;
    any other URL scheme or host causes a non-zero exit with a clear stderr message.
    Empty quay.repos → exit 0 with no output. Missing quay.repos key → exit 0.
    """
    data = _load(Path(args.values))
    quay = data.get("quay") or {}
    if not isinstance(quay, dict):
        sys.stderr.write("values_helper.py: quay must be a mapping\n")
        return 1
    repos = quay.get("repos")
    if repos is None:
        return 0
    if not isinstance(repos, list):
        sys.stderr.write("values_helper.py: quay.repos must be a list\n")
        return 1

    seen: list[str] = []
    seen_set: set[str] = set()
    for i, entry in enumerate(repos):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: quay.repos[{i}] must be a mapping\n")
            return 1
        url = entry.get("url")
        if not url:
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url is required\n"
            )
            return 1
        parsed = urlparse(str(url))
        if parsed.scheme != "https" or parsed.netloc != "github.com":
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url={url!r} is not a GitHub HTTPS URL "
                f"(expected https://github.com/<org>/<repo>)\n"
            )
            return 1
        # Strip leading/trailing slash, drop trailing `.git` suffix only,
        # take first path segment as org.
        path_parts = parsed.path.strip("/").removesuffix(".git").split("/")
        if len(path_parts) < 2 or not path_parts[0]:
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url={url!r} has no org/repo path\n"
            )
            return 1
        org = path_parts[0]
        if org not in seen_set:
            seen.append(org)
            seen_set.add(org)

    for org in seen:
        sys.stdout.write(org + "\n")
    return 0


def cmd_list_repo_rewrites(args: argparse.Namespace) -> int:
    """Emit one TSV line per quay.repos entry (<org>\\t<repo>, first-seen order).

    stage-quay-repo-auth.sh consumes this to write a narrow per-repo
    `url.git@github.com:<org>/<repo>.insteadOf` rewrite. The org-wide
    form (one rewrite for `<org>/`, no repo segment) was the original
    design but it captured every InverterNetwork repo on the box —
    including hermes-state, which authenticates over HTTPS via the
    GitHub App credential helper. See ITRY-1315.

    Same input contract as list-repo-orgs: HTTPS GitHub URLs only,
    rejects anything else with a clear stderr message. A trailing
    `.git` is stripped from the repo segment so the rewrite value
    matches both `…/<repo>` and `…/<repo>.git` clone URLs (git
    applies insteadOf by longest-prefix match).
    """
    data = _load(Path(args.values))
    quay = data.get("quay") or {}
    if not isinstance(quay, dict):
        sys.stderr.write("values_helper.py: quay must be a mapping\n")
        return 1
    repos = quay.get("repos")
    if repos is None:
        return 0
    if not isinstance(repos, list):
        sys.stderr.write("values_helper.py: quay.repos must be a list\n")
        return 1

    seen: set[tuple[str, str]] = set()
    for i, entry in enumerate(repos):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: quay.repos[{i}] must be a mapping\n")
            return 1
        url = entry.get("url")
        if not url:
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url is required\n"
            )
            return 1
        parsed = urlparse(str(url))
        if parsed.scheme != "https" or parsed.netloc != "github.com":
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url={url!r} is not a GitHub HTTPS URL "
                f"(expected https://github.com/<org>/<repo>)\n"
            )
            return 1
        path_parts = parsed.path.strip("/").removesuffix(".git").split("/")
        if len(path_parts) < 2 or not path_parts[0] or not path_parts[1]:
            sys.stderr.write(
                f"values_helper.py: quay.repos[{i}].url={url!r} has no org/repo path\n"
            )
            return 1
        org, repo = path_parts[0], path_parts[1]
        if (org, repo) in seen:
            continue
        seen.add((org, repo))
        sys.stdout.write(f"{org}\t{repo}\n")
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

    p_list = sub.add_parser("list-repos",
                            help="emit TSV rows for quay.repos entries")
    p_list.set_defaults(func=cmd_list_repos)

    p_parse = sub.add_parser(
        "parse-repo-list-ids",
        help="read `quay repo list` JSON from stdin; emit repo_id per line",
    )
    p_parse.set_defaults(func=cmd_parse_repo_list_ids)

    p_orgs = sub.add_parser(
        "list-repo-orgs",
        help="emit distinct GitHub org names from quay.repos[].url (first-seen order)",
    )
    p_orgs.set_defaults(func=cmd_list_repo_orgs)

    p_rew = sub.add_parser(
        "list-repo-rewrites",
        help="emit <org>\\t<repo> per quay.repos entry for per-repo url.insteadOf rewrites",
    )
    p_rew.set_defaults(func=cmd_list_repo_rewrites)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
