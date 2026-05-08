#!/usr/bin/env python3
"""Read deploy.values.yaml and emit shell-friendly outputs.

setup-hermes.sh shells out to this helper rather than parsing YAML in bash.
Subcommands:

  get <key>                    — print scalar at dotted path (org.name) or a
                                  list joined by `--sep` (default ",").
  render-manifest <in> <out>   — substitute ${slack.app.*} placeholders into
                                  installer/slack-manifest.json.tmpl.
  render-runtime-config <out>  — write ~/.hermes/config.yaml from
                                  slack.runtime.*. Skips if <out> already
                                  exists, so operator hand-edits survive.
  render-gateway-runtime-env <out>
                                — write <auth>/gateway-runtime.env from
                                  values.yaml. Holds non-secret env vars
                                  derived from the values file
                                  (SLACK_ALLOWED_USERS today). Always
                                  rewrites — the file is a reflection of
                                  values.yaml, not operator input.
  merge-config-model <out>     — set <out>'s ``model.provider`` and
                                  ``model.base_url`` from
                                  ``gateway.model_provider`` /
                                  ``gateway.model_base_url`` in
                                  values.yaml. Preserves every other top-
                                  level key. Run on every install so
                                  ``hermes auth add`` config-update
                                  failures (ITRY-1316) can't leave the
                                  provider pin drifted.
  render-quay-config <out>     — write ~/.hermes/quay/config.toml from the
                                  quay.* block. Skips if <out> already exists.
  list-repos                   — emit one TSV line per repos[] entry
                                  (id, url, base_branch, package_manager,
                                  install_cmd) for setup-hermes.sh to iterate.
                                  Code-only entries (no `quay:` block) emit
                                  empty package_manager + install_cmd
                                  fields. Pass `--quay` to filter to
                                  quay-managed entries only.
  parse-repo-list-ids          — read `quay repo list` JSON from stdin, emit
                                  one repo_id per line. Single source of truth
                                  for the field name on the consumer side.
                                  Exits 2 with a stderr message on parse
                                  failure (non-JSON / non-list shape).
  validate-schema              — exit 0 if the repos[] schema is well-formed;
                                  exit 1 with the migration message if legacy
                                  `quay.repos[]` is present, or with the
                                  field-level error otherwise. Used by
                                  setup-hermes.sh --verify so the legacy-key
                                  rejection lives in one place.

Legacy `quay.repos[]` is rejected on every subcommand that touches the repo
list — migrate the block to top-level `repos[]` (with the per-entry
`quay:` sub-block carrying `package_manager` + `install_cmd`).
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


def _reject_legacy_quay_repos(data: dict) -> None:
    # The new schema lives at top-level `repos[]`; silently honouring the
    # old location would let a stale values file install only the quay-
    # managed entries while skipping their code mirrors.
    quay = data.get("quay")
    if isinstance(quay, dict) and "repos" in quay:
        sys.stderr.write(
            "values_helper.py: `quay.repos[]` is no longer supported — move the\n"
            "list to top-level `repos[]` and put `package_manager` /\n"
            "`install_cmd` under each entry's `quay:` sub-block.\n"
        )
        sys.exit(1)


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


def cmd_merge_config_model(args: argparse.Namespace) -> int:
    """Set ``model.provider`` / ``model.base_url`` in ``<out>`` from values.yaml.

    Idempotent: read the existing config.yaml, overwrite the two keys (creating
    the ``model:`` block if missing), preserve every other top-level key.
    Run on every install so the gateway's provider pin matches what
    ``deploy.values.yaml`` declares — independent of whether
    ``hermes auth add`` ran successfully (per ITRY-1316, it can fail
    silently when the seeded config.yaml is root-owned).

    Roundtrips through PyYAML, so YAML-level comments inside config.yaml
    are not preserved (matching ``hermes_cli.config.save_config``'s existing
    behavior). Operator-added *keys* survive — only the model.provider and
    model.base_url scalars are touched.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    gateway = data.get("gateway") or {}
    if not isinstance(gateway, dict):
        sys.stderr.write("values_helper.py: gateway must be a mapping\n")
        return 1
    provider = gateway.get("model_provider")
    base_url = gateway.get("model_base_url")
    if not isinstance(provider, str) or not provider:
        sys.stderr.write(
            "values_helper.py: gateway.model_provider is required (non-empty string)\n"
        )
        return 1
    if base_url is not None and not isinstance(base_url, str):
        sys.stderr.write(
            "values_helper.py: gateway.model_base_url, if set, must be a string\n"
        )
        return 1

    # Load existing config.yaml. Missing-file is an error — render-runtime-config
    # is supposed to have run first; merging into a non-existent config would
    # silently swallow the operator's slack block.
    if not out_path.exists():
        sys.stderr.write(
            f"values_helper.py: {out_path} does not exist; "
            "render-runtime-config must seed it before merge-config-model\n"
        )
        return 1
    try:
        existing = yaml.safe_load(out_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        sys.stderr.write(f"values_helper.py: {out_path} is not valid YAML: {exc}\n")
        return 1
    if not isinstance(existing, dict):
        sys.stderr.write(
            f"values_helper.py: {out_path} top level must be a mapping\n"
        )
        return 1

    model_block = existing.get("model")
    if not isinstance(model_block, dict):
        model_block = {}
    model_block["provider"] = provider
    if base_url:
        model_block["base_url"] = base_url
    else:
        # Empty/unset base_url means "use the provider's default". Drop any
        # stale value so a values.yaml change to remove a CI-only override
        # actually takes effect.
        model_block.pop("base_url", None)
    existing["model"] = model_block

    out_path.write_text(
        yaml.safe_dump(existing, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    sys.stdout.write(f"merged: {out_path} model.provider={provider}\n")
    return 0


_ENV_VALUE_BAD_CHARS = re.compile(r"[\s=\x00-\x1f\x7f]")


def _env_safe(label: str, value: str) -> str:
    """Reject values that would break ``KEY=value`` env-file parsing.

    systemd's ``EnvironmentFile=`` treats each line as ``KEY=VALUE`` with no
    shell quoting, and the gateway parses the same way. Whitespace, ``=``, and
    control chars in a value would either silently truncate or get reinterpreted
    on read, so we fail fast instead of emitting a file the runtime will
    misread.
    """
    if _ENV_VALUE_BAD_CHARS.search(value):
        sys.stderr.write(
            f"values_helper.py: {label}={value!r} contains whitespace, '=', or "
            "control characters — refusing to emit unquoted env-file line\n"
        )
        sys.exit(1)
    return value


def cmd_render_gateway_runtime_env(args: argparse.Namespace) -> int:
    """Write ``<auth>/gateway-runtime.env`` from non-secret values.yaml fields.

    Holds env vars the gateway needs at runtime that are non-secret, values-
    derived config. Today: ``SLACK_ALLOWED_USERS`` from
    ``slack.runtime.allowed_users``. Future migrations land more keys here
    (``LINEAR_TEAM_ITRY`` etc.).

    Always rewritten — values.yaml is the source of truth and the file is a
    reflection. Operator hand-edits live in ``/etc/default/hermes-gateway``
    or in actual secret files (``auth/slack.env``, ``auth/hermes.env``),
    not here.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    lines: list[str] = [
        "# Seeded by setup-hermes.sh from deploy.values.yaml on every run.",
        "# Non-secret env vars derived from values.yaml; do not hand-edit",
        "# (changes are wiped next install). Override via",
        "# /etc/default/hermes-gateway instead.",
    ]

    runtime = (data.get("slack") or {}).get("runtime") or {}
    if isinstance(runtime, dict):
        au = runtime.get("allowed_users")
        if au:
            if not isinstance(au, list):
                sys.stderr.write(
                    "values_helper.py: slack.runtime.allowed_users must be a list\n"
                )
                return 1
            joined = ",".join(_env_safe("slack.runtime.allowed_users[]", str(v)) for v in au)
            lines.append(f"SLACK_ALLOWED_USERS={joined}")

    # linear.teams.<key>: <uuid>  →  LINEAR_TEAM_<KEY>=<uuid>
    # Lets skills (e.g. inverter-linear) reference team UUIDs by env var
    # instead of hardcoding them. Adding a new team to values.yaml is enough
    # to expose it to the gateway's environment on the next install.
    teams = (data.get("linear") or {}).get("teams") or {}
    if not isinstance(teams, dict):
        sys.stderr.write("values_helper.py: linear.teams must be a mapping\n")
        return 1
    for raw_key, raw_val in teams.items():
        key = str(raw_key)
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", key):
            sys.stderr.write(
                f"values_helper.py: linear.teams.{key!r} is not a valid env-var "
                "suffix (must match [A-Za-z][A-Za-z0-9_]*)\n"
            )
            return 1
        if raw_val is None or raw_val == "":
            continue
        env_key = f"LINEAR_TEAM_{key.upper()}"
        env_val = _env_safe(f"linear.teams.{key}", str(raw_val))
        lines.append(f"{env_key}={env_val}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
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


# `id` is interpolated into both the code-mirror path
# ($HERMES_HOME/code/<id>/) and the bare-clone path
# ($HERMES_HOME/quay/repos/<id>.git) by setup-hermes.sh. Reject anything
# that could escape those directories or shape-confuse `quay repo add --id`:
# disallow path separators, leading dots (no `..` / `.hidden`), leading
# dashes (would be parsed as a flag), and any character outside the
# alnum/dot/dash/underscore set.
_REPO_ID_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*$")


def _validate_repo_url(url: str, idx: int) -> str | None:
    """Return error message for invalid github.com URLs; ``None`` for OK ones.

    github.com URLs MUST be `https://github.com/<org>/<repo>` with no
    `.git` suffix — the per-repo `url.insteadOf` rewrite the installer
    wires up matches the values URL as a literal prefix, and a `.git`
    suffix on the values side would either double up (`foo.git.git`)
    or skip the rewrite entirely depending on which side carries it.
    Non-github URLs (CI's `file://...` fixtures, public mirrors elsewhere)
    pass through unchecked — the installer only applies the SSH rewrite
    on github.com URLs anyway.
    """
    # SCP-style URLs (`git@github.com:Org/repo.git`) parse with empty
    # scheme + empty netloc, so `urlparse` would otherwise let them
    # silently pass through to the "non-github" branch. Catch them
    # explicitly: the deploy-key checklist in stage-repo-auth.sh only
    # collects HTTPS github URLs, so accepting an SCP form here would
    # bypass the operator's per-repo key-registration step at clone
    # time.
    if url.startswith(("git@github.com:", "ssh://git@github.com/")):
        return (
            f"repos[{idx}].url={url!r}: github.com URLs must be HTTPS "
            f"(use https://github.com/<org>/<repo>, not the SSH form)"
        )

    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "github.com":
        # Not a github.com HTTPS URL — caller decides whether that's OK
        # in context. For github.com on http we still flag the mismatch;
        # for unrelated hosts/schemes we let the URL through.
        if parsed.netloc == "github.com":
            return (
                f"repos[{idx}].url={url!r}: github.com URLs must be HTTPS "
                f"(got scheme={parsed.scheme!r})"
            )
        return None
    # Reject trailing slashes and `.git` suffixes on the literal value:
    # the install-time url.insteadOf rewrite is a prefix match, and
    # `https://github.com/<org>/<repo>/` would leave a stray `/` for
    # git to append, producing `git@github.com:<org>/<repo>.git/` —
    # broken. Same shape problem for `.git` (would yield `.git.git`).
    if parsed.path != parsed.path.rstrip("/") or parsed.path.endswith(".git"):
        return (
            f"repos[{idx}].url={url!r}: drop the trailing `/` or `.git` — "
            f"the per-repo url.insteadOf rewrite expects a clean "
            f"`https://github.com/<org>/<repo>` prefix"
        )
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) != 2:
        return (
            f"repos[{idx}].url={url!r}: expected `https://github.com/<org>/<repo>`"
        )
    return None


def _iter_repos(data: dict) -> list[dict]:
    # Centralised entry point for repos[] schema validation: rejects legacy
    # `quay.repos[]` first so the operator sees one migration error instead
    # of N validation errors against an empty top-level `repos[]`. Exits
    # the process on schema errors — same posture as `_reject_legacy_quay_repos`.
    _reject_legacy_quay_repos(data)
    repos = data.get("repos")
    if repos is None:
        return []
    if not isinstance(repos, list):
        sys.stderr.write("values_helper.py: repos must be a list\n")
        sys.exit(1)
    return repos


def cmd_list_repos(args: argparse.Namespace) -> int:
    """Emit one TSV line per repos[] entry: <id>\\t<url>\\t<base_branch>\\t<package_manager>\\t<install_cmd>.

    Code-only entries (no `quay:` block) emit empty `package_manager` and
    `install_cmd` fields. With `--quay`, those entries are filtered out so
    setup-hermes.sh can iterate just the quay-managed subset for bare clones
    and `quay repo add` registration.

    Required-field absence is a hard fail (don't silently skip a repo).
    Tabs/newlines inside any field also fail — TSV parsing on the shell
    side would split them incorrectly. The `id` field is shape-checked
    because it lands in a filesystem path on the bash side; github.com
    URLs are shape-checked so the per-repo url.insteadOf rewrite can rely
    on a clean prefix.
    """
    data = _load(Path(args.values))
    entries = _iter_repos(data)

    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: repos[{i}] must be a mapping\n")
            return 1
        # Required scalars: id, url, base_branch.
        row: list[str] = []
        for field in ("id", "url", "base_branch"):
            v = entry.get(field)
            if v is None or v == "":
                sys.stderr.write(
                    f"values_helper.py: repos[{i}].{field} is required\n"
                )
                return 1
            s = str(v)
            if "\t" in s or "\n" in s:
                sys.stderr.write(
                    f"values_helper.py: repos[{i}].{field} contains tab or newline; "
                    "refusing to emit ambiguous TSV\n"
                )
                return 1
            row.append(s)
        if not _REPO_ID_RE.match(row[0]):
            sys.stderr.write(
                f"values_helper.py: repos[{i}].id={row[0]!r} must match "
                f"{_REPO_ID_RE.pattern} (alnum/./-/_, no path separators, no leading dot)\n"
            )
            return 1
        url_err = _validate_repo_url(row[1], i)
        if url_err:
            sys.stderr.write(f"values_helper.py: {url_err}\n")
            return 1

        # Optional `quay:` sub-block.
        quay_block = entry.get("quay")
        if quay_block is not None and not isinstance(quay_block, dict):
            sys.stderr.write(
                f"values_helper.py: repos[{i}].quay must be a mapping when present\n"
            )
            return 1
        pkg = ""
        install = ""
        if isinstance(quay_block, dict):
            for field in ("package_manager", "install_cmd"):
                v = quay_block.get(field)
                if v is None or v == "":
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field} is required "
                        f"when the `quay:` block is present\n"
                    )
                    return 1
                s = str(v)
                if "\t" in s or "\n" in s:
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field} contains "
                        "tab or newline; refusing to emit ambiguous TSV\n"
                    )
                    return 1
                if field == "package_manager":
                    pkg = s
                else:
                    install = s
        elif args.quay:
            # `--quay` filter and this entry has no quay: block — skip.
            continue

        row.extend([pkg, install])
        sys.stdout.write("\t".join(row) + "\n")
    return 0


def cmd_validate_schema(args: argparse.Namespace) -> int:
    # Single source of truth for the repos[] schema check, called from
    # setup-hermes.sh's verify path so the legacy-key migration message
    # lives in exactly one place. Walks every entry to surface validation
    # errors that would otherwise only fire at install time.
    data = _load(Path(args.values))
    entries = _iter_repos(data)
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: repos[{i}] must be a mapping\n")
            return 1
        for field in ("id", "url", "base_branch"):
            if not entry.get(field):
                sys.stderr.write(
                    f"values_helper.py: repos[{i}].{field} is required\n"
                )
                return 1
        if not _REPO_ID_RE.match(str(entry["id"])):
            sys.stderr.write(
                f"values_helper.py: repos[{i}].id={entry['id']!r} must match "
                f"{_REPO_ID_RE.pattern}\n"
            )
            return 1
        url_err = _validate_repo_url(str(entry["url"]), i)
        if url_err:
            sys.stderr.write(f"values_helper.py: {url_err}\n")
            return 1
        quay_block = entry.get("quay")
        if quay_block is not None and not isinstance(quay_block, dict):
            sys.stderr.write(
                f"values_helper.py: repos[{i}].quay must be a mapping when present\n"
            )
            return 1
        if isinstance(quay_block, dict):
            for field in ("package_manager", "install_cmd"):
                if not quay_block.get(field):
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field} is required "
                        f"when the `quay:` block is present\n"
                    )
                    return 1
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

    p_runtime_env = sub.add_parser(
        "render-gateway-runtime-env",
        help="write <auth>/gateway-runtime.env (non-secret env vars from values.yaml)",
    )
    p_runtime_env.add_argument("--out", required=True, help="output env-file path")
    p_runtime_env.set_defaults(func=cmd_render_gateway_runtime_env)

    p_merge_model = sub.add_parser(
        "merge-config-model",
        help="set config.yaml's model.provider/base_url from values.yaml gateway.*",
    )
    p_merge_model.add_argument("--out", required=True, help="path to config.yaml to update")
    p_merge_model.set_defaults(func=cmd_merge_config_model)

    p_quay = sub.add_parser("render-quay-config",
                            help="seed <target>/quay/config.toml from quay.*")
    p_quay.add_argument("--out", required=True, help="output config.toml path")
    p_quay.add_argument("--force", action="store_true",
                        help="overwrite an existing file")
    p_quay.set_defaults(func=cmd_render_quay_config)

    p_list = sub.add_parser("list-repos",
                            help="emit TSV rows for repos[] entries")
    p_list.add_argument("--quay", action="store_true",
                        help="filter to entries with a `quay:` sub-block")
    p_list.set_defaults(func=cmd_list_repos)

    p_parse = sub.add_parser(
        "parse-repo-list-ids",
        help="read `quay repo list` JSON from stdin; emit repo_id per line",
    )
    p_parse.set_defaults(func=cmd_parse_repo_list_ids)

    p_validate = sub.add_parser(
        "validate-schema",
        help="validate repos[] schema (rejects legacy quay.repos[]); silent on success",
    )
    p_validate.set_defaults(func=cmd_validate_schema)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
