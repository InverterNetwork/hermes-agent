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
                                  (SLACK_ALLOWED_USERS, LINEAR_TEAM_*).
                                  Always rewrites — the file is a reflection
                                  of values.yaml, not operator input.
  merge-config-model <out>     — set <out>'s ``model.provider`` and
                                  ``model.base_url`` from
                                  ``gateway.model_provider`` /
                                  ``gateway.model_base_url`` in
                                  values.yaml. Preserves every other top-
                                  level key. Run on every install so
                                  ``hermes auth add`` config-update
                                  failures (it can fail silently when
                                  config.yaml is root-owned) can't leave
                                  the provider pin drifted.
  render-quay-config <out>     — write ~/.hermes/quay/config.toml from the
                                  quay.* block. Skips if <out> already exists.
  render-gateway-org-defaults --out <path>
                                — write a compact org-defaults seed file
                                  derived from `repos[]` + `linear.teams`.
                                  Loaded by the gateway agent's prompt
                                  builder into the cached system prompt
                                  (one block at session start, prefix-
                                  cacheable). Tells the agent where the
                                  code mirrors live and which Linear team
                                  owns each repo. Always rewritten — the
                                  file is a reflection of values.yaml.
                                  Empty `repos[]` writes an empty file
                                  (prompt-builder treats empty as "no
                                  seed") so install-time chown/chmod
                                  doesn't race the helper.
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
  parse-task-list-count        — read `quay task list` JSON from stdin, emit
                                  the entry count to stdout. Same exit-code
                                  contract as parse-repo-list-ids.
  validate-schema              — exit 0 if the repos[] schema is well-formed;
                                  exit 1 with the migration message if legacy
                                  `quay.repos[]` is present, or with the
                                  field-level error otherwise. Used by
                                  setup-hermes.sh --verify so the legacy-key
                                  rejection lives in one place. Also walks
                                  per-repo `quay.tags` and deployment-level
                                  `quay.tag_namespaces` so a malformed vocab
                                  block fails at verify-time, not mid-install.
  get-repo-tags <repo_id>      — emit per-repo tag vocab as JSON shaped for
                                  `quay repo apply-tags --from -`. Bare-list
                                  per-repo shape is wrapped as
                                  `{values: [...], required: false}`.
                                  Empty/absent `tags:` block emits
                                  `{"namespaces": {}}` (explicit-clear
                                  payload — strict reconciliation).
  get-deployment-tags          — emit deployment tag vocab as JSON shaped for
                                  `quay tags apply-deployment --from -`.
                                  Reads `quay.tag_namespaces`. Same
                                  explicit-clear semantics on empty/absent.

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


# Header re-emitted on every config.yaml write (seeder *and* model merge) so
# the file's contract matches reality. Operator-added top-level keys
# survive across re-installs, but YAML comments (including this header)
# don't — merge-config-model round-trips through PyYAML on every install,
# which strips comments.
_CONFIG_YAML_HEADER = (
    "# Seeded by setup-hermes.sh from deploy.values.yaml.\n"
    "# Operator-added top-level keys are preserved across re-installs,\n"
    "# but YAML comments are not — model.* is rewritten on every run\n"
    "# (round-trips the file). Prefer deploy.values.yaml for non-secret\n"
    "# config knobs; edit known keys here only when they aren't yet\n"
    "# values-driven.\n"
)


def cmd_render_runtime_config(args: argparse.Namespace) -> int:
    """Seed <target>/config.yaml from slack.runtime.

    Idempotent: refuses to overwrite an existing file unless --force is
    passed. The installer relies on this so operator hand-edits to the live
    config survive subsequent re-runs (only model.* is later rewritten by
    cmd_merge_config_model).
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

    # `slack_triggers:` is a top-level block in values.yaml — flatten it
    # into the rendered `slack.triggers:` so gateway/config.py finds it
    # alongside the other slack.* keys. NB: this seed runs once on first
    # install; subsequent re-reconciliation is done by
    # cmd_merge_slack_triggers on every run.
    triggers_parsed, st_err = _validate_slack_triggers_block(
        data.get("slack_triggers"),
    )
    if st_err is not None:
        sys.stderr.write(f"values_helper.py: {st_err}\n")
        return 1
    if triggers_parsed:
        slack_block["triggers"] = triggers_parsed

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Always write a file (even with an empty body) so setup-hermes.sh can
    # rely on the path existing for chown/chmod.
    if slack_block:
        body = yaml.safe_dump({"slack": slack_block}, sort_keys=False, default_flow_style=False)
    else:
        body = "# No recognized slack.runtime keys in deploy.values.yaml.\n"
    out_path.write_text(_CONFIG_YAML_HEADER + body, encoding="utf-8")
    sys.stdout.write(f"wrote: {out_path}\n")
    return 0


def cmd_merge_config_model(args: argparse.Namespace) -> int:
    """Set ``model.provider`` / ``model.base_url`` in ``<out>`` from values.yaml.

    Idempotent: read the existing config.yaml, overwrite the two keys (creating
    the ``model:`` block if missing), preserve every other top-level key.
    Run on every install so the gateway's provider pin matches what
    ``deploy.values.yaml`` declares — independent of whether
    ``hermes auth add`` ran successfully (it can fail silently when the
    seeded config.yaml is root-owned).

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

    # Re-emit the standard header so the file's intro text doesn't quietly
    # vanish after the first merge (the round-trip strips any prior
    # comments). Keeps the operator-facing wording in sync with reality on
    # every run. Note: comments operators add inside config.yaml are still
    # not preserved — only this fixed leading block is.
    out_path.write_text(
        _CONFIG_YAML_HEADER
        + yaml.safe_dump(existing, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    sys.stdout.write(f"merged: {out_path} model.provider={provider}\n")
    return 0


def cmd_merge_slack_triggers(args: argparse.Namespace) -> int:
    """Reconcile ``slack.triggers`` in config.yaml from top-level ``slack_triggers``.

    render-runtime-config seeds the block only on first install (it
    preserves an existing config.yaml so operator hand-edits survive).
    Without an always-reconciled merge, adding a new trigger to
    deploy.values.yaml and re-running the installer would never reach
    the gateway. This command runs on every install and updates only the
    ``slack.triggers:`` scalar, preserving every other top-level key.

    Empty / absent ``slack_triggers:`` deletes ``slack.triggers:`` from
    config.yaml — declarative reconciliation, matching the rest of the
    values-driven runtime surface.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    triggers_parsed, err = _validate_slack_triggers_block(
        data.get("slack_triggers"),
    )
    if err is not None:
        sys.stderr.write(f"values_helper.py: {err}\n")
        return 1

    if not out_path.exists():
        sys.stderr.write(
            f"values_helper.py: {out_path} does not exist; "
            "render-runtime-config must seed it before merge-slack-triggers\n"
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

    slack_block = existing.get("slack")
    if not isinstance(slack_block, dict):
        slack_block = {}
    if triggers_parsed:
        slack_block["triggers"] = triggers_parsed
    else:
        slack_block.pop("triggers", None)
    if slack_block:
        existing["slack"] = slack_block
    elif "slack" in existing:
        existing["slack"] = {}

    out_path.write_text(
        _CONFIG_YAML_HEADER
        + yaml.safe_dump(existing, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    sys.stdout.write(
        f"merged: {out_path} slack.triggers={len(triggers_parsed or [])}\n"
    )
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
    derived config:

    * ``SLACK_ALLOWED_USERS`` from ``slack.runtime.allowed_users``.
    * ``LINEAR_TEAM_<KEY>`` from ``linear.teams``.

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


# Per-agent invocation tables carry exactly these role keys today; reviewer
# spawn and worker spawn are the only consumers in AST-107's design. Any
# other key is almost certainly a typo (e.g. `workers`) and should fail loud
# at validate time rather than silently drop on the floor.
_AGENT_ROLE_KEYS = ("worker", "reviewer")


def _validate_quay_agents_block(
    block: Any,
) -> tuple[
    dict[str, Any] | None,
    str | None,
]:
    """Validate ``quay.agents`` and return a normalised view (or error string).

    Schema (placeholder until AST-107 freezes — keys here match its design):

        agents:
          worker: <agent_id>       # optional default for worker spawns
          reviewer: <agent_id>     # optional default for reviewer spawns
          invocations:             # required when worker/reviewer reference an id
            <agent_id>:
              worker: "<cmd>"      # both roles optional individually; at
              reviewer: "<cmd>"    # least one must be present per agent

    Absent block → ``(None, None)`` (legacy ``agent_invocation`` path).
    """
    if block is None:
        return None, None
    if not isinstance(block, dict):
        return None, "quay.agents must be a mapping when present"

    allowed_top = {*_AGENT_ROLE_KEYS, "invocations"}
    unknown = sorted(set(block.keys()) - allowed_top)
    if unknown:
        return None, (
            f"quay.agents has unknown key(s): {unknown!r} "
            f"(allowed: {sorted(allowed_top)!r})"
        )

    out: dict[str, Any] = {}
    for role in _AGENT_ROLE_KEYS:
        if role in block:
            v = block[role]
            if not isinstance(v, str) or not v:
                return None, (
                    f"quay.agents.{role} must be a non-empty string "
                    f"(got {type(v).__name__}: {v!r})"
                )
            if not _REPO_ID_RE.match(v):
                return None, (
                    f"quay.agents.{role}={v!r} must match "
                    f"{_REPO_ID_RE.pattern}"
                )
            out[role] = v

    invocations = block.get("invocations")
    if invocations is None:
        invocations = {}
    if not isinstance(invocations, dict):
        return None, "quay.agents.invocations must be a mapping"

    parsed_inv: dict[str, dict[str, str]] = {}
    for agent_id, roles in invocations.items():
        if not isinstance(agent_id, str) or not _REPO_ID_RE.match(agent_id):
            return None, (
                f"quay.agents.invocations.{agent_id!r}: agent id must match "
                f"{_REPO_ID_RE.pattern}"
            )
        if not isinstance(roles, dict):
            return None, (
                f"quay.agents.invocations.{agent_id} must be a mapping "
                f"(got {type(roles).__name__})"
            )
        bad_role = sorted(set(roles.keys()) - set(_AGENT_ROLE_KEYS))
        if bad_role:
            return None, (
                f"quay.agents.invocations.{agent_id} has unknown key(s): "
                f"{bad_role!r} (allowed: {list(_AGENT_ROLE_KEYS)!r})"
            )
        roles_out: dict[str, str] = {}
        for role in _AGENT_ROLE_KEYS:
            if role in roles:
                v = roles[role]
                if not isinstance(v, str) or not v:
                    return None, (
                        f"quay.agents.invocations.{agent_id}.{role} must be a "
                        f"non-empty string (got {type(v).__name__}: {v!r})"
                    )
                roles_out[role] = v
        if not roles_out:
            return None, (
                f"quay.agents.invocations.{agent_id} must define at least one "
                f"of {list(_AGENT_ROLE_KEYS)!r}"
            )
        parsed_inv[agent_id] = roles_out

    # Cross-check: if `worker`/`reviewer` defaults name an agent id, that id
    # must have a matching role entry in `invocations`. Otherwise the rendered
    # config.toml would point at an agent quay can't resolve.
    for role in _AGENT_ROLE_KEYS:
        ref = out.get(role)
        if ref is None:
            continue
        if ref not in parsed_inv:
            return None, (
                f"quay.agents.{role}={ref!r} but no quay.agents.invocations."
                f"{ref} entry is defined"
            )
        if role not in parsed_inv[ref]:
            return None, (
                f"quay.agents.{role}={ref!r} but quay.agents.invocations."
                f"{ref}.{role} is not set"
            )

    out["invocations"] = parsed_inv
    return out, None


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
    """Render <target>/quay/config.toml from the quay.* block.

    Refuses to overwrite an existing file unless --force is passed. The
    installer always passes --force so deploy.values.yaml stays the source of
    truth — every key written here is configs-as-code, with no operator-edit
    domain in the file. Always creates a file when inputs validate, so
    setup-hermes.sh can chown/chmod the path without racing the helper.

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

    agents_block, agents_err = _validate_quay_agents_block(quay.get("agents"))
    if agents_err is not None:
        sys.stderr.write(f"values_helper.py: {agents_err}\n")
        return 1

    adapters = quay.get("adapters") or {}
    if not isinstance(adapters, dict):
        sys.stderr.write("values_helper.py: quay.adapters must be a mapping\n")
        return 1

    lines = [
        "# Rendered by setup-hermes.sh from deploy.values.yaml on every run.",
        "# Edit deploy.values.yaml — local edits here are reconciled away.",
        "#",
        "# data_dir comes from QUAY_DATA_DIR set in the systemd unit; repos_root",
        "# defaults to ${data_dir}/repos.",
        "",
        f"agent_invocation = {_toml_basic_string(agent_invocation)}",
    ]

    # AST-107: legacy `agent_invocation` above continues to render unchanged —
    # quay's back-compat path treats it as the worker default when no
    # `[agents]` block is present.
    if agents_block is not None:
        lines.append("")
        lines.append("[agents]")
        for role in _AGENT_ROLE_KEYS:
            v = agents_block.get(role)
            if isinstance(v, str):
                lines.append(f"{role} = {_toml_basic_string(v)}")
        # Sort invocation keys so re-rendering the same values.yaml produces
        # byte-identical TOML — hermes-sync's drift detector relies on it.
        for agent_id in sorted(agents_block["invocations"]):
            roles = agents_block["invocations"][agent_id]
            lines.append("")
            lines.append(f"[agents.invocations.{agent_id}]")
            for role in _AGENT_ROLE_KEYS:
                if role in roles:
                    lines.append(f"{role} = {_toml_basic_string(roles[role])}")

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

    reviewer = quay.get("reviewer")
    if reviewer is None:
        reviewer = {}
    if not isinstance(reviewer, dict):
        sys.stderr.write("values_helper.py: quay.reviewer must be a mapping\n")
        return 1
    # Strict bool — Python's truthiness coerces "false"/"0" to True, which
    # would silently flip the quay-owned done → pr-review gate the operator
    # was trying to disable.
    for key in ("enabled", "gate_quay_owned_done"):
        if key in reviewer and not isinstance(reviewer[key], bool):
            sys.stderr.write(
                f"values_helper.py: quay.reviewer.{key} must be a bool "
                f"(got {type(reviewer[key]).__name__}: {reviewer[key]!r})\n"
            )
            return 1
    if "login" in reviewer:
        login_val = reviewer["login"]
        if not isinstance(login_val, str) or not login_val:
            sys.stderr.write(
                "values_helper.py: quay.reviewer.login must be a non-empty "
                f"string (got {type(login_val).__name__}: {login_val!r})\n"
            )
            return 1
    if reviewer.get("enabled") is True:
        lines.append("")
        lines.append("[reviewer]")
        lines.append("enabled = true")
        if "gate_quay_owned_done" in reviewer:
            gate = reviewer["gate_quay_owned_done"]
            lines.append(f"gate_quay_owned_done = {'true' if gate else 'false'}")
        login = reviewer.get("login")
        if isinstance(login, str) and login:
            lines.append(f"login = {_toml_basic_string(login)}")

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


# Quay's tag-vocab charsets. Namespace is `[a-z0-9]+` with no dash because
# the validator splits ticket tags on the first `-` (`area-pricing` →
# namespace=`area`, value=`pricing`); a dashed namespace would be
# unaddressable from a ticket tag. Value is the legacy ticket-tag charset.
_TAG_NS_RE = re.compile(r"^[a-z0-9]+$")
_TAG_VALUE_RE = re.compile(r"^[a-z0-9-]+$")


def _validate_repo_tags_block(
    label: str, block: Any,
) -> tuple[dict[str, list[str]] | None, str | None]:
    """Validate a per-repo ``tags:`` sub-block.

    Per-repo shape is the bare-list form:

        tags:
          area: [factory-deployment, solidity-tests]
          risk: [reentrancy]

    Returns ``(parsed, None)`` on success with namespaces sorted and
    each value list de-duplicated and sorted (so the JSON the helper
    emits is deterministic — `setup-hermes.sh --verify` does a string
    compare against ``quay repo get-tags``, which itself sorts).
    Returns ``(None, error_message)`` on validation failure.
    """
    if block is None:
        return {}, None
    if not isinstance(block, dict):
        return None, f"{label} must be a mapping"
    out: dict[str, list[str]] = {}
    for ns_raw, values_raw in block.items():
        ns = str(ns_raw)
        if not _TAG_NS_RE.match(ns):
            return None, (
                f"{label}.{ns!r}: namespace must match {_TAG_NS_RE.pattern} "
                f"(no dashes — the validator splits tags on the first dash)"
            )
        if not isinstance(values_raw, list):
            return None, (
                f"{label}.{ns}: must be a list of values "
                f"(got {type(values_raw).__name__})"
            )
        seen: set[str] = set()
        cleaned: list[str] = []
        for j, v in enumerate(values_raw):
            if not isinstance(v, str):
                return None, (
                    f"{label}.{ns}[{j}]: must be a string "
                    f"(got {type(v).__name__})"
                )
            if not _TAG_VALUE_RE.match(v):
                return None, (
                    f"{label}.{ns}[{j}]={v!r}: must match {_TAG_VALUE_RE.pattern}"
                )
            if v in seen:
                return None, f"{label}.{ns}: duplicate value {v!r}"
            seen.add(v)
            cleaned.append(v)
        cleaned.sort()
        out[ns] = cleaned
    return dict(sorted(out.items())), None


def _validate_repo_issue_tracker_block(
    label: str, block: Any, valid_team_keys: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate a per-repo ``issue_tracker:`` sub-block.

    Shape today supports one adapter — Linear:

        issue_tracker:
          linear:
            team: itry        # must match a key in linear.teams.*

    Returns ``(parsed, None)`` on success, ``(None, error)`` on failure,
    or ``(None, None)`` when the block is absent. The ``team`` value is
    cross-checked against ``valid_team_keys`` so a typo fails install-time
    rather than runtime (when the agent reaches for a missing
    ``LINEAR_TEAM_<KEY>`` env var).
    """
    if block is None:
        return None, None
    if not isinstance(block, dict):
        return None, f"{label} must be a mapping"
    unknown = set(block.keys()) - {"linear"}
    if unknown:
        return None, (
            f"{label}: unknown adapter(s) {sorted(unknown)!r} "
            f"(supported: linear)"
        )
    linear = block.get("linear")
    if linear is None:
        return None, (
            f"{label}.linear is required when `issue_tracker:` is present"
        )
    if not isinstance(linear, dict):
        return None, f"{label}.linear must be a mapping"
    unknown = set(linear.keys()) - {"team"}
    if unknown:
        return None, (
            f"{label}.linear: unknown key(s) {sorted(unknown)!r} "
            f"(supported: team)"
        )
    team = linear.get("team")
    if not isinstance(team, str) or not team:
        return None, f"{label}.linear.team is required (non-empty string)"
    if team not in valid_team_keys:
        return None, (
            f"{label}.linear.team={team!r} does not match any key in "
            f"linear.teams (known: {sorted(valid_team_keys)!r})"
        )
    return {"linear": {"team": team}}, None


_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{6,}$")
_SLACK_USER_ID_RE = re.compile(r"^[UBW][A-Z0-9]{6,}$")
_SKILL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_VALID_ON_OVERFLOW = ("skip", "error")


def _validate_slack_triggers_block(
    block: Any,
    *,
    known_skills: set[str] | None = None,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Validate the top-level ``slack_triggers:`` block.

    Shape::

        slack_triggers:
          - channel_id: C0XXXXXXXXX
            channel_name: feedback         # optional, comment-only
            skill: feedback-intake
            require_top_level: true        # optional, default true
            accept_from_bots: false        # optional, default false
            synthetic_author:              # required iff accept_from_bots: true
              name: New Relic
              slack_id: BNEWRELIC
            rate_limit:                    # optional; defaults: 30/hr, skip
              max_per_hour: 30
              on_overflow: skip            # skip | error
            default_repo: iTRY-monorepo    # optional

    Returns ``(parsed, None)`` on success or ``(None, error)`` on failure.
    A ``None`` / absent block returns ``([], None)``.

    ``known_skills`` is the set of skill names present in the deploy's
    skills root (see ``_collect_known_skills``). When supplied, an unknown
    ``skill`` is a hard fail at install time. When omitted (skills root
    not available yet), the skill-existence check is skipped with
    shape-validation only.
    """
    label = "slack_triggers"
    if block is None:
        return [], None
    if not isinstance(block, list):
        return None, f"{label} must be a list"
    seen_channels: set[str] = set()
    out: list[dict[str, Any]] = []
    for i, entry in enumerate(block):
        item_label = f"{label}[{i}]"
        if not isinstance(entry, dict):
            return None, f"{item_label} must be a mapping"

        channel_id = entry.get("channel_id")
        if not isinstance(channel_id, str) or not _SLACK_CHANNEL_ID_RE.match(channel_id):
            return None, (
                f"{item_label}.channel_id must match {_SLACK_CHANNEL_ID_RE.pattern} "
                f"(uppercase Slack channel id like C0123ABCDEF)"
            )
        if channel_id in seen_channels:
            return None, (
                f"{item_label}.channel_id={channel_id!r}: already bound by an "
                f"earlier entry — one entry per channel"
            )
        seen_channels.add(channel_id)

        skill = entry.get("skill")
        if not isinstance(skill, str) or not _SKILL_NAME_RE.match(skill):
            return None, (
                f"{item_label}.skill must be a non-empty skill name matching "
                f"{_SKILL_NAME_RE.pattern}"
            )
        if known_skills is not None and skill not in known_skills:
            return None, (
                f"{item_label}.skill={skill!r} is not present in the skills "
                f"directory — typo or skill not yet synced"
            )

        for bool_field in ("require_top_level", "accept_from_bots"):
            v = entry.get(bool_field, None)
            if v is not None and not isinstance(v, bool):
                return None, f"{item_label}.{bool_field} must be a bool"

        accept_from_bots = bool(entry.get("accept_from_bots", False))
        synthetic = entry.get("synthetic_author")
        if accept_from_bots and synthetic is None:
            return None, (
                f"{item_label}.synthetic_author is required when "
                f"accept_from_bots is true (quay validate-ticket needs a "
                f"non-empty authors[] for bot-triggered tickets)"
            )
        if synthetic is not None:
            if not isinstance(synthetic, dict):
                return None, f"{item_label}.synthetic_author must be a mapping"
            sa_name = synthetic.get("name")
            sa_id = synthetic.get("slack_id")
            if not isinstance(sa_name, str) or not sa_name.strip():
                return None, (
                    f"{item_label}.synthetic_author.name must be a non-empty string"
                )
            if not isinstance(sa_id, str) or not _SLACK_USER_ID_RE.match(sa_id):
                return None, (
                    f"{item_label}.synthetic_author.slack_id must match "
                    f"{_SLACK_USER_ID_RE.pattern}"
                )

        rl_block = entry.get("rate_limit")
        if rl_block is not None:
            if not isinstance(rl_block, dict):
                return None, f"{item_label}.rate_limit must be a mapping when set"
            mph = rl_block.get("max_per_hour", 30)
            if not isinstance(mph, int) or isinstance(mph, bool) or mph <= 0:
                return None, (
                    f"{item_label}.rate_limit.max_per_hour must be a positive int"
                )
            on_overflow = rl_block.get("on_overflow", "skip")
            if on_overflow not in _VALID_ON_OVERFLOW:
                return None, (
                    f"{item_label}.rate_limit.on_overflow must be one of "
                    f"{list(_VALID_ON_OVERFLOW)!r} (got {on_overflow!r})"
                )

        default_repo = entry.get("default_repo")
        if default_repo is not None and (
            not isinstance(default_repo, str) or not default_repo.strip()
        ):
            return None, (
                f"{item_label}.default_repo must be a non-empty string when set"
            )

        channel_name = entry.get("channel_name")
        if channel_name is not None and not isinstance(channel_name, str):
            return None, f"{item_label}.channel_name must be a string when set"

        out.append(dict(entry))
    return out, None


def _collect_known_skills(skills_root: Path) -> set[str] | None:
    """Return the set of skill directory names under ``skills_root``.

    A skill is a top-level directory containing ``SKILL.md``. Sub-category
    layouts (``skills/<category>/<name>/SKILL.md``) are walked, and the
    skill name is the immediate parent of ``SKILL.md`` — matching the
    convention used by the gateway's skill loader.

    Returns ``None`` if the root doesn't exist; the caller treats that
    as "no skill-existence enforcement available" and shape-validates only.
    """
    if not skills_root.is_dir():
        return None
    names: set[str] = set()
    for skill_md in skills_root.rglob("SKILL.md"):
        try:
            names.add(skill_md.parent.name)
        except Exception:
            continue
    return names


def _validate_tag_namespaces_block(
    block: Any,
) -> tuple[dict[str, dict[str, Any]] | None, str | None]:
    """Validate the deployment-level ``quay.tag_namespaces:`` block.

    Deployment shape is the full envelope:

        tag_namespaces:
          task-type:
            required: true
            values: [bugfix, new-feature]
          risk:
            values: [pii]

    Per-namespace ``required`` defaults to ``false`` when omitted.
    ``required: true`` with an empty/absent ``values:`` is rejected
    locally — quay's apply-deployment rejects it too, but failing at
    schema-validate time gives the operator a single error rather than
    a partial install. Returns ``(parsed, None)`` on success or
    ``(None, error)`` on failure. Sorting matches
    ``_validate_repo_tags_block``.
    """
    label = "quay.tag_namespaces"
    if block is None:
        return {}, None
    if not isinstance(block, dict):
        return None, f"{label} must be a mapping"
    out: dict[str, dict[str, Any]] = {}
    for ns_raw, spec in block.items():
        ns = str(ns_raw)
        if not _TAG_NS_RE.match(ns):
            return None, (
                f"{label}.{ns!r}: namespace must match {_TAG_NS_RE.pattern}"
            )
        if not isinstance(spec, dict):
            return None, (
                f"{label}.{ns}: must be a mapping with `values:` "
                f"(and optional `required:`); got {type(spec).__name__}"
            )
        unknown = set(spec.keys()) - {"required", "values"}
        if unknown:
            return None, (
                f"{label}.{ns}: unknown key(s) {sorted(unknown)!r} "
                f"(allowed: required, values)"
            )
        required_raw = spec.get("required", False)
        if not isinstance(required_raw, bool):
            return None, (
                f"{label}.{ns}.required: must be a bool "
                f"(got {type(required_raw).__name__})"
            )
        values_raw = spec.get("values", [])
        if not isinstance(values_raw, list):
            return None, (
                f"{label}.{ns}.values: must be a list "
                f"(got {type(values_raw).__name__})"
            )
        seen: set[str] = set()
        cleaned: list[str] = []
        for j, v in enumerate(values_raw):
            if not isinstance(v, str):
                return None, (
                    f"{label}.{ns}.values[{j}]: must be a string "
                    f"(got {type(v).__name__})"
                )
            if not _TAG_VALUE_RE.match(v):
                return None, (
                    f"{label}.{ns}.values[{j}]={v!r}: must match {_TAG_VALUE_RE.pattern}"
                )
            if v in seen:
                return None, f"{label}.{ns}.values: duplicate value {v!r}"
            seen.add(v)
            cleaned.append(v)
        cleaned.sort()
        if required_raw and not cleaned:
            return None, (
                f"{label}.{ns}: required=true with no values is rejected by "
                f"`quay tags apply-deployment` (would emit TAG_REQUIRED_MISSING "
                f"on every validation with no satisfying tag possible)"
            )
        out[ns] = {"values": cleaned, "required": required_raw}
    return dict(sorted(out.items())), None


def _emit_apply_payload(namespaces: dict[str, dict[str, Any]]) -> str:
    """Render a ``namespaces`` dict in the JSON shape ``quay … apply-tags
    --from -`` and ``quay tags apply-deployment --from -`` accept.

    ``json.dumps`` with ``sort_keys=True`` is deterministic over insertion
    order, which matters because verify-path drift checks string-compare
    against ``quay … get-tags`` / ``quay tags get-deployment`` (both of
    which themselves sort). An empty namespaces dict round-trips to
    ``{"namespaces": {}}`` — the explicit "clear" payload.
    """
    return json.dumps({"namespaces": namespaces}, sort_keys=True) + "\n"


def cmd_get_repo_tags(args: argparse.Namespace) -> int:
    """Emit per-repo tag vocab as JSON for ``quay repo apply-tags --from -``.

    Reads ``repos[].quay.tags`` for the entry whose id matches
    ``--repo``. Per-repo shape is the bare-list form, so each namespace
    list is wrapped as ``{"values": [...], "required": false}`` to
    match the upstream apply payload — the per-repo path doesn't
    surface a per-repo `required` flag in v0 (deployment-level
    requireds are the load-bearing case; per-repo-only requireds are
    rare enough to defer).

    Exit codes:
      0  emitted JSON (may be ``{"namespaces": {}}`` when the repo has
         no `tags:` block — that's the explicit-clear payload upstream
         expects to revert the repo to unconfigured).
      1  unknown repo id, or schema validation failed.
    """
    data = _load(Path(args.values))
    entries = _iter_repos(data)
    target: dict | None = None
    for entry in entries:
        if isinstance(entry, dict) and str(entry.get("id", "")) == args.repo:
            target = entry
            break
    if target is None:
        sys.stderr.write(
            f"values_helper.py: repos[].id={args.repo!r} not found in "
            f"{args.values}\n"
        )
        return 1
    quay_block = target.get("quay")
    tags_block = quay_block.get("tags") if isinstance(quay_block, dict) else None
    parsed, err = _validate_repo_tags_block(
        f"repos[].quay.tags (id={args.repo})", tags_block,
    )
    if err is not None:
        sys.stderr.write(f"values_helper.py: {err}\n")
        return 1
    namespaces: dict[str, dict[str, Any]] = {
        ns: {"values": values, "required": False}
        for ns, values in (parsed or {}).items()
    }
    sys.stdout.write(_emit_apply_payload(namespaces))
    return 0


def cmd_get_deployment_tags(args: argparse.Namespace) -> int:
    """Emit deployment tag vocab as JSON for ``quay tags apply-deployment``.

    Reads ``quay.tag_namespaces``. Empty / absent block emits
    ``{"namespaces": {}}`` — the explicit-clear payload that reverts
    the deployment vocab. Strict reconciliation: dropping a namespace
    from values flows through to a removal in quay.
    """
    data = _load(Path(args.values))
    quay = data.get("quay") or {}
    if not isinstance(quay, dict):
        sys.stderr.write("values_helper.py: quay must be a mapping\n")
        return 1
    parsed, err = _validate_tag_namespaces_block(quay.get("tag_namespaces"))
    if err is not None:
        sys.stderr.write(f"values_helper.py: {err}\n")
        return 1
    sys.stdout.write(_emit_apply_payload(parsed or {}))
    return 0


def _collect_linear_team_keys(data: dict) -> tuple[set[str] | None, str | None]:
    """Return ``linear.teams`` keys (empty set if absent), or an error string."""
    teams = (data.get("linear") or {}).get("teams")
    if teams is None:
        return set(), None
    if not isinstance(teams, dict):
        return None, "linear.teams must be a mapping"
    return {str(k) for k in teams.keys()}, None


def cmd_render_gateway_org_defaults(args: argparse.Namespace) -> int:
    """Write the gateway agent's org-defaults seed file from values.yaml.

    Emits one compact paragraph the gateway's prompt builder loads into
    the cached system prompt: code-mirror location + per-repo Linear
    team mapping. Always rewritten. Empty ``repos[]`` writes an empty
    file (prompt-builder treats empty as "no seed") so setup-hermes.sh
    can chown/chmod the path without racing the helper.
    """
    data = _load(Path(args.values))
    out_path = Path(args.out)

    entries = _iter_repos(data)
    valid_team_keys, team_err = _collect_linear_team_keys(data)
    if team_err is not None:
        sys.stderr.write(f"values_helper.py: {team_err}\n")
        return 1

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not entries:
        out_path.write_text("", encoding="utf-8")
        sys.stdout.write(f"wrote: {out_path}\n")
        return 0

    repo_lines: list[str] = []
    any_linear = False
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            sys.stderr.write(f"values_helper.py: repos[{i}] must be a mapping\n")
            return 1
        # Inline schema enforcement: setup-hermes.sh runs validate-schema
        # only behind --verify, so the render must not assume it.
        for field in ("id", "url", "base_branch"):
            if not entry.get(field):
                sys.stderr.write(
                    f"values_helper.py: repos[{i}].{field} is required\n"
                )
                return 1
        repo_id = str(entry["id"])
        base_branch = str(entry["base_branch"])
        if not _REPO_ID_RE.match(repo_id):
            sys.stderr.write(
                f"values_helper.py: repos[{i}].id={repo_id!r} must match "
                f"{_REPO_ID_RE.pattern}\n"
            )
            return 1
        url_err = _validate_repo_url(str(entry["url"]), i)
        if url_err:
            sys.stderr.write(f"values_helper.py: {url_err}\n")
            return 1
        tracker, it_err = _validate_repo_issue_tracker_block(
            f"repos[{i}].issue_tracker",
            entry.get("issue_tracker"),
            valid_team_keys,
        )
        if it_err is not None:
            sys.stderr.write(f"values_helper.py: {it_err}\n")
            return 1
        if tracker is not None:
            any_linear = True
            team = tracker["linear"]["team"]
            repo_lines.append(f"{repo_id}({base_branch}, Linear:{team})")
        else:
            repo_lines.append(f"{repo_id}({base_branch})")

    parts: list[str] = [
        "Org defaults (this deployment): "
        "code mirrors live at ~/.hermes/code/<repo>/ (~5min refresh) — "
        "read from those paths, never `git clone` and never `/tmp`.",
    ]
    if any_linear:
        parts.append(
            "Issue tracking is Linear via the `inverter-linear` skill, "
            "never `gh issue`."
        )
    parts.append("Repos: " + ", ".join(repo_lines) + ".")

    out_path.write_text(" ".join(parts) + "\n", encoding="utf-8")
    sys.stdout.write(f"wrote: {out_path}\n")
    return 0


def cmd_validate_schema(args: argparse.Namespace) -> int:
    # Single source of truth for the repos[] schema check, called from
    # setup-hermes.sh's verify path so the legacy-key migration message
    # lives in exactly one place. Walks every entry to surface validation
    # errors that would otherwise only fire at install time.
    data = _load(Path(args.values))
    entries = _iter_repos(data)
    valid_team_keys, team_err = _collect_linear_team_keys(data)
    if team_err is not None:
        sys.stderr.write(f"values_helper.py: {team_err}\n")
        return 1

    quay_top = data.get("quay") if isinstance(data.get("quay"), dict) else None
    agents_parsed, ag_err = _validate_quay_agents_block(
        quay_top.get("agents") if quay_top else None
    )
    if ag_err is not None:
        sys.stderr.write(f"values_helper.py: {ag_err}\n")
        return 1
    # Derive override-target sets once so the per-repo loop can cross-check
    # `agent_worker` / `agent_reviewer` against the global invocations table
    # without re-walking it. `None` means the agents block is absent, so
    # overrides only need shape validation, not cross-checking.
    if agents_parsed is not None:
        inv = agents_parsed["invocations"]
        known_role_targets: dict[str, set[str]] | None = {
            "worker": {aid for aid, r in inv.items() if "worker" in r},
            "reviewer": {aid for aid, r in inv.items() if "reviewer" in r},
        }
    else:
        known_role_targets = None

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
            _, tags_err = _validate_repo_tags_block(
                f"repos[{i}].quay.tags", quay_block.get("tags"),
            )
            if tags_err is not None:
                sys.stderr.write(f"values_helper.py: {tags_err}\n")
                return 1
            # Per-repo agent overrides (AST-107) — schema-validated so typos
            # fail at install time, plus a cross-check that the named agent
            # actually has a matching role entry in quay.agents.invocations.
            for field, role in (
                ("agent_worker", "worker"),
                ("agent_reviewer", "reviewer"),
            ):
                if field not in quay_block:
                    continue
                v = quay_block[field]
                if not isinstance(v, str) or not v:
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field} must be a "
                        f"non-empty string (got {type(v).__name__}: {v!r})\n"
                    )
                    return 1
                if not _REPO_ID_RE.match(v):
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field}={v!r} "
                        f"must match {_REPO_ID_RE.pattern}\n"
                    )
                    return 1
                if (
                    known_role_targets is not None
                    and v not in known_role_targets[role]
                ):
                    sys.stderr.write(
                        f"values_helper.py: repos[{i}].quay.{field}={v!r} "
                        f"but quay.agents.invocations.{v}.{role} is not "
                        f"defined\n"
                    )
                    return 1
        _, it_err = _validate_repo_issue_tracker_block(
            f"repos[{i}].issue_tracker",
            entry.get("issue_tracker"),
            valid_team_keys,
        )
        if it_err is not None:
            sys.stderr.write(f"values_helper.py: {it_err}\n")
            return 1

    if quay_top is not None:
        _, ns_err = _validate_tag_namespaces_block(quay_top.get("tag_namespaces"))
        if ns_err is not None:
            sys.stderr.write(f"values_helper.py: {ns_err}\n")
            return 1

    # slack_triggers[] — top-level. `--skills-root` is optional: when set
    # and the directory exists, an unknown skill is a hard fail. When
    # unset or the directory doesn't exist yet (e.g. first install before
    # skill sync), we shape-validate only and let the gateway warn on
    # missing skills at boot.
    known_skills = None
    if getattr(args, "skills_root", None):
        known_skills = _collect_known_skills(Path(args.skills_root))
    _, st_err = _validate_slack_triggers_block(
        data.get("slack_triggers"), known_skills=known_skills,
    )
    if st_err is not None:
        sys.stderr.write(f"values_helper.py: {st_err}\n")
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


def cmd_parse_task_list_count(args: argparse.Namespace) -> int:
    """Read `quay task list` JSON from stdin, print the entry count.

    Exit codes match parse-repo-list-ids: 0 on a clean parse (count is
    on stdout), 2 on non-JSON or non-list shape. The non-zero exit is
    the signal the bash caller uses to refuse-on-uncertainty rather
    than guessing about a probe-unreachable stale DB.
    """
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"values_helper.py: quay task list: not valid JSON: {exc}\n")
        return 2
    if not isinstance(data, list):
        sys.stderr.write(
            f"values_helper.py: quay task list: expected JSON array, "
            f"got {type(data).__name__}\n"
        )
        return 2
    sys.stdout.write(f"{len(data)}\n")
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

    p_merge_triggers = sub.add_parser(
        "merge-slack-triggers",
        help=(
            "reconcile slack.triggers in config.yaml from top-level "
            "slack_triggers (declarative; absent block clears triggers)"
        ),
    )
    p_merge_triggers.add_argument("--out", required=True, help="path to config.yaml to update")
    p_merge_triggers.set_defaults(func=cmd_merge_slack_triggers)

    p_quay = sub.add_parser("render-quay-config",
                            help="seed <target>/quay/config.toml from quay.*")
    p_quay.add_argument("--out", required=True, help="output config.toml path")
    p_quay.add_argument("--force", action="store_true",
                        help="overwrite an existing file")
    p_quay.set_defaults(func=cmd_render_quay_config)

    p_org_defaults = sub.add_parser(
        "render-gateway-org-defaults",
        help="write <HERMES_HOME>/gateway-org-defaults.md from repos[] + linear.teams",
    )
    p_org_defaults.add_argument("--out", required=True, help="output md path")
    p_org_defaults.set_defaults(func=cmd_render_gateway_org_defaults)

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

    p_task = sub.add_parser(
        "parse-task-list-count",
        help="read `quay task list` JSON from stdin; print entry count",
    )
    p_task.set_defaults(func=cmd_parse_task_list_count)

    p_validate = sub.add_parser(
        "validate-schema",
        help="validate repos[] schema (rejects legacy quay.repos[]); silent on success",
    )
    p_validate.add_argument(
        "--skills-root",
        default=os.environ.get("HERMES_SKILLS_ROOT"),
        help=(
            "directory holding skill subdirs (each with SKILL.md). When set "
            "and present, slack_triggers[].skill must reference an existing "
            "skill — unknown skill names fail loud at validate time. Default: "
            "$HERMES_SKILLS_ROOT or unset (shape-validate only)."
        ),
    )
    p_validate.set_defaults(func=cmd_validate_schema)

    p_repo_tags = sub.add_parser(
        "get-repo-tags",
        help="emit per-repo tag vocab JSON for `quay repo apply-tags --from -`",
    )
    p_repo_tags.add_argument("repo", help="repos[].id to look up")
    p_repo_tags.set_defaults(func=cmd_get_repo_tags)

    p_dep_tags = sub.add_parser(
        "get-deployment-tags",
        help="emit deployment tag vocab JSON for `quay tags apply-deployment --from -`",
    )
    p_dep_tags.set_defaults(func=cmd_get_deployment_tags)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
