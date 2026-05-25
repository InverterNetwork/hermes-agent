#!/usr/bin/env bash
# Stage every hermes-agent runtime secret in one pass. Run as root.
#
# Single staging script for all runtime secrets — three files on disk:
#
#   <HERMES_HOME>/auth/slack.env    — gateway, Slack tokens
#   <HERMES_HOME>/auth/hermes.env   — gateway, adapter tokens
#                                       (LINEAR_API_KEY, QUAY_REVIEW_PR_TOKEN)
#   <HERMES_HOME>/auth/ops.env      — gateway, non-prod ops skill creds
#                                       (AWS_* dev/staging/test,
#                                        NEW_RELIC_API_KEY,
#                                        NEW_RELIC_ACCOUNT_ID,
#                                        NEW_RELIC_GRAPHQL_ENDPOINT)
#   <HERMES_HOME>/auth/ops-prod.env — gateway, PROD ops skill creds
#                                       (AWS_PROD_ACCESS_KEY_ID,
#                                        AWS_PROD_SECRET_ACCESS_KEY).
#                                       Separate file so prod is visible
#                                       at the filesystem level.
#   <HERMES_HOME>/auth/quay.env     — quay worker + Admin UI tokens
#
# AUTH_DIR defaults to /home/${AGENT_USER}/.hermes/auth — the same path
# setup-hermes.sh's `--target` defaults to. Non-default installs (e.g.
# setup-hermes.sh --target /opt/hermes) must run this script with
# AUTH_DIR=/opt/hermes/auth or the files land in the wrong place.
#
# Each unique secret is prompted once; LINEAR_API_KEY lands in BOTH
# hermes.env and quay.env from one prompt instead of two. Re-runs preserve
# values left blank, and per-file `cmp -s` short-circuits skip writes
# (and service restarts) when the new content is byte-identical to what's
# on disk.
#
# Non-secret values (SLACK_ALLOWED_USERS, …) are NOT prompted here. They
# live in deploy.values.yaml and are rendered by setup-hermes.sh into
# <AUTH_DIR>/gateway-runtime.env on every install — keeping operator
# prompts focused on actual secrets and avoiding re-typing on rotations.
#
# Detection:
#   - slack.env + hermes.env: always staged (the gateway is the core unit
#     of every hermes-agent install; if it's currently disabled, the env
#     files sit ready for when it's enabled).
#   - quay.env: staged only when /usr/local/bin/quay exists (i.e. the
#     deployment pinned quay.version in deploy.values.yaml). Skipped
#     otherwise — the prompt for ANTHROPIC_API_KEY doesn't fire on
#     Linear-only deployments.
#
# SLACK_TOKEN in quay.env is NOT prompted for. Quay's slack adapter and
# the gateway's Slack transport share one bot identity (same xoxb-), so
# the script copies SLACK_BOT_TOKEN into quay.env's SLACK_TOKEN slot
# automatically. To run quay's slack adapter as a separate bot, edit
# quay.env by hand after staging — left as an explicit edit so a typo
# during prompt entry can't silently bifurcate the two identities.
#
# QUAY_ADMIN_TOKEN in quay.env is generated server-side and preserved on
# re-runs. It protects the localhost quay-serve Admin UI/API service.
set -euo pipefail

AGENT_USER="${AGENT_USER:-hermes}"
AUTH_DIR="${AUTH_DIR:-/home/${AGENT_USER}/.hermes/auth}"
SLACK_ENV="${SLACK_ENV:-${AUTH_DIR}/slack.env}"
HERMES_ENV="${HERMES_ENV:-${AUTH_DIR}/hermes.env}"
OPS_ENV="${OPS_ENV:-${AUTH_DIR}/ops.env}"
OPS_PROD_ENV="${OPS_PROD_ENV:-${AUTH_DIR}/ops-prod.env}"
QUAY_ENV="${QUAY_ENV:-${AUTH_DIR}/quay.env}"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }

# Track every mktemp we create so a SIGINT or `install` failure can't
# leak a plaintext-secret temp file in /tmp. Happy-path code still
# rm -f's after each successful write; the trap is defense-in-depth.
TMPFILES=()
cleanup_tmpfiles() {
  local f
  for f in "${TMPFILES[@]+"${TMPFILES[@]}"}"; do
    [[ -e "$f" ]] && rm -f "$f"
  done
}
trap cleanup_tmpfiles EXIT INT TERM

manage_quay=0
[[ -x /usr/local/bin/quay ]] && manage_quay=1

# Read existing values from each env file directly rather than sourcing.
# Sourcing would execute arbitrary shell if the file ever gains command
# substitutions; we only know how to interpret declared keys.
parse_existing_env() {
  local file="$1" varname="$2" key="$3"
  [[ -r "$file" ]] || return 0
  while IFS='=' read -r k v; do
    if [[ "$k" == "$key" ]]; then
      printf -v "$varname" '%s' "$v"
      return 0
    fi
  done < "$file"
}

existing_slack_bot=""
existing_slack_app=""
existing_linear=""
existing_anthropic=""
existing_quay_review_pr_token=""
existing_quay_admin_token=""
existing_aws_key=""
existing_aws_secret=""
existing_aws_region=""
existing_new_relic=""
existing_new_relic_account=""
existing_new_relic_endpoint=""
existing_aws_prod_key=""
existing_aws_prod_secret=""

parse_existing_env "$SLACK_ENV"  existing_slack_bot   SLACK_BOT_TOKEN
parse_existing_env "$SLACK_ENV"  existing_slack_app   SLACK_APP_TOKEN

# LINEAR_API_KEY can pre-exist in either hermes.env or quay.env (or both
# from a previous staging). Prefer hermes.env's value; fall back to
# quay.env's. They should match — the prompt re-collapses them.
parse_existing_env "$HERMES_ENV" existing_linear     LINEAR_API_KEY
[[ -z "$existing_linear" ]] && parse_existing_env "$QUAY_ENV" existing_linear LINEAR_API_KEY
parse_existing_env "$HERMES_ENV" existing_quay_review_pr_token QUAY_REVIEW_PR_TOKEN

parse_existing_env "$QUAY_ENV"   existing_anthropic  ANTHROPIC_API_KEY
parse_existing_env "$QUAY_ENV"   existing_quay_admin_token QUAY_ADMIN_TOKEN

# Ops env (non-prod AWS + New Relic) and ops-prod env (prod AWS).
parse_existing_env "$OPS_ENV"      existing_aws_key             AWS_ACCESS_KEY_ID
parse_existing_env "$OPS_ENV"      existing_aws_secret          AWS_SECRET_ACCESS_KEY
parse_existing_env "$OPS_ENV"      existing_aws_region          AWS_DEFAULT_REGION
parse_existing_env "$OPS_ENV"      existing_new_relic           NEW_RELIC_API_KEY
parse_existing_env "$OPS_ENV"      existing_new_relic_account   NEW_RELIC_ACCOUNT_ID
parse_existing_env "$OPS_ENV"      existing_new_relic_endpoint  NEW_RELIC_GRAPHQL_ENDPOINT
parse_existing_env "$OPS_PROD_ENV" existing_aws_prod_key        AWS_PROD_ACCESS_KEY_ID
parse_existing_env "$OPS_PROD_ENV" existing_aws_prod_secret     AWS_PROD_SECRET_ACCESS_KEY

# Default the NerdGraph endpoint to the EU region (this deployment's account
# lives there). Operator can type a different URL when prompted; a re-run
# preserves whatever was previously written.
: "${existing_new_relic_endpoint:=https://api.eu.newrelic.com/graphql}"

# Prompt for a value with preserve-on-blank semantics, optional prefix
# check, and silent input for secrets. Empty input keeps the current
# value (if any); a prefix typo re-prompts in place rather than exiting.
prompt_value() {
  local var="$1" label="$2" required="$3" current="$4" prefix="$5" silent="$6"
  local hint="" entered=""
  [[ -n "$current" ]] && hint=" [<keep current>]"
  while true; do
    if [[ "$silent" == "1" ]]; then
      read -rsp "${label}${hint}: " entered; echo
    else
      read -rp "${label}${hint}: " entered
    fi
    if [[ -z "$entered" ]]; then
      if [[ -n "$current" ]]; then
        printf -v "$var" '%s' "$current"
        return 0
      fi
      if [[ "$required" -eq 1 ]]; then
        echo "  ${label} is required" >&2
        continue
      fi
      printf -v "$var" '%s' ""
      return 0
    fi
    if [[ -n "$prefix" && "$entered" != ${prefix}* ]]; then
      echo "  ${label} must start with $prefix" >&2
      continue
    fi
    printf -v "$var" '%s' "$entered"
    return 0
  done
}

# Slack — gateway transport. SLACK_ALLOWED_USERS is not prompted: it lives
# in deploy.values.yaml (slack.runtime.allowed_users) and is rendered by
# setup-hermes.sh into auth/gateway-runtime.env on every install. The list
# is non-secret config; keeping it version-controlled means rotations and
# fresh installs don't re-prompt for member IDs.
prompt_value SLACK_BOT   "SLACK_BOT_TOKEN (xoxb-…)" 1 "$existing_slack_bot"   "xoxb-" 1
prompt_value SLACK_APP   "SLACK_APP_TOKEN (xapp-…)" 1 "$existing_slack_app"   "xapp-" 1

# Linear — required if quay is provisioned (it gates `quay enqueue
# --linear-issue`); optional otherwise (gateway can skip the
# linear-create skill until staged).
linear_required=0
linear_label="LINEAR_API_KEY"
if (( manage_quay )); then
  linear_required=1
  linear_label+=" (required for quay)"
fi
prompt_value LINEAR "$linear_label" "$linear_required" "$existing_linear" "" 1

# Quay PR review ingress — optional; when set, the gateway's API server
# accepts POST /quay/review-pr with this bearer token and shells out to
# local quay review-pr. Keep this out of deploy.values.yaml because it is
# a shared secret used by GitHub Actions.
prompt_value QUAY_REVIEW_PR_TOKEN "QUAY_REVIEW_PR_TOKEN (optional — enables POST /quay/review-pr)" 0 "$existing_quay_review_pr_token" "" 1

# Quay-only secrets, prompted only on quay-provisioned deployments.
# (SLACK_TOKEN is auto-populated from SLACK_BOT_TOKEN below — see header.)
if (( manage_quay )); then
  prompt_value ANTHROPIC  "ANTHROPIC_API_KEY (optional — leave blank if using \"claude login\" subscription auth)" 0 "$existing_anthropic"  "" 1
  if [[ -z "$existing_quay_admin_token" ]]; then
    existing_quay_admin_token="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"
  fi
fi

# Ops skills (aws-lambda-debug, dynamodb-query, new-relic-lambda) — all
# optional. Blank input at the prompt preserves whatever is already on
# disk (matches the SLACK_BOT_TOKEN / LINEAR_API_KEY behaviour above —
# see prompt_value's body). The env files are only written when at least
# one var in their bundle is non-empty, so a Linear-only install leaves
# ops.env / ops-prod.env absent.
#
# Reminder for the operator: the staged AWS keys must come from an IAM
# user/role with read-only Lambda + CloudWatch Logs + CloudFormation +
# DynamoDB policies attached. The skills themselves don't (and can't)
# enforce read-only — IAM is the boundary. Don't paste a long-lived
# AdministratorAccess key into these prompts.
echo
echo "Ops skills (AWS / New Relic) — all optional, blank preserves existing."
prompt_value AWS_KEY             "AWS_ACCESS_KEY_ID (non-prod, read-only IAM user)"     0 "$existing_aws_key"             "" 1
prompt_value AWS_SECRET          "AWS_SECRET_ACCESS_KEY (non-prod)"                     0 "$existing_aws_secret"          "" 1
prompt_value AWS_REGION          "AWS_DEFAULT_REGION"                                   0 "$existing_aws_region"          "" 0
prompt_value NEW_RELIC           "NEW_RELIC_API_KEY (NRAK-…)"                           0 "$existing_new_relic"           "NRAK-" 1
prompt_value NEW_RELIC_ACCOUNT   "NEW_RELIC_ACCOUNT_ID (numeric)"                       0 "$existing_new_relic_account"   "" 0
prompt_value NEW_RELIC_ENDPOINT  "NEW_RELIC_GRAPHQL_ENDPOINT (EU default)"              0 "$existing_new_relic_endpoint"  "" 0
prompt_value AWS_PROD_KEY        "AWS_PROD_ACCESS_KEY_ID (read-only IAM user)"          0 "$existing_aws_prod_key"        "" 1
prompt_value AWS_PROD_SECRET     "AWS_PROD_SECRET_ACCESS_KEY"                           0 "$existing_aws_prod_secret"     "" 1

install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

# Write a single env file. The optional third arg names a caller-side
# variable to set to 1 when the file actually changed; no-op writes
# leave it untouched. Always returns 0 — failures `set -e` out.
slack_changed=0
hermes_changed=0
ops_changed=0
ops_prod_changed=0
quay_changed=0
write_env() {
  local target="$1" content="$2" changed_var="${3:-}"
  local tmp; tmp="$(mktemp)"
  TMPFILES+=("$tmp")
  printf '%s\n' "$content" > "$tmp"
  if [[ -f "$target" ]] && cmp -s "$tmp" "$target"; then
    rm -f "$tmp"
    echo "✓ $target unchanged ($(stat -c '%a %U:%G' "$target"))"
    return 0
  fi
  install -o root -g "$AGENT_USER" -m 0640 "$tmp" "$target"
  rm -f "$tmp"
  echo "✓ wrote $target ($(stat -c '%a %U:%G' "$target"))"
  [[ -n "$changed_var" ]] && printf -v "$changed_var" '1'
  return 0
}

# slack.env — secrets only. Hosts that previously staged a
# SLACK_ALLOWED_USERS line drop it on the next re-run because the new
# content no longer contains it; the gateway picks the list up from
# auth/gateway-runtime.env via the z-runtime-env.conf drop-in instead
# (the drop-in's own header documents why it sorts last).
slack_content="SLACK_BOT_TOKEN=${SLACK_BOT}
SLACK_APP_TOKEN=${SLACK_APP}"
write_env "$SLACK_ENV" "$slack_content" slack_changed

# hermes.env (gateway-side adapter tokens, when set)
hermes_content=""
[[ -n "$LINEAR" ]] && hermes_content+="LINEAR_API_KEY=${LINEAR}"
if [[ -n "$QUAY_REVIEW_PR_TOKEN" ]]; then
  [[ -n "$hermes_content" ]] && hermes_content+="
"
  hermes_content+="QUAY_REVIEW_PR_TOKEN=${QUAY_REVIEW_PR_TOKEN}"
fi
if [[ -n "$hermes_content" ]]; then
  write_env "$HERMES_ENV" "$hermes_content" hermes_changed
fi

# ops.env (gateway-side non-prod AWS + NEW_RELIC_API_KEY). Written only
# when at least one var in the bundle is set, so a Linear-only install
# doesn't leave an empty file on disk.
ops_content=""
[[ -n "$AWS_KEY" ]]    && ops_content+="AWS_ACCESS_KEY_ID=${AWS_KEY}"
if [[ -n "$AWS_SECRET" ]]; then
  [[ -n "$ops_content" ]] && ops_content+="
"
  ops_content+="AWS_SECRET_ACCESS_KEY=${AWS_SECRET}"
fi
if [[ -n "$AWS_REGION" ]]; then
  [[ -n "$ops_content" ]] && ops_content+="
"
  ops_content+="AWS_DEFAULT_REGION=${AWS_REGION}"
fi
if [[ -n "$NEW_RELIC" ]]; then
  [[ -n "$ops_content" ]] && ops_content+="
"
  ops_content+="NEW_RELIC_API_KEY=${NEW_RELIC}"
fi
if [[ -n "$NEW_RELIC_ACCOUNT" ]]; then
  [[ -n "$ops_content" ]] && ops_content+="
"
  ops_content+="NEW_RELIC_ACCOUNT_ID=${NEW_RELIC_ACCOUNT}"
fi
if [[ -n "$NEW_RELIC_ENDPOINT" ]]; then
  [[ -n "$ops_content" ]] && ops_content+="
"
  ops_content+="NEW_RELIC_GRAPHQL_ENDPOINT=${NEW_RELIC_ENDPOINT}"
fi
if [[ -n "$ops_content" ]]; then
  write_env "$OPS_ENV" "$ops_content" ops_changed
fi

# ops-prod.env (gateway-side PROD AWS only). Same write-on-content rule.
# Kept in its own file so prod creds are visible at `ls auth/`.
ops_prod_content=""
[[ -n "$AWS_PROD_KEY" ]] && ops_prod_content+="AWS_PROD_ACCESS_KEY_ID=${AWS_PROD_KEY}"
if [[ -n "$AWS_PROD_SECRET" ]]; then
  [[ -n "$ops_prod_content" ]] && ops_prod_content+="
"
  ops_prod_content+="AWS_PROD_SECRET_ACCESS_KEY=${AWS_PROD_SECRET}"
fi
if [[ -n "$ops_prod_content" ]]; then
  write_env "$OPS_PROD_ENV" "$ops_prod_content" ops_prod_changed
fi

# quay.env — only on quay-provisioned hosts. quay-tick reads its env
# file fresh per timer tick; quay-serve is long-running and gets restarted
# below when this file changes.
if (( manage_quay )); then
  # SLACK_TOKEN mirrors the gateway's SLACK_BOT_TOKEN — same xoxb-, same
  # bot identity, two consumer processes. Always set so flipping
  # adapters.slack.enabled=true in deploy.values.yaml activates the
  # adapter without a re-stage.
  quay_content="LINEAR_API_KEY=${LINEAR}
SLACK_TOKEN=${SLACK_BOT}
QUAY_ADMIN_TOKEN=${existing_quay_admin_token}"
  [[ -n "$ANTHROPIC" ]] && quay_content+="
ANTHROPIC_API_KEY=${ANTHROPIC}"
  write_env "$QUAY_ENV" "$quay_content" quay_changed
fi

# hermes-gateway is long-running and only reads EnvironmentFile= at unit
# start, so a restart is required when any gateway-loaded env file actually
# changed.
if (( slack_changed || hermes_changed || ops_changed || ops_prod_changed )); then
  if systemctl is-enabled hermes-gateway.service >/dev/null 2>&1; then
    echo "↻ restarting hermes-gateway.service to pick up new env"
    systemctl restart hermes-gateway.service
    systemctl --no-pager --lines=0 status hermes-gateway.service || true
  else
    echo "ℹ hermes-gateway.service not enabled yet — re-run setup-hermes.sh."
  fi
fi

if (( quay_changed )); then
  if systemctl is-enabled quay-serve.service >/dev/null 2>&1; then
    echo "↻ restarting quay-serve.service to pick up new env"
    systemctl restart quay-serve.service
    systemctl --no-pager --lines=0 status quay-serve.service || true
  else
    echo "ℹ quay-serve.service not enabled yet — re-run setup-hermes.sh."
  fi
fi
