#!/usr/bin/env bash
# Stage every hermes-agent runtime secret in one pass. Run as root.
#
# Replaces the per-unit triplet (stage-slack-env.sh + stage-hermes-env.sh
# + stage-quay-env.sh). Same files end up on disk:
#
#   <HERMES_HOME>/auth/slack.env   — gateway, Slack tokens
#   <HERMES_HOME>/auth/hermes.env  — gateway, adapter tokens (LINEAR_API_KEY)
#   <HERMES_HOME>/auth/quay.env    — quay-tick worker tokens
#
# Each unique secret is prompted once; LINEAR_API_KEY lands in BOTH
# hermes.env and quay.env from one prompt instead of two. Re-runs preserve
# values left blank, and per-file `cmp -s` short-circuits skip writes
# (and the gateway restart) when the new content is byte-identical to
# what's on disk.
#
# Detection:
#   - slack.env + hermes.env: always staged (the gateway is the core unit
#     of every hermes-agent install; if it's currently disabled, the env
#     files sit ready for when it's enabled).
#   - quay.env: staged only when /usr/local/bin/quay exists (i.e. the
#     deployment pinned quay.version in deploy.values.yaml). Skipped
#     otherwise — the prompts for ANTHROPIC_API_KEY / SLACK_TOKEN don't
#     fire on Linear-only deployments.
set -euo pipefail

AGENT_USER="${AGENT_USER:-hermes}"
AUTH_DIR="${AUTH_DIR:-/home/${AGENT_USER}/.hermes/auth}"
SLACK_ENV="${SLACK_ENV:-${AUTH_DIR}/slack.env}"
HERMES_ENV="${HERMES_ENV:-${AUTH_DIR}/hermes.env}"
QUAY_ENV="${QUAY_ENV:-${AUTH_DIR}/quay.env}"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }

manage_quay=0
[[ -x /usr/local/bin/quay ]] && manage_quay=1

# Read existing values from each env file directly rather than sourcing.
# Sourcing would execute arbitrary shell if the file ever gains command
# substitutions; we only know how to interpret declared keys.
parse_existing_env() {
  local file="$1" varname="$2" key="$3"
  [[ -r "$file" ]] || return 0
  while IFS='=' read -r k v; do
    [[ "$k" == "$key" ]] && printf -v "$varname" '%s' "$v"
  done < "$file"
}

existing_slack_bot=""
existing_slack_app=""
existing_slack_users=""
existing_linear=""
existing_anthropic=""
existing_quay_slack=""

parse_existing_env "$SLACK_ENV"  existing_slack_bot   SLACK_BOT_TOKEN
parse_existing_env "$SLACK_ENV"  existing_slack_app   SLACK_APP_TOKEN
parse_existing_env "$SLACK_ENV"  existing_slack_users SLACK_ALLOWED_USERS

# LINEAR_API_KEY can pre-exist in either hermes.env or quay.env (or both
# from a previous staging). Prefer hermes.env's value; fall back to
# quay.env's. They should match — the prompt re-collapses them.
parse_existing_env "$HERMES_ENV" existing_linear     LINEAR_API_KEY
[[ -z "$existing_linear" ]] && parse_existing_env "$QUAY_ENV" existing_linear LINEAR_API_KEY

parse_existing_env "$QUAY_ENV"   existing_anthropic  ANTHROPIC_API_KEY
parse_existing_env "$QUAY_ENV"   existing_quay_slack SLACK_TOKEN

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

# Slack — gateway transport
prompt_value SLACK_BOT   "SLACK_BOT_TOKEN (xoxb-…)" 1 "$existing_slack_bot"   "xoxb-" 1
prompt_value SLACK_APP   "SLACK_APP_TOKEN (xapp-…)" 1 "$existing_slack_app"   "xapp-" 1
prompt_value SLACK_USERS "SLACK_ALLOWED_USERS (comma-separated U-IDs, blank = none)" 0 "$existing_slack_users" "" 0

# Linear — required if quay is provisioned (it gates `quay enqueue
# --linear-issue`); optional otherwise (gateway can skip the
# linear-create skill until staged).
linear_required=0
(( manage_quay )) && linear_required=1
prompt_value LINEAR "LINEAR_API_KEY${manage_quay:+ (required for quay)}" "$linear_required" "$existing_linear" "" 1

# Quay-only secrets, prompted only on quay-provisioned deployments
if (( manage_quay )); then
  prompt_value ANTHROPIC  "ANTHROPIC_API_KEY (optional — leave blank if using \"claude login\" subscription auth)" 0 "$existing_anthropic"  "" 1
  prompt_value QUAY_SLACK "SLACK_TOKEN (optional — quay slack adapter, disabled in v0)"                            0 "$existing_quay_slack" "" 1
fi

install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

# Write a single env file. Returns 0 on no-op (content unchanged) or
# 1 on a real write — caller uses the return code to decide whether
# the owning unit needs a restart.
slack_changed=0
hermes_changed=0
write_env() {
  local target="$1" content="$2"
  local tmp; tmp="$(mktemp)"
  printf '%s\n' "$content" > "$tmp"
  if [[ -f "$target" ]] && cmp -s "$tmp" "$target"; then
    rm -f "$tmp"
    echo "✓ $target unchanged ($(stat -c '%a %U:%G' "$target"))"
    return 0
  fi
  install -o root -g "$AGENT_USER" -m 0640 "$tmp" "$target"
  rm -f "$tmp"
  echo "✓ wrote $target ($(stat -c '%a %U:%G' "$target"))"
  return 1
}

# slack.env
slack_content="SLACK_BOT_TOKEN=${SLACK_BOT}
SLACK_APP_TOKEN=${SLACK_APP}"
[[ -n "$SLACK_USERS" ]] && slack_content+="
SLACK_ALLOWED_USERS=${SLACK_USERS}"
write_env "$SLACK_ENV" "$slack_content" || slack_changed=1

# hermes.env (gateway-side LINEAR_API_KEY, when set)
if [[ -n "$LINEAR" ]]; then
  write_env "$HERMES_ENV" "LINEAR_API_KEY=${LINEAR}" || hermes_changed=1
fi

# quay.env — only on quay-provisioned hosts
if (( manage_quay )); then
  quay_content="LINEAR_API_KEY=${LINEAR}"
  [[ -n "$ANTHROPIC" ]]  && quay_content+="
ANTHROPIC_API_KEY=${ANTHROPIC}"
  [[ -n "$QUAY_SLACK" ]] && quay_content+="
SLACK_TOKEN=${QUAY_SLACK}"
  # quay-tick reads its env file fresh per timer tick; no restart concept.
  write_env "$QUAY_ENV" "$quay_content" || true
fi

# hermes-gateway is long-running and only reads EnvironmentFile= at unit
# start, so a restart is required when slack.env or hermes.env actually
# changed. quay.env changes don't need anything — quay-tick reads fresh.
if (( slack_changed || hermes_changed )); then
  if systemctl is-enabled hermes-gateway.service >/dev/null 2>&1; then
    echo "↻ restarting hermes-gateway.service to pick up new env"
    systemctl restart hermes-gateway.service
    systemctl --no-pager --lines=0 status hermes-gateway.service || true
  else
    echo "ℹ hermes-gateway.service not enabled yet — re-run setup-hermes.sh."
  fi
fi
