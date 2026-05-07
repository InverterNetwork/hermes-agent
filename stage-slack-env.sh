#!/usr/bin/env bash
# Stage Slack credentials for hermes-gateway. Run as root (or via sudo).
# Idempotent — re-runs preserve any value the operator leaves blank, and
# a no-op rewrite (identical content) skips the gateway restart so
# verifying tokens with Enter-Enter-Enter doesn't drop in-flight requests.
#
# Sibling of stage-quay-env.sh (worker-side tokens, quay.env) and
# stage-hermes-env.sh (gateway adapter tokens, hermes.env). Writes
# auth/slack.env, loaded by hermes-gateway.service via the slack-env.conf
# systemd drop-in.
set -euo pipefail

AGENT_USER="${AGENT_USER:-hermes}"
AUTH_DIR="${AUTH_DIR:-/home/${AGENT_USER}/.hermes/auth}"
SLACK_ENV="${SLACK_ENV:-${AUTH_DIR}/slack.env}"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }

# Read existing values from the env file directly rather than sourcing.
# Sourcing would execute arbitrary shell if the file ever gains command
# substitutions; we only know how to interpret declared keys.
existing_bot=""
existing_app=""
existing_users=""
if [[ -r "$SLACK_ENV" ]]; then
  while IFS='=' read -r key val; do
    case "$key" in
      SLACK_BOT_TOKEN)     existing_bot="$val" ;;
      SLACK_APP_TOKEN)     existing_app="$val" ;;
      SLACK_ALLOWED_USERS) existing_users="$val" ;;
    esac
  done < "$SLACK_ENV"
fi

# Prompt for a value with preserve-on-blank, optional prefix check, and
# silent input for secrets. Empty input keeps the current value (if any);
# a typo on a prefix-checked field re-prompts in place rather than exiting.
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

prompt_value BOT   "SLACK_BOT_TOKEN (xoxb-…)" 1 "$existing_bot"   "xoxb-" 1
prompt_value APP   "SLACK_APP_TOKEN (xapp-…)" 1 "$existing_app"   "xapp-" 1
prompt_value USERS "SLACK_ALLOWED_USERS (comma-separated U-IDs, blank = none)" 0 "$existing_users" "" 0

install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

umask 0137  # files default to 0640
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
{
  echo "SLACK_BOT_TOKEN=${BOT}"
  echo "SLACK_APP_TOKEN=${APP}"
  [[ -n "$USERS" ]] && echo "SLACK_ALLOWED_USERS=${USERS}"
} > "$tmp"

if [[ -f "$SLACK_ENV" ]] && cmp -s "$tmp" "$SLACK_ENV"; then
  echo "✓ $SLACK_ENV unchanged ($(stat -c '%a %U:%G' "$SLACK_ENV")); gateway not restarted"
  exit 0
fi

install -o root -g "$AGENT_USER" -m 0640 "$tmp" "$SLACK_ENV"
echo "✓ wrote $SLACK_ENV ($(stat -c '%a %U:%G' "$SLACK_ENV"))"

if systemctl is-enabled hermes-gateway.service >/dev/null 2>&1; then
  echo "↻ restarting hermes-gateway.service to pick up new env"
  systemctl restart hermes-gateway.service
  systemctl --no-pager --lines=0 status hermes-gateway.service || true
else
  echo "ℹ hermes-gateway.service not enabled yet — re-run setup-hermes.sh next."
fi
