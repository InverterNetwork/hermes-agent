#!/usr/bin/env bash
# Stage gateway-adapter credentials for hermes-gateway.service on
# krustentier. Run as root (or via sudo). Idempotent — re-runs preserve
# any value the operator leaves blank, so rotating one key doesn't
# require re-typing the others.
#
# Sibling of stage-quay-env.sh (worker-side tokens, quay.env). Writes
# auth/hermes.env, which the gateway loads via the hermes-env.conf
# systemd drop-in.
set -euo pipefail

AGENT_USER="${AGENT_USER:-hermes}"
AUTH_DIR="${AUTH_DIR:-/home/${AGENT_USER}/.hermes/auth}"
HERMES_ENV="${HERMES_ENV:-${AUTH_DIR}/hermes.env}"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }

# Read existing values so an empty prompt response means "keep current".
# Parse the file directly rather than sourcing — sourcing would execute
# arbitrary shell if the file ever gains command substitutions, and we
# only know how to interpret declared keys anyway.
existing_linear=""
if [[ -r "$HERMES_ENV" ]]; then
  while IFS='=' read -r key val; do
    case "$key" in
      LINEAR_API_KEY) existing_linear="$val" ;;
    esac
  done < "$HERMES_ENV"
fi

# Prompt for a secret. Empty input means "keep current"; if there is no
# current and the field is required, re-prompt instead of writing empty.
# Output goes through the named-var indirect assignment (printf -v), which
# avoids stuffing secrets into a shared global.
prompt_secret() {
  local var="$1" label="$2" required="$3" current="$4"
  local hint="" entered=""
  [[ -n "$current" ]] && hint=" [<keep current>]"
  while true; do
    read -rsp "${label}${hint}: " entered; echo
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
    printf -v "$var" '%s' "$entered"
    return 0
  done
}

prompt_secret LINEAR "LINEAR_API_KEY (required — gates the linear-create skill)" 1 "$existing_linear"

install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

umask 0137  # files default to 0640
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
{
  echo "LINEAR_API_KEY=${LINEAR}"
} > "$tmp"

# Skip the install + systemctl restart when the new content is byte-identical
# to what's already on disk. Otherwise an operator who re-runs to verify a
# value (Enter through every prompt) would drop in-flight Slack requests for
# nothing.
if [[ -f "$HERMES_ENV" ]] && cmp -s "$tmp" "$HERMES_ENV"; then
  echo "✓ $HERMES_ENV unchanged ($(stat -c '%a %U:%G' "$HERMES_ENV")); gateway not restarted"
  exit 0
fi

install -o root -g "$AGENT_USER" -m 0640 "$tmp" "$HERMES_ENV"
echo "✓ wrote $HERMES_ENV ($(stat -c '%a %U:%G' "$HERMES_ENV"))"

if systemctl is-enabled hermes-gateway.service >/dev/null 2>&1; then
  echo "↻ restarting hermes-gateway.service to pick up new env"
  systemctl restart hermes-gateway.service
  systemctl --no-pager --lines=0 status hermes-gateway.service || true
else
  echo "ℹ hermes-gateway.service not enabled yet — re-run setup-hermes.sh once $AUTH_DIR/slack.env exists (SLACK_BOT_TOKEN + SLACK_APP_TOKEN)."
fi
