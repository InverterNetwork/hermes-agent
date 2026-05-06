#!/usr/bin/env bash
# Stage quay credentials for the quay-tick.timer worker on krustentier.
# Run as root (or via sudo). Idempotent — re-runs preserve any value the
# operator leaves blank, so rotating one key doesn't require re-typing
# the others.
set -euo pipefail

AGENT_USER="${AGENT_USER:-hermes}"
AUTH_DIR="${AUTH_DIR:-/home/${AGENT_USER}/.hermes/auth}"
QUAY_ENV="${QUAY_ENV:-${AUTH_DIR}/quay.env}"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }

# Read existing values so an empty prompt response means "keep current".
# Parse the file directly rather than sourcing — sourcing would execute
# arbitrary shell if the file ever gains command substitutions, and we
# only know how to interpret three keys anyway.
existing_linear=""
existing_anthropic=""
existing_slack=""
if [[ -r "$QUAY_ENV" ]]; then
  while IFS='=' read -r key val; do
    case "$key" in
      LINEAR_API_KEY)    existing_linear="$val" ;;
      ANTHROPIC_API_KEY) existing_anthropic="$val" ;;
      SLACK_TOKEN)       existing_slack="$val" ;;
    esac
  done < "$QUAY_ENV"
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

prompt_secret LINEAR    "LINEAR_API_KEY (required)"    1 "$existing_linear"
prompt_secret ANTHROPIC "ANTHROPIC_API_KEY (optional)" 0 "$existing_anthropic"
prompt_secret SLACK     "SLACK_TOKEN (optional)"       0 "$existing_slack"

install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

umask 0137  # files default to 0640
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
{
  echo "LINEAR_API_KEY=${LINEAR}"
  [[ -n "$ANTHROPIC" ]] && echo "ANTHROPIC_API_KEY=${ANTHROPIC}"
  [[ -n "$SLACK" ]]     && echo "SLACK_TOKEN=${SLACK}"
} > "$tmp"
install -o root -g "$AGENT_USER" -m 0640 "$tmp" "$QUAY_ENV"

echo "✓ wrote $QUAY_ENV ($(stat -c '%a %U:%G' "$QUAY_ENV"))"

# quay-tick is a timer-driven oneshot — the next tick reads the env file
# fresh via EnvironmentFile=, so there's nothing to restart.
if systemctl is-enabled quay-tick.timer >/dev/null 2>&1; then
  echo "ℹ next quay-tick run will pick up the new env"
else
  echo "ℹ quay-tick.timer not enabled yet — re-run setup-hermes.sh once a quay.version pin is set"
fi
