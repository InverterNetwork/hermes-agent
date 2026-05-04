#!/usr/bin/env bash
# setup-hermes.sh — render the Hermes agent into ~/.hermes/ with the
# rails-vs-state OS permission boundary.
#
# v0: Linux-only. Renders rails root-owned read-only; renders state agent-owned.
# Builds the venv inside the rails so it shares the same read-only protection.
# Does NOT yet handle: state-repo clone + git identity (ITRY-1283),
# config.yaml seeding, launchd/systemd unit install, agent token provisioning,
# --verify mode, idempotency optimizations. Those land in subsequent passes.
#
# Usage:
#   sudo setup-hermes.sh \
#     --fork    /srv/hermes/repos/hermes-agent \
#     --state   /srv/hermes/repos/hermes-state \
#     --user    hermes \
#     --target  /home/hermes/.hermes
#
# Defaults assume a fresh Linux VPS provisioned per ITRY-1281 stand-up.

set -euo pipefail

# ---------- defaults ----------
FORK_DIR="/srv/hermes/repos/hermes-agent"
STATE_DIR="/srv/hermes/repos/hermes-state"
AGENT_USER="hermes"
TARGET_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fork)    FORK_DIR="$2";    shift 2 ;;
    --state)   STATE_DIR="$2";   shift 2 ;;
    --user)    AGENT_USER="$2";  shift 2 ;;
    --target)  TARGET_DIR="$2";  shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root" >&2; exit 1; }
[[ "$(uname -s)" == "Linux" ]] || { echo "v0 is Linux-only" >&2; exit 1; }

id "$AGENT_USER" >/dev/null 2>&1 \
  || { echo "agent user '$AGENT_USER' does not exist" >&2; exit 1; }

AGENT_HOME="$(getent passwd "$AGENT_USER" | cut -d: -f6)"
TARGET_DIR="${TARGET_DIR:-$AGENT_HOME/.hermes}"

[[ -d "$FORK_DIR/.git" ]] \
  || { echo "fork dir not a git repo: $FORK_DIR" >&2; exit 1; }

FORK_SHA="$(git -C "$FORK_DIR" rev-parse HEAD)"

echo "==> setup-hermes.sh"
echo "    fork:    $FORK_DIR (@ $FORK_SHA)"
echo "    state:   $STATE_DIR"
echo "    user:    $AGENT_USER"
echo "    target:  $TARGET_DIR"
echo

# ---------- rails (root-owned, read-only to agent) ----------

echo "==> rendering rails"
install -d -o root -g root -m 755 "$TARGET_DIR"

# rsync upstream source into rails dir
rsync -a --delete \
  --chown=root:root \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='.venv' --exclude='venv' \
  --exclude='node_modules' \
  --exclude='tinker-atropos' \
  "$FORK_DIR"/ "$TARGET_DIR/hermes-agent/"

# bring perms in line: dirs 755, files 644, scripts stay executable
find "$TARGET_DIR/hermes-agent" -type d -exec chmod 755 {} +
find "$TARGET_DIR/hermes-agent" -type f ! -perm -u+x -exec chmod 644 {} +
chown -R root:root "$TARGET_DIR/hermes-agent"

# overlay files at the root of the render target
install -o root -g root -m 644 "$FORK_DIR/SOUL.md" "$TARGET_DIR/SOUL.md"
install -d -o root -g root -m 755 "$TARGET_DIR/hooks"

# config.yaml is left unseeded for v0 — first-run wizard creates it.
# RUNTIME_VERSION records the fork SHA we rendered from.
echo "$FORK_SHA" > "$TARGET_DIR/RUNTIME_VERSION"
chown root:root "$TARGET_DIR/RUNTIME_VERSION"
chmod 644 "$TARGET_DIR/RUNTIME_VERSION"

# ---------- venv (rails-class) ----------

echo "==> building venv at $TARGET_DIR/hermes-agent/venv"
if [[ ! -d "$TARGET_DIR/hermes-agent/venv" ]]; then
  python3 -m venv "$TARGET_DIR/hermes-agent/venv"
fi
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet --upgrade pip wheel setuptools
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet -e "$TARGET_DIR/hermes-agent"
chown -R root:root "$TARGET_DIR/hermes-agent/venv"
find "$TARGET_DIR/hermes-agent/venv" -type d -exec chmod 755 {} +
find "$TARGET_DIR/hermes-agent/venv" -type f ! -perm -u+x -exec chmod 644 {} +
# preserve +x on scripts in venv/bin
find "$TARGET_DIR/hermes-agent/venv/bin" -type f -exec chmod 755 {} +

# ---------- state (agent-owned, writable) ----------
# v0: empty agent-owned dirs. Wiring to state repo + git identity = ITRY-1283.

echo "==> rendering state dirs (agent-owned)"
for d in skills memories cron cron/output sessions logs cache; do
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$TARGET_DIR/$d"
done

# ---------- summary ----------

echo
echo "==> summary"
ls -la "$TARGET_DIR" | head -25
echo
echo "RUNTIME_VERSION: $(cat "$TARGET_DIR/RUNTIME_VERSION")"
echo "venv python:     $("$TARGET_DIR/hermes-agent/venv/bin/python" --version 2>&1)"
echo "hermes entry:    $(ls "$TARGET_DIR/hermes-agent/venv/bin/hermes" 2>/dev/null || echo MISSING)"
echo "done."
