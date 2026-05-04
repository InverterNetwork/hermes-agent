#!/usr/bin/env bash
# setup-hermes.sh — render the Hermes agent into ~/.hermes/ with the
# rails-vs-state OS permission boundary.
#
# v0: Linux-only. Renders rails root-owned read-only; renders state agent-owned.
# Builds the venv inside the rails so it shares the same read-only protection.
#
# TODO (subsequent passes):
#   - clone the state repo + configure repo-local git identity in it
#   - seed config.yaml (or document first-run wizard handoff)
#   - install launchd plist / systemd unit for the gateway
#   - provision + store agent-scoped GitHub tokens
#   - implement --verify (drift detection without mutation)
#   - macOS branch (root:wheel, /Library/LaunchDaemons)
#
# Usage:
#   sudo setup-hermes.sh \
#     --fork    /srv/hermes/repos/hermes-agent \
#     --user    hermes \
#     --target  /home/hermes/.hermes
#
# Build deps (build-essential, python3.12-dev, python3.12-venv, libffi-dev,
# rsync) are installed automatically on Debian/Ubuntu hosts. Pass --skip-prep
# if you manage system packages externally.

set -euo pipefail

# ---------- defaults ----------
FORK_DIR="/srv/hermes/repos/hermes-agent"
AGENT_USER="hermes"
TARGET_DIR=""
PYTHON_BIN="python3.12"
SKIP_PREP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fork)       FORK_DIR="$2";    shift 2 ;;
    --user)       AGENT_USER="$2";  shift 2 ;;
    --target)     TARGET_DIR="$2";  shift 2 ;;
    --python)     PYTHON_BIN="$2";  shift 2 ;;
    --skip-prep)  SKIP_PREP=1;      shift   ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root" >&2; exit 1; }
[[ "$(uname -s)" == "Linux" ]] || { echo "v0 is Linux-only" >&2; exit 1; }

# ---------- build-deps prep (Debian/Ubuntu only; idempotent) ----------
# Compilers + python headers are needed by some deps that don't ship a wheel
# for our Python/arch combo. Idempotent: apt-get install is a no-op when the
# packages are already at the desired version.
if [[ "$SKIP_PREP" -eq 0 ]] && command -v apt-get >/dev/null 2>&1; then
  echo "==> installing build deps (apt)"
  DEBIAN_FRONTEND=noninteractive apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    build-essential python3.12-dev python3.12-venv libffi-dev rsync >/dev/null
fi

command -v "$PYTHON_BIN" >/dev/null 2>&1 \
  || { echo "$PYTHON_BIN not found on PATH (run without --skip-prep, or install manually)" >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)' \
  || { echo "$PYTHON_BIN is < 3.11; pyproject.toml requires >=3.11" >&2; exit 1; }
command -v rsync >/dev/null 2>&1 \
  || { echo "rsync not found on PATH" >&2; exit 1; }

id "$AGENT_USER" >/dev/null 2>&1 \
  || { echo "agent user '$AGENT_USER' does not exist" >&2; exit 1; }

AGENT_HOME="$(getent passwd "$AGENT_USER" | cut -d: -f6)"
TARGET_DIR="${TARGET_DIR:-$AGENT_HOME/.hermes}"

[[ -d "$FORK_DIR/.git" ]] \
  || { echo "fork dir not a git repo: $FORK_DIR" >&2; exit 1; }

# RUNTIME_VERSION must reflect what's on disk, not just what's committed.
# rsync copies the working tree, so a dirty tree means uncommitted edits will
# be installed; record that with a -dirty suffix so the fingerprint doesn't lie.
FORK_SHA="$(git -C "$FORK_DIR" describe --always --dirty --abbrev=40)"

echo "==> setup-hermes.sh"
echo "    fork:    $FORK_DIR (@ $FORK_SHA)"
echo "    user:    $AGENT_USER"
echo "    target:  $TARGET_DIR"
echo "    python:  $PYTHON_BIN ($("$PYTHON_BIN" --version 2>&1))"
echo

# ---------- rails (root-owned, read-only to agent) ----------

echo "==> rendering rails"
install -d -o root -g root -m 755 "$TARGET_DIR"

# rsync upstream source into rails dir.
# Excludes guard against accidentally rendering files that should never leave
# the developer's machine (env files, key material, sockets) — defense in
# depth against the chmod-644 step below, which would otherwise widen perms.
rsync -a --delete \
  --chown=root:root \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='.venv' --exclude='venv' \
  --exclude='node_modules' \
  --exclude='tinker-atropos' \
  --exclude='.env' --exclude='.env.*' \
  --exclude='*.pem' --exclude='*.key' \
  --exclude='id_rsa' --exclude='id_ed25519' --exclude='id_ecdsa' \
  --exclude='*.sock' \
  "$FORK_DIR"/ "$TARGET_DIR/hermes-agent/"

# Normalize perms: dirs 755, files 644, scripts stay executable.
# TODO: this widens any restrictive source perms (e.g., 600 files become 644).
# Acceptable for v0 given the rsync excludes above cover the obvious dangerous
# patterns; a future pass should clamp instead of set, or assert no source
# files have non-default modes before normalizing.
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
  "$PYTHON_BIN" -m venv "$TARGET_DIR/hermes-agent/venv"
fi
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet --upgrade pip wheel setuptools
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet -e "$TARGET_DIR/hermes-agent"
chown -R root:root "$TARGET_DIR/hermes-agent/venv"
find "$TARGET_DIR/hermes-agent/venv" -type d -exec chmod 755 {} +
find "$TARGET_DIR/hermes-agent/venv" -type f ! -perm -u+x -exec chmod 644 {} +
# preserve +x on scripts in venv/bin
find "$TARGET_DIR/hermes-agent/venv/bin" -type f -exec chmod 755 {} +

# ---------- state (agent-owned, writable) ----------
# v0: empty agent-owned dirs only.
# TODO: clone state repo into ~/.hermes/state/ with repo-local git identity
# configured, then symlink skills/memories/cron into it.

echo "==> rendering state dirs (agent-owned)"
for d in skills memories cron cron/output sessions logs cache; do
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$TARGET_DIR/$d"
done

# ---------- post-install assertion ----------

HERMES_BIN="$TARGET_DIR/hermes-agent/venv/bin/hermes"
[[ -x "$HERMES_BIN" ]] \
  || { echo "FAIL: hermes entry point missing or non-executable at $HERMES_BIN" >&2; exit 1; }

# ---------- summary ----------

echo
echo "==> summary"
ls -la "$TARGET_DIR" | head -25
echo
echo "RUNTIME_VERSION: $(cat "$TARGET_DIR/RUNTIME_VERSION")"
echo "venv python:     $("$TARGET_DIR/hermes-agent/venv/bin/python" --version 2>&1)"
echo "hermes entry:    $HERMES_BIN"
echo "done."
