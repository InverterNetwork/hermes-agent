#!/usr/bin/env bash
# setup-hermes.sh — render the Hermes agent into ~/.hermes/ with the
# rails-vs-state OS permission boundary.
#
# v0: Linux-only. Renders rails root-owned read-only; renders state agent-owned.
# Builds the venv inside the rails so it shares the same read-only protection.
#
# TODO (subsequent passes):
#   - seed config.yaml (or document first-run wizard handoff)
#   - install launchd plist / systemd unit for the gateway
#   - implement --verify (drift detection without mutation)
#   - macOS branch (root:wheel, /Library/LaunchDaemons)
#
# Usage:
#   sudo setup-hermes.sh \
#     --fork    /srv/hermes/repos/hermes-agent \
#     --state   /srv/hermes/repos/hermes-state \
#     --user    hermes \
#     --target  /home/hermes/.hermes
#
# Build deps (build-essential, python3.12-dev, python3.12-venv, libffi-dev,
# rsync) are installed automatically on Debian/Ubuntu hosts. Pass --skip-prep
# if you manage system packages externally.
#
# State repo: pass exactly one of
#   --state <path>     local clone of hermes-state (legacy / CI fixture path)
#   --state-url <url>  HTTPS URL of hermes-state (clones direct from GitHub
#                      using the configured auth)
# The installer clones into $TARGET/state/ on first run; subsequent runs leave
# the existing clone untouched so the agent's uncommitted work is never
# destroyed. Pass --force-state to force a fresh re-clone.
#
# Auth: --auth-method app wires a GitHub App credential helper for state/, so
# `git push` from the agent works without interactive auth. First-run requires
# --app-id, --app-installation-id, and --app-key-path (path to the App's PEM
# private key). Subsequent runs reuse the credentials persisted under
# $TARGET/auth/. --auth-method none (the default) skips auth wiring and is
# what CI uses with the local fixture.

set -euo pipefail

# ---------- defaults ----------
FORK_DIR="/srv/hermes/repos/hermes-agent"
STATE_DIR=""
STATE_URL=""
AGENT_USER="hermes"
TARGET_DIR=""
PYTHON_BIN="python3.12"
SKIP_PREP=0
FORCE_STATE=0
GIT_IDENTITY_NAME="didier"
# Email default is derived per-host so each deployment self-identifies
# in the state repo's commit log (e.g. didier@krustentier).
GIT_IDENTITY_EMAIL=""

# Auth wiring (off by default; CI uses the local fixture without auth).
AUTH_METHOD="none"
APP_ID=""
APP_INSTALLATION_ID=""
APP_KEY_PATH=""
# API base override — only set in CI where we point at a localhost mock.
GH_API_BASE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fork)                  FORK_DIR="$2";             shift 2 ;;
    --state)                 STATE_DIR="$2";            shift 2 ;;
    --state-url)             STATE_URL="$2";            shift 2 ;;
    --user)                  AGENT_USER="$2";           shift 2 ;;
    --target)                TARGET_DIR="$2";           shift 2 ;;
    --python)                PYTHON_BIN="$2";           shift 2 ;;
    --skip-prep)             SKIP_PREP=1;               shift   ;;
    --force-state)           FORCE_STATE=1;             shift   ;;
    --git-identity-email)    GIT_IDENTITY_EMAIL="$2";   shift 2 ;;
    --auth-method)           AUTH_METHOD="$2";          shift 2 ;;
    --app-id)                APP_ID="$2";               shift 2 ;;
    --app-installation-id)   APP_INSTALLATION_ID="$2";  shift 2 ;;
    --app-key-path)          APP_KEY_PATH="$2";         shift 2 ;;
    --gh-api-base)           GH_API_BASE="$2";          shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# State source: exactly one of --state or --state-url.
if [[ -n "$STATE_DIR" && -n "$STATE_URL" ]]; then
  echo "pass exactly one of --state or --state-url" >&2; exit 2
fi
if [[ -z "$STATE_DIR" && -z "$STATE_URL" ]]; then
  echo "must pass --state <path> or --state-url <url>" >&2; exit 2
fi

case "$AUTH_METHOD" in
  none|app) ;;
  *) echo "--auth-method must be 'none' or 'app' (got: $AUTH_METHOD)" >&2; exit 2 ;;
esac

# --state-url requires authenticated cloning, so the auth method must be set.
if [[ -n "$STATE_URL" && "$AUTH_METHOD" == "none" ]]; then
  echo "--state-url requires --auth-method app" >&2; exit 2
fi

if [[ -z "$GIT_IDENTITY_EMAIL" ]]; then
  GIT_IDENTITY_EMAIL="didier@$(hostname -s)"
fi

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root" >&2; exit 1; }
[[ "$(uname -s)" == "Linux" ]] || { echo "v0 is Linux-only" >&2; exit 1; }

# Python interpreter must exist (and meet version) before we can derive its
# X.Y version for the apt prep package names below.
command -v "$PYTHON_BIN" >/dev/null 2>&1 \
  || { echo "$PYTHON_BIN not found on PATH; install it manually first (e.g. apt install python3.12)" >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)' \
  || { echo "$PYTHON_BIN is < 3.11; pyproject.toml requires >=3.11" >&2; exit 1; }

PY_VERSION="$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

# ---------- build-deps prep (Debian/Ubuntu only; idempotent) ----------
# Compilers + python headers are needed by some deps that don't ship a wheel
# for our Python/arch combo. Idempotent: apt-get install is a no-op when the
# packages are already at the desired version. Package names are derived from
# the chosen interpreter so --python python3.11 installs python3.11-{dev,venv}.
if [[ "$SKIP_PREP" -eq 0 ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "==> installing build deps (apt) for python${PY_VERSION}"
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      build-essential "python${PY_VERSION}-dev" "python${PY_VERSION}-venv" libffi-dev rsync >/dev/null
  else
    echo "==> no apt-get detected; skipping automated prep" >&2
    echo "    ensure these are installed manually if anything below fails:" >&2
    echo "    build-essential, python${PY_VERSION}-dev, python${PY_VERSION}-venv, libffi-dev, rsync" >&2
  fi
fi

command -v rsync >/dev/null 2>&1 \
  || { echo "rsync not found on PATH" >&2; exit 1; }

id "$AGENT_USER" >/dev/null 2>&1 \
  || { echo "agent user '$AGENT_USER' does not exist" >&2; exit 1; }

AGENT_HOME="$(getent passwd "$AGENT_USER" | cut -d: -f6)"
TARGET_DIR="${TARGET_DIR:-$AGENT_HOME/.hermes}"

[[ -d "$FORK_DIR/.git" ]] \
  || { echo "fork dir not a git repo: $FORK_DIR" >&2; exit 1; }
if [[ -n "$STATE_DIR" ]]; then
  [[ -d "$STATE_DIR/.git" ]] \
    || { echo "state dir not a git repo: $STATE_DIR (clone hermes-state from GitHub manually first, or pass --state-url)" >&2; exit 1; }
fi

# RUNTIME_VERSION must reflect what's on disk, not just what's committed.
# rsync copies the working tree, so a dirty tree means uncommitted edits will
# be installed; record that with a -dirty suffix so the fingerprint doesn't lie.
FORK_SHA="$(git -C "$FORK_DIR" describe --always --dirty --abbrev=40)"

# State source's origin URL — re-applied to the destination clone so the agent
# pushes back to GitHub, not to the local source path. Empty if source has no
# origin (e.g. a fresh local fixture in CI), in which case the clone keeps its
# default origin pointing at $STATE_DIR.
STATE_ORIGIN_URL=""
if [[ -n "$STATE_DIR" ]]; then
  STATE_ORIGIN_URL="$(git -C "$STATE_DIR" remote get-url origin 2>/dev/null || true)"
fi

echo "==> setup-hermes.sh"
echo "    fork:    $FORK_DIR (@ $FORK_SHA)"
if [[ -n "$STATE_DIR" ]]; then
  echo "    state:   $STATE_DIR${STATE_ORIGIN_URL:+ (origin: $STATE_ORIGIN_URL)}"
else
  echo "    state:   $STATE_URL (direct clone)"
fi
echo "    user:    $AGENT_USER"
echo "    target:  $TARGET_DIR"
echo "    python:  $PYTHON_BIN ($("$PYTHON_BIN" --version 2>&1))"
echo "    git id:  $GIT_IDENTITY_NAME <$GIT_IDENTITY_EMAIL>"
echo "    auth:    $AUTH_METHOD${APP_ID:+ (app=$APP_ID, install=$APP_INSTALLATION_ID)}"
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

# ---------- auth (root-owned dir, group-readable by agent) ----------
# Stored under $TARGET/auth/:
#   github-app.pem  — the GitHub App private key (mode 0640, root:hermes)
#   github-app.env  — config consumed by hermes_github_token.py
#
# auth/ is mode 0750 root:hermes — agent (group member) can read both files
# but can't write them, and the rails (root) own provisioning. The token cache
# lives in $TARGET/cache/ (agent-writable) so this dir stays read-only to it.

AUTH_DIR="$TARGET_DIR/auth"
GH_APP_KEY_DST="$AUTH_DIR/github-app.pem"
GH_APP_ENV="$AUTH_DIR/github-app.env"
TOKEN_HELPER_PY="$TARGET_DIR/hermes-agent/installer/hermes_github_token.py"
VENV_PY="$TARGET_DIR/hermes-agent/venv/bin/python"

if [[ "$AUTH_METHOD" == "app" ]]; then
  echo "==> wiring GitHub App auth at $AUTH_DIR"
  install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"

  if [[ -n "$APP_KEY_PATH" ]]; then
    [[ -f "$APP_KEY_PATH" ]] \
      || { echo "FAIL: --app-key-path file not found: $APP_KEY_PATH" >&2; exit 1; }
    head -1 "$APP_KEY_PATH" | grep -q "BEGIN .*PRIVATE KEY" \
      || { echo "FAIL: $APP_KEY_PATH does not look like a PEM private key" >&2; exit 1; }
    install -o root -g "$AGENT_USER" -m 0640 "$APP_KEY_PATH" "$GH_APP_KEY_DST"
  elif [[ ! -f "$GH_APP_KEY_DST" ]]; then
    echo "FAIL: --auth-method=app but no key staged at $GH_APP_KEY_DST and no --app-key-path given" >&2
    exit 1
  fi

  # Re-assert key perms every run (idempotent; corrects drift).
  chown root:"$AGENT_USER" "$GH_APP_KEY_DST"
  chmod 0640 "$GH_APP_KEY_DST"

  # Persist App identity. APP_ID / APP_INSTALLATION_ID are required on first
  # install; on re-runs we read whatever's in the existing env file unless the
  # operator passed new values.
  if [[ -f "$GH_APP_ENV" ]]; then
    # shellcheck disable=SC1090
    source "$GH_APP_ENV"
    APP_ID="${APP_ID:-${HERMES_GH_APP_ID:-}}"
    APP_INSTALLATION_ID="${APP_INSTALLATION_ID:-${HERMES_GH_INSTALLATION_ID:-}}"
    GH_API_BASE="${GH_API_BASE:-${HERMES_GH_API:-}}"
  fi
  [[ -n "$APP_ID" ]] \
    || { echo "FAIL: --app-id required (or persisted in $GH_APP_ENV)" >&2; exit 1; }
  [[ -n "$APP_INSTALLATION_ID" ]] \
    || { echo "FAIL: --app-installation-id required (or persisted in $GH_APP_ENV)" >&2; exit 1; }

  umask 0027
  cat > "$GH_APP_ENV" <<EOF
HERMES_GH_APP_ID=$APP_ID
HERMES_GH_INSTALLATION_ID=$APP_INSTALLATION_ID
HERMES_GH_APP_KEY=$GH_APP_KEY_DST
${GH_API_BASE:+HERMES_GH_API=$GH_API_BASE}
EOF
  umask 0022
  chown root:"$AGENT_USER" "$GH_APP_ENV"
  chmod 0640 "$GH_APP_ENV"

  [[ -x "$TOKEN_HELPER_PY" ]] \
    || { echo "FAIL: token helper missing at $TOKEN_HELPER_PY" >&2; exit 1; }
  [[ -x "$VENV_PY" ]] \
    || { echo "FAIL: venv python missing at $VENV_PY" >&2; exit 1; }
fi

# ---------- state (agent-owned, writable) ----------
# state/ is a clone of the private hermes-state repo. skills/memories/cron at
# the render-target root are symlinks into state/, so the agent's writes show
# up as git changes that the auto-commit pipeline (future work) can ship.
#
# Re-run policy: if state/ already has a .git, leave it alone — destroying it
# would clobber any uncommitted agent work. --force-state is the opt-in escape
# hatch (re-clones from $STATE_DIR).

STATE_TARGET="$TARGET_DIR/state"

if [[ "$FORCE_STATE" -eq 1 && -e "$STATE_TARGET" ]]; then
  echo "==> --force-state: removing existing $STATE_TARGET"
  rm -rf "$STATE_TARGET"
fi

# Credential helper command — used both for the initial --state-url clone and
# persisted into state/.git/config so subsequent fetches/pushes use the App.
# The leading '!' makes git execute via shell; git appends the action
# (get/store/erase) as the next argv to the helper.
GIT_CRED_HELPER=""
if [[ "$AUTH_METHOD" == "app" ]]; then
  GIT_CRED_HELPER="!$VENV_PY $TOKEN_HELPER_PY credential"
fi

if [[ ! -d "$STATE_TARGET/.git" ]]; then
  if [[ -n "$STATE_DIR" ]]; then
    echo "==> cloning state repo from local source into $STATE_TARGET"
    # Clone as root; chown to agent afterwards so .git/ is fully agent-owned
    # and the agent can run `git fetch` / `git commit` without sudo.
    # --no-hardlinks is required: git's default local-clone optimization
    # hardlinks .git/objects/ from source to dest, which would mean our
    # chown -R on the dest also flips ownership on the source repo's inodes
    # (same inode, two paths).
    git clone --quiet --no-hardlinks "$STATE_DIR" "$STATE_TARGET"
    if [[ -n "$STATE_ORIGIN_URL" ]]; then
      git -C "$STATE_TARGET" remote set-url origin "$STATE_ORIGIN_URL"
    fi
    chown -R "$AGENT_USER:$AGENT_USER" "$STATE_TARGET"
  else
    echo "==> cloning state repo from $STATE_URL into $STATE_TARGET"
    # Clone as the agent user with the credential helper wired in via -c, so
    # the resulting .git/ is agent-owned and auth works on the very first
    # fetch — no chown -R needed.
    install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$STATE_TARGET"
    sudo -u "$AGENT_USER" \
      env HERMES_HOME="$TARGET_DIR" \
      git -c "credential.helper=$GIT_CRED_HELPER" \
        clone --quiet "$STATE_URL" "$STATE_TARGET"
  fi
else
  echo "==> state repo already present at $STATE_TARGET (preserving; pass --force-state to re-clone)"
fi

# Identity is re-applied every run (idempotent) so config drift gets fixed.
sudo -u "$AGENT_USER" git -C "$STATE_TARGET" config user.name  "$GIT_IDENTITY_NAME"
sudo -u "$AGENT_USER" git -C "$STATE_TARGET" config user.email "$GIT_IDENTITY_EMAIL"

# Persist credential helper into state/.git/config for fetch/push. Scoped to
# https://github.com/ so the helper doesn't fire for other hosts.
if [[ -n "$GIT_CRED_HELPER" ]]; then
  sudo -u "$AGENT_USER" git -C "$STATE_TARGET" \
    config credential.https://github.com.helper "$GIT_CRED_HELPER"
fi

# Real agent-owned dirs that don't belong in git (gitignored anyway).
for d in sessions logs cache; do
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$TARGET_DIR/$d"
done

# Symlinks at render-target root so the agent's existing paths
# ($TARGET/skills, $TARGET/memories, $TARGET/cron) keep working but writes
# land inside the git repo. ln -snf is idempotent: replaces existing symlinks
# in place and refuses to descend into a real directory (we error below if so).
echo "==> wiring state symlinks (skills, memories, cron)"
for d in skills memories cron; do
  link="$TARGET_DIR/$d"
  # Pre-clean: if a previous v0 install left real dirs here, they need to go.
  # Safe-to-remove check: no regular files / symlinks / sockets anywhere in
  # the tree — only empty dirs (e.g. v0 left cron/output/ as an empty subdir).
  # If any data file exists, refuse and let the operator decide.
  if [[ -d "$link" && ! -L "$link" ]]; then
    if [[ -z "$(find "$link" -mindepth 1 -not -type d -print -quit)" ]]; then
      rm -rf "$link"
    else
      echo "FAIL: $link contains data from a previous install." >&2
      echo "      Move its contents into $STATE_TARGET/$d/ and retry, or remove it manually." >&2
      exit 1
    fi
  fi
  ln -snf "state/$d" "$link"
  chown -h "$AGENT_USER:$AGENT_USER" "$link"
done

# ---------- post-install assertion ----------

HERMES_BIN="$TARGET_DIR/hermes-agent/venv/bin/hermes"
[[ -x "$HERMES_BIN" ]] \
  || { echo "FAIL: hermes entry point missing or non-executable at $HERMES_BIN" >&2; exit 1; }

# State assertions: the agent must own .git/ and the symlinks must resolve
# into the clone, otherwise the auto-commit pipeline will silently fail later.
[[ -d "$STATE_TARGET/.git" ]] \
  || { echo "FAIL: state clone missing .git at $STATE_TARGET" >&2; exit 1; }
state_git_owner="$(stat -c '%U' "$STATE_TARGET/.git")"
[[ "$state_git_owner" == "$AGENT_USER" ]] \
  || { echo "FAIL: $STATE_TARGET/.git owned by $state_git_owner, expected $AGENT_USER" >&2; exit 1; }
for d in skills memories cron; do
  [[ -L "$TARGET_DIR/$d" ]] \
    || { echo "FAIL: $TARGET_DIR/$d is not a symlink" >&2; exit 1; }
  resolved="$(readlink "$TARGET_DIR/$d")"
  [[ "$resolved" == "state/$d" ]] \
    || { echo "FAIL: $TARGET_DIR/$d -> $resolved (expected state/$d)" >&2; exit 1; }
done
configured_email="$(sudo -u "$AGENT_USER" git -C "$STATE_TARGET" config user.email)"
[[ "$configured_email" == "$GIT_IDENTITY_EMAIL" ]] \
  || { echo "FAIL: state git user.email=$configured_email (expected $GIT_IDENTITY_EMAIL)" >&2; exit 1; }

if [[ "$AUTH_METHOD" == "app" ]]; then
  configured_helper="$(sudo -u "$AGENT_USER" git -C "$STATE_TARGET" \
    config credential.https://github.com.helper 2>/dev/null || true)"
  [[ "$configured_helper" == "$GIT_CRED_HELPER" ]] \
    || { echo "FAIL: state credential helper not configured (got: $configured_helper)" >&2; exit 1; }
  # Smoke-test the helper end-to-end: the agent must be able to read the App
  # key, sign a JWT, hit the API, and emit a credential — without sudo.
  if ! sudo -u "$AGENT_USER" \
        env HERMES_HOME="$TARGET_DIR" \
        "$VENV_PY" "$TOKEN_HELPER_PY" mint >/dev/null 2>&1; then
    echo "FAIL: token helper mint failed for $AGENT_USER" >&2
    sudo -u "$AGENT_USER" env HERMES_HOME="$TARGET_DIR" \
      "$VENV_PY" "$TOKEN_HELPER_PY" mint >&2 || true
    exit 1
  fi
fi

# Read state HEAD as the agent — root would otherwise hit git's safe.directory
# guard ("dubious ownership") because the clone is hermes-owned.
STATE_SHA="$(sudo -u "$AGENT_USER" git -C "$STATE_TARGET" rev-parse --short HEAD 2>/dev/null || echo unknown)"

# ---------- summary ----------

echo
echo "==> summary"
ls -la "$TARGET_DIR" | head -25
echo
echo "RUNTIME_VERSION: $(cat "$TARGET_DIR/RUNTIME_VERSION")"
echo "venv python:     $("$TARGET_DIR/hermes-agent/venv/bin/python" --version 2>&1)"
echo "hermes entry:    $HERMES_BIN"
echo "state HEAD:      $STATE_SHA"
echo "git identity:    $GIT_IDENTITY_NAME <$GIT_IDENTITY_EMAIL>"
echo "done."
