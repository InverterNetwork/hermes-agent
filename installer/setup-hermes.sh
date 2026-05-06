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
#   - macOS branch (root:wheel, /Library/LaunchDaemons)
#
# Usage:
#   sudo setup-hermes.sh \
#     --fork    /srv/hermes/repos/hermes-agent \
#     --state   /srv/hermes/repos/hermes-state \
#     --user    hermes \
#     --target  /home/hermes/.hermes
#
# Health check (read-only, no root required):
#   bash setup-hermes.sh --verify \
#     --fork    /srv/hermes/repos/hermes-agent \
#     --target  /home/hermes/.hermes \
#     --user    hermes \
#     --auth-method app
#   # exits 0 if no drift; 1 with [DRIFT] lines on stderr otherwise.
#   # add --quiet to suppress [OK] lines.
#
# Build deps (build-essential, python3.12-dev, python3.12-venv, libffi-dev,
# rsync, python3-yaml) are installed automatically on Debian/Ubuntu hosts.
# Pass --skip-prep if you manage system packages externally.
#
# Org-specific values (identity, Slack manifest fields, runtime allowlist)
# are read from $FORK_DIR/deploy.values.yaml. Override the path with
# --values <file>; see FORK.md for the re-fork flow.
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
VERIFY=0
QUIET=0
# Filled from deploy.values.yaml.org.agent_identity_name; email default is
# <name>@<short-hostname> so each deployment self-identifies in the state
# repo's commit log.
VALUES_FILE=""
GIT_IDENTITY_NAME=""
GIT_IDENTITY_EMAIL=""

# Auth wiring (off by default; CI uses the local fixture without auth).
AUTH_METHOD="none"
APP_ID=""
APP_INSTALLATION_ID=""
APP_KEY_PATH=""
SKIP_AUTH_CHECK=0
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
    --values)                VALUES_FILE="$2";          shift 2 ;;
    --auth-method)           AUTH_METHOD="$2";          shift 2 ;;
    --app-id)                APP_ID="$2";               shift 2 ;;
    --app-installation-id)   APP_INSTALLATION_ID="$2";  shift 2 ;;
    --app-key-path)          APP_KEY_PATH="$2";         shift 2 ;;
    --skip-auth-check)       SKIP_AUTH_CHECK=1;         shift   ;;
    --gh-api-base)           GH_API_BASE="$2";          shift 2 ;;
    --verify)                VERIFY=1;                  shift   ;;
    --quiet)                 QUIET=1;                   shift   ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# ---------- shared validation (install + verify) ----------

case "$AUTH_METHOD" in
  none|app) ;;
  *) echo "--auth-method must be 'none' or 'app' (got: $AUTH_METHOD)" >&2; exit 2 ;;
esac

# Agent user must exist (verify and install both need to know its home).
# getent is Linux-only; fall back to tilde expansion so verify works on dev
# macOS too. Production install still gates on uname=Linux below.
id "$AGENT_USER" >/dev/null 2>&1 \
  || { echo "agent user '$AGENT_USER' does not exist" >&2; exit 1; }
if command -v getent >/dev/null 2>&1; then
  AGENT_HOME="$(getent passwd "$AGENT_USER" | cut -d: -f6)"
else
  AGENT_HOME="$(eval echo "~$AGENT_USER")"
fi
TARGET_DIR="${TARGET_DIR:-$AGENT_HOME/.hermes}"

# ---------- verify mode ----------
# Read-only health check. Inspects the live install for drift, prints
# [OK]/[DRIFT] lines, exits 0 (no drift) or 1 (drift detected). Never writes —
# operators run it before/after deploys to catch ownership, perms, symlink,
# git-config, RUNTIME_VERSION, systemd-unit, and auth-helper drift in one shot.
#
# Each check is independent (no early exit); the closing summary always prints.

V_TOTAL=0
V_DRIFT=0

v_ok() {
  V_TOTAL=$((V_TOTAL + 1))
  [[ "$QUIET" -eq 1 ]] || echo "[OK] $1"
}
v_drift() {
  V_TOTAL=$((V_TOTAL + 1))
  V_DRIFT=$((V_DRIFT + 1))
  echo "[DRIFT] $1: $2" >&2
}

# Cross-platform stat — GNU first, BSD fallback. The mode helper strips the
# leading zero so callers can compare against "750"/"640" string-style.
_owner() { stat -c '%U' "$1" 2>/dev/null || stat -f '%Su' "$1" 2>/dev/null; }
_group() { stat -c '%G' "$1" 2>/dev/null || stat -f '%Sg' "$1" 2>/dev/null; }
_mode()  {
  # GNU `%a` returns full octal perms including setuid/setgid/sticky.
  # BSD `%OLp` would drop the high bits, so concat `%Mp%Lp` to keep them.
  local m
  m=$(stat -c '%a' "$1" 2>/dev/null) || m=$(stat -f '%Mp%Lp' "$1" 2>/dev/null) || return 1
  echo "${m#0}"
}

# Run a git command on a repo as the user that owns its .git/. Sidesteps git's
# safe.directory guard when verify runs as root over an agent-owned repo. When
# we're neither root nor the owner, just fire it directly and let git decide.
_git_as_owner() {
  local repo="$1"; shift
  local owner
  owner="$(_owner "$repo/.git" 2>/dev/null || _owner "$repo" 2>/dev/null || true)"
  if [[ -z "$owner" || "$(id -un)" == "$owner" ]]; then
    git -C "$repo" "$@"
  elif [[ "$(id -u)" -eq 0 ]]; then
    sudo -u "$owner" git -C "$repo" "$@"
  else
    git -C "$repo" "$@"
  fi
}

do_verify() {
  local target="$TARGET_DIR"
  local fork="$FORK_DIR"
  # Internal env-var overrides for the test fixture, which can't actually
  # chown to root. In production these stay at their defaults.
  #
  # Note on agent_group: on Linux, useradd creates a primary group with the
  # same name as the user, so the installer's `-g "$AGENT_USER"` does the
  # right thing for both ownership and group. On macOS dev (where tests run)
  # the user's primary group is `staff`, which is why this is overridable.
  local rails_owner="${HERMES_VERIFY_EXPECT_RAILS_OWNER:-root}"
  local agent_owner="${HERMES_VERIFY_EXPECT_AGENT_OWNER:-$AGENT_USER}"
  local agent_group="${HERMES_VERIFY_EXPECT_AGENT_GROUP:-$agent_owner}"

  # Quay artefacts are gated on `quay.version` in the values file — empty (or
  # values file absent, e.g. in the verify-only test fixture) means quay isn't
  # provisioned on this host, and every quay-* check below silently skips.
  # values_file/values_helper resolution mirrors the install-flow defaults
  # but is tolerant: missing files just disable the gate instead of
  # erroring, since `--verify` is also exercised against fixtures that
  # don't carry a values file.
  local values_file="${VALUES_FILE:-$fork/deploy.values.yaml}"
  local values_helper="${VALUES_HELPER:-$fork/installer/values_helper.py}"
  local quay_version=""
  if [[ -f "$values_file" && -f "$values_helper" ]]; then
    quay_version="$(python3 "$values_helper" --values "$values_file" get quay.version 2>/dev/null || true)"
  fi
  local quay_bin="${HERMES_VERIFY_QUAY_BIN:-/usr/local/bin/quay}"

  echo "==> verify: $target (rails=$rails_owner agent=$agent_owner)"

  if [[ ! -d "$target" ]]; then
    v_drift "target dir" "missing: $target"
    echo "==> verify: $V_TOTAL checks, $V_DRIFT drift"
    return 1
  fi
  v_ok "target dir present: $target"

  # ---- target dir mode + group ownership ----
  # HERMES_HOME must be root:$AGENT_USER mode 02775 (setgid). Drift here
  # silently breaks the gateway: without g+w the runtime can't create
  # gateway.lock / gateway.pid / platforms/, and without setgid+group
  # those files won't inherit the hermes group on creation.
  local tmode towner_g
  tmode="$(_mode "$target")"
  towner_g="$(_owner "$target"):$(_group "$target")"
  if [[ "$tmode" == "2775" && "$towner_g" == "$rails_owner:$agent_group" ]]; then
    v_ok "target dir: $tmode $towner_g"
  else
    v_drift "target dir" "mode=$tmode owner=$towner_g (expected 2775 $rails_owner:$agent_group)"
  fi

  # ---- rails ----
  local rails="$target/hermes-agent"
  if [[ -d "$rails" ]]; then
    local owner
    owner="$(_owner "$rails")"
    if [[ "$owner" == "$rails_owner" ]]; then
      v_ok "rails ownership: $owner"
    else
      v_drift "rails ownership" "expected $rails_owner, got $owner"
    fi
    # Rails must not be writable by group or world — that's the protection
    # boundary that keeps the agent from rewriting its own code paths.
    # Skip symlinks: they're always created with mode lrwxrwxrwx and would
    # trip `-perm -g+w` even though their effective writability comes from
    # the target. The venv ships several (lib64 → lib, bin/python → python3.12).
    local bad
    bad=$(find "$rails" ! -type l \( -perm -g+w -o -perm -o+w \) -print 2>/dev/null | head -3 | tr '\n' ' ')
    if [[ -z "$bad" ]]; then
      v_ok "rails perms: no group/world-writable files"
    else
      v_drift "rails perms" "writable: ${bad% }"
    fi
  else
    v_drift "rails" "missing: $rails"
  fi

  # ---- auth dir + files ----
  # When --auth-method=app the operator declares the install was provisioned
  # with GitHub App auth, so all artefacts MUST be present. Without that
  # contract, deleting auth/ silently passes verify because every check below
  # is gated on existence — see review of #10 for the failure mode.
  local auth_dir="$target/auth"
  local auth_present=0
  local app_auth_expected=0
  [[ -d "$auth_dir" ]] && auth_present=1
  [[ "$AUTH_METHOD" == "app" ]] && app_auth_expected=1

  if [[ "$app_auth_expected" -eq 1 && "$auth_present" -eq 0 ]]; then
    v_drift "auth dir" "missing: $auth_dir (required by --auth-method app)"
  fi

  if [[ "$auth_present" -eq 1 ]]; then
    local mode owner_g
    mode="$(_mode "$auth_dir")"
    owner_g="$(_owner "$auth_dir"):$(_group "$auth_dir")"
    if [[ "$mode" == "750" && "$owner_g" == "$rails_owner:$agent_group" ]]; then
      v_ok "auth dir: $mode $owner_g"
    else
      v_drift "auth dir" "mode=$mode owner=$owner_g (expected 750 $rails_owner:$agent_group)"
    fi
  fi

  # Required artefacts under app auth — list explicitly so a missing file is
  # named in the drift line, instead of relying on the glob below to find it.
  if [[ "$app_auth_expected" -eq 1 ]]; then
    local required
    for required in github-app.env github-app.pem; do
      if [[ ! -f "$auth_dir/$required" ]]; then
        v_drift "auth file $required" "missing (required by --auth-method app)"
      fi
    done
  fi

  if [[ "$auth_present" -eq 1 ]]; then
    local f
    for f in "$auth_dir"/*.pem "$auth_dir"/*.env; do
      [[ -e "$f" ]] || continue
      local fm
      fm="$(_mode "$f")"
      if [[ "$fm" == "640" ]]; then
        v_ok "auth file $(basename "$f"): $fm"
      else
        v_drift "auth file $(basename "$f")" "mode=$fm (expected 640)"
      fi
    done
  fi

  # ---- agent-owned dirs ----
  local d own
  for d in sessions logs cache platforms platforms/pairing; do
    if [[ -d "$target/$d" ]]; then
      own="$(_owner "$target/$d")"
      if [[ "$own" == "$agent_owner" ]]; then
        v_ok "$d ownership: $own"
      else
        v_drift "$d ownership" "expected $agent_owner, got $own"
      fi
    else
      v_drift "$d" "missing: $target/$d"
    fi
  done

  # ---- state symlinks ----
  local link tgt
  for d in skills memories cron; do
    link="$target/$d"
    if [[ -L "$link" ]]; then
      tgt="$(readlink "$link")"
      if [[ "$tgt" == "state/$d" ]]; then
        v_ok "symlink $d -> $tgt"
      else
        v_drift "symlink $d" "points to $tgt (expected state/$d)"
      fi
    else
      v_drift "symlink $d" "not a symlink: $link"
    fi
  done

  # ---- state repo ----
  local state="$target/state"
  if [[ -d "$state/.git" ]]; then
    local sown
    sown="$(_owner "$state/.git")"
    if [[ "$sown" == "$agent_owner" ]]; then
      v_ok "state .git ownership: $sown"
    else
      v_drift "state ownership" "expected $agent_owner, got $sown"
    fi
    local k val
    for k in user.name user.email; do
      # `--local` so we only read this repo's own config — the agent user's
      # global config (if any) is not what the installer wrote, and accepting
      # it would mask drift after a `git config --unset`.
      val="$(_git_as_owner "$state" config --local "$k" 2>/dev/null || true)"
      if [[ -n "$val" ]]; then
        v_ok "state git $k: $val"
      else
        v_drift "state git $k" "not configured"
      fi
    done
    local origin_url
    origin_url="$(_git_as_owner "$state" remote get-url origin 2>/dev/null || true)"
    if [[ -z "$origin_url" ]]; then
      v_drift "state origin" "not configured"
    elif [[ "$origin_url" =~ ^(https?://|ssh://|git://|git@) ]]; then
      v_ok "state origin: $origin_url"
    else
      v_drift "state origin" "filesystem path: $origin_url"
    fi
    # Under --auth-method=app, the credential helper must be persisted into
    # state/.git/config — otherwise hermes-sync pushes silently lose
    # authentication on re-clone or config drift. Tied to AUTH_METHOD rather
    # than auth/ presence so a wiped auth/ doesn't sneak past this check.
    if [[ "$app_auth_expected" -eq 1 ]]; then
      local helper
      helper="$(_git_as_owner "$state" config --local credential.https://github.com.helper 2>/dev/null || true)"
      if [[ -n "$helper" ]]; then
        v_ok "state credential helper configured"
      else
        v_drift "state credential helper" "missing (required by --auth-method app)"
      fi
    fi
  else
    v_drift "state repo" "missing or not a git repo: $state"
  fi

  # ---- RUNTIME_VERSION freshness ----
  if [[ -f "$target/RUNTIME_VERSION" ]]; then
    local rv fork_sha
    rv="$(< "$target/RUNTIME_VERSION")"
    if [[ -d "$fork/.git" ]]; then
      fork_sha="$(_git_as_owner "$fork" describe --always --dirty --abbrev=40 2>/dev/null || true)"
      if [[ -n "$fork_sha" && "$rv" == "$fork_sha" ]]; then
        v_ok "RUNTIME_VERSION matches fork HEAD: $rv"
      else
        v_drift "RUNTIME_VERSION" "$rv != fork HEAD ${fork_sha:-<unavailable>}"
      fi
    else
      # Fork unavailable — record what's installed but don't claim it's fresh.
      v_ok "RUNTIME_VERSION: $rv (fork unavailable for comparison)"
    fi
  else
    v_drift "RUNTIME_VERSION" "missing at $target/RUNTIME_VERSION"
  fi

  # ---- quay artefacts (gated on quay.version) ----
  # Mirror the install-time provisioning: binary in rails-class
  # /usr/local/bin, agent-owned data dir + config.toml, one bare clone
  # per quay.repos entry, and each id registered with the quay CLI. All
  # checks no-op when quay.version is empty.
  if [[ -n "$quay_version" ]]; then
    local quay_dir="$target/quay"

    # Binary: rails-class root:root 0755 + version pin must match the
    # value the values file claims is deployed. The version compare is a
    # substring check — `quay --version` may print extra context (commit,
    # build date) and we just need to assert the tag isn't lying.
    if [[ ! -x "$quay_bin" ]]; then
      v_drift "quay binary" "missing or non-executable: $quay_bin"
    else
      local qmode qowner
      qmode="$(_mode "$quay_bin")"
      qowner="$(_owner "$quay_bin")"
      # Owner (not owner:group) — matches the rails ownership check; the
      # primary defense is mode 0755 + root-owned, and on some distros the
      # default group on /usr/local/bin/* is wheel rather than root.
      if [[ "$qmode" == "755" && "$qowner" == "$rails_owner" ]]; then
        v_ok "quay binary: $qmode $qowner"
      else
        v_drift "quay binary" "mode=$qmode owner=$qowner (expected 755 $rails_owner)"
      fi
      local actual_version
      actual_version="$("$quay_bin" --version 2>/dev/null | head -n1 || true)"
      if [[ -n "$actual_version" ]] && grep -Fq "$quay_version" <<<"$actual_version"; then
        v_ok "quay binary version: $actual_version"
      else
        v_drift "quay binary version" "got '${actual_version:-?}' (expected to contain '$quay_version')"
      fi
    fi

    # Data dir + config.toml: agent-owned, mirrors the install-time
    # provisioning. config.toml is the rendered runtime config; its
    # absence is a drift, not a soft skip.
    if [[ ! -d "$quay_dir" ]]; then
      v_drift "quay data dir" "missing: $quay_dir"
    else
      local dmode downer_g
      dmode="$(_mode "$quay_dir")"
      downer_g="$(_owner "$quay_dir"):$(_group "$quay_dir")"
      if [[ "$dmode" == "755" && "$downer_g" == "$agent_owner:$agent_group" ]]; then
        v_ok "quay data dir: $dmode $downer_g"
      else
        v_drift "quay data dir" "mode=$dmode owner=$downer_g (expected 755 $agent_owner:$agent_group)"
      fi
      local quay_cfg="$quay_dir/config.toml"
      if [[ ! -f "$quay_cfg" ]]; then
        v_drift "quay config.toml" "missing: $quay_cfg"
      else
        local cmode cowner
        cmode="$(_mode "$quay_cfg")"
        cowner="$(_owner "$quay_cfg")"
        if [[ "$cmode" == "644" && "$cowner" == "$agent_owner" ]]; then
          v_ok "quay config.toml: $cmode $cowner"
        else
          v_drift "quay config.toml" "mode=$cmode owner=$cowner (expected 644 $agent_owner)"
        fi
      fi
    fi

    # Per-repo: bare clone present + agent-owned + origin matches values
    # file, AND id appears in `quay repo list`. Mirrors the install-time
    # invocation pattern (sudo -u env QUAY_DATA_DIR=…) — env_reset on
    # Debian/Ubuntu strips prefix-style env vars, so the env wrapper is
    # required, not stylistic. See PR #18 review.
    local quay_repos_tsv=""
    if [[ -f "$values_file" && -f "$values_helper" ]]; then
      quay_repos_tsv="$(python3 "$values_helper" --values "$values_file" list-repos 2>/dev/null || true)"
    fi
    if [[ -n "$quay_repos_tsv" ]]; then
      local registered_ids=""
      if [[ -x "$quay_bin" && -d "$quay_dir" ]]; then
        # Same invocation pattern as install-time: prefix with sudo only
        # when we're root running as a different user; otherwise call
        # directly. env QUAY_DATA_DIR=… must come AFTER any sudo prefix
        # because env_reset strips prefix-style vars.
        local cmd_prefix=()
        if [[ "$(id -u)" -eq 0 && "$(id -un)" != "$agent_owner" ]]; then
          cmd_prefix=(sudo -u "$agent_owner")
        fi
        # bash 3.2 (macOS default) under set -u trips on "${arr[@]}" when
        # arr is empty; the +"${arr[@]}" guard expands to nothing in that
        # case rather than referencing an unbound element.
        registered_ids="$(${cmd_prefix[@]+"${cmd_prefix[@]}"} env "QUAY_DATA_DIR=$quay_dir" "$quay_bin" repo list 2>/dev/null \
          | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if isinstance(data, list):
    for r in data:
        if isinstance(r, dict) and r.get("id"):
            print(r["id"])
' 2>/dev/null || true)"
      fi
      local repo_id repo_url repo_base repo_pkg repo_install bare bowner bare_origin
      while IFS=$'\t' read -r repo_id repo_url repo_base repo_pkg repo_install; do
        [[ -z "$repo_id" ]] && continue
        bare="$quay_dir/repos/${repo_id}.git"
        if [[ ! -d "$bare" ]]; then
          v_drift "quay repo $repo_id" "bare clone missing: $bare"
        else
          bowner="$(_owner "$bare")"
          if [[ "$bowner" == "$agent_owner" ]]; then
            v_ok "quay repo $repo_id ownership: $bowner"
          else
            v_drift "quay repo $repo_id ownership" "expected $agent_owner, got $bowner"
          fi
          bare_origin="$(_git_as_owner "$bare" remote get-url origin 2>/dev/null || true)"
          if [[ "$bare_origin" == "$repo_url" ]]; then
            v_ok "quay repo $repo_id origin: $bare_origin"
          else
            v_drift "quay repo $repo_id origin" "got '${bare_origin:-?}' (expected '$repo_url')"
          fi
        fi
        if [[ -n "$registered_ids" ]] && grep -Fxq "$repo_id" <<<"$registered_ids"; then
          v_ok "quay repo $repo_id registered"
        else
          v_drift "quay repo $repo_id" "not registered with quay"
        fi
      done <<<"$quay_repos_tsv"
    fi
  fi

  # ---- systemd units ----
  # Parse `systemctl show` output line-by-line and compare each field with
  # exact equality — substring matching ("active" in "*active*") quietly
  # accepts ActiveState=inactive, which is exactly the failure mode this
  # check is supposed to catch. See review of #10.
  if command -v systemctl >/dev/null 2>&1; then
    local u base unit_file so
    local active load unitfile
    local timers=(hermes-sync.timer hermes-upstream-sync.timer)
    [[ -n "$quay_version" ]] && timers+=(quay-tick.timer)
    for u in "${timers[@]}"; do
      active="$(systemctl show -p ActiveState --value "$u" 2>/dev/null)"
      load="$(systemctl show -p LoadState --value "$u" 2>/dev/null)"
      unitfile="$(systemctl show -p UnitFileState --value "$u" 2>/dev/null)"
      if [[ "$active" == "active" && "$load" == "loaded" && "$unitfile" == "enabled" ]]; then
        v_ok "$u: active loaded enabled"
      else
        v_drift "$u" "active=${active:-?} load=${load:-?} unitfile=${unitfile:-?}"
      fi
      # Both .service and .timer files ship together, so check both for
      # ownership drift — a chowned .timer is just as bad as a chowned .service.
      base="${u%.timer}"
      for unit_file in "/etc/systemd/system/$base.service" "/etc/systemd/system/$base.timer"; do
        if [[ -f "$unit_file" ]]; then
          so="$(_owner "$unit_file")"
          if [[ "$so" == "$rails_owner" ]]; then
            v_ok "$unit_file ownership: $so"
          else
            v_drift "$unit_file ownership" "expected $rails_owner, got $so"
          fi
        fi
      done
    done
  fi

  # ---- token helper smoke ----
  # Under --auth-method=app, the helper must be runnable end-to-end. We name
  # missing helper vs missing venv python separately so the drift line points
  # at the actual broken artefact instead of a vague "auth check failed".
  if [[ "$app_auth_expected" -eq 1 ]]; then
    local helper_py="$rails/installer/hermes_github_token.py"
    local venv_py="$rails/venv/bin/python"
    if [[ ! -f "$helper_py" ]]; then
      v_drift "token helper" "missing helper at $helper_py"
    elif [[ ! -x "$venv_py" ]]; then
      v_drift "token helper" "missing venv python at $venv_py"
    else
      local rc=0
      if [[ "$(id -un)" == "$agent_owner" ]]; then
        env "HERMES_HOME=$target" "$venv_py" "$helper_py" check >/dev/null 2>&1 || rc=$?
      elif [[ "$(id -u)" -eq 0 ]]; then
        sudo -u "$agent_owner" env "HERMES_HOME=$target" "$venv_py" "$helper_py" check >/dev/null 2>&1 || rc=$?
      else
        env "HERMES_HOME=$target" "$venv_py" "$helper_py" check >/dev/null 2>&1 || rc=$?
      fi
      if [[ "$rc" -eq 0 ]]; then
        v_ok "token helper check passes"
      else
        v_drift "token helper check" "exited $rc"
      fi
    fi
  fi

  # ---- upstream-workspace ----
  local ws="$target/upstream-workspace"
  if [[ -d "$ws/.git" ]]; then
    local rname url
    for rname in origin upstream; do
      url="$(_git_as_owner "$ws" remote get-url "$rname" 2>/dev/null || true)"
      if [[ -z "$url" ]]; then
        v_drift "upstream-workspace $rname" "not configured"
      elif [[ "$url" == https://github.com/* || "$url" == git@github.com:* || "$url" == ssh://git@github.com/* ]]; then
        v_ok "upstream-workspace $rname: $url"
      else
        v_drift "upstream-workspace $rname" "not a github.com URL: $url"
      fi
    done
  fi

  echo "==> verify: $V_TOTAL checks, $V_DRIFT drift"
  [[ "$V_DRIFT" -eq 0 ]]
}

if [[ "$VERIFY" -eq 1 ]]; then
  do_verify
  exit $?
fi

# ---------- install-only validation ----------

# State source: exactly one of --state or --state-url.
if [[ -n "$STATE_DIR" && -n "$STATE_URL" ]]; then
  echo "pass exactly one of --state or --state-url" >&2; exit 2
fi
if [[ -z "$STATE_DIR" && -z "$STATE_URL" ]]; then
  echo "must pass --state <path> or --state-url <url>" >&2; exit 2
fi

# --state-url requires authenticated cloning, so the auth method must be set.
if [[ -n "$STATE_URL" && "$AUTH_METHOD" == "none" ]]; then
  echo "--state-url requires --auth-method app" >&2; exit 2
fi

# The credential helper serves a GitHub installation token. Refuse any URL
# that isn't on github.com so a typo'd or compromised STATE_URL can't trick
# the helper into shipping the token to a different host.
if [[ -n "$STATE_URL" && "$STATE_URL" != https://github.com/* ]]; then
  echo "--state-url must be an https://github.com/ URL (got: $STATE_URL)" >&2; exit 2
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
    # python3-yaml gives the system python3 PyYAML so values_helper.py can run
    # before the rails venv is built (we read deploy.values.yaml at apt-prep
    # time to seed identity, manifest, and config.yaml).
    # tmux + gh are runtime deps of quay: tmux hosts each task's worker
    # session, gh is what the worker uses to open PRs.
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      build-essential "python${PY_VERSION}-dev" "python${PY_VERSION}-venv" libffi-dev rsync python3-yaml \
      curl tmux gh >/dev/null
  else
    echo "==> no apt-get detected; skipping automated prep" >&2
    echo "    ensure these are installed manually if anything below fails:" >&2
    echo "    build-essential, python${PY_VERSION}-dev, python${PY_VERSION}-venv, libffi-dev, rsync, python3-yaml, curl, tmux, gh" >&2
  fi
fi

command -v rsync >/dev/null 2>&1 \
  || { echo "rsync not found on PATH" >&2; exit 1; }
command -v curl >/dev/null 2>&1 \
  || { echo "curl not found on PATH (needed to fetch the quay binary)" >&2; exit 1; }

# ---------- values file ----------
VALUES_FILE="${VALUES_FILE:-$FORK_DIR/deploy.values.yaml}"
[[ -f "$VALUES_FILE" ]] \
  || { echo "deploy.values.yaml not found at $VALUES_FILE (re-fork: copy + edit, or pass --values)" >&2; exit 1; }
VALUES_HELPER="$FORK_DIR/installer/values_helper.py"
[[ -f "$VALUES_HELPER" ]] \
  || { echo "values helper missing at $VALUES_HELPER" >&2; exit 1; }

if [[ -z "$GIT_IDENTITY_NAME" ]]; then
  GIT_IDENTITY_NAME="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get org.agent_identity_name)"
fi
if [[ -z "$GIT_IDENTITY_EMAIL" ]]; then
  GIT_IDENTITY_EMAIL="${GIT_IDENTITY_NAME}@$(hostname -s)"
fi

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

# ---------- quay binary (rails-class, /usr/local/bin/quay) ----------
# The version pin is the security boundary: no override flag, SHA256
# verified against the matching GitHub Release on every install run.
# Empty `quay.version` is treated as "quay not enabled for this fork yet"
# — staging the fork (or running CI) before a quay release is cut skips
# every quay-related step below. Once a v* tag is pinned, all quay
# provisioning kicks in on the next install.

QUAY_VERSION="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get quay.version)"
QUAY_BIN_DST="/usr/local/bin/quay"
QUAY_ENABLED=0

if [[ -z "$QUAY_VERSION" ]]; then
  echo "==> quay.version unset in $VALUES_FILE; skipping quay provisioning"
else
  QUAY_ENABLED=1
  case "$(uname -m)" in
    x86_64)  QUAY_ARCH="amd64" ;;
    aarch64) QUAY_ARCH="arm64" ;;
    *) echo "FAIL: unsupported architecture $(uname -m); quay ships amd64/arm64 Linux only" >&2; exit 1 ;;
  esac

  QUAY_RELEASE_URL="https://github.com/lafawnduh1966/quay/releases/download/${QUAY_VERSION}"
  QUAY_ASSET="quay-linux-${QUAY_ARCH}"

  echo "==> installing quay binary ${QUAY_VERSION} (${QUAY_ARCH}) to $QUAY_BIN_DST"
  QUAY_TMP="$(mktemp -d)"
  trap 'rm -rf "$QUAY_TMP"' EXIT
  curl -fsSL --retry 3 -o "$QUAY_TMP/$QUAY_ASSET" "$QUAY_RELEASE_URL/$QUAY_ASSET"
  curl -fsSL --retry 3 -o "$QUAY_TMP/SHA256SUMS"  "$QUAY_RELEASE_URL/SHA256SUMS"

  # Match on field-2 (filename) so the asset name is treated as a literal
  # rather than a regex. An empty match would otherwise feed an empty
  # stream to `sha256sum -c`, which exits 0 and silently passes. The
  # leading-`*` strip keeps the match working if the SHA file is ever
  # produced in binary mode (`sha256sum -b` emits `<hash> *<filename>`).
  QUAY_EXPECTED_LINE="$(awk -v asset="$QUAY_ASSET" \
    '{ name=$2; sub(/^\*/, "", name) } name == asset { print; exit }' \
    "$QUAY_TMP/SHA256SUMS")"
  [[ -n "$QUAY_EXPECTED_LINE" ]] \
    || { echo "FAIL: $QUAY_ASSET not listed in SHA256SUMS for ${QUAY_VERSION}" >&2; exit 1; }
  ( cd "$QUAY_TMP" && echo "$QUAY_EXPECTED_LINE" | sha256sum -c --strict --status ) \
    || { echo "FAIL: SHA256 mismatch for $QUAY_ASSET (release ${QUAY_VERSION})" >&2; exit 1; }

  install -o root -g root -m 0755 "$QUAY_TMP/$QUAY_ASSET" "$QUAY_BIN_DST"
  rm -rf "$QUAY_TMP"
  trap - EXIT
fi

# ---------- rails (root-owned, read-only to agent) ----------

# HERMES_HOME itself is root:hermes 02775 (setgid). Rails subdirs and the
# top-level rails files (hermes-agent/, hooks/, SOUL.md, RUNTIME_VERSION)
# stay root-owned and read-only to the agent — that's the protection that
# keeps the agent from rewriting its own code paths. The setgid bit lets
# the gateway (running as $AGENT_USER) create its own runtime files
# (gateway.lock, gateway.pid, platforms/) at HERMES_HOME root, which the
# upstream runtime expects to write directly there. Trade-off: with write
# access to the parent dir, the agent can `rm` a top-level rails file and
# recreate it under its own ownership with arbitrary content (in-place
# modification is still blocked by the file's 0644 mode). hermes-sync
# drift detection picks up the deletion of any rails file and the next
# setup-hermes.sh run re-seeds them; rails/ ownership drift on the
# replacement is caught by do_verify's rails check.
echo "==> rendering rails"
install -d -o root -g "$AGENT_USER" -m 02775 "$TARGET_DIR"

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

# overlay files at the root of the render target.
#
# SOUL.md: seed from the fork on first install only. The runtime no longer
# writes SOUL.md (it's rails — root-owned, read-only to the agent), so the
# installer is the only thing that can place it. Idempotent on re-run: an
# operator's customized SOUL.md survives subsequent installs. To force a
# refresh from the fork, delete $TARGET_DIR/SOUL.md before re-running.
[[ -f "$FORK_DIR/SOUL.md" ]] \
  || { echo "FAIL: fork is missing SOUL.md at $FORK_DIR/SOUL.md" >&2; exit 1; }
if [[ -f "$TARGET_DIR/SOUL.md" ]]; then
  echo "==> SOUL.md already present at $TARGET_DIR/SOUL.md (preserving)"
else
  echo "==> seeding SOUL.md from $FORK_DIR/SOUL.md"
  install -o root -g root -m 644 "$FORK_DIR/SOUL.md" "$TARGET_DIR/SOUL.md"
fi
install -d -o root -g root -m 755 "$TARGET_DIR/hooks"

# RUNTIME_VERSION records the fork SHA we rendered from.
echo "$FORK_SHA" > "$TARGET_DIR/RUNTIME_VERSION"
chown root:root "$TARGET_DIR/RUNTIME_VERSION"
chmod 644 "$TARGET_DIR/RUNTIME_VERSION"

# Re-rendered every run so values.yaml edits propagate; operator pastes this
# into Slack's manifest UI to update the App.
SLACK_MANIFEST_TMPL="$FORK_DIR/installer/slack-manifest.json.tmpl"
SLACK_MANIFEST_OUT="$TARGET_DIR/slack-manifest.json"
if [[ -f "$SLACK_MANIFEST_TMPL" ]]; then
  echo "==> rendering Slack manifest to $SLACK_MANIFEST_OUT"
  python3 "$VALUES_HELPER" --values "$VALUES_FILE" \
    render-manifest --in "$SLACK_MANIFEST_TMPL" --out "$SLACK_MANIFEST_OUT"
  chown root:root "$SLACK_MANIFEST_OUT"
  chmod 644 "$SLACK_MANIFEST_OUT"
else
  echo "==> WARNING: $SLACK_MANIFEST_TMPL missing; skipping manifest render" >&2
fi

# First-install seed only; preserved on re-runs so operator hand-edits
# survive. Delete the file to force a refresh from values.yaml.
CONFIG_YAML_OUT="$TARGET_DIR/config.yaml"
if [[ -f "$CONFIG_YAML_OUT" ]]; then
  echo "==> $CONFIG_YAML_OUT already present (preserving)"
else
  echo "==> seeding $CONFIG_YAML_OUT from $VALUES_FILE"
  python3 "$VALUES_HELPER" --values "$VALUES_FILE" \
    render-runtime-config --out "$CONFIG_YAML_OUT"
  chown root:root "$CONFIG_YAML_OUT"
  chmod 644 "$CONFIG_YAML_OUT"
fi

# ---------- venv (rails-class) ----------

echo "==> building venv at $TARGET_DIR/hermes-agent/venv"
if [[ ! -d "$TARGET_DIR/hermes-agent/venv" ]]; then
  "$PYTHON_BIN" -m venv "$TARGET_DIR/hermes-agent/venv"
fi
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet --upgrade pip wheel setuptools
# [slack] pulls in slack-bolt + slack-sdk so the gateway's Slack adapter is
# usable on first start. Without this extra, hermes-gateway.service crashes
# immediately with "slack-bolt not installed" until an operator runs pip
# manually inside the venv.
"$TARGET_DIR/hermes-agent/venv/bin/pip" install --quiet -e "$TARGET_DIR/hermes-agent[slack]"
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
HERMES_BIN="$TARGET_DIR/hermes-agent/venv/bin/hermes"

if [[ "$AUTH_METHOD" == "app" ]]; then
  echo "==> wiring GitHub App auth at $AUTH_DIR"
  install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"
  # Strip setgid that propagates from $TARGET_DIR's setgid bit on Linux
  # (System V dir semantics). auth/ has a strict mode-750 invariant —
  # files inside are explicitly chmod'd, no need for group inheritance.
  # `install -m 0750` doesn't reliably clear the bit on coreutils we've
  # seen in CI; `g-s` is unambiguous.
  chmod g-s "$AUTH_DIR"

  if [[ -n "$APP_KEY_PATH" ]]; then
    [[ -f "$APP_KEY_PATH" ]] \
      || { echo "FAIL: --app-key-path file not found: $APP_KEY_PATH" >&2; exit 1; }
    # Tight regex: only PKCS#1 (RSA), SEC1 (EC), and PKCS#8 unencrypted PEMs.
    # PyJWT can't read passphrase-protected keys, so reject ENCRYPTED upfront
    # rather than letting it surface as a misleading error at the smoke step.
    head -1 "$APP_KEY_PATH" | grep -Eq '^-----BEGIN (RSA |EC )?PRIVATE KEY-----$' \
      || { echo "FAIL: $APP_KEY_PATH does not look like an unencrypted PEM private key" >&2; exit 1; }
    if grep -q 'ENCRYPTED' "$APP_KEY_PATH"; then
      echo "FAIL: $APP_KEY_PATH is passphrase-protected; PyJWT requires an unencrypted key" >&2
      exit 1
    fi
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
  # operator passed new values. Extract via awk rather than `source`, because
  # `source` evaluates the values in our shell — `--app-id 'foo$(rm -rf /)'`
  # would be game-over on the next install.
  if [[ -f "$GH_APP_ENV" ]]; then
    APP_ID="${APP_ID:-$(awk -F= '$1=="HERMES_GH_APP_ID"{print $2; exit}' "$GH_APP_ENV")}"
    APP_INSTALLATION_ID="${APP_INSTALLATION_ID:-$(awk -F= '$1=="HERMES_GH_INSTALLATION_ID"{print $2; exit}' "$GH_APP_ENV")}"
    GH_API_BASE="${GH_API_BASE:-$(awk -F= '$1=="HERMES_GH_API"{print $2; exit}' "$GH_APP_ENV")}"
  fi
  # GitHub App and installation IDs are positive integers — any other shape is
  # a typo at best, an injection attempt at worst. Validate before persisting.
  [[ "$APP_ID" =~ ^[1-9][0-9]*$ ]] \
    || { echo "FAIL: --app-id must be a positive integer (got: '$APP_ID')" >&2; exit 1; }
  [[ "$APP_INSTALLATION_ID" =~ ^[1-9][0-9]*$ ]] \
    || { echo "FAIL: --app-installation-id must be a positive integer (got: '$APP_INSTALLATION_ID')" >&2; exit 1; }

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
# (get/store/erase) as the next argv to the helper. HERMES_HOME is baked in
# so non-default --target installs (e.g. /opt/hermes) point the helper at the
# correct auth/ + cache/ directories at git-fetch time, when the agent's env
# may not carry HERMES_HOME.
GIT_CRED_HELPER=""
if [[ "$AUTH_METHOD" == "app" ]]; then
  GIT_CRED_HELPER="!HERMES_HOME='$TARGET_DIR' $VENV_PY $TOKEN_HELPER_PY credential"
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
    # Scope the helper to https://github.com for symmetry with the persisted
    # config: even though we already asserted the URL is on github.com above,
    # an unscoped helper would fire on any redirected host git follows.
    sudo -u "$AGENT_USER" \
      env HERMES_HOME="$TARGET_DIR" \
      git -c "credential.https://github.com.helper=$GIT_CRED_HELPER" \
        clone --quiet "$STATE_URL" "$STATE_TARGET"
  fi
else
  echo "==> state repo already present at $STATE_TARGET (preserving; pass --force-state to re-clone)"
fi

# Guardrail: if the state repo's origin is a filesystem path the sync timer's
# pushes will fail every tick (the agent user can't write into a root-owned
# fixture's .git/objects/). This happens when a `--state <path>` bootstrap
# inherits a local-fixture origin from the source repo and propagates it to
# the installed clone. Surface the misconfig here so operators see it on
# every install run instead of digging through journalctl.
LIVE_STATE_ORIGIN="$(sudo -u "$AGENT_USER" git -C "$STATE_TARGET" remote get-url origin 2>/dev/null || true)"
if [[ -n "$LIVE_STATE_ORIGIN" && ! "$LIVE_STATE_ORIGIN" =~ ^(https?://|ssh://|git://|git@) ]]; then
  echo "==> WARNING: state repo origin is a filesystem path: $LIVE_STATE_ORIGIN" >&2
  echo "    hermes-sync push will fail every tick until repointed. To fix:" >&2
  echo "      sudo -u $AGENT_USER git -C $STATE_TARGET remote set-url origin <github-url>" >&2
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
# `platforms/` and `platforms/pairing/` host the gateway's per-platform
# pairing/auth state — the upstream runtime mkdirs them lazily, but
# pre-creating with agent ownership avoids the mkdir failing under the
# rails-mode HERMES_HOME on the very first start.
for d in sessions logs cache platforms platforms/pairing; do
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$TARGET_DIR/$d"
done

# `quay/` is the data dir consumed by quay (sqlite, worktrees, repo bare
# clones, logs); QUAY_DATA_DIR will point here from the systemd unit.
# Skipped entirely when quay isn't enabled — see the binary block above.
if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 755 "$TARGET_DIR/quay"

  # quay/config.toml — same first-install-seed pattern as config.yaml above.
  # The bash gate scopes chown/chmod to first install (the helper itself
  # also preserves the file body, but the gate avoids re-flipping perms on
  # every re-run, which would clobber any operator chmod).
  QUAY_CONFIG_OUT="$TARGET_DIR/quay/config.toml"
  if [[ -f "$QUAY_CONFIG_OUT" ]]; then
    echo "==> $QUAY_CONFIG_OUT already present (preserving)"
  else
    echo "==> seeding $QUAY_CONFIG_OUT from $VALUES_FILE"
    python3 "$VALUES_HELPER" --values "$VALUES_FILE" \
      render-quay-config --out "$QUAY_CONFIG_OUT"
    chown "$AGENT_USER:$AGENT_USER" "$QUAY_CONFIG_OUT"
    chmod 0644 "$QUAY_CONFIG_OUT"
  fi

  # Bare clones + `quay repo add` registration per quay.repos entry.
  # quay does NOT clone repos itself — the quickstart documents that the
  # operator pre-provisions a bare clone at $QUAY_DATA_DIR/repos/<id>.git
  # and then registers it. Bare clones are agent-owned: quay fetches into
  # them at runtime (worktrees originate here), but we never push from
  # them, so the agent's group-write is enough.
  #
  # Idempotency: an existing bare clone is preserved iff its origin URL
  # matches the values-file URL — a mismatch is a hard fail rather than a
  # silent re-point (matches the state-clone posture). Registration is
  # gated on `quay repo list` so re-runs are no-ops.
  QUAY_REPOS_ROOT="$TARGET_DIR/quay/repos"
  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0755 "$QUAY_REPOS_ROOT"

  QUAY_REPOS_TSV="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repos)"

  if [[ -n "$QUAY_REPOS_TSV" ]]; then
    # Snapshot already-registered ids once. `quay repo list` emits a JSON
    # array (per docs/user/cli-reference.md); a fresh data dir applies
    # the embedded migrations on first invocation and returns []. A parse
    # failure aborts the install — silently treating it as "no
    # registrations" would re-invoke `quay repo add` on every re-run,
    # which is not documented as idempotent.
    QUAY_REGISTERED_IDS="$(
      sudo -u "$AGENT_USER" \
        env QUAY_DATA_DIR="$TARGET_DIR/quay" "$QUAY_BIN_DST" repo list \
        | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except json.JSONDecodeError as exc:
    sys.stderr.write(f"quay repo list: not valid JSON: {exc}\n")
    sys.exit(2)
if not isinstance(data, list):
    sys.stderr.write(f"quay repo list: expected JSON array, got {type(data).__name__}\n")
    sys.exit(2)
for r in data:
    if isinstance(r, dict) and r.get("id"):
        print(r["id"])
'
    )"

    while IFS=$'\t' read -r repo_id repo_url repo_base repo_pkg repo_install; do
      [[ -z "$repo_id" ]] && continue
      bare="$QUAY_REPOS_ROOT/${repo_id}.git"
      if [[ -d "$bare" ]]; then
        actual_url="$(_git_as_owner "$bare" remote get-url origin 2>/dev/null || true)"
        if [[ "$actual_url" != "$repo_url" ]]; then
          echo "FAIL: $bare origin=$actual_url, expected $repo_url" >&2
          echo "      refusing to silently re-point an existing bare clone." >&2
          echo "      Move it aside (mv $bare $bare.bak) and re-run to re-clone." >&2
          exit 1
        fi
        echo "==> quay bare clone $repo_id present (preserving)"
      else
        echo "==> cloning $repo_url into $bare"
        sudo -u "$AGENT_USER" \
          git clone --quiet --bare "$repo_url" "$bare"
      fi

      if grep -Fxq "$repo_id" <<<"$QUAY_REGISTERED_IDS"; then
        echo "==> quay repo $repo_id already registered (preserving)"
      else
        echo "==> registering $repo_id with quay"
        sudo -u "$AGENT_USER" \
          env QUAY_DATA_DIR="$TARGET_DIR/quay" "$QUAY_BIN_DST" repo add \
            --id "$repo_id" \
            --url "$repo_url" \
            --base-branch "$repo_base" \
            --package-manager "$repo_pkg" \
            --install-cmd "$repo_install" >/dev/null
      fi
    done <<<"$QUAY_REPOS_TSV"
  fi
fi

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

# ---------- hermes-sync ----------
# Install the periodic two-way sync script and its systemd timer. The script
# lives at /usr/local/sbin so the agent (group $AGENT_USER) can read but not
# modify it; the unit files are also root-owned for the same reason.
#
# v0 is Linux/systemd-only — same scope as the rest of this installer. The
# launchd path tracks under the existing macOS TODO at the top of this file.

OPS_DIR="$FORK_DIR/ops"
SYNC_SCRIPT_SRC="$OPS_DIR/hermes-sync"
SYNC_SCRIPT_DST="/usr/local/sbin/hermes-sync"

if [[ -f "$SYNC_SCRIPT_SRC" ]]; then
  echo "==> installing hermes-sync at $SYNC_SCRIPT_DST"
  install -o root -g root -m 0755 "$SYNC_SCRIPT_SRC" "$SYNC_SCRIPT_DST"

  # Drop a small environment override so the script targets this install's
  # state dir even if the agent's $HOME/.hermes layout ever moves.
  # Idempotent on re-run: an operator-customized /etc/default/hermes-sync
  # survives subsequent installs (matches SOUL.md / state-clone behavior).
  # To force a refresh from defaults, delete the file before re-running.
  install -d -o root -g root -m 0755 /etc/default
  if [[ -f /etc/default/hermes-sync ]]; then
    echo "==> /etc/default/hermes-sync already present (preserving)"
  else
    echo "==> seeding /etc/default/hermes-sync"
    cat >/etc/default/hermes-sync <<EOF
# Generated by setup-hermes.sh — overrides for hermes-sync.service.
# Edits here survive re-runs of the installer; delete this file to force
# regeneration from defaults.
HERMES_STATE_DIR=$STATE_TARGET
HOME=$AGENT_HOME
EOF
    chown root:root /etc/default/hermes-sync
    chmod 0644 /etc/default/hermes-sync
  fi

  if command -v systemctl >/dev/null 2>&1; then
    echo "==> installing systemd timer for hermes-sync (user=$AGENT_USER)"
    # Template the service unit's User=/Group= from the install-time agent
    # user so non-default --user values don't get a unit hard-coded to
    # "hermes". The source file uses __AGENT_USER__ placeholders.
    sed "s|__AGENT_USER__|$AGENT_USER|g" \
      "$OPS_DIR/hermes-sync.service" \
      | install -o root -g root -m 0644 /dev/stdin /etc/systemd/system/hermes-sync.service
    install -o root -g root -m 0644 \
      "$OPS_DIR/hermes-sync.timer" /etc/systemd/system/hermes-sync.timer
    systemctl daemon-reload
    systemctl enable --now hermes-sync.timer
  else
    echo "==> systemctl not present; skipping timer enable (script installed)" >&2
  fi
else
  echo "==> WARNING: $SYNC_SCRIPT_SRC missing; skipping hermes-sync install" >&2
fi

# ---------- hermes-upstream-sync ----------
# Weekly proposer: detects upstream/main divergence and opens a PR for human
# review. Branch protection on the fork's `main` ensures the agent can't
# self-merge. Same install model as hermes-sync — script + service +
# templated User=$AGENT_USER, /etc/default/ env preserved across re-runs.
#
# Workspace: the agent runs the script as $AGENT_USER, so it needs an
# agent-writable git checkout with both `origin` (this fork) and `upstream`
# remotes configured. We provision one at $TARGET/upstream-workspace/ on
# first install. Pre-existing checkouts are left alone (operator may have
# customized remotes); delete to force a fresh clone.

UPSTREAM_SYNC_SRC="$OPS_DIR/hermes-upstream-sync"
UPSTREAM_SYNC_DST="/usr/local/sbin/hermes-upstream-sync"
UPSTREAM_WORKSPACE="$TARGET_DIR/upstream-workspace"
# Hardcoded for the hermes-agent fork. If a fork ever tracks a different
# upstream, surface this as a flag — for now the only consumer is hermes.
UPSTREAM_REMOTE_URL="${UPSTREAM_REMOTE_URL:-https://github.com/nousresearch/hermes-agent.git}"
FORK_ORIGIN_URL="$(git -C "$FORK_DIR" remote get-url origin 2>/dev/null || true)"

if [[ -f "$UPSTREAM_SYNC_SRC" ]]; then
  echo "==> installing hermes-upstream-sync at $UPSTREAM_SYNC_DST"
  install -o root -g root -m 0755 "$UPSTREAM_SYNC_SRC" "$UPSTREAM_SYNC_DST"

  if [[ -d "$UPSTREAM_WORKSPACE/.git" ]]; then
    echo "==> upstream-sync workspace already present at $UPSTREAM_WORKSPACE (preserving)"
  elif [[ -z "$FORK_ORIGIN_URL" ]]; then
    echo "==> WARNING: $FORK_DIR has no origin remote; skipping workspace provision" >&2
  else
    echo "==> provisioning upstream-sync workspace at $UPSTREAM_WORKSPACE"
    install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0750 "$UPSTREAM_WORKSPACE"
    sudo -u "$AGENT_USER" git clone --quiet "$FORK_ORIGIN_URL" "$UPSTREAM_WORKSPACE"
    sudo -u "$AGENT_USER" git -C "$UPSTREAM_WORKSPACE" \
      remote add upstream "$UPSTREAM_REMOTE_URL"
    sudo -u "$AGENT_USER" git -C "$UPSTREAM_WORKSPACE" \
      config user.email "$GIT_IDENTITY_EMAIL"
    sudo -u "$AGENT_USER" git -C "$UPSTREAM_WORKSPACE" \
      config user.name "$GIT_IDENTITY_NAME"
  fi

  if [[ -f /etc/default/hermes-upstream-sync ]]; then
    echo "==> /etc/default/hermes-upstream-sync already present (preserving)"
  else
    echo "==> seeding /etc/default/hermes-upstream-sync"
    cat >/etc/default/hermes-upstream-sync <<EOF
# Generated by setup-hermes.sh — overrides for hermes-upstream-sync.service.
# Edits here survive re-runs of the installer; delete this file to force
# regeneration from defaults.
FORK_DIR=$UPSTREAM_WORKSPACE
HOME=$AGENT_HOME
EOF
    chown root:root /etc/default/hermes-upstream-sync
    chmod 0644 /etc/default/hermes-upstream-sync
  fi

  if command -v systemctl >/dev/null 2>&1; then
    echo "==> installing systemd timer for hermes-upstream-sync (user=$AGENT_USER)"
    sed "s|__AGENT_USER__|$AGENT_USER|g" \
      "$OPS_DIR/hermes-upstream-sync.service" \
      | install -o root -g root -m 0644 /dev/stdin \
          /etc/systemd/system/hermes-upstream-sync.service
    install -o root -g root -m 0644 \
      "$OPS_DIR/hermes-upstream-sync.timer" \
      /etc/systemd/system/hermes-upstream-sync.timer
    systemctl daemon-reload
    # Only enable when the workspace is actually present — otherwise the
    # first tick would die loudly until an operator provisions it.
    if [[ -d "$UPSTREAM_WORKSPACE/.git" ]]; then
      systemctl enable --now hermes-upstream-sync.timer
    else
      echo "==> upstream-sync workspace missing; timer left disabled" >&2
    fi
  else
    echo "==> systemctl not present; skipping upstream-sync timer (script installed)" >&2
  fi
else
  echo "==> $UPSTREAM_SYNC_SRC missing; skipping hermes-upstream-sync install" >&2
fi

# ---------- quay-tick ----------
# Periodic supervisor that drives queued quay tasks forward (claim →
# worker → submit-brief). Same install model as hermes-sync — sed-
# templated unit, /etc/default/ env preserved across re-runs. The unit
# itself runs `/usr/local/bin/quay tick` directly, so unlike hermes-sync
# there's no companion script under /usr/local/sbin.
#
# Skipped entirely when quay isn't enabled — see the binary block at the
# top of the script.

QUAY_TICK_SRC="$OPS_DIR/quay-tick.service"

if [[ "$QUAY_ENABLED" -eq 1 && -f "$QUAY_TICK_SRC" ]]; then
  install -d -o root -g root -m 0755 /etc/default
  if [[ -f /etc/default/quay-tick ]]; then
    echo "==> /etc/default/quay-tick already present (preserving)"
  else
    echo "==> seeding /etc/default/quay-tick"
    cat >/etc/default/quay-tick <<'EOF'
# Generated by setup-hermes.sh — overrides for quay-tick.service.
# Edits here survive re-runs of the installer; delete this file to force
# regeneration from defaults. Empty by default — QUAY_DATA_DIR is set
# directly on the unit, and adapter tokens flow from <HERMES_HOME>/auth/
# quay.env (staged by stage-quay-env.sh).
EOF
    chown root:root /etc/default/quay-tick
    chmod 0644 /etc/default/quay-tick
  fi

  if command -v systemctl >/dev/null 2>&1; then
    echo "==> installing systemd timer for quay-tick (user=$AGENT_USER, data=$TARGET_DIR/quay)"
    # The service unit references both __AGENT_USER__ and __TARGET_DIR__;
    # both are substituted at install time so non-default --user / --target
    # values get a correctly-targeted unit.
    sed -e "s|__AGENT_USER__|$AGENT_USER|g" \
        -e "s|__TARGET_DIR__|$TARGET_DIR|g" \
        "$QUAY_TICK_SRC" \
      | install -o root -g root -m 0644 /dev/stdin /etc/systemd/system/quay-tick.service
    install -o root -g root -m 0644 \
      "$OPS_DIR/quay-tick.timer" /etc/systemd/system/quay-tick.timer
    systemctl daemon-reload
    systemctl enable --now quay-tick.timer
  else
    echo "==> systemctl not present; skipping quay-tick timer enable" >&2
  fi
fi

# ---------- hermes-gateway ----------
# The canonical unit is generated by `hermes gateway install --system`
# and refreshed by `hermes update`; we layer our additions via a systemd
# drop-in. See ops/README.md for the rationale.

GATEWAY_DROPIN_SRC="$OPS_DIR/hermes-gateway.service.d/slack-env.conf"
GATEWAY_DROPIN_DIR="/etc/systemd/system/hermes-gateway.service.d"
GATEWAY_DROPIN_DST="$GATEWAY_DROPIN_DIR/slack-env.conf"
GATEWAY_SLACK_ENV="$AUTH_DIR/slack.env"

if [[ -f "$GATEWAY_DROPIN_SRC" && -x "$HERMES_BIN" ]]; then
  if [[ -f /etc/default/hermes-gateway ]]; then
    echo "==> /etc/default/hermes-gateway already present (preserving)"
  else
    echo "==> seeding /etc/default/hermes-gateway (empty; runtime values flow via config.yaml)"
    cat >/etc/default/hermes-gateway <<'EOF'
# Operator overrides for hermes-gateway.service. Survives re-runs of
# setup-hermes.sh; delete to regenerate from defaults.
#
# Empty by default — org-specific runtime values live in deploy.values.yaml
# and are seeded into <HERMES_HOME>/config.yaml on first install. Set env
# vars here only as a temporary override; env > config.yaml > defaults.
EOF
    chown root:root /etc/default/hermes-gateway
    chmod 0644 /etc/default/hermes-gateway
  fi

  if command -v systemctl >/dev/null 2>&1; then
    # HERMES_HOME pins the unit's HERMES_HOME to $TARGET — without it the CLI
    # resolves /root/.hermes (since we run as root) and the remap logic only
    # handles the default ~/.hermes shape.
    #
    # HERMES_HOME_MODE=02775 preserves the setgid+group-write mode the
    # rails-rendering step set on $TARGET_DIR. The CLI's ensure_hermes_home()
    # chmods $HERMES_HOME via _secure_dir, which defaults to 0700 and would
    # strip both world-x (locking the agent out of traversal) and the
    # group-write bit (locking the gateway out of writing gateway.lock /
    # gateway.pid / platforms/ at HERMES_HOME root). _secure_dir's docstring
    # documents HERMES_HOME_MODE as the escape hatch.
    echo "==> installing canonical hermes-gateway unit via upstream CLI"
    HERMES_HOME="$TARGET_DIR" HERMES_HOME_MODE=02775 \
      "$HERMES_BIN" gateway install --system \
      --force --run-as-user "$AGENT_USER"
    # Re-assert mode + group ownership regardless: if a future CLI change
    # ignores HERMES_HOME_MODE or _secure_dir clamps stricter, the gateway
    # would lose write access to its runtime files.
    chgrp "$AGENT_USER" "$TARGET_DIR"
    chmod 02775 "$TARGET_DIR"

    echo "==> installing slack-env drop-in at $GATEWAY_DROPIN_DST"
    install -d -o root -g root -m 0755 "$GATEWAY_DROPIN_DIR"
    sed -e "s|__TARGET_DIR__|$TARGET_DIR|g" "$GATEWAY_DROPIN_SRC" \
      | install -o root -g root -m 0644 /dev/stdin "$GATEWAY_DROPIN_DST"
    systemctl daemon-reload

    if [[ -f "$GATEWAY_SLACK_ENV" ]]; then
      systemctl enable --now hermes-gateway.service
    else
      echo "==> $GATEWAY_SLACK_ENV not staged; gateway unit installed but left disabled" >&2
      echo "    Stage the file with SLACK_BOT_TOKEN / SLACK_APP_TOKEN, then:" >&2
      echo "      sudo systemctl enable --now hermes-gateway.service" >&2
    fi
  else
    echo "==> systemctl not present; skipping hermes-gateway enable" >&2
  fi
else
  if [[ ! -x "$HERMES_BIN" ]]; then
    echo "==> WARNING: $HERMES_BIN missing or non-executable; skipping hermes-gateway install" >&2
  else
    echo "==> WARNING: $GATEWAY_DROPIN_SRC missing; skipping hermes-gateway install" >&2
  fi
fi

# ---------- post-install assertion ----------

[[ -x "$HERMES_BIN" ]] \
  || { echo "FAIL: hermes entry point missing or non-executable at $HERMES_BIN" >&2; exit 1; }
if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  [[ -x "$QUAY_BIN_DST" ]] \
    || { echo "FAIL: quay binary missing or non-executable at $QUAY_BIN_DST" >&2; exit 1; }
fi

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
  if [[ "$SKIP_AUTH_CHECK" -eq 0 ]]; then
    # Smoke-test the helper end-to-end: the agent must be able to read the App
    # key, sign a JWT, hit the API, and emit credentials — without sudo. We use
    # `check` (not `mint`) so the live token never lands in install logs even
    # if a retry succeeds after a transient flake.
    if ! sudo -u "$AGENT_USER" \
          env HERMES_HOME="$TARGET_DIR" \
          "$VENV_PY" "$TOKEN_HELPER_PY" check >/dev/null 2>&1; then
      echo "FAIL: token helper check failed for $AGENT_USER" >&2
      # Re-run with stderr only — `check` never writes to stdout, but pin both
      # streams to fd 2 anyway so any future helper change can't leak the token.
      sudo -u "$AGENT_USER" env HERMES_HOME="$TARGET_DIR" \
        "$VENV_PY" "$TOKEN_HELPER_PY" check >/dev/null || true
      exit 1
    fi
  else
    echo "==> --skip-auth-check: skipping live token mint smoke"
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
