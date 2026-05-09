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
# --app-key-path (path to the App's PEM private key, the real secret). The
# numeric App ID and installation ID default to deploy.values.yaml's
# auth.github_app.{id, installation_id}; --app-id / --app-installation-id
# are still honored as per-run overrides for staging. Subsequent runs reuse
# the PEM persisted under $TARGET/auth/. --auth-method none (the default)
# skips auth wiring and is what CI uses with the local fixture.

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

# Print the SHA-256 of $1, or empty when the file doesn't exist. Callers
# capture before/after a write and compare; an absent-then-present file
# (first install) reads as a mismatch and triggers downstream actions
# (e.g. a gateway restart) just like a content change would.
file_sha() {
  [[ -f "$1" ]] || return 0
  sha256sum "$1" | cut -d' ' -f1
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

# Read the literal stored origin URL (sidestepping url.insteadOf rewrites
# that may bridge HTTPS values URLs to SSH transport — we want the
# comparand the values file declared, not what git resolves to). Exit
# non-zero with a "move it aside" hint when the existing clone points
# somewhere else; re-pointing silently would let a values-file edit
# steal a populated clone and is the same posture as the state-clone
# preserve check.
_check_clone_origin_or_die() {
  local kind="$1" dir="$2" expected_url="$3"
  local actual_url
  actual_url="$(_git_as_owner "$dir" config --get remote.origin.url 2>/dev/null || true)"
  if [[ "$actual_url" != "$expected_url" ]]; then
    echo "FAIL: $dir origin=$actual_url, expected $expected_url" >&2
    echo "      refusing to silently re-point an existing $kind." >&2
    echo "      Move it aside (mv $dir $dir.bak) and re-run to re-clone." >&2
    exit 1
  fi
}

# Verify-side counterpart to _check_clone_origin_or_die: check that an
# existing clone is owned by the expected agent user and points at the
# values-file URL, emitting v_ok/v_drift instead of dying. Origin read
# uses the literal stored value, same rationale as the install path.
# Caller is responsible for the existence check on $clone_path/.git
# before calling — we treat absence as a separate drift category.
_v_check_clone_basics() {
  local label="$1" clone_path="$2" expected_url="$3" expected_owner="$4"
  local owner origin
  owner="$(_owner "$clone_path/.git" 2>/dev/null || _owner "$clone_path" 2>/dev/null)"
  if [[ "$owner" == "$expected_owner" ]]; then
    v_ok "$label ownership: $owner"
  else
    v_drift "$label ownership" "expected $expected_owner, got $owner"
  fi
  origin="$(_git_as_owner "$clone_path" config --get remote.origin.url 2>/dev/null || true)"
  if [[ "$origin" == "$expected_url" ]]; then
    v_ok "$label origin: $origin"
  else
    v_drift "$label origin" "got '${origin:-?}' (expected '$expected_url')"
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

  # ---- config.yaml ----
  # Must be agent-writable — see seed block below for the full WHY.
  local cfg="$target/config.yaml"
  if [[ ! -f "$cfg" ]]; then
    v_drift "config.yaml" "missing: $cfg"
  else
    local cmode cowner
    cmode="$(_mode "$cfg")"
    cowner="$(_owner "$cfg")"
    if [[ "$cmode" == "644" && "$cowner" == "$agent_owner" ]]; then
      v_ok "config.yaml: $cmode $cowner"
    else
      v_drift "config.yaml" "mode=$cmode owner=$cowner (expected 644 $agent_owner)"
    fi
  fi

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

  # ---- repos[] schema + legacy-key rejection ----
  # validate-schema is the single source of truth for the migration error;
  # its stderr is what the operator sees. Capture both stdout/stderr so
  # the drift detail names the actual problem instead of "exit 1".
  local schema_err=""
  if [[ -f "$values_file" && -f "$values_helper" ]]; then
    if ! schema_err="$(python3 "$values_helper" --values "$values_file" validate-schema 2>&1)"; then
      v_drift "repos[] schema" "$(echo "$schema_err" | head -n3 | tr '\n' ' ')"
    else
      v_ok "repos[] schema valid"
    fi
  fi

  # ---- repos[] (code mirrors always; bare clones gated on quay) ----
  # Every entry produces a code mirror at $target/code/<id>/, agent-owned,
  # on origin/<base_branch>. Entries with a `quay:` block (non-empty
  # package_manager) additionally produce a bare clone at $target/quay/
  # repos/<id>.git/ + a quay registration. The list-repos invocation
  # surfaces helper-side schema errors (missing required fields, legacy
  # key, bad URL shape) as a `repos[] schema` drift, distinct from the
  # per-entry checks below.
  local code_root="$target/code"
  if [[ ! -d "$code_root" ]]; then
    v_drift "code mirrors root" "missing: $code_root"
  else
    local crowner
    crowner="$(_owner "$code_root")"
    if [[ "$crowner" == "$agent_owner" ]]; then
      v_ok "code mirrors root ownership: $crowner"
    else
      v_drift "code mirrors root ownership" "expected $agent_owner, got $crowner"
    fi
  fi

  local repos_tsv="" repos_tsv_failed=0
  if [[ -f "$values_file" && -f "$values_helper" ]]; then
    repos_tsv="$(python3 "$values_helper" --values "$values_file" list-repos 2>/dev/null)" || repos_tsv_failed=1
  fi
  if [[ "$repos_tsv_failed" -eq 1 ]]; then
    v_drift "repos[] schema" "values_helper.py list-repos exited non-zero (run it manually for details)"
  fi

  # ---- quay artefacts (gated on quay.version) ----
  # Binary + data dir + config.toml + repos_root install on every quay-
  # enabled host regardless of which entries (if any) carry a quay: block.
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
      # quay.version is a git tag (`v0.1.0`); the binary embeds
      # `${pkg.version}+${shortSHA}` (`0.1.0+abc1234`) — no `v` prefix.
      # Strip the leading `v` from the pin for the substring compare so a
      # clean release-built binary doesn't fire false drift.
      local pin_semver="${quay_version#v}"
      if [[ -n "$actual_version" ]] && grep -Fq "$pin_semver" <<<"$actual_version"; then
        v_ok "quay binary version: $actual_version"
      else
        v_drift "quay binary version" "got '${actual_version:-?}' (expected to contain '$pin_semver')"
      fi
    fi

    # Data dir + config.toml: agent-owned, mirrors the install-time
    # provisioning. config.toml is the rendered runtime config; its
    # absence is a drift, not a soft skip.
    if [[ ! -d "$quay_dir" ]]; then
      v_drift "quay data dir" "missing: $quay_dir"
    else
      # Owner-only, matching the agent-dir pattern at ~line 304. The data
      # dir inherits a setgid bit from the 02775 parent ($HERMES_HOME), so
      # exact-mode matching against 755 is broken on Linux even though
      # `install -d -m 755` was used. The security-meaningful invariant
      # is owner = agent (so quay can read/write), not the mode bits.
      local downer
      downer="$(_owner "$quay_dir")"
      if [[ "$downer" == "$agent_owner" ]]; then
        v_ok "quay data dir ownership: $downer"
      else
        v_drift "quay data dir ownership" "expected $agent_owner, got $downer"
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

    # No verify-side check on ~/.quay/ presence: the quay binary
    # re-creates that path as a workspace side effect on every
    # invocation, even with QUAY_DATA_DIR pinned elsewhere, so any
    # post-install snapshot finds it present again. Drift detection
    # for ad-hoc adds lives in the install-time reconciler, which
    # probes the dir's contents (registrations + tasks) rather than
    # its existence — see installer/setup-hermes.sh →
    # reconcile_stale_quay_dir.

    # ---- operator-invocation glue ----
    # All three paths are overridable so the test fixture can seed stubs
    # in tmp dirs instead of writing under /usr/local/{bin,sbin} or /etc.
    local quay_wrapper="${HERMES_VERIFY_QUAY_WRAPPER:-/usr/local/bin/quay-as-hermes}"
    if [[ -x "$quay_wrapper" ]]; then
      local wmode wowner
      wmode="$(_mode "$quay_wrapper")"
      wowner="$(_owner "$quay_wrapper")"
      if [[ "$wmode" == "755" && "$wowner" == "$rails_owner" ]]; then
        v_ok "quay-as-hermes wrapper: $wmode $wowner"
      else
        v_drift "quay-as-hermes wrapper" "mode=$wmode owner=$wowner (expected 755 $rails_owner)"
      fi
    else
      v_drift "quay-as-hermes wrapper" "missing or non-executable: $quay_wrapper"
    fi
    local quay_runner="${HERMES_VERIFY_QUAY_RUNNER:-/usr/local/sbin/quay-tick-runner}"
    if [[ -x "$quay_runner" ]]; then
      local rmode rowner
      rmode="$(_mode "$quay_runner")"
      rowner="$(_owner "$quay_runner")"
      # Same 755 root contract as the operator wrapper above — the unit's
      # ExecStart= dispatches through this script as the agent user;
      # an agent-writable copy would let the agent shim arbitrary code
      # into every tick.
      if [[ "$rmode" == "755" && "$rowner" == "$rails_owner" ]]; then
        v_ok "quay-tick-runner: $rmode $rowner"
      else
        v_drift "quay-tick-runner" "mode=$rmode owner=$rowner (expected 755 $rails_owner)"
      fi
    else
      v_drift "quay-tick-runner" "missing or non-executable: $quay_runner"
    fi
    local quay_profile="${HERMES_VERIFY_QUAY_PROFILE:-/etc/profile.d/quay-data-dir.sh}"
    if [[ -f "$quay_profile" ]]; then
      local pmode powner
      pmode="$(_mode "$quay_profile")"
      powner="$(_owner "$quay_profile")"
      # Login shells source this; an agent-writable copy would let the
      # agent override the canonical export with arbitrary env. Same
      # owner/mode contract as the wrapper above (mode 644 instead of
      # 755 — sourced, not executed).
      if [[ "$pmode" == "644" && "$powner" == "$rails_owner" ]]; then
        v_ok "quay profile.d drop-in: $pmode $powner"
      else
        v_drift "quay profile.d drop-in" "mode=$pmode owner=$powner (expected 644 $rails_owner)"
      fi
    else
      v_drift "quay profile.d drop-in" "missing: $quay_profile"
    fi
  fi

  # ---- per-entry code mirror checks + optional bare clone + registration ----
  # Iterates list-repos[]; the loop runs even when quay is disabled (code
  # mirrors are the always-on subsystem). Bare-clone and registration
  # sub-checks are conditional on (a) the entry carrying a quay: block
  # (non-empty repo_pkg) and (b) quay being enabled on this host.
  if [[ -n "$repos_tsv" ]]; then
    local registered_ids=""
    local list_failed=0
    if [[ -n "$quay_version" && -x "$quay_bin" && -d "$target/quay" ]]; then
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
      # Capture parse failure into list_failed instead of swallowing it
      # — a binary that crashes on `repo list` would otherwise produce
      # N misleading "not registered" drifts (one per quay-enabled entry)
      # when the real problem is a single broken pipeline.
      registered_ids="$(${cmd_prefix[@]+"${cmd_prefix[@]}"} env "QUAY_DATA_DIR=$target/quay" "$quay_bin" repo list 2>/dev/null \
        | python3 "$values_helper" parse-repo-list-ids 2>/dev/null)" || list_failed=1
    fi
    if [[ "$list_failed" -eq 1 ]]; then
      v_drift "quay repo list" "non-list/non-JSON output (binary crash, data dir corruption, or format drift)"
    fi
    local repo_id repo_url repo_base repo_pkg repo_install
    local code_dir mhead expected_head bare
    while IFS=$'\t' read -r repo_id repo_url repo_base repo_pkg repo_install; do
      [[ -z "$repo_id" ]] && continue

      # ---- code mirror checks (every entry) ----
      code_dir="$code_root/$repo_id"
      if [[ ! -d "$code_dir/.git" ]]; then
        v_drift "code mirror $repo_id" "missing or not a git repo: $code_dir"
      else
        _v_check_clone_basics "code mirror $repo_id" "$code_dir" "$repo_url" "$agent_owner"
        # HEAD must be on origin/<base_branch> (or rather, point at the
        # same commit). hermes-code-sync hard-resets to origin/<base>
        # every tick; a mismatch here means the timer hasn't run since
        # the install, which is itself worth surfacing.
        mhead="$(_git_as_owner "$code_dir" rev-parse HEAD 2>/dev/null || true)"
        expected_head="$(_git_as_owner "$code_dir" rev-parse "origin/$repo_base" 2>/dev/null || true)"
        if [[ -n "$mhead" && -n "$expected_head" && "$mhead" == "$expected_head" ]]; then
          v_ok "code mirror $repo_id at origin/$repo_base"
        else
          v_drift "code mirror $repo_id branch" "HEAD=${mhead:-?} expected origin/$repo_base=${expected_head:-?}"
        fi
      fi

      # ---- quay-side checks (entries with quay: block, gated on quay enabled) ----
      if [[ -n "$repo_pkg" && -n "$quay_version" ]]; then
        bare="$target/quay/repos/${repo_id}.git"
        if [[ ! -d "$bare" ]]; then
          v_drift "quay repo $repo_id" "bare clone missing: $bare"
        else
          _v_check_clone_basics "quay repo $repo_id" "$bare" "$repo_url" "$agent_owner"
        fi
        # Skip the per-id registration sub-check when the list pipeline
        # failed — the single named drift above already named the
        # actionable problem.
        if [[ "$list_failed" -eq 0 ]]; then
          if [[ -n "$registered_ids" ]] && grep -Fxq "$repo_id" <<<"$registered_ids"; then
            v_ok "quay repo $repo_id registered"
          else
            v_drift "quay repo $repo_id" "not registered with quay"
          fi
        fi
      elif [[ -n "$repo_pkg" && -z "$quay_version" ]]; then
        # Values file declares this entry quay-managed but quay isn't
        # enabled on this host — surface as drift so the operator either
        # pins quay.version or removes the per-entry quay: block.
        v_drift "quay repo $repo_id" "values file carries quay: block but quay.version is unset"
      fi
    done <<<"$repos_tsv"
  fi

  # ---- systemd units ----
  # Parse `systemctl show` output line-by-line and compare each field with
  # exact equality — substring matching ("active" in "*active*") quietly
  # accepts ActiveState=inactive, which is exactly the failure mode this
  # check is supposed to catch. See review of #10.
  if command -v systemctl >/dev/null 2>&1; then
    local u base unit_file so
    local active load unitfile
    local timers=(hermes-sync.timer hermes-code-sync.timer hermes-upstream-sync.timer)
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
      # `mint` doubles as the smoke check (succeeds iff a token can be
      # obtained) AND produces the token for the App-scope check below —
      # one subprocess instead of `check` then `mint`.
      local agent_cmd_prefix=()
      if [[ "$(id -u)" -eq 0 && "$(id -un)" != "$agent_owner" ]]; then
        agent_cmd_prefix=(sudo -u "$agent_owner")
      fi
      local app_token
      app_token="$(${agent_cmd_prefix[@]+"${agent_cmd_prefix[@]}"} \
        env "HERMES_HOME=$target" "$venv_py" "$helper_py" mint 2>/dev/null || true)"
      if [[ -n "$app_token" ]]; then
        v_ok "token helper check passes"
      else
        v_drift "token helper check" "mint failed"
      fi

      # Uses curl (not gh) so verify has no dependency on a gh install.
      # Auth header flows in through `curl --config -` via printf (a bash
      # builtin, no separate process), so $app_token never lands in any
      # argv visible through /proc/<pid>/cmdline.
      if [[ -n "$app_token" && -n "$repos_tsv" ]]; then
        local gh_api_base_v="${GH_API_BASE:-https://api.github.com}"
        local scope_id scope_url scope_pkg scope_base scope_install http_code
        while IFS=$'\t' read -r scope_id scope_url scope_base scope_pkg scope_install; do
          [[ -z "$scope_id" || -z "$scope_pkg" ]] && continue
          [[ "$scope_url" =~ ^https://github\.com/([^/]+)/([^/]+)$ ]] || continue
          local api_url="${gh_api_base_v}/repos/${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
          http_code="$(printf 'header = "Authorization: Bearer %s"\n' "$app_token" \
            | curl -sS -o /dev/null -w '%{http_code}' \
              --config - \
              -H "Accept: application/vnd.github+json" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              "$api_url" 2>/dev/null || echo 000)"
          if [[ "$http_code" == "200" ]]; then
            v_ok "App scope $scope_id: HTTP 200"
          else
            v_drift "App scope $scope_id" "$api_url returned HTTP $http_code"
          fi
        done <<<"$repos_tsv"
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

# quay-managed github.com entries require the App credential helper for HTTPS
# clone/push; SSH deploy-key rewrites are skipped for those entries. Fail fast
# before any provisioning so the operator doesn't half-install with an auth
# model that can't push.
_quay_gh_ids=()
while IFS=$'\t' read -r repo_id repo_url repo_base repo_pkg repo_install; do
  [[ -z "$repo_id" || -z "$repo_pkg" ]] && continue
  [[ "$repo_url" =~ ^https://github\.com/[^/]+/[^/]+$ ]] || continue
  _quay_gh_ids+=("$repo_id")
done < <(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repos 2>/dev/null || true)
if (( ${#_quay_gh_ids[@]} > 0 )) && [[ "$AUTH_METHOD" != "app" ]]; then
  echo "FAIL: repos[] has quay-managed github.com entries (${_quay_gh_ids[*]}) but --auth-method app was not passed." >&2
  echo "      These entries clone over HTTPS using the GitHub App credential helper." >&2
  echo "      Re-run with --auth-method app and the usual --app-* flags." >&2
  exit 2
fi

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

# ---------- operator-invocation glue (wrapper + profile.d) ----------
# Source files at ops/quay-as-hermes and ops/profile.d/quay-data-dir.sh
# carry the rationale; this block is just the templated install pair.

if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  QUAY_WRAPPER_SRC="$FORK_DIR/ops/quay-as-hermes"
  if [[ -f "$QUAY_WRAPPER_SRC" ]]; then
    echo "==> installing /usr/local/bin/quay-as-hermes wrapper"
    sed -e "s|__AGENT_USER__|$AGENT_USER|g" \
        -e "s|__TARGET_DIR__|$TARGET_DIR|g" \
        "$QUAY_WRAPPER_SRC" \
      | install -o root -g root -m 0755 /dev/stdin /usr/local/bin/quay-as-hermes
  else
    echo "==> WARNING: $QUAY_WRAPPER_SRC missing; skipping quay-as-hermes wrapper" >&2
  fi

  QUAY_PROFILE_SRC="$FORK_DIR/ops/profile.d/quay-data-dir.sh"
  if [[ -f "$QUAY_PROFILE_SRC" ]]; then
    echo "==> installing /etc/profile.d/quay-data-dir.sh"
    install -d -o root -g root -m 0755 /etc/profile.d
    sed -e "s|__AGENT_USER__|$AGENT_USER|g" \
        -e "s|__TARGET_DIR__|$TARGET_DIR|g" \
        "$QUAY_PROFILE_SRC" \
      | install -o root -g root -m 0644 /dev/stdin /etc/profile.d/quay-data-dir.sh
  else
    echo "==> WARNING: $QUAY_PROFILE_SRC missing; skipping profile.d drop-in" >&2
  fi
fi

# ---------- claude CLI prerequisite check ----------
# Fail loud here (before any user-side provisioning) rather than letting
# the agent invoke fail with a cryptic "command not found" hours later.
if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  agent_invocation="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get quay.agent_invocation)"
  if [[ "$agent_invocation" == *claude* ]]; then
    if ! sudo -u "$AGENT_USER" -H bash -c 'command -v claude' >/dev/null 2>&1; then
      echo "FAIL: quay.agent_invocation references 'claude' but the claude binary is not on PATH for $AGENT_USER" >&2
      echo "      Install it (as $AGENT_USER) before re-running setup-hermes.sh:" >&2
      echo "        sudo -u $AGENT_USER -H bash -c 'curl -fsSL https://claude.ai/install.sh | bash'" >&2
      echo "        sudo ln -sf ~$AGENT_USER/.local/bin/claude /usr/local/bin/claude" >&2
      echo "        sudo -u $AGENT_USER -H claude login" >&2
      echo "      See ops/README.md → 'Pre-install: claude CLI' for details." >&2
      exit 1
    fi
  fi
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
fi

# model.* is rewritten on every run, even when the file above was preserved
# — the helper docstring covers the rationale (silent `hermes auth add`
# failures leaving the pin drifted).
config_sha_pre="$(file_sha "$CONFIG_YAML_OUT")"
echo "==> merging gateway.model_* into $CONFIG_YAML_OUT from $VALUES_FILE"
python3 "$VALUES_HELPER" --values "$VALUES_FILE" \
  merge-config-model --out "$CONFIG_YAML_OUT"
# config.yaml must be agent-writable: `hermes auth add` rewrites
# model.provider after the OAuth flow, and `hermes model` does the same on
# the interactive picker. Root-owned silently no-ops those writes.
# merge-config-model always runs (even on preserved files), so this single
# chown self-heals legacy root-owned hosts on the next install.
chown "$AGENT_USER:$AGENT_USER" "$CONFIG_YAML_OUT"
chmod 0644 "$CONFIG_YAML_OUT"
[[ "$config_sha_pre" != "$(file_sha "$CONFIG_YAML_OUT")" ]] && GATEWAY_NEEDS_RESTART=1

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
#   github-app.pem      — the GitHub App private key (mode 0640, root:hermes)
#   github-app.env      — config consumed by hermes_github_token.py
#   gateway-runtime.env — non-secret env vars derived from deploy.values.yaml
#                         (SLACK_ALLOWED_USERS, …). Rewritten every run so
#                         values.yaml stays the single source of truth.
#   slack.env / hermes.env — staged out-of-band by stage-secrets.sh.
#
# auth/ is mode 0750 root:hermes — agent (group member) can read every file
# but can't write any of them, and the rails (root) own provisioning. The
# token cache lives in $TARGET/cache/ (agent-writable) so this dir stays
# read-only to it.

AUTH_DIR="$TARGET_DIR/auth"
GH_APP_KEY_DST="$AUTH_DIR/github-app.pem"
GH_APP_ENV="$AUTH_DIR/github-app.env"
GATEWAY_RUNTIME_ENV="$AUTH_DIR/gateway-runtime.env"
TOKEN_HELPER_PY="$TARGET_DIR/hermes-agent/installer/hermes_github_token.py"
VENV_PY="$TARGET_DIR/hermes-agent/venv/bin/python"
HERMES_BIN="$TARGET_DIR/hermes-agent/venv/bin/hermes"

# auth/ is created here regardless of --auth-method because gateway-runtime.env
# (values-derived, written below) lives in it. Strip setgid that propagates
# from $TARGET_DIR's setgid bit on Linux (System V dir semantics) — auth/
# has a strict mode-750 invariant, so files inside are explicitly chmod'd
# without group inheritance. `install -m 0750` doesn't reliably clear the
# bit on coreutils we've seen in CI; `g-s` is unambiguous.
install -d -o root -g "$AGENT_USER" -m 0750 "$AUTH_DIR"
chmod g-s "$AUTH_DIR"

# Values-derived runtime env file. Rewritten every run — values.yaml is the
# source of truth; the file is a reflection. Operator hand-edits belong in
# /etc/default/hermes-gateway, not here.
#
# GATEWAY_NEEDS_RESTART accumulates content drift across this and the two
# other env-affecting writes below (config.yaml model merge, runtime-env
# drop-in). systemd doesn't re-read EnvironmentFile= for a running process
# on `daemon-reload`, so we have to restart explicitly when content
# actually changed.
GATEWAY_NEEDS_RESTART=0
runtime_sha_pre="$(file_sha "$GATEWAY_RUNTIME_ENV")"
echo "==> rendering $GATEWAY_RUNTIME_ENV from $VALUES_FILE"
python3 "$VALUES_HELPER" --values "$VALUES_FILE" \
  render-gateway-runtime-env --out "$GATEWAY_RUNTIME_ENV"
chown root:"$AGENT_USER" "$GATEWAY_RUNTIME_ENV"
chmod 0640 "$GATEWAY_RUNTIME_ENV"
[[ "$runtime_sha_pre" != "$(file_sha "$GATEWAY_RUNTIME_ENV")" ]] && GATEWAY_NEEDS_RESTART=1

if [[ "$AUTH_METHOD" == "app" ]]; then
  echo "==> wiring GitHub App auth at $AUTH_DIR"

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

  # Persist App identity. Resolution order: CLI flag > deploy.values.yaml >
  # existing env file (legacy fallback for installs from before values.yaml
  # carried these IDs). values.yaml is the durable home for the IDs (public
  # identifiers — the PEM is the real secret); CLI flags stay as ad-hoc
  # overrides for staging. Extract from the env file via awk rather than
  # `source`, because `source` evaluates the values in our shell —
  # `--app-id 'foo$(rm -rf /)'` would be game-over on the next install.
  APP_ID="${APP_ID:-$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get auth.github_app.id 2>/dev/null || true)}"
  APP_INSTALLATION_ID="${APP_INSTALLATION_ID:-$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get auth.github_app.installation_id 2>/dev/null || true)}"
  GH_API_BASE="${GH_API_BASE:-$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get auth.github_app.api_base 2>/dev/null || true)}"
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
# Skipped entirely when quay isn't enabled — the unified repos[] loop
# below provisions code mirrors regardless and only runs the quay-side
# branch when both QUAY_ENABLED and the entry's `quay:` block are present.
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

  install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0755 "$TARGET_DIR/quay/repos"
fi

# ---------- repo provisioning (code mirrors + optional quay) ----------
# Every entry in repos[] gets a working-tree code mirror at
# $TARGET/code/<id>/. Entries with a `quay:` block additionally get a
# bare clone + `quay repo add` registration when quay is enabled; a
# `quay:` block with quay disabled produces a code mirror plus a warning.
#
# Auth bifurcation:
#   quay-managed github.com entries — clones go over HTTPS with the App
#     credential helper (-c credential.https://github.com.helper=…) so
#     the App's GitHub UI repo scope is the only operator-side toggle;
#     no url.insteadOf rewrite or deploy key required.
#   code-only entries (no quay: block) — per-repo url.insteadOf rewrites
#     land in ~hermes/.gitconfig, bridging the HTTPS values URL to SSH
#     against the deploy keys staged by stage-repo-auth.sh.

CODE_ROOT="$TARGET_DIR/code"
install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0755 "$CODE_ROOT"

ALL_REPOS_TSV="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repos)"

# ---------- stale ~/.quay/ cleanup + drift refusal ----------
# Empty/equivalent stale dirs are removed declaratively (configs-as-code,
# no opt-in flag); stale dirs holding tasks or non-declared registrations
# refuse the install with a remediation hint. Runs BEFORE any quay
# invocation: the binary creates ~/.quay/ as a side effect (workspace
# cache) even with QUAY_DATA_DIR set, so reconciling after a snapshot
# would loop on freshly-side-effected state and risks corrupting
# canonical when the post-install tick fires against a torn-down dir.

reconcile_stale_quay_dir() {
  local stale_dir="$AGENT_HOME/.quay"
  # `rm -rf` on a symlink unlinks the link, not its target — safe even
  # if an operator pointed it at the canonical dir as a workaround.
  [[ -e "$stale_dir" || -L "$stale_dir" ]] || return 0

  echo "==> detected stale $stale_dir (pre-wrapper ad-hoc invocations)"

  # Symlinks (and any other non-directory) carry no independent state —
  # just unlink.
  if [[ ! -d "$stale_dir" || -L "$stale_dir" ]]; then
    echo "==> $stale_dir is not a real data dir; removing"
    rm -rf "$stale_dir"
    return 0
  fi

  if [[ ! -f "$stale_dir/quay.db" ]]; then
    echo "==> $stale_dir has no quay.db; removing"
    rm -rf "$stale_dir"
    return 0
  fi

  # Probe the stale DB via the binary, not raw SQLite — the schema is
  # quay's to define, and the binary is the only thing that knows the
  # current shape. Run as the agent (dir is agent-owned) with
  # QUAY_DATA_DIR pinned to stale, so reads target THAT db. Both probes
  # capture pipeline failure into probe_failed (refuse-on-uncertainty);
  # the parser's stderr is intentionally not silenced so a future format
  # drift surfaces a "values_helper.py: …" diagnostic to the operator.
  local stale_repo_ids stale_task_count probe_failed=0
  if ! stale_repo_ids="$(sudo -u "$AGENT_USER" \
        env QUAY_DATA_DIR="$stale_dir" "$QUAY_BIN_DST" repo list 2>/dev/null \
        | python3 "$VALUES_HELPER" parse-repo-list-ids)"; then
    probe_failed=1
    stale_repo_ids=""
  fi
  if ! stale_task_count="$(sudo -u "$AGENT_USER" \
        env QUAY_DATA_DIR="$stale_dir" "$QUAY_BIN_DST" task list 2>/dev/null \
        | python3 "$VALUES_HELPER" parse-task-list-count)"; then
    probe_failed=1
    stale_task_count=0
  fi

  # Expected = values-file repos[]. Configs-as-code: declared state is
  # the source of truth, so stale ids outside it are undeclared ad-hoc
  # adds the operator must reconcile (declare or discard) before we
  # can safely remove the stale dir.
  local expected_ids=""
  [[ -n "$ALL_REPOS_TSV" ]] && expected_ids="$(cut -f1 <<<"$ALL_REPOS_TSV")"
  local stale_extra_ids=""
  while IFS= read -r sid; do
    [[ -z "$sid" ]] && continue
    grep -Fxq "$sid" <<<"$expected_ids" || stale_extra_ids+="$sid"$'\n'
  done <<<"$stale_repo_ids"
  stale_extra_ids="${stale_extra_ids%$'\n'}"

  if [[ "$probe_failed" -eq 0 && "$stale_task_count" -eq 0 && -z "$stale_extra_ids" ]]; then
    echo "==> $stale_dir is empty/equivalent to canonical; removing"
    rm -rf "$stale_dir"
    return 0
  fi

  {
    echo "FAIL: $stale_dir holds data not accounted for in $TARGET_DIR/quay/."
    if [[ "$probe_failed" -eq 1 ]]; then
      echo "      could not probe stale DB via \`quay {repo,task} list\`"
      echo "      (refuse-on-uncertainty: cannot safely drop unknown state)"
    elif [[ "$stale_task_count" -gt 0 ]]; then
      echo "      tasks in stale DB: $stale_task_count"
    fi
    if [[ -n "$stale_extra_ids" ]]; then
      echo "      registrations in stale DB not declared in deploy.values.yaml repos[]"
      echo "      and not present in canonical:"
      while IFS= read -r sid; do
        [[ -n "$sid" ]] && echo "        - $sid"
      done <<<"$stale_extra_ids"
    fi
    echo
    echo "      Inspect with the same env the stale dir was created against:"
    echo "        sudo -u $AGENT_USER env QUAY_DATA_DIR=$stale_dir \\"
    echo "          $QUAY_BIN_DST repo list"
    echo "        sudo -u $AGENT_USER env QUAY_DATA_DIR=$stale_dir \\"
    echo "          $QUAY_BIN_DST task list"
    echo "      Then either declare the registrations in deploy.values.yaml"
    echo "      repos[] (and let the next install pick them up), or discard"
    echo "      them by removing $stale_dir, and re-run the installer."
  } >&2
  exit 1
}

if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  reconcile_stale_quay_dir
fi

# Snapshot quay's already-registered ids ONCE per install, before the
# per-repo loop, so re-runs of `quay repo add` are no-ops. A fresh data
# dir applies embedded migrations on first invocation and returns [];
# parse failure aborts the install — silently treating it as "no
# registrations" would re-invoke `quay repo add` on every re-run, which
# is not documented as idempotent. Runs AFTER reconcile_stale_quay_dir
# because the binary creates ~/.quay/ as a side effect when invoked,
# and we don't want our own snapshot to leave drift the next reconciler
# pass would have to clean up.
QUAY_REGISTERED_IDS=""
if [[ "$QUAY_ENABLED" -eq 1 ]]; then
  QUAY_REGISTERED_IDS="$(
    sudo -u "$AGENT_USER" \
      env QUAY_DATA_DIR="$TARGET_DIR/quay" "$QUAY_BIN_DST" repo list \
      | python3 "$VALUES_HELPER" parse-repo-list-ids
  )"
fi

if [[ -n "$ALL_REPOS_TSV" ]]; then
  AGENT_GITCONFIG="$AGENT_HOME/.gitconfig"

  while IFS=$'\t' read -r repo_id repo_url repo_base repo_pkg repo_install; do
    [[ -z "$repo_id" ]] && continue

    # ---- per-repo url.insteadOf rewrite (code-only github.com entries) ----
    # quay-managed github.com entries skip the SSH rewrite and use the App
    # credential helper instead. For code-only entries the rewrite bridges
    # the HTTPS values URL to SSH against the deploy key.
    # Two unrelated traps avoided:
    #   * git always stats CWD as part of repo discovery (even with --file
    #     and --global). When CWD is outside the agent user's read scope,
    #     the stat fails and git aborts. Subshell-cd to $AGENT_HOME so
    #     hermes inherits a CWD it can stat.
    #   * `--file` is used instead of `--global` because under sudo,
    #     $HOME and $XDG_CONFIG_HOME may point at the caller's home
    #     regardless of -H — `--global` then reads/writes the wrong file.
    if [[ "$repo_url" =~ ^https://github\.com/([^/]+)/([^/]+)$ ]]; then
      org="${BASH_REMATCH[1]}"
      repo_short="${BASH_REMATCH[2]}"
      ssh_url="git@github.com:${org}/${repo_short}.git"
      sudo -u "$AGENT_USER" touch "$AGENT_GITCONFIG"
      if [[ -z "$repo_pkg" ]]; then
        ( cd "$AGENT_HOME" && \
          sudo -u "$AGENT_USER" git config --file "$AGENT_GITCONFIG" \
            "url.${ssh_url}.insteadOf" "$repo_url" )
      else
        # git applies url.insteadOf before credential lookup, so a stale
        # rewrite from a pre-consolidation install would silently shadow
        # the App helper and keep pushing via the deploy key. Exit 5 =
        # "key not present"; anything else is a real failure (perm denied,
        # malformed gitconfig) and shouldn't be swallowed.
        unset_rc=0
        ( cd "$AGENT_HOME" && \
          sudo -u "$AGENT_USER" git config --file "$AGENT_GITCONFIG" \
            --unset-all "url.${ssh_url}.insteadOf" ) || unset_rc=$?
        if (( unset_rc != 0 && unset_rc != 5 )); then
          echo "FAIL: clearing stale url.insteadOf for $repo_id failed (git config exit $unset_rc)" >&2
          exit 1
        fi
      fi
    fi

    # ---- code mirror at $TARGET/code/<id>/ ----
    code_dir="$CODE_ROOT/$repo_id"
    if [[ -d "$code_dir/.git" ]]; then
      _check_clone_origin_or_die "code mirror" "$code_dir" "$repo_url"
      echo "==> code mirror $repo_id present at $code_dir (preserving)"
    else
      echo "==> cloning $repo_url into $code_dir"
      if [[ -n "$repo_pkg" && -n "$GIT_CRED_HELPER" && "$repo_url" =~ ^https://github\.com/ ]]; then
        sudo -u "$AGENT_USER" \
          env HERMES_HOME="$TARGET_DIR" \
          git -c "credential.https://github.com.helper=$GIT_CRED_HELPER" \
            clone --quiet --branch "$repo_base" "$repo_url" "$code_dir"
      else
        sudo -u "$AGENT_USER" \
          git clone --quiet --branch "$repo_base" "$repo_url" "$code_dir"
      fi
    fi
    # Persist the credential helper into the code mirror's .git/config for
    # subsequent fetches by the hermes-code-sync timer.
    if [[ -n "$repo_pkg" && -n "$GIT_CRED_HELPER" && "$repo_url" =~ ^https://github\.com/ ]]; then
      sudo -u "$AGENT_USER" git -C "$code_dir" \
        config credential.https://github.com.helper "$GIT_CRED_HELPER"
    fi

    # ---- bare clone + quay registration (entries with `quay:` block) ----
    if [[ -n "$repo_pkg" ]]; then
      if [[ "$QUAY_ENABLED" -eq 0 ]]; then
        echo "==> WARNING: $repo_id carries a quay: block but quay.version is unset; skipping bare clone + registration" >&2
      else
        bare="$TARGET_DIR/quay/repos/${repo_id}.git"
        if [[ -d "$bare" ]]; then
          _check_clone_origin_or_die "bare clone" "$bare" "$repo_url"
          echo "==> quay bare clone $repo_id present (preserving)"
        else
          echo "==> cloning $repo_url into $bare"
          if [[ -n "$GIT_CRED_HELPER" && "$repo_url" =~ ^https://github\.com/ ]]; then
            sudo -u "$AGENT_USER" \
              env HERMES_HOME="$TARGET_DIR" \
              git -c "credential.https://github.com.helper=$GIT_CRED_HELPER" \
                clone --quiet --bare "$repo_url" "$bare"
          else
            sudo -u "$AGENT_USER" \
              git clone --quiet --bare "$repo_url" "$bare"
          fi
        fi
        # Persist the credential helper into the bare clone's config so
        # worker git push from worktrees authenticates without per-spawn env.
        if [[ -n "$GIT_CRED_HELPER" && "$repo_url" =~ ^https://github\.com/ ]]; then
          sudo -u "$AGENT_USER" git -C "$bare" \
            config credential.https://github.com.helper "$GIT_CRED_HELPER"
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
      fi
    fi
  done <<<"$ALL_REPOS_TSV"
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

# ---------- hermes-code-sync ----------
# Periodic refresh of the working-tree code mirrors at $TARGET/code/<id>/.
# Independent of quay-tick (which keeps bare clones fresh on its own
# cadence). The script iterates every direct subdirectory of $TARGET/code
# with a `.git` and runs `git fetch + git reset --hard origin/<base>`,
# warn-and-continue on per-repo failure. Same install model as hermes-
# sync: root-owned script + sed-templated service + preserved
# /etc/default/ env.

CODE_SYNC_SCRIPT_SRC="$OPS_DIR/hermes-code-sync"
CODE_SYNC_SCRIPT_DST="/usr/local/sbin/hermes-code-sync"

if [[ -f "$CODE_SYNC_SCRIPT_SRC" ]]; then
  echo "==> installing hermes-code-sync at $CODE_SYNC_SCRIPT_DST"
  install -o root -g root -m 0755 "$CODE_SYNC_SCRIPT_SRC" "$CODE_SYNC_SCRIPT_DST"

  if [[ -f /etc/default/hermes-code-sync ]]; then
    echo "==> /etc/default/hermes-code-sync already present (preserving)"
  else
    echo "==> seeding /etc/default/hermes-code-sync"
    # Pin values-file/helper paths so every tick reads the same source the
    # installer was driven from, regardless of operator CWD.
    cat >/etc/default/hermes-code-sync <<EOF
# Generated by setup-hermes.sh — overrides for hermes-code-sync.service.
# Edits here survive re-runs of the installer; delete this file to force
# regeneration from defaults.
HERMES_CODE_DIR=$CODE_ROOT
HERMES_VALUES_FILE=$VALUES_FILE
HERMES_VALUES_HELPER=$VALUES_HELPER
EOF
    chown root:root /etc/default/hermes-code-sync
    chmod 0644 /etc/default/hermes-code-sync
  fi

  if command -v systemctl >/dev/null 2>&1; then
    echo "==> installing systemd timer for hermes-code-sync (user=$AGENT_USER)"
    sed "s|__AGENT_USER__|$AGENT_USER|g" \
      "$OPS_DIR/hermes-code-sync.service" \
      | install -o root -g root -m 0644 /dev/stdin /etc/systemd/system/hermes-code-sync.service
    install -o root -g root -m 0644 \
      "$OPS_DIR/hermes-code-sync.timer" /etc/systemd/system/hermes-code-sync.timer
    systemctl daemon-reload
    systemctl enable --now hermes-code-sync.timer
  else
    echo "==> systemctl not present; skipping hermes-code-sync timer enable" >&2
  fi
else
  echo "==> WARNING: $CODE_SYNC_SCRIPT_SRC missing; skipping hermes-code-sync install" >&2
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
# templated unit, /etc/default/ env preserved across re-runs, plus a
# companion script at /usr/local/sbin/quay-tick-runner that mints
# $GH_TOKEN before exec'ing `quay tick` so the worker pipeline's `gh`
# calls authenticate against the App identity.
#
# Skipped entirely when quay isn't enabled — see the binary block at the
# top of the script.

QUAY_TICK_SRC="$OPS_DIR/quay-tick.service"
QUAY_TICK_RUNNER_SRC="$OPS_DIR/quay-tick-runner"
QUAY_TICK_RUNNER_DST="/usr/local/sbin/quay-tick-runner"

if [[ "$QUAY_ENABLED" -eq 1 && -f "$QUAY_TICK_SRC" && -f "$QUAY_TICK_RUNNER_SRC" ]]; then
  echo "==> installing quay-tick-runner at $QUAY_TICK_RUNNER_DST"
  install -o root -g root -m 0755 "$QUAY_TICK_RUNNER_SRC" "$QUAY_TICK_RUNNER_DST"

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
# quay.env (staged by stage-secrets.sh).
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
elif [[ "$QUAY_ENABLED" -eq 1 ]]; then
  [[ -f "$QUAY_TICK_SRC" ]] \
    || echo "==> WARNING: $QUAY_TICK_SRC missing; skipping quay-tick install" >&2
  [[ -f "$QUAY_TICK_RUNNER_SRC" ]] \
    || echo "==> WARNING: $QUAY_TICK_RUNNER_SRC missing; skipping quay-tick install" >&2
fi

# ---------- hermes-gateway ----------
# The canonical unit is generated by `hermes gateway install --system`
# and refreshed by `hermes update`; we layer our additions via a systemd
# drop-in. See ops/README.md for the rationale.

GATEWAY_DROPIN_SRC="$OPS_DIR/hermes-gateway.service.d/slack-env.conf"
GATEWAY_HERMES_DROPIN_SRC="$OPS_DIR/hermes-gateway.service.d/hermes-env.conf"
# z- prefix is load-order sensitive — see ops/hermes-gateway.service.d/z-runtime-env.conf.
GATEWAY_RUNTIME_DROPIN_SRC="$OPS_DIR/hermes-gateway.service.d/z-runtime-env.conf"
GATEWAY_DROPIN_DIR="/etc/systemd/system/hermes-gateway.service.d"
GATEWAY_DROPIN_DST="$GATEWAY_DROPIN_DIR/slack-env.conf"
GATEWAY_HERMES_DROPIN_DST="$GATEWAY_DROPIN_DIR/hermes-env.conf"
GATEWAY_RUNTIME_DROPIN_DST="$GATEWAY_DROPIN_DIR/z-runtime-env.conf"
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

    echo "==> installing hermes-env drop-in at $GATEWAY_HERMES_DROPIN_DST"
    sed -e "s|__TARGET_DIR__|$TARGET_DIR|g" "$GATEWAY_HERMES_DROPIN_SRC" \
      | install -o root -g root -m 0644 /dev/stdin "$GATEWAY_HERMES_DROPIN_DST"

    # First-time install of z-runtime-env.conf on an existing host counts as
    # an env-source change for the running gateway — track it so the restart
    # below picks up the new EnvironmentFile= line.
    z_dropin_sha_pre="$(file_sha "$GATEWAY_RUNTIME_DROPIN_DST")"
    echo "==> installing runtime-env drop-in at $GATEWAY_RUNTIME_DROPIN_DST"
    sed -e "s|__TARGET_DIR__|$TARGET_DIR|g" "$GATEWAY_RUNTIME_DROPIN_SRC" \
      | install -o root -g root -m 0644 /dev/stdin "$GATEWAY_RUNTIME_DROPIN_DST"
    [[ "$z_dropin_sha_pre" != "$(file_sha "$GATEWAY_RUNTIME_DROPIN_DST")" ]] && GATEWAY_NEEDS_RESTART=1

    systemctl daemon-reload

    # Capture the gateway's pre-install activity so we can decide whether a
    # restart is actually warranted. `enable --now` on an inactive unit
    # starts it fresh with the new EnvironmentFile= already loaded — no
    # restart needed. An already-active gateway picks up no env changes
    # from `daemon-reload` alone (it only refreshes systemd's view of unit
    # definitions, not the running process's environment), so we have to
    # `try-restart` it explicitly when env/config content actually changed.
    gateway_was_active=0
    systemctl is-active hermes-gateway.service >/dev/null 2>&1 && gateway_was_active=1

    if [[ -f "$GATEWAY_SLACK_ENV" ]]; then
      systemctl enable --now hermes-gateway.service
      if (( GATEWAY_NEEDS_RESTART && gateway_was_active )); then
        echo "==> gateway env/config content changed; restarting hermes-gateway.service"
        systemctl try-restart hermes-gateway.service
        systemctl --no-pager --lines=0 status hermes-gateway.service || true
      fi
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
