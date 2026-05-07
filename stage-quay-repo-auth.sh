#!/usr/bin/env bash
# Stage SSH deploy-key material for the quay worker on krustentier.
# Run as root (or via sudo). Idempotent — re-runs skip key generation and
# converge ownership/permissions without clobbering anything.
#
# What it does:
#   1. Generates ~hermes/.ssh/id_ed25519 if absent (no passphrase).
#   2. Pre-seeds ~hermes/.ssh/known_hosts with github.com host keys.
#   3. Removes any legacy org-wide url.insteadOf rewrite
#      (`url.git@github.com:<org>/.insteadOf`) — that form swallowed
#      hermes-state and the fork on the same host, breaking hermes-sync
#      on krustentier (ITRY-1315). The migration runs unconditionally.
#   4. Reads quay.repos[] from VALUES_FILE and wires one narrow per-repo
#      git rewrite (HTTPS → SSH) for the agent user — keyed on
#      `url.git@github.com:<org>/<repo>.insteadOf` so hermes-state and
#      other InverterNetwork repos keep using HTTPS via the GitHub App
#      credential helper.
#   5. Prints the public key so the operator can add it to each repo's
#      GitHub deploy-key UI.
#
# Environment overrides (useful in CI):
#   AGENT_USER   — defaults to hermes
#   VALUES_FILE  — defaults to the deploy.values.yaml next to this script
#                  (or $HERMES_VALUES_FILE if set). Override to point at a
#                  different YAML fixture without touching the live file.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_USER="${AGENT_USER:-hermes}"
VALUES_FILE="${VALUES_FILE:-${HERMES_VALUES_FILE:-$SCRIPT_DIR/deploy.values.yaml}}"
VALUES_HELPER="$SCRIPT_DIR/installer/values_helper.py"

[[ "$(id -u)" -eq 0 ]] || { echo "must run as root (try: sudo $0)" >&2; exit 1; }
id "$AGENT_USER" >/dev/null 2>&1 || { echo "user '$AGENT_USER' does not exist" >&2; exit 1; }
[[ -f "$VALUES_FILE" ]] || { echo "values file not found: $VALUES_FILE" >&2; exit 1; }
[[ -f "$VALUES_HELPER" ]] || { echo "values helper not found: $VALUES_HELPER" >&2; exit 1; }

AGENT_HOME="$(getent passwd "$AGENT_USER" | cut -d: -f6)"
SSH_DIR="$AGENT_HOME/.ssh"
KEY_FILE="$SSH_DIR/id_ed25519"
PUB_FILE="$SSH_DIR/id_ed25519.pub"
KNOWN_HOSTS="$SSH_DIR/known_hosts"

# ---------- SSH directory ----------

install -d -o "$AGENT_USER" -g "$AGENT_USER" -m 0700 "$SSH_DIR"

# ---------- Generate key (once) ----------

if [[ -f "$KEY_FILE" ]]; then
  echo "ℹ $KEY_FILE already exists — skipping key generation"
else
  sudo -u "$AGENT_USER" ssh-keygen -t ed25519 -N "" \
    -C "${AGENT_USER}@$(hostname -s) (quay deploy key)" \
    -f "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  chmod 644 "$PUB_FILE"
  chown "$AGENT_USER:$AGENT_USER" "$KEY_FILE" "$PUB_FILE"
  echo "✓ generated $KEY_FILE"
fi

# ---------- known_hosts: seed github.com host keys ----------
# Append only lines not already present — avoids churn on re-runs without
# requiring the operator to pre-wipe the file. ssh-keyscan output is
# deterministic for a given host; sort -u on the combined set deduplicates.

touch "$KNOWN_HOSTS"
chown "$AGENT_USER:$AGENT_USER" "$KNOWN_HOSTS"
chmod 644 "$KNOWN_HOSTS"

# Fetch current github.com keys into a temp file; merge with existing.
# stderr is left unredirected so a firewall/DNS/outbound-22 failure surfaces
# directly to the operator instead of dying silently under `set -e`.
GH_KEYS_TMP="$(mktemp)"
trap 'rm -f "$GH_KEYS_TMP"' EXIT
ssh-keyscan -t ed25519,rsa,ecdsa github.com > "$GH_KEYS_TMP"

# Combine existing + new, deduplicate, write back as the agent user so
# ownership stays correct even if root writes the temp file above.
sort -u "$KNOWN_HOSTS" "$GH_KEYS_TMP" | sudo -u "$AGENT_USER" tee "$KNOWN_HOSTS" > /dev/null
echo "✓ updated $KNOWN_HOSTS"

# ---------- url.insteadOf rewrites (HTTPS → SSH per repo) ----------
# Two passes:
#   1. MIGRATION: scan the agent's gitconfig for any legacy org-wide
#      `url.git@github.com:<org>/.insteadOf` key and unset it. That form
#      captured every InverterNetwork repo on the box (including hermes-state,
#      which authenticates over HTTPS via the GitHub App credential helper),
#      breaking hermes-sync on krustentier (ITRY-1315). The migration runs
#      unconditionally — even if quay.repos is empty/absent — because the
#      legacy entry is broken regardless of whether we're (re-)provisioning.
#   2. INSTALL: write one narrow per-repo rewrite per quay.repos[] entry,
#      keyed on `url.git@github.com:<org>/<repo>.insteadOf`. Git applies
#      insteadOf by longest-prefix match, so the same key catches both
#      `https://github.com/<org>/<repo>` and `…/<repo>.git` clone URLs.
#
# Two unrelated traps avoided in the git invocations below:
#   * git always stats CWD as part of repo discovery (even with --file
#     and --global). When CWD is outside the agent user's read scope
#     (CI: $GITHUB_WORKSPACE owned by `runner`; on-box: any operator
#     CWD they chose), the stat fails and git aborts. Subshell-cd to
#     $AGENT_HOME so hermes inherits a CWD it can stat.
#   * `--file` is used instead of `--global` because under sudo,
#     $HOME and $XDG_CONFIG_HOME may point at the caller's home
#     regardless of -H — `--global` then reads/writes the wrong file
#     (observed: a stray /home/runner/.config/git/config tripped the
#     read side even when the write landed correctly).

GITCONFIG="$AGENT_HOME/.gitconfig"
sudo -u "$AGENT_USER" touch "$GITCONFIG"

# ---- Pass 1: migrate legacy org-wide rewrites ----
# `--get-regexp` on the key namespace yields one line per matching
# key/value pair: `<full-key> <value>`. A multi-valued key (same key
# repeated with two values, possible via manual edits or merged
# configs) emits the same key twice. `sort -u` on the awk output
# ensures we don't iterate it twice — the first `--unset-all`
# already removes every value, and a second pass on a now-absent
# key exits 5 and aborts under `set -e`.
#
# The regex matches exactly the org-wide form: `<org>` segment followed
# by a single trailing `/` (no embedded slash), then `.insteadof`. The
# per-repo form (`<org>/<repo>.insteadof`) doesn't match because the
# repo segment contains no further `/`. NB: git canonicalises the
# variable name (`insteadOf` → `insteadof`) in `--get-regexp` output,
# so the regex must use lowercase to match.
LEGACY_KEYS="$( \
  cd "$AGENT_HOME" && \
  sudo -u "$AGENT_USER" git config --file "$GITCONFIG" --get-regexp \
    '^url\.git@github\.com:[^/]+/\.insteadof$' 2>/dev/null \
    | awk '{print $1}' | sort -u \
    || true \
)"
if [[ -n "$LEGACY_KEYS" ]]; then
  while IFS= read -r key; do
    [[ -z "$key" ]] && continue
    ( cd "$AGENT_HOME" && \
      sudo -u "$AGENT_USER" git config --file "$GITCONFIG" --unset-all "$key" )
    echo "✓ migrated: removed legacy org-wide rewrite ($key)"
  done <<< "$LEGACY_KEYS"
fi

# ---- Pass 2: write per-repo rewrites ----
QUAY_VERSION="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get quay.version 2>/dev/null || true)"

if [[ -z "$QUAY_VERSION" ]]; then
  echo "ℹ quay.version not set in $VALUES_FILE — skipping url.insteadOf rewrites"
else
  REWRITES="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repo-rewrites)"
  if [[ -z "$REWRITES" ]]; then
    echo "ℹ no quay.repos entries found — skipping url.insteadOf rewrites"
  else
    while IFS=$'\t' read -r org repo; do
      [[ -z "$org" || -z "$repo" ]] && continue
      # Idempotent — `git config` overwrites a duplicate key.
      ( cd "$AGENT_HOME" && \
        sudo -u "$AGENT_USER" git config --file "$GITCONFIG" \
          "url.git@github.com:${org}/${repo}.insteadOf" \
          "https://github.com/${org}/${repo}" )
      echo "✓ git insteadOf: https://github.com/${org}/${repo} → git@github.com:${org}/${repo}"
    done <<< "$REWRITES"
  fi
fi

# ---------- Print public key for deploy-key registration ----------

echo ""
echo "Add this to each repo's deploy-key UI (Settings → Deploy keys → Add deploy key):"
echo "-----------------------------------------------------------------------"
cat "$PUB_FILE"
echo "-----------------------------------------------------------------------"
echo "ℹ The key is read-only — set 'Allow write access' only if quay needs push."
