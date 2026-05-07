#!/usr/bin/env bash
# Stage SSH deploy-key material for the quay worker on krustentier.
# Run as root (or via sudo). Idempotent — re-runs skip key generation and
# converge ownership/permissions without clobbering anything.
#
# What it does:
#   1. Generates ~hermes/.ssh/id_ed25519 if absent (no passphrase).
#   2. Pre-seeds ~hermes/.ssh/known_hosts with github.com host keys.
#   3. Reads distinct <org> prefixes from quay.repos[].url in VALUES_FILE
#      and wires url.insteadOf git rewrites (HTTPS → SSH) for the agent user.
#   4. Prints the public key so the operator can add it to each repo's
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

# ---------- url.insteadOf rewrites (HTTPS → SSH per org) ----------
# Read distinct GitHub orgs from quay.repos[].url. A missing quay.version
# or absent quay.repos block is fine — key + known_hosts are always useful,
# but there's nothing to rewrite if no repos are configured.

QUAY_VERSION="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" get quay.version 2>/dev/null || true)"

if [[ -z "$QUAY_VERSION" ]]; then
  echo "ℹ quay.version not set in $VALUES_FILE — skipping url.insteadOf rewrites"
else
  ORGS="$(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repo-orgs)"
  if [[ -z "$ORGS" ]]; then
    echo "ℹ no quay.repos entries found — skipping url.insteadOf rewrites"
  else
    while IFS= read -r org; do
      [[ -z "$org" ]] && continue
      # cd to $HOME before invoking git: with --global, git still walks up
      # from CWD looking for a .git/ and stats every parent. If CWD is
      # outside the agent user's read scope (CI runner: $GITHUB_WORKSPACE
      # is owned by `runner`; on-box: any path the operator happens to
      # invoke from), the stat fails before git realizes it's a global op.
      # Idempotent — a duplicate config key simply overwrites.
      sudo -u "$AGENT_USER" -H sh -c 'cd && git config --global "$1" "$2"' \
        _ "url.git@github.com:${org}/.insteadOf" "https://github.com/${org}/"
      echo "✓ git insteadOf: https://github.com/${org}/ → git@github.com:${org}/"
    done <<< "$ORGS"
  fi
fi

# ---------- Print public key for deploy-key registration ----------

echo ""
echo "Add this to each repo's deploy-key UI (Settings → Deploy keys → Add deploy key):"
echo "-----------------------------------------------------------------------"
cat "$PUB_FILE"
echo "-----------------------------------------------------------------------"
echo "ℹ The key is read-only — set 'Allow write access' only if quay needs push."
