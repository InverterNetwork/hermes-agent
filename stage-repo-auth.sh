#!/usr/bin/env bash
# Stage SSH deploy-key material for the agent user.
# Run as root (or via sudo). Idempotent — re-runs skip key generation and
# converge ownership/permissions without clobbering anything.
#
# What it does:
#   1. Generates ~hermes/.ssh/id_ed25519 if absent (no passphrase).
#   2. Pre-seeds ~hermes/.ssh/known_hosts with github.com host keys.
#   3. Prints the public key plus a checklist of github.com repos[] entries
#      the operator needs to register the key against (Settings → Deploy
#      keys → Add deploy key, on each repo).
#
# The url.insteadOf rewrites that bridge HTTPS values URLs to SSH transport
# are NOT done here — setup-hermes.sh wires them per-repo as part of its
# repos[] provisioning loop. This script only handles the key material the
# agent needs before clone-time auth can succeed.
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
    -C "${AGENT_USER}@$(hostname -s) (hermes-agent deploy key)" \
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

# ---------- Enumerate github.com repos for the deploy-key checklist ----------
# Read every github.com URL out of repos[]; the agent needs the same key
# registered on each repo (deploy keys are per-repo, not per-org). list-repos
# rejects unmigrated `quay.repos[]` and bad URLs at this point, surfacing
# schema problems before the operator pastes anything into GitHub.

GITHUB_URLS=()
while IFS=$'\t' read -r repo_id repo_url _rest; do
  [[ -z "$repo_id" ]] && continue
  if [[ "$repo_url" =~ ^https://github\.com/[^/]+/[^/]+$ ]]; then
    GITHUB_URLS+=("$repo_url")
  fi
done < <(python3 "$VALUES_HELPER" --values "$VALUES_FILE" list-repos)

# ---------- Print public key for deploy-key registration ----------

echo ""
if (( ${#GITHUB_URLS[@]} == 0 )); then
  echo "ℹ No github.com entries in repos[] — public key staged but no repos to register against."
else
  echo "Add this public key to each repo's deploy-key UI (Settings → Deploy keys → Add deploy key):"
  for url in "${GITHUB_URLS[@]}"; do
    echo "  - ${url}/settings/keys"
  done
fi
echo "-----------------------------------------------------------------------"
cat "$PUB_FILE"
echo "-----------------------------------------------------------------------"
echo "ℹ The key is read-only — set 'Allow write access' only if a worker eventually needs to push from a clone."
