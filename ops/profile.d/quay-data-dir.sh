# Pin QUAY_DATA_DIR for the agent user's interactive login shells so
# `sudo -u __AGENT_USER__ -i` followed by `quay …` lands in the same
# data dir the systemd tick writes to, not the silent fallback at
# ~/.quay/. The wrapper at /usr/local/bin/quay-as-hermes covers the
# non-interactive `sudo -u … quay` path; this file covers the operator
# who logs in interactively and types `quay` directly.
#
# Gated on the user identity so the export doesn't pollute root's or
# anyone else's environment when /etc/profile.d/* is sourced for them.
# __AGENT_USER__ and __TARGET_DIR__ are substituted by setup-hermes.sh
# at install time. See ops/README.md → quay-tick → manual operations.
if [ "$(id -un)" = "__AGENT_USER__" ]; then
  export QUAY_DATA_DIR="__TARGET_DIR__/quay"
fi
