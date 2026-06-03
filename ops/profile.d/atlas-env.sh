# Pin Atlas runtime defaults for the Hermes agent user's interactive login
# shells. Non-interactive operator calls should use /usr/local/bin/atlas-as-hermes.
#
# __AGENT_USER__, __TARGET_DIR__, and __ATLAS_*__ placeholders are rendered by
# setup-hermes.sh from deploy.values.yaml.
if [ "$(id -un)" = "__AGENT_USER__" ]; then
  export HERMES_HOME="__TARGET_DIR__"
  export ATLAS_CONFIG="__ATLAS_CONFIG__"
  export ATLAS_KB_ROOT="__ATLAS_KB_ROOT__"
  export ATLAS_AI_MODE="__ATLAS_AI_MODE__"
  export ATLAS_CODEX_BIN="__ATLAS_CODEX_BIN__"
  export ATLAS_CODEX_TIMEOUT_MS="__ATLAS_CODEX_TIMEOUT_MS__"
  export ATLAS_SESSION_ID="${ATLAS_SESSION_ID:-hermes-agent}"
fi
