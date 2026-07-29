"""Install-side glue invoked from ``setup-hermes.sh``.

Provisions pinned runtime managers (bun, ...) and Quay worker toolchain
binaries (anvil, ...) before quay's runtime shells out under a minimal PATH,
and provisions the pinned Codex CLI when quay's active agent path uses it.
"""
