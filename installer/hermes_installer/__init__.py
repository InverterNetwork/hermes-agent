"""hermes_installer — install-side glue.

Provisions pinned runtime managers (bun, ...) before quay's bootstrap
shells out to ``install_cmd`` under a minimal-PATH ``/bin/sh -c``.
Designed to grow as the bash ``setup-hermes.sh`` is ported feature by
feature; new install-side work lands here, not in bash.
"""
