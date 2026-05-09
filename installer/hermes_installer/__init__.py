"""Install-side glue invoked from ``setup-hermes.sh``.

Provisions pinned runtime managers (bun, ...) before quay's bootstrap
shells out to ``install_cmd`` under a minimal-PATH ``/bin/sh -c``.
"""
