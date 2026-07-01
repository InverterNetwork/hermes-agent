"""Compatibility alias for the bundled Slack platform plugin."""

import sys

from plugins.platforms.slack import adapter as _adapter

sys.modules[__name__] = _adapter
