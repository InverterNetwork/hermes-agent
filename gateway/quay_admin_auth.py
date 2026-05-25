"""Authorization helpers for Quay Admin access requests."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterable


QUAY_ADMIN_ALLOWED_USERS_ENV = "QUAY_ADMIN_ALLOWED_USERS"
_SLACK_USER_ID_RE = re.compile(r"^U[A-Z0-9]{2,}$")


@dataclass(frozen=True)
class QuayAdminAuthorization:
    """Result of a Quay Admin Slack-user authorization check."""

    allowed: bool
    user_id: str
    allowed_users: frozenset[str]
    reason: str


def normalize_quay_admin_slack_user_id(value: object) -> str:
    """Normalize one Slack user ID for Quay Admin allowlist matching."""
    candidate = str(value or "").strip().upper()
    if not candidate or not _SLACK_USER_ID_RE.fullmatch(candidate):
        return ""
    return candidate


def parse_quay_admin_allowed_users(value: object = None) -> frozenset[str]:
    """Parse a comma-separated Quay Admin Slack user allowlist.

    Only explicit Slack user IDs are accepted. Wildcards and malformed entries
    are ignored, so empty or invalid config fails closed.
    """
    if value is None:
        value = os.getenv(QUAY_ADMIN_ALLOWED_USERS_ENV, "")

    if isinstance(value, str):
        entries: Iterable[object] = value.split(",")
    elif isinstance(value, (list, tuple, set, frozenset)):
        entries = value
    else:
        entries = [value]

    return frozenset(
        normalized
        for normalized in (normalize_quay_admin_slack_user_id(entry) for entry in entries)
        if normalized
    )


def authorize_quay_admin_slack_user(
    slack_user_id: object,
    allowed_users: object = None,
) -> QuayAdminAuthorization:
    """Return whether ``slack_user_id`` may request Quay Admin access.

    Quay Admin authorization is intentionally independent of
    ``SLACK_ALLOWED_USERS``, ``GATEWAY_ALLOWED_USERS``, and
    ``GATEWAY_ALLOW_ALL_USERS``. Admin access requires this dedicated
    allowlist and denies everyone when it is unset, empty, or malformed.
    """
    parsed_allowed = parse_quay_admin_allowed_users(allowed_users)
    normalized_user = normalize_quay_admin_slack_user_id(slack_user_id)

    if not parsed_allowed:
        return QuayAdminAuthorization(
            allowed=False,
            user_id=normalized_user,
            allowed_users=parsed_allowed,
            reason="quay_admin_allowlist_empty",
        )
    if not normalized_user:
        return QuayAdminAuthorization(
            allowed=False,
            user_id="",
            allowed_users=parsed_allowed,
            reason="invalid_slack_user_id",
        )
    if normalized_user not in parsed_allowed:
        return QuayAdminAuthorization(
            allowed=False,
            user_id=normalized_user,
            allowed_users=parsed_allowed,
            reason="slack_user_not_allowlisted",
        )
    return QuayAdminAuthorization(
        allowed=True,
        user_id=normalized_user,
        allowed_users=parsed_allowed,
        reason="allowed",
    )
