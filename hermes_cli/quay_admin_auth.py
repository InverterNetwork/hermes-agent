"""One-time Slack-issued login links for the Quay Admin UI."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home


DEFAULT_LOGIN_TTL_SECONDS = 5 * 60
DEFAULT_SESSION_TTL_SECONDS = 12 * 60 * 60

_STATE_LOCK = threading.Lock()


def allowed_slack_users(raw: str | None = None) -> set[str]:
    """Return normalized Slack user IDs from QUAY_ADMIN_ALLOWED_USERS."""
    value = os.getenv("QUAY_ADMIN_ALLOWED_USERS", "") if raw is None else raw
    users: set[str] = set()
    for item in value.replace(";", ",").split(","):
        user = item.strip()
        if user:
            users.add(user)
    return users


def is_slack_user_allowed(user_id: str, raw: str | None = None) -> bool:
    return bool(user_id) and user_id in allowed_slack_users(raw)


def login_ttl_seconds() -> int:
    return _positive_int_env("QUAY_ADMIN_LOGIN_TTL_SECONDS", DEFAULT_LOGIN_TTL_SECONDS)


def session_ttl_seconds() -> int:
    return _positive_int_env("QUAY_ADMIN_SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)


def create_login_token(slack_user_id: str, *, now: float | None = None) -> tuple[str, dict[str, Any]]:
    """Create a short-lived one-time login token and persist only its hash."""
    if not slack_user_id:
        raise ValueError("slack_user_id is required")

    issued_at = time.time() if now is None else now
    token = secrets.token_urlsafe(32)
    token_hash = _hash_token(token)
    record = {
        "token_hash": token_hash,
        "slack_user_id": slack_user_id,
        "created_at": issued_at,
        "expires_at": issued_at + login_ttl_seconds(),
        "used_at": None,
    }
    with _STATE_LOCK:
        state = _load_state_unlocked()
        _gc_state_unlocked(state, issued_at)
        state.setdefault("login_tokens", {})[token_hash] = record
        _save_state_unlocked(state)
    return token, record


def inspect_login_token(token: str, *, now: float | None = None) -> dict[str, Any] | None:
    """Return a valid login token record without consuming it."""
    if not token:
        return None
    current = time.time() if now is None else now
    token_hash = _hash_token(token)
    with _STATE_LOCK:
        state = _load_state_unlocked()
        _gc_state_unlocked(state, current)
        tokens = state.setdefault("login_tokens", {})
        record = tokens.get(token_hash)
        if not isinstance(record, dict):
            return None
        if record.get("used_at") is not None:
            return None
        expires_at = float(record.get("expires_at") or 0)
        if expires_at <= current:
            return None
        return dict(record)


def consume_login_token(token: str, *, now: float | None = None) -> dict[str, Any] | None:
    """Consume a login token once, returning its record when valid."""
    if not token:
        return None
    current = time.time() if now is None else now
    token_hash = _hash_token(token)
    with _STATE_LOCK:
        state = _load_state_unlocked()
        _gc_state_unlocked(state, current)
        tokens = state.setdefault("login_tokens", {})
        record = tokens.get(token_hash)
        if not isinstance(record, dict):
            return None
        if record.get("used_at") is not None:
            return None
        expires_at = float(record.get("expires_at") or 0)
        if expires_at <= current:
            return None
        record["used_at"] = current
        _save_state_unlocked(state)
        return dict(record)


def create_session(slack_user_id: str, *, now: float | None = None) -> tuple[str, dict[str, Any]]:
    current = time.time() if now is None else now
    session_id = secrets.token_urlsafe(32)
    return session_id, {
        "session_id": session_id,
        "slack_user_id": slack_user_id,
        "created_at": current,
        "expires_at": current + session_ttl_seconds(),
    }


def build_login_url(token: str, base_url: str | None = None) -> str:
    base = (
        base_url
        or os.getenv("QUAY_ADMIN_PUBLIC_BASE_URL")
        or os.getenv("HERMES_DASHBOARD_URL")
        or "http://localhost:9119"
    ).rstrip("/")
    return f"{base}/quay/admin/login?token={token}"


def state_path() -> Path:
    return get_hermes_home() / "quay" / "admin_login_links.json"


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _load_state_unlocked() -> dict[str, Any]:
    path = state_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {"login_tokens": {}}
    except Exception:
        return {"login_tokens": {}}
    if not isinstance(data, dict):
        return {"login_tokens": {}}
    if not isinstance(data.get("login_tokens"), dict):
        data["login_tokens"] = {}
    return data


def _save_state_unlocked(state: dict[str, Any]) -> None:
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, sort_keys=True)
    tmp.replace(path)
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _gc_state_unlocked(state: dict[str, Any], now: float) -> None:
    tokens = state.setdefault("login_tokens", {})
    stale = [
        token_hash
        for token_hash, record in tokens.items()
        if not isinstance(record, dict)
        or float(record.get("expires_at") or 0) <= now
        or record.get("used_at") is not None
    ]
    for token_hash in stale:
        tokens.pop(token_hash, None)
