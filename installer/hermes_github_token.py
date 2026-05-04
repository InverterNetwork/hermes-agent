#!/usr/bin/env python3
# hermes-github-token — mint GitHub App installation tokens for the agent and
# serve them to git via the credential-helper protocol.
#
# Usage:
#   hermes_github_token.py mint              # print a token to stdout
#   hermes_github_token.py credential get    # git credential helper protocol
#
# Configuration is read from $HERMES_GH_CONFIG (default: ~/.hermes/auth/github-app.env),
# with each line in the form KEY=VALUE. Recognized keys:
#
#   HERMES_GH_APP_ID            (required) GitHub App id
#   HERMES_GH_INSTALLATION_ID   (required) installation id for this host
#   HERMES_GH_APP_KEY           (required) path to the App's PEM private key
#   HERMES_GH_API               (optional) API base, default https://api.github.com
#   HERMES_GH_TOKEN_CACHE       (optional) cache file, default ~/.hermes/cache/github-token.json
#
# Env vars of the same name override the config file. HERMES_GH_TOKEN_OVERRIDE,
# if set, short-circuits the API call and is returned verbatim — used by tests.

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import jwt
import requests

JWT_TTL_S = 540          # 9 min — under GitHub's 10-min hard cap, leaves slack for clock skew
TOKEN_REFRESH_MARGIN_S = 300  # refresh when <5 min remaining on the cached installation token
HTTP_TIMEOUT_S = 15


class ConfigError(RuntimeError):
    pass


def _hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))


def _default_config_path() -> Path:
    return _hermes_home() / "auth" / "github-app.env"


def _default_cache_path() -> Path:
    return _hermes_home() / "cache" / "github-token.json"


def load_config(config_path: Path | None = None) -> dict[str, str]:
    """Load KEY=VALUE pairs from the config file, then overlay env vars.

    Env wins so callers (tests, ad-hoc invocations) can override without editing
    the file. Missing config file is fine — env may carry everything.
    """
    cfg: dict[str, str] = {}
    path = config_path or Path(os.environ.get("HERMES_GH_CONFIG") or _default_config_path())
    if path.exists():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip().strip('"').strip("'")

    for k in (
        "HERMES_GH_APP_ID",
        "HERMES_GH_INSTALLATION_ID",
        "HERMES_GH_APP_KEY",
        "HERMES_GH_API",
        "HERMES_GH_TOKEN_CACHE",
        "HERMES_GH_TOKEN_OVERRIDE",
    ):
        if v := os.environ.get(k):
            cfg[k] = v
    return cfg


def _require(cfg: dict[str, str], key: str) -> str:
    val = cfg.get(key)
    if not val:
        raise ConfigError(f"missing required config: {key}")
    return val


def build_app_jwt(app_id: str, private_key_pem: str, *, now: int | None = None) -> str:
    # GitHub requires iss=app_id and exp <= now+10min. iat backdated 60s to
    # absorb clock skew between us and github.com.
    issued = (now or int(time.time())) - 60
    payload = {"iat": issued, "exp": issued + JWT_TTL_S, "iss": app_id}
    return jwt.encode(payload, private_key_pem, algorithm="RS256")


def fetch_installation_token(
    app_id: str,
    installation_id: str,
    private_key_pem: str,
    *,
    api_base: str = "https://api.github.com",
    session: requests.Session | None = None,
) -> dict[str, Any]:
    bearer = build_app_jwt(app_id, private_key_pem)
    sess = session or requests.Session()
    resp = sess.post(
        f"{api_base}/app/installations/{installation_id}/access_tokens",
        headers={
            "Authorization": f"Bearer {bearer}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        timeout=HTTP_TIMEOUT_S,
    )
    if resp.status_code != 201:
        raise RuntimeError(
            f"GitHub installation token request failed: {resp.status_code} {resp.text}"
        )
    body = resp.json()
    return {"token": body["token"], "expires_at": body["expires_at"]}


def _parse_expiry(expires_at: str) -> int:
    # GitHub returns ISO-8601 with trailing "Z"; strptime can't handle that on
    # all stdlib versions, so normalize first.
    s = expires_at.replace("Z", "+00:00")
    from datetime import datetime
    return int(datetime.fromisoformat(s).timestamp())


def _read_cache(cache_path: Path) -> dict[str, Any] | None:
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(cache_path: Path, payload: dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload))
    os.chmod(tmp, 0o600)
    tmp.replace(cache_path)


def get_token(cfg: dict[str, str] | None = None, *, now: int | None = None) -> str:
    """Return a valid installation token, refreshing the on-disk cache if stale."""
    cfg = cfg if cfg is not None else load_config()
    if override := cfg.get("HERMES_GH_TOKEN_OVERRIDE"):
        return override

    cache_path = Path(cfg.get("HERMES_GH_TOKEN_CACHE") or _default_cache_path())
    current = int(now or time.time())

    cached = _read_cache(cache_path)
    if cached and cached.get("expires_at_ts", 0) - TOKEN_REFRESH_MARGIN_S > current:
        return cached["token"]

    app_id = _require(cfg, "HERMES_GH_APP_ID")
    installation_id = _require(cfg, "HERMES_GH_INSTALLATION_ID")
    key_path = Path(_require(cfg, "HERMES_GH_APP_KEY"))
    api_base = cfg.get("HERMES_GH_API", "https://api.github.com")

    private_key_pem = key_path.read_text()
    fetched = fetch_installation_token(
        app_id, installation_id, private_key_pem, api_base=api_base
    )
    payload = {
        "token": fetched["token"],
        "expires_at": fetched["expires_at"],
        "expires_at_ts": _parse_expiry(fetched["expires_at"]),
        "fetched_at_ts": current,
    }
    _write_cache(cache_path, payload)
    return fetched["token"]


# ---------- git credential helper protocol ----------
# git invokes us with the action ("get"/"store"/"erase") as the last positional
# arg. We only handle "get"; "store"/"erase" are no-ops because we don't trust
# git's view of credential lifecycle (we own the cache).

def credential_protocol(action: str, stdin: str = "") -> str:
    if action != "get":
        return ""
    # Parse incoming key=value lines; we don't actually need them (always serve
    # the same App token) but consuming stdin keeps git happy.
    _ = {
        k: v
        for line in stdin.splitlines()
        if "=" in line
        for k, _, v in [line.partition("=")]
    }
    token = get_token()
    return f"username=x-access-token\npassword={token}\n"


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd = argv[1]
    if cmd == "mint":
        print(get_token())
        return 0
    if cmd == "credential":
        action = argv[2] if len(argv) > 2 else "get"
        sys.stdout.write(credential_protocol(action, sys.stdin.read()))
        return 0
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
