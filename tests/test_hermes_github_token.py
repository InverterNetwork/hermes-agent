# Tests for installer/hermes_github_token.py.
#
# Mock GitHub's API rather than hit it: the helper has three moving parts —
# JWT construction, the API exchange, and on-disk caching — and the tests
# exercise each independently so a regression in one doesn't mask the others.

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# installer/ isn't on sys.path by default; tests run from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from installer import hermes_github_token as hgt  # noqa: E402


@pytest.fixture
def rsa_keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_priv = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    pem_pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return pem_priv, pem_pub


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "auth").mkdir()
    (tmp_path / "cache").mkdir()
    # Wipe any inherited config so each test starts clean.
    for k in (
        "HERMES_GH_APP_ID",
        "HERMES_GH_INSTALLATION_ID",
        "HERMES_GH_APP_KEY",
        "HERMES_GH_API",
        "HERMES_GH_TOKEN_CACHE",
        "HERMES_GH_TOKEN_OVERRIDE",
        "HERMES_GH_CONFIG",
    ):
        monkeypatch.delenv(k, raising=False)
    return tmp_path


def test_build_app_jwt_round_trip(rsa_keypair):
    priv, pub = rsa_keypair
    token = hgt.build_app_jwt("3599473", priv)
    decoded = jwt.decode(token, pub, algorithms=["RS256"], options={"verify_aud": False})
    assert decoded["iss"] == "3599473"
    assert decoded["exp"] - decoded["iat"] == hgt.JWT_TTL_S
    # iat is backdated to absorb clock skew, so it must not be in the future.
    assert decoded["iat"] <= int(time.time())


def test_load_config_env_overrides_file(hermes_home, monkeypatch):
    cfg_file = hermes_home / "auth" / "github-app.env"
    cfg_file.write_text(
        "HERMES_GH_APP_ID=from-file\n"
        "# comment line\n"
        "HERMES_GH_INSTALLATION_ID=129473146\n"
        'HERMES_GH_APP_KEY="/quoted/path.pem"\n'
    )
    monkeypatch.setenv("HERMES_GH_APP_ID", "from-env")

    cfg = hgt.load_config()
    assert cfg["HERMES_GH_APP_ID"] == "from-env"
    assert cfg["HERMES_GH_INSTALLATION_ID"] == "129473146"
    assert cfg["HERMES_GH_APP_KEY"] == "/quoted/path.pem"


def test_get_token_uses_override(hermes_home, monkeypatch):
    monkeypatch.setenv("HERMES_GH_TOKEN_OVERRIDE", "ghs_fake")
    assert hgt.get_token() == "ghs_fake"


def test_get_token_caches_and_refreshes(hermes_home, rsa_keypair, monkeypatch):
    priv, _ = rsa_keypair
    key_path = hermes_home / "auth" / "github-app.pem"
    key_path.write_text(priv)

    monkeypatch.setenv("HERMES_GH_APP_ID", "3599473")
    monkeypatch.setenv("HERMES_GH_INSTALLATION_ID", "129473146")
    monkeypatch.setenv("HERMES_GH_APP_KEY", str(key_path))

    calls = {"n": 0}

    def fake_fetch(app_id, install_id, pem, *, api_base, session=None):
        calls["n"] += 1
        # Token expires 1h from now (GitHub's standard TTL).
        return {
            "token": f"ghs_token_{calls['n']}",
            "expires_at": "2099-01-01T00:00:00Z",
        }

    with patch.object(hgt, "fetch_installation_token", side_effect=fake_fetch):
        first = hgt.get_token()
        second = hgt.get_token()
        # Second call should be served from cache (still far from expiry).
        assert first == second == "ghs_token_1"
        assert calls["n"] == 1

        # Stomp the cache to look near-expiry; next call must refresh.
        cache_path = hermes_home / "cache" / "github-token.json"
        stale = json.loads(cache_path.read_text())
        stale["expires_at_ts"] = int(time.time()) + 60  # less than TOKEN_REFRESH_MARGIN_S
        cache_path.write_text(json.dumps(stale))
        third = hgt.get_token()
        assert third == "ghs_token_2"
        assert calls["n"] == 2


def test_cache_file_permissions(hermes_home, rsa_keypair, monkeypatch):
    priv, _ = rsa_keypair
    key_path = hermes_home / "auth" / "github-app.pem"
    key_path.write_text(priv)

    monkeypatch.setenv("HERMES_GH_APP_ID", "1")
    monkeypatch.setenv("HERMES_GH_INSTALLATION_ID", "1")
    monkeypatch.setenv("HERMES_GH_APP_KEY", str(key_path))
    monkeypatch.setenv("HERMES_GH_TOKEN_OVERRIDE", "ghs_x")
    # Override path skips cache-write entirely; switch back and use a fake fetch.
    monkeypatch.delenv("HERMES_GH_TOKEN_OVERRIDE")

    with patch.object(
        hgt,
        "fetch_installation_token",
        return_value={"token": "ghs_x", "expires_at": "2099-01-01T00:00:00Z"},
    ):
        hgt.get_token()

    cache_path = hermes_home / "cache" / "github-token.json"
    mode = os.stat(cache_path).st_mode & 0o777
    assert mode == 0o600, f"cache mode {oct(mode)} (expected 0600)"


def test_credential_protocol_get(hermes_home, monkeypatch):
    monkeypatch.setenv("HERMES_GH_TOKEN_OVERRIDE", "ghs_z")
    out = hgt.credential_protocol(
        "get",
        stdin="protocol=https\nhost=github.com\npath=InverterNetwork/hermes-state\n",
    )
    assert "username=x-access-token" in out
    assert "password=ghs_z" in out


def test_credential_protocol_no_op(hermes_home):
    assert hgt.credential_protocol("store", "") == ""
    assert hgt.credential_protocol("erase", "") == ""


def test_fetch_installation_token_posts_to_correct_endpoint(rsa_keypair):
    priv, _ = rsa_keypair

    class FakeResp:
        status_code = 201

        def json(self):
            return {"token": "ghs_real", "expires_at": "2099-01-01T00:00:00Z"}

    class FakeSession:
        def __init__(self):
            self.calls = []

        def post(self, url, headers=None, timeout=None):
            self.calls.append((url, headers))
            return FakeResp()

    sess = FakeSession()
    out = hgt.fetch_installation_token(
        "3599473", "129473146", priv, api_base="https://api.github.com", session=sess
    )
    url, headers = sess.calls[0]
    assert url == "https://api.github.com/app/installations/129473146/access_tokens"
    assert headers["Authorization"].startswith("Bearer ")
    assert out["token"] == "ghs_real"


def test_missing_required_config(hermes_home):
    with pytest.raises(hgt.ConfigError):
        hgt.get_token({})


def test_check_action_silent_success(hermes_home, monkeypatch, capsys):
    # `check` exercises the same path as `mint` but must not print the token
    # to stdout — the installer's smoke test relies on this so retries on
    # transient flakes can't leak credentials into operator logs.
    monkeypatch.setenv("HERMES_GH_TOKEN_OVERRIDE", "ghs_secret_should_never_print")
    rc = hgt.main(["hermes_github_token.py", "check"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "ghs_secret_should_never_print" not in captured.out
    assert captured.out == ""


def test_cache_created_with_secure_mode(hermes_home, rsa_keypair, monkeypatch):
    # The cache file must never be world-readable, even briefly between create
    # and chmod. We verify by running with a permissive umask; the file should
    # still land at 0600 because it's opened with the mode upfront.
    priv, _ = rsa_keypair
    key_path = hermes_home / "auth" / "github-app.pem"
    key_path.write_text(priv)

    monkeypatch.setenv("HERMES_GH_APP_ID", "1")
    monkeypatch.setenv("HERMES_GH_INSTALLATION_ID", "1")
    monkeypatch.setenv("HERMES_GH_APP_KEY", str(key_path))

    old_umask = os.umask(0o000)  # the most permissive case
    try:
        with patch.object(
            hgt,
            "fetch_installation_token",
            return_value={"token": "x", "expires_at": "2099-01-01T00:00:00Z"},
        ):
            hgt.get_token()
    finally:
        os.umask(old_umask)

    cache_path = hermes_home / "cache" / "github-token.json"
    mode = os.stat(cache_path).st_mode & 0o777
    assert mode == 0o600, f"cache mode {oct(mode)} (expected 0600)"
