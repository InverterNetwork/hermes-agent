import json
import time

from hermes_cli import quay_admin_auth


def test_allowed_user_can_create_and_consume_one_time_token(monkeypatch, _isolate_hermes_home):
    monkeypatch.setenv("QUAY_ADMIN_ALLOWED_USERS", "U123,U456")

    assert quay_admin_auth.is_slack_user_allowed("U123")

    token, record = quay_admin_auth.create_login_token("U123", now=1000)
    assert token
    assert record["slack_user_id"] == "U123"
    assert token not in json.dumps(quay_admin_auth.state_path().read_text(encoding="utf-8"))

    consumed = quay_admin_auth.consume_login_token(token, now=1001)
    assert consumed is not None
    assert consumed["slack_user_id"] == "U123"
    assert quay_admin_auth.consume_login_token(token, now=1002) is None


def test_non_allowlisted_user_is_denied(monkeypatch):
    monkeypatch.setenv("QUAY_ADMIN_ALLOWED_USERS", "U123")

    assert not quay_admin_auth.is_slack_user_allowed("U999")


def test_expired_token_is_rejected(monkeypatch, _isolate_hermes_home):
    monkeypatch.setenv("QUAY_ADMIN_LOGIN_TTL_SECONDS", "2")
    token, _record = quay_admin_auth.create_login_token("U123", now=1000)

    assert quay_admin_auth.consume_login_token(token, now=1003) is None


def test_build_login_url_uses_public_base_url(monkeypatch):
    monkeypatch.setenv("QUAY_ADMIN_PUBLIC_BASE_URL", "https://hermes.example.test/")

    assert quay_admin_auth.build_login_url("abc") == "https://hermes.example.test/quay/admin/login?token=abc"


def test_create_session_binds_slack_user_and_expires():
    session_id, session = quay_admin_auth.create_session("U123", now=time.time())

    assert session_id
    assert session["slack_user_id"] == "U123"
    assert session["expires_at"] > session["created_at"]
