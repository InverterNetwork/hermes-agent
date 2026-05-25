from hermes_cli import quay_admin_auth


def test_quay_admin_login_sets_http_only_secure_cookie(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    token, _record = quay_admin_auth.create_login_token("U123", now=1000)
    web_server._QUAY_ADMIN_SESSIONS.clear()

    client = TestClient(web_server.app)
    resp = client.get(f"/quay/admin/login?token={token}", follow_redirects=False)

    assert resp.status_code == 303
    assert resp.headers["location"] == "/quay/admin/"
    cookie = resp.headers["set-cookie"]
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "SameSite=lax" in cookie
    assert "QUAY_ADMIN_TOKEN" not in cookie
    assert "service-token" not in cookie
    assert len(web_server._QUAY_ADMIN_SESSIONS) == 1
    assert next(iter(web_server._QUAY_ADMIN_SESSIONS.values()))["slack_user_id"] == "U123"


def test_quay_admin_login_rejects_reused_token(_isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    token, _record = quay_admin_auth.create_login_token("U123", now=1000)
    client = TestClient(web_server.app)

    first = client.get(f"/quay/admin/login?token={token}", follow_redirects=False)
    second = client.get(f"/quay/admin/login?token={token}", follow_redirects=False)

    assert first.status_code == 303
    assert second.status_code == 401


def test_quay_admin_login_rejects_expired_token(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    monkeypatch.setenv("QUAY_ADMIN_LOGIN_TTL_SECONDS", "1")
    token, _record = quay_admin_auth.create_login_token("U123", now=1000)
    monkeypatch.setattr(quay_admin_auth.time, "time", lambda: 1002)

    client = TestClient(web_server.app)
    resp = client.get(f"/quay/admin/login?token={token}", follow_redirects=False)

    assert resp.status_code == 401


def test_quay_admin_proxy_uses_server_side_token_and_slack_identity(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    web_server._QUAY_ADMIN_SESSIONS.clear()
    web_server._QUAY_ADMIN_SESSIONS["sid"] = {
        "session_id": "sid",
        "slack_user_id": "U123",
        "created_at": 1000,
        "expires_at": 9999999999,
    }
    monkeypatch.setenv("QUAY_ADMIN_TOKEN", "service-token")
    seen = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok":true}'
        encoding = "utf-8"
        headers = {"content-type": "application/json", "set-cookie": "bad=1"}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def request(self, method, url, content=None, headers=None):
            seen["method"] = method
            seen["url"] = url
            seen["headers"] = headers
            return FakeResponse()

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = TestClient(web_server.app)
    resp = client.get("/quay/admin/v1/status", cookies={web_server._QUAY_ADMIN_COOKIE_NAME: "sid"})

    assert resp.status_code == 200
    assert seen["url"] == "http://127.0.0.1:9731/v1/status"
    assert seen["headers"]["Authorization"] == "Bearer service-token"
    assert seen["headers"]["X-Hermes-User-Id"] == "U123"
    assert "set-cookie" not in {k.lower() for k in resp.headers}
