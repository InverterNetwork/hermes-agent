from hermes_cli import quay_admin_auth


def _install_quay_admin_session(web_server, slack_user_id="U123"):
    web_server._QUAY_ADMIN_SESSIONS.clear()
    web_server._QUAY_ADMIN_SESSIONS["sid"] = {
        "session_id": "sid",
        "slack_user_id": slack_user_id,
        "created_at": 1000,
        "expires_at": 9999999999,
    }
    return {web_server._QUAY_ADMIN_COOKIE_NAME: "sid"}


def test_quay_admin_login_sets_http_only_secure_cookie(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    token, _record = quay_admin_auth.create_login_token("U123")
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

    token, _record = quay_admin_auth.create_login_token("U123")
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

    cookies = _install_quay_admin_session(web_server)
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
    resp = client.get("/quay/admin/v1/status", cookies=cookies)

    assert resp.status_code == 200
    assert seen["url"] == "http://127.0.0.1:9731/v1/status"
    assert seen["headers"]["Authorization"] == "Bearer service-token"
    assert seen["headers"]["X-Hermes-User-Id"] == "U123"
    assert "set-cookie" not in {k.lower() for k in resp.headers}


def test_quay_admin_proxy_requires_admin_browser_session(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    web_server._QUAY_ADMIN_SESSIONS.clear()
    monkeypatch.setenv("QUAY_ADMIN_TOKEN", "service-token")

    client = TestClient(web_server.app)
    resp = client.get("/quay/admin/")

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Quay admin login required"


def test_quay_admin_proxy_serves_static_ui_with_hosted_api_base(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    cookies = _install_quay_admin_session(web_server)
    monkeypatch.setenv("QUAY_ADMIN_TOKEN", "service-token")
    seen_urls = []

    class FakeResponse:
        def __init__(self, content, content_type):
            self.status_code = 200
            self.content = content
            self.encoding = "utf-8"
            self.headers = {"content-type": content_type}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def request(self, method, url, content=None, headers=None):
            seen_urls.append(url)
            if url.endswith("/assets/app.js"):
                return FakeResponse(b"console.log('quay')", "application/javascript")
            return FakeResponse(
                b'<html><head><title>Quay</title>'
                b'<link rel="preconnect" href="https://fonts.googleapis.com" />'
                b'<link rel="icon" type="image/svg+xml" href="/favicon.svg" />'
                b'<link rel="stylesheet" crossorigin href="/assets/app.css">'
                b'<script type="module" crossorigin src="/assets/app.js"></script></head>'
                b'<body><a href="/v1/status">status</a></body></html>',
                "text/html",
            )

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = TestClient(web_server.app)
    resp = client.get("/quay/admin/", cookies=cookies)

    assert resp.status_code == 200
    assert 'window.__QUAY_API_BASE_URL__="/quay/admin"' in resp.text
    assert 'href="https://fonts.googleapis.com"' in resp.text
    assert 'href="/quay/admin/favicon.svg"' in resp.text
    assert 'href="/quay/admin/assets/app.css"' in resp.text
    assert 'src="/quay/admin/assets/app.js"' in resp.text
    assert 'href="/quay/admin/v1/status"' in resp.text
    assert "service-token" not in resp.text
    assert resp.headers["cache-control"] == "no-store"

    asset = client.get("/quay/admin/assets/app.js", cookies=cookies)
    assert asset.status_code == 200
    assert asset.text == "console.log('quay')"
    assert seen_urls == [
        "http://127.0.0.1:9731/",
        "http://127.0.0.1:9731/assets/app.js",
    ]


def test_quay_admin_proxy_preserves_method_body_query_and_strips_browser_auth(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    cookies = _install_quay_admin_session(web_server, slack_user_id="U456")
    monkeypatch.setenv("QUAY_ADMIN_TOKEN", "service-token")
    monkeypatch.setenv("QUAY_ADMIN_BASE_URL", "http://127.0.0.1:9999/root")
    seen = {}

    class FakeResponse:
        status_code = 201
        content = b'{"ok":true}'
        encoding = "utf-8"
        headers = {"content-type": "application/json"}

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
            seen["content"] = content
            seen["headers"] = headers
            return FakeResponse()

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = TestClient(web_server.app)
    resp = client.post(
        "/quay/admin/v1/changes/apply?dry_run=1",
        content=b'{"revision":"r1"}',
        headers={
            "Authorization": "Bearer browser-token",
            "Content-Type": "application/json",
        },
        cookies=cookies,
    )

    assert resp.status_code == 201
    assert seen["method"] == "POST"
    assert seen["url"] == "http://127.0.0.1:9999/root/v1/changes/apply?dry_run=1"
    assert seen["content"] == b'{"revision":"r1"}'
    assert seen["headers"]["Authorization"] == "Bearer service-token"
    assert seen["headers"]["X-Hermes-User-Id"] == "U456"
    assert "browser-token" not in str(seen["headers"])


def test_quay_admin_proxy_reports_upstream_failure_without_token_leak(monkeypatch, _isolate_hermes_home):
    from starlette.testclient import TestClient
    import hermes_cli.web_server as web_server

    cookies = _install_quay_admin_session(web_server)
    monkeypatch.setenv("QUAY_ADMIN_TOKEN", "service-token")

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def request(self, method, url, content=None, headers=None):
            raise OSError("connection refused")

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = TestClient(web_server.app)
    resp = client.get("/quay/admin/v1/meta", cookies=cookies)

    assert resp.status_code == 502
    assert resp.json()["detail"] == "Quay admin service unavailable"
    assert "service-token" not in resp.text
