import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig


def _ensure_slack_mock():
    if "slack_bolt" in sys.modules and hasattr(sys.modules["slack_bolt"], "__file__"):
        return

    slack_mod = MagicMock()
    for name in ("slack_bolt", "slack_bolt.async_app", "slack_sdk", "slack_sdk.web.async_client"):
        sys.modules.setdefault(name, slack_mod)


_ensure_slack_mock()

from gateway.platforms.slack import SlackAdapter  # noqa: E402


@pytest.fixture
def adapter():
    config = PlatformConfig(enabled=True, token="xoxb-fake")
    return SlackAdapter(config)


def test_quay_admin_is_registered_as_native_slack_command():
    from hermes_cli.commands import slack_native_slashes

    assert "quay-admin" in {name for name, _desc, _usage in slack_native_slashes()}


@pytest.mark.asyncio
async def test_quay_admin_slash_denies_non_allowlisted_user(monkeypatch, adapter):
    monkeypatch.setenv("QUAY_ADMIN_ALLOWED_USERS", "U123")
    adapter._respond_to_slash_command = AsyncMock()

    await adapter._handle_quay_admin_slash_command(
        {"user_id": "U999", "channel_id": "C1", "response_url": "https://slack.test/response"}
    )

    adapter._respond_to_slash_command.assert_awaited_once()
    assert "not allowed" in adapter._respond_to_slash_command.call_args.args[1]


@pytest.mark.asyncio
async def test_quay_admin_slash_dms_allowed_user(monkeypatch, adapter, _isolate_hermes_home):
    monkeypatch.setenv("QUAY_ADMIN_ALLOWED_USERS", "U123")
    monkeypatch.setenv("QUAY_ADMIN_PUBLIC_BASE_URL", "https://hermes.example.test")
    adapter._respond_to_slash_command = AsyncMock()

    client = MagicMock()
    client.conversations_open = AsyncMock(return_value={"channel": {"id": "D123"}})
    client.chat_postMessage = AsyncMock(return_value={"ok": True})
    adapter._team_clients["T1"] = client

    await adapter._handle_quay_admin_slash_command(
        {
            "user_id": "U123",
            "team_id": "T1",
            "channel_id": "C1",
            "response_url": "https://slack.test/response",
        }
    )

    client.conversations_open.assert_awaited_once_with(users="U123")
    client.chat_postMessage.assert_awaited_once()
    dm_kwargs = client.chat_postMessage.call_args.kwargs
    assert dm_kwargs["channel"] == "D123"
    assert "https://hermes.example.test/quay/admin/login?token=" in dm_kwargs["text"]
    assert "QUAY_ADMIN_TOKEN" not in dm_kwargs["text"]
    adapter._respond_to_slash_command.assert_awaited_once()
    assert "DM" in adapter._respond_to_slash_command.call_args.args[1]


@pytest.mark.asyncio
async def test_quay_admin_slash_dm_failure_never_posts_login_url_to_channel(
    monkeypatch, adapter, _isolate_hermes_home
):
    monkeypatch.setenv("QUAY_ADMIN_ALLOWED_USERS", "U123")
    monkeypatch.setenv("QUAY_ADMIN_PUBLIC_BASE_URL", "https://hermes.example.test")
    adapter.send = AsyncMock()

    client = MagicMock()
    client.conversations_open = AsyncMock(side_effect=RuntimeError("dm unavailable"))
    adapter._team_clients["T1"] = client

    await adapter._handle_quay_admin_slash_command(
        {
            "user_id": "U123",
            "team_id": "T1",
            "channel_id": "C1",
        }
    )

    adapter.send.assert_awaited_once()
    channel_id, content = adapter.send.call_args.args[:2]
    assert channel_id == "C1"
    assert "https://hermes.example.test/quay/admin/login?token=" not in content
    assert "token=" not in content
    assert "could not DM" in content
