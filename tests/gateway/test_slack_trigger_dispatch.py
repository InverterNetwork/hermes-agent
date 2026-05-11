"""Adapter-level tests for the Slack channel-trigger router.

Exercises the wire between the Slack adapter and ``gateway/slack_triggers``:
* end-to-end happy path: a top-level message in a trigger-bound channel
  produces a single ``handle_message`` invocation with the entry skill
  auto-loaded and the envelope prepended.
* loop prevention: a threaded reply in the same channel falls through
  to the normal flow (no router invocation).
* re-delivery dedup: the same ``message.ts`` only fires once.
* unrelated channel: events in non-trigger channels reach the normal
  flow untouched (router consumes nothing).
"""

import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig


def _ensure_slack_mock():
    if "slack_bolt" in sys.modules and hasattr(sys.modules["slack_bolt"], "__file__"):
        return
    slack_bolt = MagicMock()
    slack_bolt.async_app.AsyncApp = MagicMock
    slack_bolt.adapter.socket_mode.async_handler.AsyncSocketModeHandler = MagicMock
    slack_sdk = MagicMock()
    slack_sdk.web.async_client.AsyncWebClient = MagicMock
    for name, mod in [
        ("slack_bolt", slack_bolt),
        ("slack_bolt.async_app", slack_bolt.async_app),
        ("slack_bolt.adapter", slack_bolt.adapter),
        ("slack_bolt.adapter.socket_mode", slack_bolt.adapter.socket_mode),
        ("slack_bolt.adapter.socket_mode.async_handler",
         slack_bolt.adapter.socket_mode.async_handler),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("aiohttp", MagicMock())


_ensure_slack_mock()

import gateway.platforms.slack as _slack_mod  # noqa: E402
_slack_mod.SLACK_AVAILABLE = True

from gateway.platforms.slack import SlackAdapter  # noqa: E402


CHAN = "C0FEEDBACK"


@pytest.fixture
def adapter_with_trigger():
    """Slack adapter with a single feedback-intake trigger configured."""
    config = PlatformConfig(
        enabled=True,
        token="xoxb-fake",
        extra={
            "triggers": [
                {
                    "channel_id": CHAN,
                    "channel_name": "feedback",
                    "skill": "feedback-intake",
                    "default_repo": "iTRY-monorepo",
                }
            ],
        },
    )
    adapter = SlackAdapter(config)
    adapter._app = MagicMock()
    adapter._app.client = AsyncMock()
    # chat.getPermalink returns a stable shape — assert handle_message sees it.
    adapter._app.client.chat_getPermalink = AsyncMock(
        return_value={"ok": True, "permalink": "https://example.slack.com/p17"}
    )
    adapter._bot_user_id = "U_BOT"
    adapter._running = True
    # Capture MessageEvents instead of dispatching them downstream.
    adapter.handle_message = AsyncMock()
    # The default user resolver hits Slack API; short-circuit it.
    adapter._resolve_user_name = AsyncMock(return_value="Alice")
    return adapter


@pytest.fixture
def adapter_no_triggers():
    config = PlatformConfig(enabled=True, token="xoxb-fake")
    adapter = SlackAdapter(config)
    adapter._app = MagicMock()
    adapter._app.client = AsyncMock()
    adapter._bot_user_id = "U_BOT"
    adapter._running = True
    adapter.handle_message = AsyncMock()
    return adapter


class TestTriggerDispatch:
    @pytest.mark.asyncio
    async def test_top_level_message_dispatches_with_envelope_and_auto_skill(
        self, adapter_with_trigger,
    ):
        event = {
            "channel": CHAN,
            "ts": "1700000000.000100",
            "user": "U_ALICE",
            "text": "the app crashes on login",
            "team": "T1",
        }

        await adapter_with_trigger._handle_slack_message(event)

        adapter_with_trigger.handle_message.assert_awaited_once()
        msg_event = adapter_with_trigger.handle_message.call_args.args[0]
        assert msg_event.auto_skill == "feedback-intake"
        # Envelope is prepended to the text, original message body is included.
        assert "[Slack channel trigger]" in msg_event.text
        assert "channel_id: C0FEEDBACK" in msg_event.text
        assert "the app crashes on login" in msg_event.text
        assert "permalink: https://example.slack.com/p17" in msg_event.text
        # User-source trigger → internal=False so SLACK_ALLOWED_USERS still
        # gates downstream authz.
        assert msg_event.internal is False
        # Permalink lookup was actually performed.
        adapter_with_trigger._app.client.chat_getPermalink.assert_awaited_once_with(
            channel=CHAN, message_ts="1700000000.000100",
        )

    @pytest.mark.asyncio
    async def test_thread_reply_in_trigger_channel_falls_through(
        self, adapter_with_trigger,
    ):
        """Loop-prevention: a threaded reply must NOT re-trigger the router.

        The entry skill posts its reply threaded on the source message, and
        Slack delivers that reply back through ``message`` events. If the
        router fired on threaded replies the bot would infinite-loop.
        """
        event = {
            "channel": CHAN,
            "ts": "1700000000.000200",
            "thread_ts": "1700000000.000100",
            "user": "U_BOT",  # the bot's own reply
            "text": "Filed BRIX-123",
            "team": "T1",
        }

        await adapter_with_trigger._handle_slack_message(event)

        # The router skipped — but the global bot/self filter in the
        # normal _handle_slack_message path also drops the bot's own
        # message. Either way, handle_message must not be invoked.
        adapter_with_trigger.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_redelivery_only_fires_once(self, adapter_with_trigger):
        event = {
            "channel": CHAN,
            "ts": "1700000000.000300",
            "user": "U_ALICE",
            "text": "feedback A",
            "team": "T1",
        }

        await adapter_with_trigger._handle_slack_message(event)
        await adapter_with_trigger._handle_slack_message(event)

        # The global MessageDeduplicator catches the second pass; even if it
        # didn't, the router's own ts-set would. Either path: one dispatch.
        assert adapter_with_trigger.handle_message.await_count == 1

    @pytest.mark.asyncio
    async def test_unbound_channel_falls_through(self, adapter_with_trigger):
        """A message in a channel not listed in slack_triggers reaches the
        normal Slack flow — the router must not consume it."""
        event = {
            "channel": "C_UNBOUND",
            "ts": "1700000000.000400",
            "user": "U_ALICE",
            "text": "<@U_BOT> hello",
            "team": "T1",
        }

        # Avoid the full mention/allowlist plumbing in the existing flow —
        # this test just asserts the router didn't consume the event.
        # The downstream flow may or may not call handle_message based on
        # gating; we only assert no permalink lookup happened (proving the
        # router didn't take it).
        await adapter_with_trigger._handle_slack_message(event)

        adapter_with_trigger._app.client.chat_getPermalink.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_adapter_without_triggers_skips_router(
        self, adapter_no_triggers,
    ):
        assert adapter_no_triggers._trigger_router is None
        event = {
            "channel": CHAN,
            "ts": "1700000000.000500",
            "user": "U_ALICE",
            "text": "noise",
        }
        # No router → the dispatch helper is never entered. The normal
        # flow handles the message (and likely drops it on mention/allow
        # gating). Just confirm we don't blow up.
        await adapter_no_triggers._handle_slack_message(event)
