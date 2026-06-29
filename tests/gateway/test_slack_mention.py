"""
Tests for Slack mention gating (require_mention / free_response_channels).

Follows the same pattern as test_whatsapp_group_gating.py.
"""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

from gateway.config import Platform, PlatformConfig


# ---------------------------------------------------------------------------
# Mock slack-bolt if not installed (same as test_slack.py)
# ---------------------------------------------------------------------------

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
        ("slack_bolt.adapter.socket_mode.async_handler", slack_bolt.adapter.socket_mode.async_handler),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)


_ensure_slack_mock()

import plugins.platforms.slack.adapter as _slack_mod
_slack_mod.SLACK_AVAILABLE = True

from plugins.platforms.slack.adapter import SlackAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BOT_USER_ID = "U_BOT_123"
CHANNEL_ID = "C0AQWDLHY9M"
OTHER_CHANNEL_ID = "C9999999999"


<<<<<<< HEAD
def _make_adapter(
    require_mention=None,
    strict_mention=None,
    free_response_channels=None,
    response_policy=None,
    allowed_channels=None,
):
=======
def _make_adapter(require_mention=None, strict_mention=None, free_response_channels=None,
                  allowed_channels=None, mention_patterns=None):
>>>>>>> upstream/main
    extra = {}
    if require_mention is not None:
        extra["require_mention"] = require_mention
    if strict_mention is not None:
        extra["strict_mention"] = strict_mention
    if free_response_channels is not None:
        extra["free_response_channels"] = free_response_channels
    if response_policy is not None:
        extra["response_policy"] = response_policy
    if allowed_channels is not None:
        extra["allowed_channels"] = allowed_channels
    if mention_patterns is not None:
        extra["mention_patterns"] = mention_patterns

    adapter = object.__new__(SlackAdapter)
    adapter.platform = Platform.SLACK
    adapter.config = PlatformConfig(enabled=True, extra=extra)
    adapter._bot_user_id = BOT_USER_ID
    adapter._team_bot_user_ids = {}
    return adapter


# ---------------------------------------------------------------------------
# Tests: _slack_require_mention
# ---------------------------------------------------------------------------

def test_require_mention_defaults_to_true(monkeypatch):
    monkeypatch.delenv("SLACK_REQUIRE_MENTION", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_require_mention() is True


def test_require_mention_false():
    adapter = _make_adapter(require_mention=False)
    assert adapter._slack_require_mention() is False


def test_require_mention_true():
    adapter = _make_adapter(require_mention=True)
    assert adapter._slack_require_mention() is True


def test_require_mention_string_true():
    adapter = _make_adapter(require_mention="true")
    assert adapter._slack_require_mention() is True


def test_require_mention_string_false():
    adapter = _make_adapter(require_mention="false")
    assert adapter._slack_require_mention() is False


def test_require_mention_string_no():
    adapter = _make_adapter(require_mention="no")
    assert adapter._slack_require_mention() is False


def test_require_mention_string_yes():
    adapter = _make_adapter(require_mention="yes")
    assert adapter._slack_require_mention() is True


def test_require_mention_empty_string_stays_true():
    """Empty/malformed strings keep gating ON (explicit-false parser)."""
    adapter = _make_adapter(require_mention="")
    assert adapter._slack_require_mention() is True


def test_require_mention_malformed_string_stays_true():
    """Unrecognised values keep gating ON (fail-closed)."""
    adapter = _make_adapter(require_mention="maybe")
    assert adapter._slack_require_mention() is True


def test_require_mention_env_var_fallback(monkeypatch):
    monkeypatch.setenv("SLACK_REQUIRE_MENTION", "false")
    adapter = _make_adapter()  # no config value -> falls back to env
    assert adapter._slack_require_mention() is False


def test_require_mention_env_var_default_true(monkeypatch):
    monkeypatch.delenv("SLACK_REQUIRE_MENTION", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_require_mention() is True


# ---------------------------------------------------------------------------
# Tests: _slack_strict_mention
# ---------------------------------------------------------------------------

def test_strict_mention_defaults_to_true(monkeypatch):
    monkeypatch.delenv("SLACK_STRICT_MENTION", raising=False)
    monkeypatch.delenv("SLACK_RESPONSE_POLICY", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_strict_mention() is True


def test_strict_mention_true():
    adapter = _make_adapter(strict_mention=True)
    assert adapter._slack_strict_mention() is True


def test_strict_mention_false():
    adapter = _make_adapter(strict_mention=False)
    assert adapter._slack_strict_mention() is False


def test_strict_mention_string_true():
    adapter = _make_adapter(strict_mention="true")
    assert adapter._slack_strict_mention() is True


def test_strict_mention_string_off():
    adapter = _make_adapter(strict_mention="off")
    assert adapter._slack_strict_mention() is False


def test_strict_mention_malformed_stays_true():
    """Unrecognised values keep strict mode ON (fail-closed to mention-only behavior)."""
    adapter = _make_adapter(strict_mention="maybe")
    assert adapter._slack_strict_mention() is True


def test_strict_mention_env_var_fallback(monkeypatch):
    monkeypatch.setenv("SLACK_STRICT_MENTION", "true")
    adapter = _make_adapter()  # no config value -> falls back to env
    assert adapter._slack_strict_mention() is True


# ---------------------------------------------------------------------------
# Tests: response policy / quiet thread filtering
# ---------------------------------------------------------------------------

def test_response_policy_defaults_to_quiet_thread(monkeypatch):
    monkeypatch.delenv("SLACK_RESPONSE_POLICY", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_response_policy() == "mention_to_wake_quiet_thread"


def test_response_policy_legacy_alias_allows_thread_followups():
    adapter = _make_adapter(response_policy="legacy")
    assert adapter._slack_response_policy() == "thread_followup"
    assert adapter._slack_thread_followup_is_actionable("lol", event={}) is True


def test_response_policy_does_not_disable_default_strict_mention():
    adapter = _make_adapter(response_policy="strict_mention")
    assert adapter._slack_response_policy() == "mention_to_wake_quiet_thread"
    assert adapter._slack_strict_mention() is True


def test_quiet_thread_suppresses_acknowledgements_and_jokes():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable("thanks!", event={}) is False
    assert adapter._slack_thread_followup_is_actionable("lol", event={}) is False


def test_quiet_thread_suppresses_bare_file_or_gif_share():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable(
        "",
        event={"subtype": "file_share", "files": [{"mimetype": "image/gif"}]},
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "nice",
        event={"files": [{"mimetype": "image/gif"}]},
    ) is False


def test_quiet_thread_allows_direct_asks_and_task_state_updates():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable("can you check the logs?", event={}) is True
    assert adapter._slack_thread_followup_is_actionable("deploy failed with timeout", event={}) is True


def test_quiet_thread_preserves_pending_prompt_continuations():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable(
        "yes",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "no",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "frontend",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "option 2",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "B",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "Alice",
        event={},
        has_session=True,
        has_pending_user_prompt=True,
    ) is True


def test_quiet_thread_session_presence_does_not_bypass_actionable_checks():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable(
        "frontend",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "that makes sense",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "I talked with Alice",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "can you check frontend?",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is True
    assert adapter._slack_thread_followup_is_actionable(
        "deploy failed",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is True


def test_quiet_thread_suppresses_live_session_chatter_without_pending_prompt():
    adapter = _make_adapter()
    assert adapter._slack_thread_followup_is_actionable(
        "thanks",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "lol",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False
    assert adapter._slack_thread_followup_is_actionable(
        "that makes sense",
        event={},
        has_session=True,
        has_pending_user_prompt=False,
    ) is False


def test_slack_text_is_gateway_command_recognizes_control_commands():
    adapter = _make_adapter()
    assert adapter._slack_text_is_gateway_command("/approve") is True
    assert adapter._slack_text_is_gateway_command("/deny all") is True
    assert adapter._slack_text_is_gateway_command("/stop please") is True
    assert adapter._slack_text_is_gateway_command("/reset@Hermes") is True


def test_slack_text_is_gateway_command_rejects_unknown_or_non_commands():
    adapter = _make_adapter()
    assert adapter._slack_text_is_gateway_command("approve") is False
    assert adapter._slack_text_is_gateway_command("/not-a-real-command") is False
    assert adapter._slack_text_is_gateway_command("/tmp/file") is False


class _FakeSessionStore:
    def __init__(self, session_key, messages):
        self.config = SimpleNamespace(
            group_sessions_per_user=True,
            thread_sessions_per_user=False,
        )
        self._entries = {
            session_key: SimpleNamespace(session_id="session-1"),
        }
        self._messages = messages

    def _ensure_loaded(self):
        return None

    def load_transcript(self, session_id):
        assert session_id == "session-1"
        return self._messages


def test_thread_has_pending_user_prompt_uses_trailing_assistant_question():
    from gateway.session import SessionSource, build_session_key

    adapter = _make_adapter()
    source = SessionSource(
        platform=Platform.SLACK,
        chat_id=CHANNEL_ID,
        chat_type="group",
        user_id="U123",
        thread_id="171234.000100",
    )
    session_key = build_session_key(
        source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    )
    adapter._session_store = _FakeSessionStore(
        session_key,
        [{"role": "assistant", "content": "Which package should I use?"}],
    )

    assert adapter._thread_has_pending_user_prompt(CHANNEL_ID, "171234.000100", "U123") is True


def test_thread_has_pending_user_prompt_rejects_ordinary_trailing_assistant_text():
    from gateway.session import SessionSource, build_session_key

    adapter = _make_adapter()
    source = SessionSource(
        platform=Platform.SLACK,
        chat_id=CHANNEL_ID,
        chat_type="group",
        user_id="U123",
        thread_id="171234.000100",
    )
    session_key = build_session_key(
        source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    )
    adapter._session_store = _FakeSessionStore(
        session_key,
        [{"role": "assistant", "content": "I updated the docs."}],
    )

    assert adapter._thread_has_pending_user_prompt(CHANNEL_ID, "171234.000100", "U123") is False


def test_thread_has_pending_user_prompt_rejects_after_user_reply():
    from gateway.session import SessionSource, build_session_key

    adapter = _make_adapter()
    source = SessionSource(
        platform=Platform.SLACK,
        chat_id=CHANNEL_ID,
        chat_type="group",
        user_id="U123",
        thread_id="171234.000100",
    )
    session_key = build_session_key(
        source,
        group_sessions_per_user=True,
        thread_sessions_per_user=False,
    )
    adapter._session_store = _FakeSessionStore(
        session_key,
        [
            {"role": "assistant", "content": "Which package should I use?"},
            {"role": "user", "content": "frontend"},
        ],
    )

    assert adapter._thread_has_pending_user_prompt(CHANNEL_ID, "171234.000100", "U123") is False


# ---------------------------------------------------------------------------
# Tests: _slack_free_response_channels
# ---------------------------------------------------------------------------

def test_free_response_channels_default_empty(monkeypatch):
    monkeypatch.delenv("SLACK_FREE_RESPONSE_CHANNELS", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_free_response_channels() == set()


def test_free_response_channels_list():
    adapter = _make_adapter(free_response_channels=[CHANNEL_ID, OTHER_CHANNEL_ID])
    result = adapter._slack_free_response_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


def test_free_response_channels_csv_string():
    adapter = _make_adapter(free_response_channels=f"{CHANNEL_ID}, {OTHER_CHANNEL_ID}")
    result = adapter._slack_free_response_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


def test_free_response_channels_empty_string():
    adapter = _make_adapter(free_response_channels="")
    assert adapter._slack_free_response_channels() == set()


def test_free_response_channels_env_var_fallback(monkeypatch):
    monkeypatch.setenv("SLACK_FREE_RESPONSE_CHANNELS", f"{CHANNEL_ID},{OTHER_CHANNEL_ID}")
    adapter = _make_adapter()  # no config value → falls back to env
    result = adapter._slack_free_response_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


def test_free_response_channels_bare_int():
    # YAML `free_response_channels: 1491973769726791812` (single bare integer)
    # is loaded as an int and would previously fall through the isinstance(str)
    # branch to return an empty set.  Coerce scalar → str so single-channel
    # config without quoting works as users expect.
    adapter = _make_adapter(free_response_channels=1491973769726791812)
    result = adapter._slack_free_response_channels()
    assert result == {"1491973769726791812"}


def test_free_response_channels_int_list():
    # YAML list form with bare numeric entries — each element should be coerced.
    adapter = _make_adapter(free_response_channels=[1491973769726791812, 99999])
    result = adapter._slack_free_response_channels()
    assert result == {"1491973769726791812", "99999"}


# ---------------------------------------------------------------------------
# Tests: mention gating integration (simulating _handle_slack_message logic)
# ---------------------------------------------------------------------------

def _would_process(adapter, *, is_dm=False, channel_id=CHANNEL_ID,
                   text="hello", mentioned=False, thread_reply=False,
                   active_session=False, pending_user_prompt=False):
    """Simulate the mention gating logic from _handle_slack_message.

    Returns True if the message would be processed, False if it would be
    skipped (returned early).
    """
    bot_uid = adapter._team_bot_user_ids.get("T1", adapter._bot_user_id)
    if mentioned:
        text = f"<@{bot_uid}> {text}"
    is_mentioned = bool(
        (bot_uid and f"<@{bot_uid}>" in text)
        or adapter._slack_message_matches_mention_patterns(text)
    )

    if not is_dm and bot_uid:
        # allowed_channels check (whitelist — must pass before other gating)
        allowed = adapter._slack_allowed_channels()
        if allowed and channel_id not in allowed:
            return False

        if channel_id in adapter._slack_free_response_channels():
            return True
        elif adapter._slack_strict_mention() and not is_mentioned:
            return bool(
                thread_reply
                and active_session
                and adapter._slack_text_is_gateway_command(text)
            )
        elif not adapter._slack_require_mention():
            return True
        elif not is_mentioned:
            if thread_reply and active_session:
                if adapter._slack_text_is_gateway_command(text):
                    return True
                return adapter._slack_thread_followup_is_actionable(
                    text,
                    event={},
                    has_session=active_session,
                    has_pending_user_prompt=pending_user_prompt,
                )
            else:
                return False
    return True


def test_default_require_mention_channel_without_mention_ignored():
    adapter = _make_adapter()  # default: require_mention=True
    assert _would_process(adapter, text="hello everyone") is False


def test_require_mention_false_channel_without_mention_processed():
    adapter = _make_adapter(require_mention=False, strict_mention=False)
    assert _would_process(adapter, text="hello everyone") is True


def test_default_strict_mention_overrides_require_mention_false():
    adapter = _make_adapter(require_mention=False)
    assert _would_process(adapter, text="hello everyone") is False


def test_require_mention_false_thread_reply_still_requires_mention_by_default():
    adapter = _make_adapter(require_mention=False)
    assert _would_process(
        adapter,
        text="can you check the logs?",
        thread_reply=True,
        active_session=True,
    ) is False


def test_channel_in_free_response_processed_without_mention():
    adapter = _make_adapter(
        require_mention=True,
        free_response_channels=[CHANNEL_ID],
    )
    assert _would_process(adapter, channel_id=CHANNEL_ID, text="hello") is True


def test_channel_in_free_response_thread_reply_processed_without_mention():
    adapter = _make_adapter(
        require_mention=True,
        free_response_channels=[CHANNEL_ID],
    )
    assert _would_process(
        adapter,
        channel_id=CHANNEL_ID,
        text="can you check the logs?",
        thread_reply=True,
        active_session=True,
    ) is True


def test_other_channel_not_in_free_response_still_gated():
    adapter = _make_adapter(
        require_mention=True,
        free_response_channels=[CHANNEL_ID],
    )
    assert _would_process(adapter, channel_id=OTHER_CHANNEL_ID, text="hello") is False


def test_dm_always_processed_regardless_of_setting():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(adapter, is_dm=True, text="hello") is True


def test_mentioned_message_always_processed():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(adapter, mentioned=True, text="what's up") is True


def test_thread_reply_with_active_session_processed():
    adapter = _make_adapter(require_mention=True, strict_mention=False)
    assert _would_process(
        adapter, text="can you check the logs?",
        thread_reply=True, active_session=True,
    ) is True


def test_thread_reply_with_active_session_allows_short_clarification_answer():
    adapter = _make_adapter(require_mention=True, strict_mention=False)
    assert _would_process(
        adapter, text="yes",
        thread_reply=True, active_session=True, pending_user_prompt=True,
    ) is True


def test_thread_reply_with_active_session_but_chatter_ignored():
    adapter = _make_adapter(require_mention=True, strict_mention=False)
    assert _would_process(
        adapter, text="thanks",
        thread_reply=True, active_session=True,
    ) is False


def test_thread_reply_with_active_session_allows_gateway_control_command():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(
        adapter, text="/approve",
        thread_reply=True, active_session=True,
    ) is True
    assert _would_process(
        adapter, text="/stop",
        thread_reply=True, active_session=True,
    ) is True


def test_thread_reply_without_active_session_still_suppresses_gateway_command():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(
        adapter, text="/approve",
        thread_reply=True, active_session=False,
    ) is False


def test_strict_mention_allows_active_session_gateway_control_command():
    adapter = _make_adapter(require_mention=True, strict_mention=True)
    assert _would_process(
        adapter, text="/deny",
        thread_reply=True, active_session=True,
    ) is True
    assert _would_process(
        adapter, text="can you check the logs?",
        thread_reply=True, active_session=True,
    ) is False


def test_legacy_response_policy_processes_thread_chatter():
    adapter = _make_adapter(
        require_mention=True,
        strict_mention=False,
        response_policy="thread_followup",
    )
    assert _would_process(
        adapter, text="thanks",
        thread_reply=True, active_session=True,
    ) is True


def test_default_strict_thread_reply_with_direct_ask_ignored_without_mention():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(
        adapter, text="can you check the logs?",
        thread_reply=True, active_session=True,
    ) is False


def test_default_strict_thread_reply_with_pending_prompt_ignored_without_mention():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(
        adapter, text="yes",
        thread_reply=True, active_session=True, pending_user_prompt=True,
    ) is False


def test_thread_reply_without_active_session_ignored():
    adapter = _make_adapter(require_mention=True)
    assert _would_process(
        adapter, text="followup",
        thread_reply=True, active_session=False,
    ) is False


def test_bot_uid_none_processes_channel_message():
    """When bot_uid is None (before auth_test), channel messages pass through.

    This preserves the old behavior: the gating block is skipped entirely
    when bot_uid is falsy, so messages are not silently dropped during
    startup or for new workspaces.
    """
    adapter = _make_adapter(require_mention=True)
    adapter._bot_user_id = None
    adapter._team_bot_user_ids = {}

    # With bot_uid=None, the `if not is_dm and bot_uid:` condition is False,
    # so the gating block is skipped — message passes through.
    bot_uid = adapter._team_bot_user_ids.get("T1", adapter._bot_user_id)
    assert bot_uid is None

    # Simulate: gating block not entered when bot_uid is falsy
    is_dm = False
    if not is_dm and bot_uid:
        result = False  # would enter gating
    else:
        result = True  # gating skipped, message processed
    assert result is True


# ---------------------------------------------------------------------------
# Tests: config bridging
# ---------------------------------------------------------------------------

def test_config_bridges_slack_free_response_channels(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  require_mention: false\n"
        "  response_policy: thread_followup\n"
        "  free_response_channels:\n"
        "    - C0AQWDLHY9M\n"
        "    - C9999999999\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.delenv("SLACK_REQUIRE_MENTION", raising=False)
    monkeypatch.delenv("SLACK_RESPONSE_POLICY", raising=False)
    monkeypatch.delenv("SLACK_FREE_RESPONSE_CHANNELS", raising=False)

    config = load_gateway_config()

    assert config is not None
    slack_extra = config.platforms[Platform.SLACK].extra
    assert slack_extra.get("require_mention") is False
    assert slack_extra.get("response_policy") == "thread_followup"
    assert slack_extra.get("free_response_channels") == ["C0AQWDLHY9M", "C9999999999"]
    # Verify env vars were set by config bridging
    import os as _os
    assert _os.environ["SLACK_REQUIRE_MENTION"] == "false"
    assert _os.environ["SLACK_RESPONSE_POLICY"] == "thread_followup"
    assert _os.environ["SLACK_FREE_RESPONSE_CHANNELS"] == "C0AQWDLHY9M,C9999999999"


def test_top_level_slack_settings_do_not_disable_env_token_setup(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  require_mention: false\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("SLACK_REQUIRE_MENTION", raising=False)

    config = load_gateway_config()

    slack_config = config.platforms[Platform.SLACK]
    assert slack_config.enabled is True
    assert slack_config.token == "xoxb-test"
    assert slack_config.extra.get("require_mention") is False
    assert "_enabled_explicit" not in slack_config.extra


def test_explicit_top_level_slack_enabled_false_wins_over_env_token(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  enabled: false\n"
        "  require_mention: false\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("SLACK_REQUIRE_MENTION", raising=False)

    config = load_gateway_config()

    slack_config = config.platforms[Platform.SLACK]
    assert slack_config.enabled is False
    assert slack_config.token == "xoxb-test"
    assert slack_config.extra.get("require_mention") is False
    assert "_enabled_explicit" not in slack_config.extra


def test_explicit_platforms_slack_enabled_false_wins_over_env_token(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "platforms:\n"
        "  slack:\n"
        "    enabled: false\n"
        "    extra:\n"
        "      reply_in_thread: false\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")

    config = load_gateway_config()

    slack_config = config.platforms[Platform.SLACK]
    assert slack_config.enabled is False
    assert slack_config.token == "xoxb-test"
    assert slack_config.extra.get("reply_in_thread") is False
    assert "_enabled_explicit" not in slack_config.extra


def test_config_bridges_slack_reply_in_thread(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  reply_in_thread: false\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")

    config = load_gateway_config()

    assert config is not None
    slack_config = config.platforms[Platform.SLACK]
    assert slack_config.extra.get("reply_in_thread") is False

    adapter = SlackAdapter(slack_config)
    assert adapter._resolve_thread_ts(reply_to="171.000", metadata={}) is None

    # Top-level channel messages arrive with metadata.thread_id == reply_to
    # because the inbound handler uses event.ts as a session-keying fallback.
    # Those must be treated as non-threaded so reply_in_thread=false takes
    # effect in channels, not just DMs.
    assert adapter._resolve_thread_ts(
        reply_to="171.000",
        metadata={"thread_id": "171.000"},
    ) is None

    # Real thread replies (reply_to differs from thread parent) must still
    # resolve to the parent thread so conversation context is preserved.
    assert adapter._resolve_thread_ts(
        reply_to="171.500",
        metadata={"thread_id": "171.000"},
    ) == "171.000"


def test_config_bridges_slack_strict_mention(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  strict_mention: true\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.delenv("SLACK_STRICT_MENTION", raising=False)

    config = load_gateway_config()

    assert config is not None
    import os as _os
    assert _os.environ["SLACK_STRICT_MENTION"] == "true"


# ---------------------------------------------------------------------------
# Regression: strict mode must NOT persist mentions into _mentioned_threads
# ---------------------------------------------------------------------------
# Prevents agent-to-agent ack loops — if a strict-mode bot remembered every
# thread it was mentioned in, the next message from the other agent in that
# thread would re-trigger the bot and defeat the entire feature.

def test_mention_in_strict_mode_does_not_register_thread():
    adapter = _make_adapter(strict_mention=True)
    adapter._bot_user_id = "U_BOT"
    adapter._mentioned_threads = set()
    adapter._MENTIONED_THREADS_MAX = 5000

    thread_ts = "1700000000.100200"
    event_thread_ts = thread_ts  # incoming message is inside an existing thread

    # Mirror the handler's @mention + strict-mode guard that protects
    # _mentioned_threads.add(). If strict is on, we must skip the add.
    text = "<@U_BOT> hello"
    is_mentioned = f"<@{adapter._bot_user_id}>" in text
    assert is_mentioned
    if event_thread_ts and not adapter._slack_strict_mention():
        adapter._mentioned_threads.add(event_thread_ts)

    assert thread_ts not in adapter._mentioned_threads


def test_mention_outside_strict_mode_still_registers_thread():
    adapter = _make_adapter(strict_mention=False)
    adapter._bot_user_id = "U_BOT"
    adapter._mentioned_threads = set()
    adapter._MENTIONED_THREADS_MAX = 5000

    thread_ts = "1700000000.100200"
    event_thread_ts = thread_ts

    text = "<@U_BOT> hello"
    is_mentioned = f"<@{adapter._bot_user_id}>" in text
    assert is_mentioned
    if event_thread_ts and not adapter._slack_strict_mention():
        adapter._mentioned_threads.add(event_thread_ts)

    assert thread_ts in adapter._mentioned_threads


# ---------------------------------------------------------------------------
# Tests: _slack_allowed_channels
# ---------------------------------------------------------------------------

def test_allowed_channels_default_empty(monkeypatch):
    monkeypatch.delenv("SLACK_ALLOWED_CHANNELS", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_allowed_channels() == set()


def test_allowed_channels_list():
    adapter = _make_adapter(allowed_channels=[CHANNEL_ID, OTHER_CHANNEL_ID])
    result = adapter._slack_allowed_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


def test_allowed_channels_csv_string():
    adapter = _make_adapter(allowed_channels=f"{CHANNEL_ID}, {OTHER_CHANNEL_ID}")
    result = adapter._slack_allowed_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


def test_allowed_channels_empty_string():
    adapter = _make_adapter(allowed_channels="")
    assert adapter._slack_allowed_channels() == set()


def test_allowed_channels_env_var_fallback(monkeypatch):
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", f"{CHANNEL_ID},{OTHER_CHANNEL_ID}")
    adapter = _make_adapter()  # no config value → falls back to env
    result = adapter._slack_allowed_channels()
    assert CHANNEL_ID in result
    assert OTHER_CHANNEL_ID in result


# ---------------------------------------------------------------------------
# Tests: allowed_channels gating integration
# ---------------------------------------------------------------------------

def test_allowed_channels_blocks_non_whitelisted_channel():
    """Messages in channels not in allowed_channels are silently ignored."""
    adapter = _make_adapter(allowed_channels=[CHANNEL_ID])
    assert _would_process(adapter, channel_id=OTHER_CHANNEL_ID, text="hello") is False


def test_allowed_channels_permits_whitelisted_channel():
    """Messages in the allowed channel are processed normally."""
    adapter = _make_adapter(allowed_channels=[CHANNEL_ID])
    assert _would_process(adapter, channel_id=CHANNEL_ID, mentioned=True) is True


def test_allowed_channels_empty_no_restriction():
    """Empty allowed_channels imposes no restriction (fully backward compatible)."""
    adapter = _make_adapter(allowed_channels="")
    assert _would_process(adapter, channel_id=OTHER_CHANNEL_ID, mentioned=True) is True


def test_allowed_channels_blocks_even_when_mentioned():
    """Whitelist takes precedence — @mention in a non-allowed channel is ignored."""
    adapter = _make_adapter(allowed_channels=[CHANNEL_ID])
    assert _would_process(adapter, channel_id=OTHER_CHANNEL_ID, mentioned=True) is False


def test_allowed_channels_dm_unaffected():
    """DMs bypass the allowed_channels check entirely."""
    adapter = _make_adapter(allowed_channels=[CHANNEL_ID])
    # DM channel IDs typically start with D; the check is guarded by `not is_dm`
    assert _would_process(adapter, is_dm=True, channel_id="DDMCHANNEL") is True


def test_allowed_channels_env_var_blocks_channel(monkeypatch):
    """SLACK_ALLOWED_CHANNELS env var (no config) also gates messages."""
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", CHANNEL_ID)
    adapter = _make_adapter()  # no config value → falls back to env
    assert _would_process(adapter, channel_id=OTHER_CHANNEL_ID, text="hello") is False
    assert _would_process(adapter, channel_id=CHANNEL_ID, mentioned=True) is True


# ---------------------------------------------------------------------------
# Tests: config bridging for allowed_channels
# ---------------------------------------------------------------------------

def test_config_bridges_slack_allowed_channels(monkeypatch, tmp_path):
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        "  allowed_channels:\n"
        f"    - {CHANNEL_ID}\n"
        f"    - {OTHER_CHANNEL_ID}\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.delenv("SLACK_ALLOWED_CHANNELS", raising=False)

    load_gateway_config()

    import os as _os
    assert _os.environ["SLACK_ALLOWED_CHANNELS"] == f"{CHANNEL_ID},{OTHER_CHANNEL_ID}"


def test_config_bridges_slack_allowed_channels_env_takes_precedence(monkeypatch, tmp_path):
    """Env var set before load_gateway_config() should not be overwritten."""
    from gateway.config import load_gateway_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "slack:\n"
        f"  allowed_channels: {CHANNEL_ID}\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("SLACK_ALLOWED_CHANNELS", OTHER_CHANNEL_ID)  # already set

    load_gateway_config()

    import os as _os
    # env var must not be overwritten by config.yaml
    assert _os.environ["SLACK_ALLOWED_CHANNELS"] == OTHER_CHANNEL_ID


# ---------------------------------------------------------------------------
# Tests: mention_patterns (wake words) — parity with other adapters (#50732)
# ---------------------------------------------------------------------------

def test_mention_patterns_default_no_match(monkeypatch):
    monkeypatch.delenv("SLACK_MENTION_PATTERNS", raising=False)
    adapter = _make_adapter()
    assert adapter._slack_mention_patterns() == []
    assert adapter._slack_message_matches_mention_patterns("hello there") is False


def test_mention_patterns_list_matches():
    adapter = _make_adapter(mention_patterns=["hey hermes", "hermes,"])
    assert adapter._slack_message_matches_mention_patterns("hey hermes, you there?") is True
    assert adapter._slack_message_matches_mention_patterns("just chatting") is False


def test_mention_patterns_case_insensitive():
    adapter = _make_adapter(mention_patterns=["hey hermes"])
    assert adapter._slack_message_matches_mention_patterns("HEY HERMES!") is True


def test_mention_patterns_single_string():
    adapter = _make_adapter(mention_patterns="^hermes")
    assert adapter._slack_message_matches_mention_patterns("hermes do this") is True
    assert adapter._slack_message_matches_mention_patterns("ok hermes") is False


def test_mention_patterns_invalid_regex_skipped_without_crash():
    # An invalid pattern is dropped; valid siblings still work.
    adapter = _make_adapter(mention_patterns=["(unclosed", "hey hermes"])
    assert adapter._slack_message_matches_mention_patterns("hey hermes") is True


def test_mention_patterns_env_var_fallback(monkeypatch):
    monkeypatch.setenv("SLACK_MENTION_PATTERNS", '["hey hermes", "hermes,"]')
    adapter = _make_adapter()  # no config value -> falls back to env
    assert adapter._slack_message_matches_mention_patterns("hey hermes") is True


def test_mention_patterns_env_var_csv_fallback_splits_patterns(monkeypatch):
    monkeypatch.setenv("SLACK_MENTION_PATTERNS", "hey hermes,hermes,")
    adapter = _make_adapter()  # no config value -> falls back to env

    patterns = adapter._slack_mention_patterns()

    assert [pattern.pattern for pattern in patterns] == ["hey hermes", "hermes"]
    assert adapter._slack_message_matches_mention_patterns("hey hermes") is True


def test_mention_patterns_trigger_in_channel_without_literal_mention():
    """A wake word triggers the bot in a channel even with require_mention on."""
    adapter = _make_adapter(require_mention=True, mention_patterns=["hey hermes"])
    assert _would_process(adapter, text="hey hermes what's the status") is True
    # Unrelated channel chatter is still ignored.
    assert _would_process(adapter, text="lunch anyone?") is False
