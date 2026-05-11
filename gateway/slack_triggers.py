"""Slack channel-trigger router.

Maps top-level Slack messages in pre-bound channels onto autonomous
invocations of an entry skill. Config lives in ``deploy.values.yaml``
under ``slack_triggers:`` (top-level), bridged into the rendered
``config.yaml`` as ``slack.triggers:`` and then into the Slack adapter's
``config.extra["triggers"]`` at gateway boot.

This module is pure: it consumes a parsed Slack message event and
returns a routing decision. State (dedup, sliding-window rate limit)
lives on the :class:`SlackTriggerRouter` instance owned by the adapter.

Loop prevention is structural: entry skills MUST post replies threaded
on the trigger message — a threaded reply has ``thread_ts`` set, so the
top-level filter rejects it and it can't re-trigger the router. As a
defense-in-depth net the router also rejects messages from the
gateway's own bot user (matched on ``user``/``bot_id``).
"""

from __future__ import annotations

import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, Optional

from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)


def is_bot_message(*, bot_id: Optional[str], subtype: Optional[str]) -> bool:
    """Slack flags bot-authored messages on either field; check both."""
    return bool(bot_id) or subtype == "bot_message"


# Status strings emitted in the structured per-event log row. Kept short
# and stable so log greps and dashboards don't break on rewording.
STATUS_STARTED = "started"
STATUS_FINISHED = "finished"
STATUS_FAILED = "failed"
STATUS_SKIPPED_NOT_TOP_LEVEL = "skipped_not_top_level"
STATUS_SKIPPED_BOT = "skipped_bot"
STATUS_SKIPPED_SELF = "skipped_self"
STATUS_SKIPPED_DUPLICATE = "skipped_duplicate"
STATUS_RATE_LIMITED = "rate_limited"


_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{6,}$")
_SLACK_USER_ID_RE = re.compile(r"^[UBW][A-Z0-9]{6,}$")
_SKILL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

_DEFAULT_MAX_PER_HOUR = 30
_DEFAULT_ON_OVERFLOW: Literal["skip", "error"] = "skip"
_VALID_ON_OVERFLOW = frozenset({"skip", "error"})


class SlackTriggerConfigError(ValueError):
    """Raised when a ``slack_triggers[]`` entry fails shape validation."""


@dataclass(frozen=True)
class SyntheticAuthor:
    name: str
    slack_id: str


@dataclass(frozen=True)
class RateLimit:
    max_per_hour: int = _DEFAULT_MAX_PER_HOUR
    on_overflow: Literal["skip", "error"] = _DEFAULT_ON_OVERFLOW


@dataclass(frozen=True)
class SlackTriggerConfig:
    """One ``slack_triggers[]`` entry, post-validation."""

    channel_id: str
    skill: str
    require_top_level: bool = True
    accept_from_bots: bool = False
    synthetic_author: Optional[SyntheticAuthor] = None
    rate_limit: RateLimit = field(default_factory=RateLimit)
    default_repo: Optional[str] = None
    # Free-text comment in deploy.values.yaml — channel_id is the canonical
    # key (rename-stable). Surfaced in the envelope for human readability.
    channel_name: Optional[str] = None


@dataclass(frozen=True)
class SlackTriggerMatch:
    trigger: SlackTriggerConfig
    envelope_text: str
    author_name: str
    author_slack_id: str


def parse_triggers(raw: Any, *, known_skills: Optional[Iterable[str]] = None) -> list[SlackTriggerConfig]:
    """Validate and parse the ``slack_triggers:`` block.

    ``raw`` is the value read from config (the YAML list). ``known_skills``,
    if provided, is the set of skill names the router will refuse to bind
    to anything outside (install-time guard against typos).

    Raises :class:`SlackTriggerConfigError` on any shape problem with a
    concise message naming the offending field/index. Fails on the first
    error — the surrounding caller decides whether to render it as
    install-time fail-loud or runtime warn-and-skip.
    """
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise SlackTriggerConfigError("slack_triggers must be a list")
    skills_set = {str(s) for s in (known_skills or ())}

    seen_channels: set[str] = set()
    out: list[SlackTriggerConfig] = []
    for i, entry in enumerate(raw):
        label = f"slack_triggers[{i}]"
        if not isinstance(entry, dict):
            raise SlackTriggerConfigError(f"{label} must be a mapping")

        channel_id = entry.get("channel_id")
        if not isinstance(channel_id, str) or not _CHANNEL_ID_RE.match(channel_id):
            raise SlackTriggerConfigError(
                f"{label}.channel_id must match {_CHANNEL_ID_RE.pattern} "
                f"(uppercase Slack channel id like C0123ABCDEF)"
            )
        if channel_id in seen_channels:
            raise SlackTriggerConfigError(
                f"{label}.channel_id={channel_id!r} already bound by an "
                f"earlier entry — one entry per channel"
            )
        seen_channels.add(channel_id)

        skill = entry.get("skill")
        if not isinstance(skill, str) or not _SKILL_NAME_RE.match(skill):
            raise SlackTriggerConfigError(
                f"{label}.skill must be a non-empty skill name matching "
                f"{_SKILL_NAME_RE.pattern}"
            )
        if known_skills is not None and skill not in skills_set:
            raise SlackTriggerConfigError(
                f"{label}.skill={skill!r} is not present in the skills "
                f"directory — typo or skill not yet synced"
            )

        require_top_level = entry.get("require_top_level", True)
        if not isinstance(require_top_level, bool):
            raise SlackTriggerConfigError(
                f"{label}.require_top_level must be a bool"
            )

        accept_from_bots = entry.get("accept_from_bots", False)
        if not isinstance(accept_from_bots, bool):
            raise SlackTriggerConfigError(
                f"{label}.accept_from_bots must be a bool"
            )

        synthetic_raw = entry.get("synthetic_author")
        synthetic: Optional[SyntheticAuthor] = None
        if accept_from_bots:
            if synthetic_raw is None:
                raise SlackTriggerConfigError(
                    f"{label}.synthetic_author is required when "
                    f"accept_from_bots is true (quay validate-ticket needs "
                    f"a non-empty authors[] on bot-triggered tickets)"
                )
        if synthetic_raw is not None:
            if not isinstance(synthetic_raw, dict):
                raise SlackTriggerConfigError(
                    f"{label}.synthetic_author must be a mapping"
                )
            sa_name = synthetic_raw.get("name")
            sa_id = synthetic_raw.get("slack_id")
            if not isinstance(sa_name, str) or not sa_name.strip():
                raise SlackTriggerConfigError(
                    f"{label}.synthetic_author.name must be a non-empty string"
                )
            if not isinstance(sa_id, str) or not _SLACK_USER_ID_RE.match(sa_id):
                raise SlackTriggerConfigError(
                    f"{label}.synthetic_author.slack_id must match "
                    f"{_SLACK_USER_ID_RE.pattern}"
                )
            synthetic = SyntheticAuthor(name=sa_name.strip(), slack_id=sa_id)

        rl_raw = entry.get("rate_limit") or {}
        if not isinstance(rl_raw, dict):
            raise SlackTriggerConfigError(
                f"{label}.rate_limit must be a mapping when present"
            )
        max_per_hour = rl_raw.get("max_per_hour", _DEFAULT_MAX_PER_HOUR)
        if not isinstance(max_per_hour, int) or max_per_hour <= 0:
            raise SlackTriggerConfigError(
                f"{label}.rate_limit.max_per_hour must be a positive int"
            )
        on_overflow = rl_raw.get("on_overflow", _DEFAULT_ON_OVERFLOW)
        if on_overflow not in _VALID_ON_OVERFLOW:
            raise SlackTriggerConfigError(
                f"{label}.rate_limit.on_overflow must be one of "
                f"{sorted(_VALID_ON_OVERFLOW)} (got {on_overflow!r})"
            )

        default_repo = entry.get("default_repo")
        if default_repo is not None and (
            not isinstance(default_repo, str) or not default_repo.strip()
        ):
            raise SlackTriggerConfigError(
                f"{label}.default_repo must be a non-empty string when set"
            )

        channel_name = entry.get("channel_name")
        if channel_name is not None and not isinstance(channel_name, str):
            raise SlackTriggerConfigError(
                f"{label}.channel_name must be a string when set"
            )

        out.append(
            SlackTriggerConfig(
                channel_id=channel_id,
                skill=skill,
                require_top_level=require_top_level,
                accept_from_bots=accept_from_bots,
                synthetic_author=synthetic,
                rate_limit=RateLimit(
                    max_per_hour=max_per_hour,
                    on_overflow=on_overflow,
                ),
                default_repo=default_repo,
                channel_name=channel_name,
            )
        )
    return out


def build_envelope(
    *,
    trigger: SlackTriggerConfig,
    permalink: str,
    message_ts: str,
    author_name: str,
    author_slack_id: str,
    message_text: str,
) -> str:
    """Render the trigger-envelope brief prepended to the entry skill's input.

    Kept small and stable: the entry skill SKILL.md parses these fields,
    so reformatting silently breaks every skill bound to this router.
    """
    lines: list[str] = [
        "[Slack channel trigger]",
        f"channel_id: {trigger.channel_id}",
    ]
    if trigger.channel_name:
        lines.append(f"channel_name: {trigger.channel_name}")
    lines.append(f"message_ts: {message_ts}")
    lines.append(f"permalink: {permalink}")
    lines.append(f"author.name: {author_name}")
    lines.append(f"author.slack_id: {author_slack_id}")
    if trigger.default_repo:
        lines.append(f"default_repo: {trigger.default_repo}")
    lines.append("")
    lines.append("Message:")
    lines.append(message_text or "(empty)")
    return "\n".join(lines)


class SlackTriggerRouter:
    """Owns per-channel dedup + rate-limit state for the trigger flow.

    Stateless across restarts — the gateway re-derives state from
    incoming events. Slack re-delivery races inside a single process
    are what the dedup guards against; longer-window duplicates are
    rare and best handled by the entry skill's own idempotency check
    (e.g. inverter-linear searches for an existing ticket before
    creating a new one).
    """

    def __init__(
        self,
        triggers: Iterable[SlackTriggerConfig],
        *,
        bot_user_id: Optional[str] = None,
        time_source: Optional[Any] = None,
    ) -> None:
        self._by_channel: dict[str, SlackTriggerConfig] = {
            t.channel_id: t for t in triggers
        }
        self._bot_user_id: Optional[str] = bot_user_id
        self._now = time_source or time.monotonic
        # Defense-in-depth: the Slack adapter already dedups on ``ts`` via
        # its own MessageDeduplicator before the router sees the event, but
        # keeping a router-owned ring lets the router be unit-tested in
        # isolation and survives any future caller that bypasses the
        # adapter's pre-filter.
        self._dedup = MessageDeduplicator()
        self._buckets: dict[str, deque[float]] = {}

    @property
    def channel_ids(self) -> frozenset[str]:
        return frozenset(self._by_channel.keys())

    def get(self, channel_id: str) -> Optional[SlackTriggerConfig]:
        return self._by_channel.get(channel_id)

    def set_bot_user_id(self, bot_user_id: Optional[str]) -> None:
        self._bot_user_id = bot_user_id

    def evaluate(
        self,
        *,
        channel_id: str,
        message_ts: str,
        thread_ts: Optional[str],
        bot_id: Optional[str],
        user_id: Optional[str],
        subtype: Optional[str],
    ) -> tuple[Optional[SlackTriggerConfig], str]:
        """Decide whether ``(channel_id, message_ts)`` should fire a trigger.

        Returns ``(trigger, status)``. When ``trigger`` is None the caller
        falls through to the normal Slack message path; the status is
        emitted in the structured log row and lets the caller distinguish
        "this channel isn't trigger-bound" (no trigger) from "trigger
        bound but skipped for reason X".
        """
        trigger = self._by_channel.get(channel_id)
        if trigger is None:
            return None, ""

        if trigger.require_top_level and thread_ts and thread_ts != message_ts:
            return None, STATUS_SKIPPED_NOT_TOP_LEVEL

        if is_bot_message(bot_id=bot_id, subtype=subtype):
            # Defense-in-depth: never re-trigger on our own posts even if
            # an operator misconfigures accept_from_bots: true.
            if user_id and self._bot_user_id and user_id == self._bot_user_id:
                return None, STATUS_SKIPPED_SELF
            if not trigger.accept_from_bots:
                return None, STATUS_SKIPPED_BOT

        if self._dedup.is_duplicate(message_ts):
            return None, STATUS_SKIPPED_DUPLICATE

        # Rate limit BEFORE recording so an over-cap event doesn't bump
        # the bucket's effective count.
        if self._rate_limited(channel_id, trigger.rate_limit.max_per_hour):
            return None, STATUS_RATE_LIMITED

        self._buckets.setdefault(channel_id, deque()).append(self._now())
        return trigger, STATUS_STARTED

    def _rate_limited(self, channel_id: str, max_per_hour: int) -> bool:
        bucket = self._buckets.setdefault(channel_id, deque())
        now = self._now()
        cutoff = now - 3600.0
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        return len(bucket) >= max_per_hour
