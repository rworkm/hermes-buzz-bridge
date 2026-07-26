"""Buzz platform adapter for the Hermes gateway.

Receive path:  poll `buzz messages get --since <ts>` per watched channel,
               drop our own events, require a #p mention, backfill recent
               context, then hand off via BasePlatformAdapter.handle_message().
Reply path:    BasePlatformAdapter.send() -> `buzz messages send --reply-to`.

buzz-cli is REST-only (no live subscribe), so polling is the receive mechanism.
Interval defaults to 5s, which is well inside conversational latency.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

from .buzzcli import (
    BuzzCli,
    BuzzCliError,
    channel_of,
    format_history,
    is_mentioned,
    is_own,
    is_shutdown,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "buzz"
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_BACKFILL = 12
# Buzz stores messages as Nostr events with no hard length cap, but very long
# posts render badly in the desktop client. Chunk at a sane width.
MAX_MESSAGE_LENGTH = 8000
# Bound the dedup set so a long-lived gateway doesn't grow without limit.
SEEN_CAP = 2000
# How often to re-check channel membership when BUZZ_CHANNELS='*'. Creating a
# channel is rare, so this is deliberately much slower than the poll interval.
REDISCOVER_INTERVAL = 60.0


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


class BuzzAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(PLATFORM_NAME))
        extra = config.extra or {}

        self.relay_url = os.getenv("BUZZ_RELAY_URL") or extra.get("relay_url") or "http://localhost:3000"
        self.private_key = os.getenv("BUZZ_PRIVATE_KEY") or extra.get("private_key", "")
        self.pubkey = (os.getenv("BUZZ_AGENT_PUBKEY") or extra.get("pubkey", "")).strip().lower()

        raw_channels = os.getenv("BUZZ_CHANNELS") or extra.get("channels", "")
        if isinstance(raw_channels, (list, tuple)):
            self._configured_channels = [str(c).strip() for c in raw_channels if str(c).strip()]
        else:
            self._configured_channels = [c.strip() for c in str(raw_channels).split(",") if c.strip()]
        # '*' or empty means auto-discover all channels the agent is a member of
        self._auto_channels = not self._configured_channels or self._configured_channels == ["*"]
        self.channels: list[str] = []  # resolved at connect() time

        self.poll_interval = _float_env("BUZZ_POLL_INTERVAL", DEFAULT_POLL_INTERVAL)
        self.backfill_count = _int_env("BUZZ_BACKFILL_COUNT", DEFAULT_BACKFILL)

        self.cli = BuzzCli(relay_url=self.relay_url, private_key=self.private_key)

        # Per-channel high-water mark (unix ts) + global seen-event dedup.
        self._cursor: dict[str, int] = {}
        self._seen: set[str] = set()
        self._seen_order: list[str] = []
        self._poll_task: asyncio.Task | None = None
        self._running = False

    # ---------- lifecycle ----------

    async def connect(self, is_reconnect: bool = False) -> bool:
        if not self.private_key:
            logger.error("buzz: BUZZ_PRIVATE_KEY is not set")
            return False
        if not self.pubkey or len(self.pubkey) != 64:
            logger.error("buzz: BUZZ_AGENT_PUBKEY must be a 64-char hex pubkey")
            return False
        if not self._auto_channels and not self._configured_channels:
            logger.error("buzz: no channels configured (set BUZZ_CHANNELS to '*' or a comma-separated list)")
            return False

        # Resolve channels: if BUZZ_CHANNELS is '*' or empty, discover all.
        try:
            all_channels = await self.cli.list_channels()
            visible = {c.get("channel_id") or c.get("id") for c in all_channels}
        except BuzzCliError as exc:
            logger.error("buzz: cannot reach relay at %s (%s)", self.relay_url, exc)
            return False

        if self._auto_channels:
            if not visible:
                logger.error("buzz: auto-discover found no channels — agent must be a member of at least one")
                return False
            self.channels = sorted(visible)
        else:
            self.channels = list(self._configured_channels)
            for ch in self._configured_channels:
                if visible and ch not in visible:
                    logger.warning(
                        "buzz: agent is not a member of channel %s — it will see nothing there. "
                        "Add its pubkey with: buzz channels add-member --channel %s --pubkey %s",
                        ch, ch, self.pubkey,
                    )

        now = int(time.time())
        for ch in self.channels:
            self._cursor[ch] = now  # only react to messages from startup forward

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        logger.info(
            "buzz: connected to %s, watching %d channel(s), poll=%.1fs",
            self.relay_url, len(self.channels), self.poll_interval,
        )
        return True

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
        self._mark_disconnected()

    # ---------- receive ----------

    async def _poll_loop(self) -> None:
        backoff = self.poll_interval
        # connect() has just discovered channels, so start at 1 — the first
        # re-discovery lands one REDISCOVER_INTERVAL in, not immediately.
        cycle = 1
        rediscover_every = max(1, int(REDISCOVER_INTERVAL / max(self.poll_interval, 0.1)))
        while self._running:
            try:
                if self._auto_channels and cycle % rediscover_every == 0:
                    await self._rediscover_channels()
                for channel in self.channels:
                    await self._poll_channel(channel)
                backoff = self.poll_interval  # reset after a clean pass
            except asyncio.CancelledError:
                raise
            except BuzzCliError as exc:
                # Network blips are expected; back off rather than hammering.
                logger.warning("buzz: poll failed (%s), backing off", exc)
                backoff = min(backoff * 2, 60.0)
            except Exception:
                logger.exception("buzz: unexpected poll error")
                backoff = min(backoff * 2, 60.0)
            cycle += 1
            await asyncio.sleep(backoff)

    async def _rediscover_channels(self) -> None:
        """Pick up channels the agent was added to after startup.

        Auto-discover mode only. New channels start with their cursor at now so
        the agent doesn't backfill and answer a channel's whole history — it
        only reacts to mentions from the moment it noticed the channel.
        Membership *removal* is not tracked; polling a channel we lost access to
        yields empty results or an error the existing backoff already handles.
        """
        try:
            all_channels = await self.cli.list_channels()
        except BuzzCliError as exc:
            logger.debug("buzz: channel re-discovery failed (%s), will retry", exc)
            return  # transient, try again next cycle

        visible = {
            str(c.get("channel_id") or c.get("id") or "").strip()
            for c in all_channels
            if isinstance(c, dict)
        }
        new = {ch for ch in visible if ch} - set(self.channels)
        if not new:
            return

        now = int(time.time())
        for ch in sorted(new):
            self.channels.append(ch)
            self._cursor[ch] = now  # don't backfill, only see new messages
            logger.info("buzz: discovered new channel %s", ch)

    async def _poll_channel(self, channel: str) -> None:
        since = self._cursor.get(channel, int(time.time()))
        # `since` is inclusive on the relay side, so we re-see the boundary
        # event each poll — _mark_seen() is what actually prevents reprocessing.
        events = await self.cli.get_messages(channel, limit=200, since=since)
        if not events:
            return

        newest = since
        for ev in events:
            created = int(ev.get("created_at") or 0)
            newest = max(newest, created)

            eid = str(ev.get("id") or "")
            if not eid or eid in self._seen:
                continue
            self._mark_seen(eid)

            if is_own(ev, self.pubkey):
                continue

            if is_shutdown(ev, self.pubkey):
                logger.info("buzz: received !shutdown from %s", ev.get("pubkey"))
                asyncio.create_task(self.disconnect())
                return

            content = (ev.get("content") or "").strip()
            is_slash_command = content.startswith("/")
            if not is_mentioned(ev, self.pubkey) and not is_slash_command:
                continue  # ignore normal chatter, respond to @mentions and /commands

            if not self._authorized(str(ev.get("pubkey") or "")):
                logger.info("buzz: ignoring mention from unauthorized %s", ev.get("pubkey"))
                continue

            await self._dispatch(channel, ev)

        self._cursor[channel] = newest

    def _authorized(self, pubkey: str) -> bool:
        if os.getenv("BUZZ_ALLOW_ALL_USERS", "").strip().lower() in ("1", "true", "yes"):
            return True
        allow = [p.strip().lower() for p in os.getenv("BUZZ_ALLOWED_USERS", "").split(",") if p.strip()]
        if not allow:
            return True  # no allowlist configured -> channel membership is the gate
        return pubkey.lower() in allow

    async def _dispatch(self, channel: str, event: dict) -> None:
        """Build context and hand the mention to the Hermes gateway runner."""
        try:
            history = await self.cli.get_messages(channel, limit=self.backfill_count)
        except BuzzCliError as exc:
            logger.warning("buzz: backfill failed (%s), proceeding without context", exc)
            history = []

        # Drop the triggering event from the backfill so it isn't duplicated.
        trigger_id = str(event.get("id") or "")
        history = [h for h in history if str(h.get("id") or "") != trigger_id]

        author = str(event.get("pubkey") or "")
        content = (event.get("content") or "").strip()

        parts = []
        if history:
            parts.append(
                "<buzz_recent_history>\n"
                + format_history(history)
                + "\n</buzz_recent_history>\n"
                "(Only the last few messages are shown. Use search_buzz_history "
                "if the mention refers to something older.)"
            )
        parts.append(f"{author[:8]} mentioned you in Buzz:\n{content}")
        text = "\n\n".join(parts)

        source = self.build_source(
            chat_id=channel,
            chat_name=f"buzz:{channel[:8]}",
            chat_type="group",
            user_id=author,
            user_name=author[:8],
        )
        msg = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=trigger_id,
        )
        await self.handle_message(msg)

    def _mark_seen(self, event_id: str) -> None:
        self._seen.add(event_id)
        self._seen_order.append(event_id)
        while len(self._seen_order) > SEEN_CAP:
            self._seen.discard(self._seen_order.pop(0))

    # ---------- send ----------

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        try:
            result = await self.cli.send_message(
                channel=str(chat_id), content=content, reply_to=reply_to
            )
        except BuzzCliError as exc:
            logger.error("buzz: send failed (%s)", exc)
            return SendResult(success=False, error=str(exc))

        event_id = ""
        if isinstance(result, dict):
            event_id = str(result.get("id") or result.get("event_id") or "")
        # Our own reply must never re-trigger us on the next poll.
        if event_id:
            self._mark_seen(event_id)
        return SendResult(success=True, message_id=event_id)

    async def get_chat_info(self, chat_id):
        try:
            info = await self.cli.get_channel(str(chat_id))
            return {"name": info.get("name", str(chat_id)), "type": "group"}
        except BuzzCliError:
            return {"name": str(chat_id), "type": "group"}


# ---------- registry hooks ----------


def check_requirements() -> bool:
    return bool(os.getenv("BUZZ_PRIVATE_KEY") and os.getenv("BUZZ_AGENT_PUBKEY"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    has_key = bool(os.getenv("BUZZ_PRIVATE_KEY") or extra.get("private_key"))
    has_chan = bool(os.getenv("BUZZ_CHANNELS") or extra.get("channels"))
    return has_key and has_chan


def _env_enablement() -> dict | None:
    """Auto-enable the platform when the env vars are present in ~/.hermes/.env."""
    key = os.getenv("BUZZ_PRIVATE_KEY", "").strip()
    channels = os.getenv("BUZZ_CHANNELS", "").strip()
    if not (key and channels):
        return None
    seed = {
        "private_key": key,
        "channels": channels,
        "relay_url": os.getenv("BUZZ_RELAY_URL", "http://localhost:3000").strip(),
        "pubkey": os.getenv("BUZZ_AGENT_PUBKEY", "").strip(),
    }
    home = os.getenv("BUZZ_HOME_CHANNEL", "").strip() or channels.split(",")[0].strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": "Buzz"}
    return seed


async def _standalone_send(pconfig, chat_id, message, *, thread_id=None,
                           media_files=None, force_document=False):
    """Cron delivery when `hermes cron run` is a separate process from the gateway.

    Without this, `deliver=buzz` cron jobs fail with 'No live adapter'.
    """
    cli = BuzzCli()
    try:
        result = await cli.send_message(channel=str(chat_id), content=message)
        eid = result.get("id") if isinstance(result, dict) else None
        return {"success": True, "message_id": eid or ""}
    except BuzzCliError as exc:
        return {"error": str(exc)}


PLATFORM_HINT = (
    "You are speaking in a Buzz channel (a Nostr relay workspace) alongside "
    "humans and other agents. You were invoked because someone @mentioned you; "
    "everything else said in the channel is context you were not asked about. "
    "Reply once, concisely, and in markdown. Only the last few messages are in "
    "your context — call search_buzz_history if the mention refers to something "
    "older. If nothing is needed from you, reply with [SILENT]."
)


def register(ctx):
    """Plugin entry point."""
    from . import tools

    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Buzz",
        adapter_factory=lambda cfg: BuzzAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["BUZZ_PRIVATE_KEY", "BUZZ_AGENT_PUBKEY", "BUZZ_CHANNELS"],
        install_hint="cargo install --path crates/buzz-cli  (from the block/buzz checkout)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="BUZZ_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="BUZZ_ALLOWED_USERS",
        allow_all_env="BUZZ_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=PLATFORM_HINT,
        emoji="🐝",
    )

    tools.register_tools(ctx)
