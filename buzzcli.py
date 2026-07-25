"""Thin async wrapper around the `buzz` CLI binary.

Every relay operation shells out to buzz-cli, which is JSON in / JSON out
(stdout = relay JSON, stderr = {"error","message"}, exit 0=ok 1=user 2=network
3=auth 4=other 5=write-conflict). This keeps the plugin free of any Nostr
library dependency and inherits buzz-cli's NIP-98 signing for free.

Nothing in this module imports Hermes, so it can be unit-tested standalone.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from typing import Any

logger = logging.getLogger(__name__)

# From crates/buzz-core/src/kind.rs — the kinds `buzz messages get` requests.
# KIND_STREAM_MESSAGE=9 is the NIP-29 group chat message; 40002 is the V2 form;
# 40008 is a diff/patch message; 45001/45003 are forum post kinds.
STREAM_MESSAGE_KINDS = (9, 40002, 40008, 45001, 45003)

# Exit codes from buzz-cli's error.rs
EXIT_OK = 0
EXIT_USER = 1
EXIT_NETWORK = 2
EXIT_AUTH = 3


class BuzzCliError(RuntimeError):
    """A buzz-cli invocation failed."""

    def __init__(self, code: int, message: str, argv: list[str]) -> None:
        super().__init__(f"buzz-cli exit {code}: {message}")
        self.code = code
        self.message = message
        self.argv = argv


class BuzzCli:
    """Async wrapper around the buzz binary."""

    def __init__(
        self,
        binary: str | None = None,
        relay_url: str | None = None,
        private_key: str | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.binary = binary or os.getenv("BUZZ_CLI_PATH") or "buzz"
        self.relay_url = relay_url or os.getenv("BUZZ_RELAY_URL", "http://localhost:3000")
        self.private_key = private_key or os.getenv("BUZZ_PRIVATE_KEY", "")
        self.timeout = timeout
        self._extra_env = env or {}

    # ---------- plumbing ----------

    def available(self) -> bool:
        """True if the buzz binary is resolvable and we have a key."""
        found = shutil.which(self.binary) is not None or os.path.isfile(self.binary)
        return bool(found and self.private_key)

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["BUZZ_RELAY_URL"] = self.relay_url
        if self.private_key:
            env["BUZZ_PRIVATE_KEY"] = self.private_key
        env.update(self._extra_env)
        return env

    async def _run(self, *args: str, stdin: str | None = None) -> str:
        argv = [self.binary, *args]
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE if stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env(),
            )
            out, err = await asyncio.wait_for(
                proc.communicate(stdin.encode() if stdin is not None else None),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError as exc:
            raise BuzzCliError(-1, f"timed out after {self.timeout}s", argv) from exc
        except FileNotFoundError as exc:
            raise BuzzCliError(-1, f"binary not found: {self.binary}", argv) from exc

        if proc.returncode != EXIT_OK:
            detail = _decode_cli_error(err.decode(errors="replace"))
            raise BuzzCliError(proc.returncode or -1, detail, argv)
        return out.decode(errors="replace")

    async def _run_json(self, *args: str, stdin: str | None = None) -> Any:
        raw = (await self._run(*args, stdin=stdin)).strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # `mem get` prints a raw value, not JSON. Return it verbatim.
            return raw

    # ---------- messages ----------

    async def get_messages(
        self,
        channel: str,
        limit: int = 50,
        since: int | None = None,
        before: int | None = None,
        kinds: str | None = None,
    ) -> list[dict]:
        """Fetch messages in a channel, oldest-first (buzz-cli sorts ascending).

        Each event is normalized by buzz-cli to:
          {"id","pubkey","kind","content","created_at","tags"}
        """
        args = ["messages", "get", "--channel", channel, "--limit", str(min(limit, 200))]
        if since is not None:
            args += ["--since", str(int(since))]
        if before is not None:
            args += ["--before", str(int(before))]
        if kinds:
            args += ["--kinds", kinds]
        result = await self._run_json(*args)
        return result if isinstance(result, list) else []

    async def send_message(
        self,
        channel: str,
        content: str,
        reply_to: str | None = None,
        broadcast: bool = False,
    ) -> dict:
        """Send a message. Content goes over stdin so it is never shell-quoted."""
        args = ["messages", "send", "--channel", channel, "--content", "-"]
        if reply_to:
            args += ["--reply-to", reply_to]
        if broadcast:
            args += ["--broadcast"]
        result = await self._run_json(*args, stdin=content)
        return result if isinstance(result, dict) else {"raw": result}

    async def search(
        self,
        query: str | None = None,
        author: str | None = None,
        since: int | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Full-text search across channels the caller can read.

        Note: search is relay-wide, not channel-scoped — the relay re-authorizes
        every hit against channel membership before returning it, so results are
        already limited to what this agent is allowed to see.
        """
        args = ["messages", "search", "--limit", str(min(limit, 100))]
        if query:
            args += ["--query", query]
        if author:
            args += ["--author", author]
        if since is not None:
            args += ["--since", str(int(since))]
        result = await self._run_json(*args)
        return result if isinstance(result, list) else []

    async def get_thread(self, channel: str, event: str, limit: int = 100) -> list[dict]:
        result = await self._run_json(
            "messages", "thread", "--channel", channel, "--event", event,
            "--limit", str(min(limit, 500)),
        )
        return result if isinstance(result, list) else []

    async def add_reaction(self, event: str, emoji: str) -> Any:
        return await self._run_json("reactions", "add", "--event", event, "--emoji", emoji)

    # ---------- channels ----------

    async def list_channels(self) -> list[dict]:
        result = await self._run_json("channels", "list")
        return result if isinstance(result, list) else []

    async def get_channel(self, channel: str) -> dict:
        result = await self._run_json("channels", "get", "--channel", channel)
        return result if isinstance(result, dict) else {}

    # ---------- engrams (NIP-AE agent memory) ----------

    async def mem_ls(self) -> Any:
        return await self._run_json("mem", "ls")

    async def mem_get(self, slug: str) -> str:
        return (await self._run("mem", "get", slug)).strip()

    async def mem_set(self, slug: str, value: str) -> Any:
        return await self._run_json("mem", "set", slug, "-", stdin=value)

    async def mem_rm(self, slug: str) -> Any:
        return await self._run_json("mem", "rm", slug)


def _decode_cli_error(stderr: str) -> str:
    """buzz-cli writes {"error": category, "message": detail} to stderr."""
    stderr = stderr.strip()
    if not stderr:
        return "unknown error"
    try:
        parsed = json.loads(stderr)
        if isinstance(parsed, dict):
            return f"{parsed.get('error', 'error')}: {parsed.get('message', stderr)}"
    except json.JSONDecodeError:
        pass
    return stderr


# ---------- pure event helpers (no I/O — unit tested) ----------


def tag_values(event: dict, key: str) -> list[str]:
    """All values for a given tag key, e.g. tag_values(ev, 'p') -> [pubkey, ...]."""
    out: list[str] = []
    for tag in event.get("tags") or []:
        if isinstance(tag, list) and len(tag) >= 2 and tag[0] == key:
            out.append(str(tag[1]))
    return out


def is_mentioned(event: dict, pubkey_hex: str) -> bool:
    """True if the event #p-tags the given pubkey.

    This is Buzz's own mention convention — kind.rs documents the agent
    shutdown protocol as 'a kind:9 message with a #p tag mentioning the agent',
    so #p is how a Buzz client encodes @agent.
    """
    if not pubkey_hex:
        return False
    target = pubkey_hex.lower()
    return any(v.lower() == target for v in tag_values(event, "p"))


def is_own(event: dict, pubkey_hex: str) -> bool:
    """True if we authored this event. Critical: prevents self-reply loops."""
    return bool(pubkey_hex) and str(event.get("pubkey", "")).lower() == pubkey_hex.lower()


def channel_of(event: dict) -> str | None:
    """The channel UUID from the NIP-29 'h' tag."""
    vals = tag_values(event, "h")
    return vals[0] if vals else None


def is_shutdown(event: dict, pubkey_hex: str) -> bool:
    """Buzz's documented agent shutdown convention: kind:9 '!shutdown' + #p tag."""
    return (
        event.get("kind") == 9
        and (event.get("content") or "").strip() == "!shutdown"
        and is_mentioned(event, pubkey_hex)
    )


def format_history(events: list[dict], names: dict[str, str] | None = None) -> str:
    """Render recent messages as compact context lines."""
    names = names or {}
    lines = []
    for ev in events:
        pk = str(ev.get("pubkey", ""))
        who = names.get(pk) or (pk[:8] if pk else "unknown")
        content = (ev.get("content") or "").strip()
        if content:
            lines.append(f"{who}: {content}")
    return "\n".join(lines)
