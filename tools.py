"""Tools Oakie can call on demand.

The 12-message backfill is the cheap always-on default. These are the escape
hatch: when a mention references something older, the agent goes and digs
rather than every mention paying for a huge context.

Handler contract (from the Hermes plugin guide):
  def handler(args: dict, **kwargs) -> str   # ALWAYS returns a JSON string
  never raises — catch everything and return error JSON
"""

from __future__ import annotations

import asyncio
import json
import os

from .buzzcli import BuzzCli, BuzzCliError

SEARCH_SCHEMA = {
    "name": "search_buzz_history",
    "description": (
        "Full-text search the history of Buzz channels you are a member of. "
        "Use this when a message references a decision, incident, person, or "
        "thread that is older than the recent messages already in your context "
        "— for example 'what did we decide about pricing last month'. Returns "
        "matching messages with author, timestamp, channel, and event id. "
        "Filter by author to find what one person said."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Keywords to search for. Omit only if filtering by author alone.",
            },
            "author": {
                "type": "string",
                "description": "Optional. Hex pubkey, npub, or display name to filter by.",
            },
            "since_days": {
                "type": "integer",
                "description": "Optional. Only return messages from the last N days.",
            },
            "limit": {
                "type": "integer",
                "description": "Max results, 1-100. Default 20.",
            },
        },
        "required": [],
    },
}

THREAD_SCHEMA = {
    "name": "read_buzz_thread",
    "description": (
        "Read the full reply thread rooted at a specific Buzz message. Use "
        "after search_buzz_history returns a promising hit and you need the "
        "whole conversation around it rather than the single matching message."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "channel": {"type": "string", "description": "Channel UUID the message lives in"},
            "event_id": {"type": "string", "description": "64-char hex event id of the thread root"},
        },
        "required": ["channel", "event_id"],
    },
}

MEM_GET_SCHEMA = {
    "name": "buzz_memory_get",
    "description": (
        "Read one of your persistent Buzz engrams (NIP-AE agent memory). These "
        "are encrypted notes only you and your owner can read, stored on the "
        "relay and scoped to this community — they survive across sessions. "
        "Call with no slug to list what you have stored."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "slug": {
                "type": "string",
                "description": "Memory slug to read. Omit to list all stored slugs.",
            },
        },
        "required": [],
    },
}

MEM_SET_SCHEMA = {
    "name": "buzz_memory_set",
    "description": (
        "Write a persistent Buzz engram (NIP-AE agent memory), encrypted to you "
        "and your owner and stored on the relay. Use for durable facts worth "
        "carrying across sessions — project conventions, standing decisions, "
        "who owns what. Do not use for transient conversation detail. Writing "
        "an existing slug replaces its value."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "slug": {"type": "string", "description": "Short kebab-case identifier"},
            "value": {"type": "string", "description": "The content to store"},
        },
        "required": ["slug", "value"],
    },
}


def _cli() -> BuzzCli:
    return BuzzCli()


def _run(coro):
    """Bridge Hermes' sync tool handlers to our async CLI wrapper.

    Tool handlers are called from a worker thread with no running loop, so a
    fresh loop is correct here. If we ever are on a loop, fall back to a
    dedicated thread so we never deadlock.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def _trim(events: list[dict], max_chars: int = 600) -> list[dict]:
    out = []
    for ev in events:
        content = (ev.get("content") or "").strip()
        if len(content) > max_chars:
            content = content[:max_chars] + "…"
        h = [t[1] for t in (ev.get("tags") or []) if isinstance(t, list) and len(t) > 1 and t[0] == "h"]
        out.append({
            "id": ev.get("id"),
            "author": ev.get("pubkey"),
            "created_at": ev.get("created_at"),
            "channel": h[0] if h else None,
            "content": content,
        })
    return out


def search_buzz_history(args: dict, **kwargs) -> str:
    query = (args.get("query") or "").strip() or None
    author = (args.get("author") or "").strip() or None
    if not query and not author:
        return json.dumps({"error": "provide at least one of 'query' or 'author'"})

    since = None
    if args.get("since_days"):
        import time
        try:
            since = int(time.time()) - int(args["since_days"]) * 86400
        except (TypeError, ValueError):
            since = None

    try:
        limit = max(1, min(int(args.get("limit") or 20), 100))
    except (TypeError, ValueError):
        limit = 20

    try:
        events = _run(_cli().search(query=query, author=author, since=since, limit=limit))
        return json.dumps({"count": len(events), "results": _trim(events)})
    except BuzzCliError as exc:
        return json.dumps({"error": exc.message, "exit_code": exc.code})
    except Exception as exc:  # never raise out of a handler
        return json.dumps({"error": f"search failed: {exc}"})


def read_buzz_thread(args: dict, **kwargs) -> str:
    channel = (args.get("channel") or "").strip()
    event_id = (args.get("event_id") or "").strip()
    if not channel or not event_id:
        return json.dumps({"error": "both 'channel' and 'event_id' are required"})
    try:
        events = _run(_cli().get_thread(channel, event_id))
        return json.dumps({"count": len(events), "thread": _trim(events, max_chars=1500)})
    except BuzzCliError as exc:
        return json.dumps({"error": exc.message, "exit_code": exc.code})
    except Exception as exc:
        return json.dumps({"error": f"thread read failed: {exc}"})


def buzz_memory_get(args: dict, **kwargs) -> str:
    slug = (args.get("slug") or "").strip()
    try:
        if not slug:
            return json.dumps({"memories": _run(_cli().mem_ls())})
        return json.dumps({"slug": slug, "value": _run(_cli().mem_get(slug))})
    except BuzzCliError as exc:
        hint = ""
        if exc.code in (1, 3):
            hint = " (engrams need BUZZ_AUTH_TAG or an --owner pubkey configured)"
        return json.dumps({"error": exc.message + hint, "exit_code": exc.code})
    except Exception as exc:
        return json.dumps({"error": f"memory read failed: {exc}"})


def buzz_memory_set(args: dict, **kwargs) -> str:
    slug = (args.get("slug") or "").strip()
    value = args.get("value")
    if not slug or value is None:
        return json.dumps({"error": "both 'slug' and 'value' are required"})
    try:
        _run(_cli().mem_set(slug, str(value)))
        return json.dumps({"ok": True, "slug": slug})
    except BuzzCliError as exc:
        return json.dumps({"error": exc.message, "exit_code": exc.code})
    except Exception as exc:
        return json.dumps({"error": f"memory write failed: {exc}"})


def _engrams_configured() -> bool:
    """Only expose the memory tools if engram auth is actually set up."""
    return bool(os.getenv("BUZZ_AUTH_TAG") or os.getenv("BUZZ_OWNER_PUBKEY"))


def register_tools(ctx) -> None:
    ctx.register_tool(
        name="search_buzz_history", toolset="buzz",
        schema=SEARCH_SCHEMA, handler=search_buzz_history,
    )
    ctx.register_tool(
        name="read_buzz_thread", toolset="buzz",
        schema=THREAD_SCHEMA, handler=read_buzz_thread,
    )
    ctx.register_tool(
        name="buzz_memory_get", toolset="buzz",
        schema=MEM_GET_SCHEMA, handler=buzz_memory_get,
        check_fn=_engrams_configured,
    )
    ctx.register_tool(
        name="buzz_memory_set", toolset="buzz",
        schema=MEM_SET_SCHEMA, handler=buzz_memory_set,
        check_fn=_engrams_configured,
    )
