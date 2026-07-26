# hermes-buzz-bridge

Bridges [Hermes Agent](https://github.com/NousResearch/hermes-agent) into a
[Buzz](https://github.com/block/buzz) relay channel over Nostr. The agent joins
as a channel member with its own keypair and replies when `@mentioned`.

Registers as a **Hermes gateway platform** (like Telegram or Discord), so it
inherits sessions, cron delivery, slash commands, allowlists, and message
chunking for free.

## How it works

```
Buzz relay  ──poll──▶  buzz-cli  ──▶  BuzzAdapter  ──▶  Gateway Runner  ──▶  Hermes
     ▲                                                                        │
     └────────────  buzz messages send --reply-to  ◀───── adapter.send() ◀─────┘
```

All relay I/O shells out to `buzz-cli` (JSON in / JSON out), so **no Nostr
library is required** and NIP-98 request signing is handled by the CLI.

`buzz-cli` is REST-only — there is no live subscribe — so the receive path
polls `buzz messages get --since <ts>` every 5s. Dedup is by event id, because
the relay treats `--since` as inclusive and re-returns the boundary event.

## Design decisions

| Decision | Why |
|---|---|
| **Mention + slash-command** | Responds when `#p`-tagged (`@agent`) or when a message starts with `/`. Keeps the agent quiet in normal chatter while supporting gateway slash commands like `/sethome`. |
| **Auto-channel discovery** | Set `BUZZ_CHANNELS=*` and the adapter discovers all channels the agent belongs to at startup, then re-checks every 60s. Add the agent to a new channel and it starts responding without a restart. No need to hardcode UUIDs. |
| **12-message backfill** | A mention often refers to something said a few messages ago. Cheap always-on context. |
| **`search_buzz_history` tool** | The escape hatch. If the mention refers to something older, the agent digs on demand instead of every mention paying for a huge context. |
| **Self-filter on `pubkey`** | Without it the agent sees its own replies and loops forever. Reply event ids are also pre-marked as seen. |
| **Poll, not WebSocket** | Avoids the NIP-42 auth handshake entirely; `buzz-cli` already authenticates every REST call. |
| **One session per channel** | Chat id = channel UUID, so Hermes' per-chat session store keeps channel contexts from bleeding together. |

## Install

**1. Build `buzz-cli`** from a `block/buzz` checkout:
```bash
cargo install --path crates/buzz-cli
buzz --help   # confirm it's on PATH
```

**2. Generate the agent's keypair.** `buzz-admin` handles key generation — keep
both halves; you need the nsec *and* the hex pubkey.

**3. Add the agent to your channel** (same as adding a human):
```bash
buzz channels list                                    # find the channel UUID
buzz channels add-member --channel <uuid> --pubkey <agent-hex-pubkey>
```

**4. Install the plugin:**
```bash
cp -r buzz-nostr ~/.hermes/plugins/
```

**5. Configure** `~/.hermes/.env`:
```bash
BUZZ_RELAY_URL=http://your-relay:3000     # http(s), NOT ws://
BUZZ_PRIVATE_KEY=nsec1...                 # the agent's key
BUZZ_AGENT_PUBKEY=<64-char hex>           # the agent's own pubkey
BUZZ_CHANNELS=*                            # '*' = all channels, or comma-separated UUIDs

# optional
BUZZ_POLL_INTERVAL=5
BUZZ_BACKFILL_COUNT=12
BUZZ_HOME_CHANNEL=<uuid>                  # target for `deliver=buzz` cron jobs
BUZZ_ALLOWED_USERS=<hex>,<hex>            # empty = any channel member
BUZZ_AUTH_TAG=...                         # required for the engram memory tools
```

**6. Enable and run:**
```bash
hermes plugins enable buzz-nostr
hermes gateway
```

## Tools

| Tool | Purpose |
|---|---|
| `search_buzz_history` | Full-text search across channels the agent can read. Filter by author or time window. |
| `read_buzz_thread` | Pull the full reply thread around a specific event id. |
| `buzz_memory_get` / `buzz_memory_set` | NIP-AE engrams — encrypted persistent memory on the relay, readable by the agent and its owner. Hidden unless `BUZZ_AUTH_TAG` or `BUZZ_OWNER_PUBKEY` is set. |

Search is relay-wide, but the relay re-authorizes every hit against channel
membership before returning it — the agent can only find what it's allowed to see.

## Operational notes

- **Cursor starts at connect time.** The agent won't reply to mentions that
  happened while it was offline. Change `connect()` if you want catch-up on restart.
- **`[SILENT]`** — Hermes suppresses delivery when the whole response is that
  token. The platform hint tells the agent to use it when nothing is needed.
- **`!shutdown`** — a kind:9 message with content exactly `!shutdown` plus a
  `#p` tag mentioning the agent triggers graceful disconnect. This is Buzz's
  documented convention, so it works from any Buzz client.
- **Backoff** — poll failures double the interval up to 60s, then recover on
  the first clean pass. Relay restarts won't spin the CPU.
- **Cron out-of-process** — `standalone_sender_fn` is registered, so
  `hermes cron run` can deliver to Buzz without the gateway in the same process.

## Verified against source

Values below were read from `block/buzz@main`, not assumed:

- `KIND_STREAM_MESSAGE = 9` (`crates/buzz-core/src/kind.rs`) — plus 40002,
  40008, 45001, 45003, matching what `messages get` requests.
- Channel scoping uses the NIP-29 **`h` tag**, not `e` tags.
- `buzz-cli` normalizes every event to
  `{id, pubkey, kind, content, created_at, tags}` (`client.rs::normalize_events`).
- Exit codes: `0=ok 1=user 2=network 3=auth 4=other 5=write-conflict`.
- `messages get` accepts `--limit --before --since --kinds`; `--limit` caps at 200.

## Known gaps

- **NIP-OA attestation for engrams** isn't automated — you must supply
  `BUZZ_AUTH_TAG` yourself, or the memory tools stay hidden.
- **No media/attachment support.** Buzz has Blossom uploads (`buzz upload file`);
  the adapter is text-only today.
- **No typing indicator.** Buzz has kind 20002 (`KIND_TYPING_INDICATOR`) but
  `buzz-cli` exposes no command for it, so `send_typing()` is unimplemented.
- **Reactions are wired in `BuzzCli` but unused.** Reacting 👀 on mention before
  the LLM finishes would be a nice touch for slow local models.
