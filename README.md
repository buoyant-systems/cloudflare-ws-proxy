# cloudflare-ws-proxy

A deployable Cloudflare Worker template that proxies messages from serverless backends (e.g. Google Cloud Run, AWS Lambda) to browser clients via WebSocket connections held open at the edge — at near-zero cost when idle.

## The Problem

Serverless platforms like Cloud Run charge for active container time and have request timeout limits. Keeping thousands of long-lived WebSocket connections open directly against your serverless instances is expensive and fragile.

## The Solution

**cloudflare-ws-proxy** offloads the "connection holding" to Cloudflare's edge network using [Durable Objects](https://developers.cloudflare.com/durable-objects/) with [WebSocket Hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).

- **Your backend** sends short-lived HTTP requests to publish messages → Cloud Run shuts down immediately.
- **Cloudflare** holds all the WebSocket connections to browsers in hibernation mode → near-zero cost when no messages are flowing.
- **Browsers** receive messages in real-time via WebSocket with automatic reconnection support via cursor-based replay.

```
┌──────────────┐    HTTP POST     ┌──────────────────┐   WebSocket    ┌─────────┐
│   Backend    │ ───────────────→ │  cloudflare-ws-  │ ←───────────→  │ Browser │
│ (Cloud Run)  │   (publish msg)  │      proxy       │  (hibernated)  │ Client  │
└──────────────┘                  │  Durable Object  │                └─────────┘
       │                          └──────────────────┘               ┌─────────┐
       │   short-lived HTTP              │                      ←──→ │ Browser │
       └── connection closes             │   long-lived WS           │ Client  │
           immediately                   │   connections held        └─────────┘
                                         │   at zero cost
                                         │   during hibernation
```

## Deploy

### One-off deploy (no repo to maintain)

Copy-paste this to deploy directly to your Cloudflare account. No fork, no CI/CD — just a running Worker.

```bash
npm create cloudflare@latest my-ws-proxy -- --template https://github.com/buoyant-systems/cloudflare-ws-proxy
cd my-ws-proxy
npx wrangler secret put BACKEND_SECRET    # enter a strong, random secret
npm run deploy
```

You'll receive a URL like `https://cloudflare-ws-proxy.<your-subdomain>.workers.dev`. Done — you can delete the local directory if you want, the Worker lives in your Cloudflare account.

### One-click deploy (with CI/CD)

If you want a GitHub repo with automatic deploys on every push:

[![Deploy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/buoyant-systems/cloudflare-ws-proxy)

This forks the repo into your GitHub account and sets up a GitHub Actions workflow. You'll still need to set the secret afterward:

```bash
npx wrangler secret put BACKEND_SECRET
```

### Local development

```bash
git clone https://github.com/buoyant-systems/cloudflare-ws-proxy.git
cd cloudflare-ws-proxy
npm install

# Create a .dev.vars file with your secret for local dev
echo 'BACKEND_SECRET=my-dev-secret' > .dev.vars

npm run dev
```

## API Reference

All backend endpoints require the `Authorization: Bearer <BACKEND_SECRET>` header.

Topic IDs are always bi-keyed: `<shard>/<topic>`. The shard determines which Durable Object handles the request; the topic identifies an independent channel within that DO. All topic routes live under the `/t` prefix. See [Topic ID Format](#topic-id-format) for details.

### Generate Client Connection URL

Creates a short-lived, authenticated WebSocket URL that can be passed to a browser client.

```
POST /t/:shard/:topic/auth
```

**Request body (optional):**
```json
{
  "token_ttl_seconds": 300,
  "cursor": 0
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token_ttl_seconds` | number | `300` (5 min) | How long the generated URL remains valid |
| `cursor` | number | _none_ | If set, the client will receive buffered messages starting from this sequence number on connect |

**Response:**
```json
{
  "url": "wss://your-worker.workers.dev/t/user:123/notifications/connect?token=<signed-token>",
  "topic_id": "user:123/notifications",
  "expires_at": 1713234567890
}
```

**Example:**
```bash
curl -X POST https://your-worker.workers.dev/t/user:123/notifications/auth \
  -H "Authorization: Bearer YOUR_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"token_ttl_seconds": 600}'
```

---

### Publish a Message

Broadcasts a message to all connected WebSocket clients on a topic. If no clients are connected, the message is still buffered for future replay.

```
POST /t/:shard/:topic/publish
```

**Request body:**
```json
{
  "message": "hello world",
  "ttl": 3600,
  "max_buffer": 100
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `message` | string | _required_ | The message payload. For binary data, base64-encode it and set `encoding: "base64"` |
| `encoding` | string | `"text"` | Set to `"base64"` if `message` contains base64-encoded binary data |
| `ttl` | number | `3600` (1 hour) | Message time-to-live in seconds. **Set on first publish; ignored on subsequent publishes** |
| `max_buffer` | number | `100` | Maximum number of messages to buffer. **Set on first publish; ignored on subsequent publishes** |

> **Note:** `ttl` and `max_buffer` are locked when the topic is created (first publish). Subsequent publishes to the same topic use the original values. To change them, delete the topic and recreate it.

**Response:**
```json
{
  "seq": 42,
  "topic_id": "user:123/notifications",
  "generation": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "connections": 5
}
```

| Field | Type | Description |
|-------|------|-------------|
| `seq` | number | Sequence number assigned to this message (monotonically increasing, resets on topic teardown) |
| `topic_id` | string | The full topic ID (`shard/topic`) |
| `generation` | string | Unique ID for this topic lifecycle. Changes when the topic is deleted or expires and is recreated. Clients can compare this to detect stale cursors |
| `connections` | number | Number of currently connected WebSocket clients on this topic |

**Example:**
```bash
# Text message
curl -X POST https://your-worker.workers.dev/t/user:123/notifications/publish \
  -H "Authorization: Bearer YOUR_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"message": "{\"event\": \"update\", \"data\": {\"score\": 42}}"}'

# Binary message (base64-encoded)
curl -X POST https://your-worker.workers.dev/t/chat.room.5/messages/publish \
  -H "Authorization: Bearer YOUR_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"message": "SGVsbG8gV29ybGQ=", "encoding": "base64"}'
```

---

### Bulk Publish

Publishes multiple messages to one or more topics in a single HTTP request. Messages are grouped by shard internally — only one Durable Object subrequest is made per unique shard, regardless of how many messages or topics target it.

```
POST /bulk-publish
```

**Request body:**
```json
{
  "messages": [
    { "topic_id": "chat.room.1/messages", "message": "{\"text\": \"hello\"}" },
    { "topic_id": "chat.room.1/messages", "message": "{\"text\": \"world\"}" },
    { "topic_id": "chat.room.1/reactions", "message": "{\"emoji\": \"👍\"}" },
    { "topic_id": "chat.room.2/messages", "message": "SGVsbG8=", "encoding": "base64" }
  ],
  "ttl": 3600,
  "max_buffer": 100
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `messages` | array | _required_ | Array of message objects to publish |
| `messages[].topic_id` | string | _required_ | Target topic (`shard/topic` format) |
| `messages[].message` | string | _required_ | The message payload |
| `messages[].encoding` | string | `"text"` | Set to `"base64"` for binary data |
| `ttl` | number | `3600` (1 hour) | TTL in seconds. **Only used when creating a new topic; ignored for existing topics** |
| `max_buffer` | number | `100` | Buffer size. **Only used when creating a new topic; ignored for existing topics** |

**Response:**
```json
{
  "topics": 3,
  "messages": 4,
  "results": [
    {
      "topic_id": "chat.room.1/messages",
      "generation": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "status": 200,
      "messages_published": 2,
      "first_seq": 0,
      "last_seq": 1,
      "connections": 3
    },
    {
      "topic_id": "chat.room.1/reactions",
      "generation": "11223344-5566-7788-9900-aabbccddeeff",
      "status": 200,
      "messages_published": 1,
      "first_seq": 0,
      "last_seq": 0,
      "connections": 3
    },
    {
      "topic_id": "chat.room.2/messages",
      "generation": "f7e8d9c0-b1a2-3456-fedc-ba0987654321",
      "status": 200,
      "messages_published": 1,
      "first_seq": 0,
      "last_seq": 0,
      "connections": 0
    }
  ]
}
```

Returns `200` if all topics succeeded, `207 Multi-Status` if any topic failed.

**Example:**
```bash
curl -X POST https://your-worker.workers.dev/bulk-publish \
  -H "Authorization: Bearer YOUR_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"topic_id": "user:123/notifications", "message": "{\"event\": \"notify\"}"},
      {"topic_id": "user:123/typing", "message": "{\"event\": \"started\"}"},
      {"topic_id": "user:456/notifications", "message": "{\"event\": \"notify\"}"}
    ]
  }'
```

> **Efficiency note:** 3 messages across 2 shards = only 2 Durable Object requests (one per unique shard). `user:123/notifications` and `user:123/typing` share the same shard and are handled in a single DO call.

---

### Connect (Browser WebSocket)

Opens a WebSocket connection to receive messages on a topic. This endpoint is called by browser clients using the URL generated by the auth endpoint.

```
GET /t/:shard/:topic/connect?token=<signed-token>&cursor=<seq>
```

| Param | Type | Description |
|-------|------|-------------|
| `token` | string | The signed HMAC token from the auth endpoint |
| `cursor` | number | _(optional)_ Replay buffered messages starting from this sequence number |

**Client-side usage:**
```javascript
// 1. Your backend calls the auth endpoint and passes the URL to the client
const url = "wss://your-worker.workers.dev/t/user:123/notifications/connect?token=abc123";

// 2. Open the WebSocket
const ws = new WebSocket(url);

let lastSeq = 0;
let generation = null;

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // msg = { generation: "...", seq: 42, data: "hello world", timestamp: 1713234567890 }

  // Detect topic recycling — generation changes when topic is deleted/expired
  if (generation && msg.generation !== generation) {
    console.warn("Topic was recycled — cursor is stale, resetting");
    lastSeq = 0;
  }
  generation = msg.generation;
  lastSeq = msg.seq;
  console.log("Received:", msg.data);
};

ws.onclose = () => {
  // Reconnect with cursor to replay missed messages
  // (request a new auth URL from your backend with cursor = lastSeq)
};
```

**Message envelope:**
```json
{
  "generation": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "seq": 42,
  "data": "the message payload",
  "encoding": "text",
  "timestamp": 1713234567890
}
```

---

### Delete Topic

Force-closes all WebSocket connections and wipes all buffered messages for a single topic within a shard. Other topics in the same shard are unaffected. The next publish to this topic starts a new lifecycle with a fresh `generation` and sequence numbers from 0.

```
DELETE /t/:shard/:topic
```

**Response:**
```json
{
  "deleted": true,
  "topic_id": "user:123/notifications",
  "connections_closed": 5
}
```

**Example:**
```bash
curl -X DELETE https://your-worker.workers.dev/t/user:123/notifications \
  -H "Authorization: Bearer YOUR_SECRET"
```

---

### Delete Shard

Force-closes all WebSocket connections and wipes all topics and buffered messages for an entire shard. Use this when the parent entity is being removed (e.g., user account deleted, chat room closed).

```
DELETE /t/:shard
```

**Response:**
```json
{
  "deleted": true,
  "shard": "user:123",
  "topics_deleted": 3,
  "connections_closed": 12
}
```

**Example:**
```bash
curl -X DELETE https://your-worker.workers.dev/t/user:123 \
  -H "Authorization: Bearer YOUR_SECRET"
```

---

## Topic ID Format

All topic IDs use a two-part format with `/` as the separator:

```
<shard>/<topic>
```

| Component | Purpose | Constraints |
|-----------|---------|-------------|
| `shard` | Determines which Durable Object instance handles this topic (via `idFromName()`) | Must match `^[a-zA-Z0-9_.:~-]{1,128}$` |
| `topic` | Identifies an independent channel within that DO | Must match `^[a-zA-Z0-9_.:~-]{1,128}$` |

**Examples:**
```
user:123/notifications    → DO("user:123"), topic = "notifications"
user:123/typing           → DO("user:123"), topic = "typing"
user:123/presence         → DO("user:123"), topic = "presence"
chat.room.5/messages      → DO("chat.room.5"), topic = "messages"
chat.room.5/reactions     → DO("chat.room.5"), topic = "reactions"
```

Topics that share a shard live in the same Durable Object. They have **fully independent** message buffers, sequences, generations, TTLs, and WebSocket sessions — one topic's clients never receive another topic's messages.

Since `/` is the separator and also the URL path delimiter, the two-part topic ID maps directly to URL path segments under the `/t` prefix: `/t/:shard/:topic/publish`.

**Invalid IDs** return `400 Bad Request`. Both segments are required and must be non-empty.

## How Costs Stay Low

| State | What's Happening | Cost |
|-------|-----------------|------|
| **Hibernating** | Clients connected, no messages flowing | ~$0 (no compute charges) |
| **Active** | Message received, broadcast to N clients | Billed for milliseconds of compute |
| **Empty** | No connections, no storage | $0 (object evicted from memory) |
| **Buffering** | Messages stored, no clients | Storage charges only (~$0.20/GB-month) |

Durable Object hibernation means you only pay for the brief moments when messages are actually being processed and broadcast. Between messages, the DO sleeps while Cloudflare's edge infrastructure keeps the WebSocket connections alive for free.

### Multi-Topic Cost Savings

Grouping related topics under the same shard consolidates them into a single Durable Object:

- **Duration billing** — DOs are billed at a fixed 128 MB allocation per active object, regardless of actual memory usage. Consolidating 5 topics into 1 DO = 5× fewer billable DO-seconds.
- **Fewer wake-ups** — Publishing to related topics (e.g., `user:123/notifications` and `user:123/typing`) only wakes one DO instead of two.
- **Bulk publish efficiency** — `POST /bulk-publish` with messages targeting the same shard results in a single DO subrequest instead of one per topic.

> **Rule of thumb:** If you have N related topics per entity (e.g., per user, per room), grouping them under one shard reduces DO duration costs by ~N×.

## Topic Lifecycle

Each topic has a **static configuration** that is locked on creation (first publish):

- **TTL** — how long messages live before automatic cleanup
- **Buffer size** — maximum number of messages to retain (oldest are pruned when exceeded)
- **Generation** — a unique UUID identifying this topic lifecycle

When a topic is torn down (via `DELETE` or when all messages expire via TTL), its storage is fully wiped. Other topics in the same shard are unaffected. The next publish creates a **new lifecycle** with a fresh generation, new config, and sequence numbers starting from 0.

Clients can detect topic recycling by comparing the `generation` field in message envelopes. A generation change means the cursor is stale and should be discarded.

## Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `BACKEND_SECRET` | Yes | Shared secret for authenticating backend requests. Set via `wrangler secret put BACKEND_SECRET` |

### Default Limits

| Setting | Default | Configurable Via |
|---------|---------|-----------------| 
| Message buffer size | 100 messages | `max_buffer` field on first publish |
| Message TTL | 1 hour (3600s) | `ttl` field on first publish |
| Auth token TTL | 5 minutes (300s) | `token_ttl_seconds` field on auth |

> **Note:** `ttl` and `max_buffer` are set once per topic lifecycle (on the first publish). To change them, delete the topic and let the next publish create a new lifecycle with the desired values.

## Architecture

This project uses two Cloudflare primitives:

1. **Cloudflare Worker** — The HTTP entry point. Routes requests, validates auth, and forwards to Durable Objects. Extracts the shard key from the URL to resolve the correct DO instance.
2. **Durable Object (`ProxyDO`)** — One instance per shard. Manages multiple independent topics with isolated storage, WebSocket sessions, and lifecycles. Uses WebSocket Hibernation for zero-cost idle connections and the Alarms API for TTL-based cleanup across all topics.

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed sequence and component diagrams.

```
src/
├── index.ts       # Worker fetch handler (router + auth gatekeeper)
├── topic-key.ts   # Topic ID parsing (shard/topic extraction)
├── auth.ts        # HMAC-SHA256 token generation & verification
└── proxy-do.ts    # ProxyDO Durable Object class (multi-topic)
```

## License

[MIT](LICENSE) — Copyright (c) 2026 Buoyant Systems
