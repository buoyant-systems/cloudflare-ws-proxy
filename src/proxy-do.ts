import { DurableObject } from "cloudflare:workers";

/**
 * ProxyDO — A hibernation-enabled Durable Object that holds WebSocket
 * connections on behalf of browser clients and broadcasts messages published
 * by the backend.
 *
 * One instance per topic. Uses SQLite-backed storage for message buffering
 * and the Alarms API for TTL-based cleanup.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Env {
  PROXY_DO: DurableObjectNamespace<ProxyDO>;
  BACKEND_SECRET: string;
}

interface StoredMessage {
  seq: number;
  data: string;
  encoding: "text" | "base64";
  timestamp: number;
}

interface TopicMeta {
  nextSeq: number;
  maxBufferSize: number;
  messageTtlMs: number;
}

interface SessionAttachment {
  id: string;
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_MAX_BUFFER_SIZE = 100;
const DEFAULT_MESSAGE_TTL_MS = 3600 * 1000; // 1 hour

// ---------------------------------------------------------------------------
// ProxyDO
// ---------------------------------------------------------------------------

export class ProxyDO extends DurableObject<Env> {
  private sessions: Map<WebSocket, SessionAttachment>;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sessions = new Map();

    // Restore sessions from hibernated WebSockets
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as SessionAttachment | null;
      if (attachment) {
        this.sessions.set(ws, { ...attachment });
      }
    }

    // Application-level auto ping/pong — does NOT wake the DO
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair("ping", "pong")
    );
  }

  // -------------------------------------------------------------------------
  // Fetch handler — internal routing from the Worker
  // -------------------------------------------------------------------------

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path.endsWith("/connect")) {
      return this.handleConnect(request, url);
    }
    if (path.endsWith("/publish")) {
      return this.handlePublish(request);
    }
    if (path.endsWith("/delete")) {
      return this.handleDelete();
    }

    return new Response("Not found", { status: 404 });
  }

  // -------------------------------------------------------------------------
  // /connect — WebSocket upgrade
  // -------------------------------------------------------------------------

  private async handleConnect(request: Request, url: URL): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const client = webSocketPair[0];
    const server = webSocketPair[1];

    // Accept via hibernation API
    this.ctx.acceptWebSocket(server);

    const sessionId = crypto.randomUUID();
    const attachment: SessionAttachment = { id: sessionId };
    server.serializeAttachment(attachment);
    this.sessions.set(server, attachment);

    // Replay buffered messages if cursor is provided
    const cursorParam = url.searchParams.get("cursor");
    if (cursorParam !== null) {
      const cursor = parseInt(cursorParam, 10);
      if (!isNaN(cursor)) {
        await this.replayMessages(server, cursor);
      }
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // -------------------------------------------------------------------------
  // /publish — Broadcast a message
  // -------------------------------------------------------------------------

  private async handlePublish(request: Request): Promise<Response> {
    let body: {
      message?: string;
      encoding?: "text" | "base64";
      ttl?: number;
      max_buffer?: number;
    };
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (typeof body.message !== "string") {
      return Response.json(
        { error: "Missing or invalid 'message' field" },
        { status: 400 }
      );
    }

    const encoding = body.encoding === "base64" ? "base64" : "text";
    const ttlMs = (body.ttl ?? DEFAULT_MESSAGE_TTL_MS / 1000) * 1000;
    const maxBuffer = body.max_buffer ?? DEFAULT_MAX_BUFFER_SIZE;

    // Load or initialize metadata
    const meta = await this.getMeta();
    meta.messageTtlMs = ttlMs;
    meta.maxBufferSize = maxBuffer;

    const seq = meta.nextSeq;
    meta.nextSeq = seq + 1;

    const storedMessage: StoredMessage = {
      seq,
      data: body.message,
      encoding,
      timestamp: Date.now(),
    };

    // Store the message and updated metadata
    await this.ctx.storage.put(`msg:${seq}`, storedMessage);
    await this.ctx.storage.put("meta", meta);

    // Prune oldest messages if buffer exceeds max
    await this.pruneBuffer(meta);

    // Set alarm for TTL-based cleanup
    await this.ctx.storage.setAlarm(Date.now() + ttlMs);

    // Broadcast to all connected WebSockets
    const envelope = JSON.stringify({
      seq: storedMessage.seq,
      data: storedMessage.data,
      encoding: storedMessage.encoding,
      timestamp: storedMessage.timestamp,
    });

    const sockets = this.ctx.getWebSockets();
    for (const ws of sockets) {
      try {
        ws.send(envelope);
      } catch {
        // Socket may have died — will be cleaned up on close event
      }
    }

    return Response.json({
      seq,
      topic_id: extractTopicId(new URL(request.url)),
      connections: sockets.length,
    });
  }

  // -------------------------------------------------------------------------
  // /delete — Force teardown
  // -------------------------------------------------------------------------

  private async handleDelete(): Promise<Response> {
    const sockets = this.ctx.getWebSockets();
    const count = sockets.length;

    for (const ws of sockets) {
      try {
        ws.close(1000, "topic deleted");
      } catch {
        // Ignore errors on already-closed sockets
      }
    }

    this.sessions.clear();
    await this.ctx.storage.deleteAll();

    return Response.json({ deleted: true, connections_closed: count });
  }

  // -------------------------------------------------------------------------
  // Hibernation event handlers
  // -------------------------------------------------------------------------

  async webSocketMessage(ws: WebSocket, _message: ArrayBuffer | string): Promise<void> {
    // Clients are listen-only in this architecture.
    // Silently ignore any messages from the client.
  }

  async webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    _wasClean: boolean
  ): Promise<void> {
    ws.close(code, reason);
    this.sessions.delete(ws);
  }

  async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    ws.close(1011, "WebSocket error");
    this.sessions.delete(ws);
  }

  // -------------------------------------------------------------------------
  // Alarm — TTL-based message cleanup
  // -------------------------------------------------------------------------

  async alarm(): Promise<void> {
    const meta = await this.getMeta();
    const now = Date.now();
    const cutoff = now - meta.messageTtlMs;

    // Scan all stored messages and delete expired ones
    const entries = await this.ctx.storage.list<StoredMessage>({ prefix: "msg:" });
    let earliestRemainingTimestamp = Infinity;
    let hasMessages = false;

    for (const [key, msg] of entries) {
      if (msg.timestamp <= cutoff) {
        await this.ctx.storage.delete(key);
      } else {
        hasMessages = true;
        if (msg.timestamp < earliestRemainingTimestamp) {
          earliestRemainingTimestamp = msg.timestamp;
        }
      }
    }

    if (hasMessages) {
      // Reschedule alarm for the next message expiry
      const nextAlarmTime = earliestRemainingTimestamp + meta.messageTtlMs;
      await this.ctx.storage.setAlarm(Math.max(nextAlarmTime, now + 1000));
    } else {
      // No messages, no connections → full cleanup for zero-cost state
      const sockets = this.ctx.getWebSockets();
      if (sockets.length === 0) {
        await this.ctx.storage.deleteAll();
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private async getMeta(): Promise<TopicMeta> {
    const meta = await this.ctx.storage.get<TopicMeta>("meta");
    return (
      meta ?? {
        nextSeq: 0,
        maxBufferSize: DEFAULT_MAX_BUFFER_SIZE,
        messageTtlMs: DEFAULT_MESSAGE_TTL_MS,
      }
    );
  }

  private async pruneBuffer(meta: TopicMeta): Promise<void> {
    const entries = await this.ctx.storage.list<StoredMessage>({ prefix: "msg:" });

    if (entries.size <= meta.maxBufferSize) {
      return;
    }

    // Sort by seq ascending and delete the oldest
    const sorted = [...entries.entries()].sort((a, b) => a[1].seq - b[1].seq);
    const toDelete = sorted.length - meta.maxBufferSize;

    for (let i = 0; i < toDelete; i++) {
      await this.ctx.storage.delete(sorted[i]![0]);
    }
  }

  private async replayMessages(ws: WebSocket, cursor: number): Promise<void> {
    const entries = await this.ctx.storage.list<StoredMessage>({ prefix: "msg:" });

    // Sort by seq ascending and replay messages at or after the cursor
    const sorted = [...entries.values()]
      .filter((msg) => msg.seq >= cursor)
      .sort((a, b) => a.seq - b.seq);

    for (const msg of sorted) {
      try {
        ws.send(
          JSON.stringify({
            seq: msg.seq,
            data: msg.data,
            encoding: msg.encoding,
            timestamp: msg.timestamp,
          })
        );
      } catch {
        break; // Socket died during replay
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

function extractTopicId(url: URL): string {
  // URL path is /.../topic/:id/...
  const parts = url.pathname.split("/");
  const topicIndex = parts.indexOf("topic");
  const topicId = topicIndex >= 0 ? parts[topicIndex + 1] : undefined;
  return topicId ?? "unknown";
}
