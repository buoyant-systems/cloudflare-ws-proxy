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
  /** Random ID created once per topic lifecycle — changes on delete/expire+recreate */
  generation: string;
  nextSeq: number;
  oldestSeq: number;
  /** Set on first publish, immutable for the topic's lifetime */
  maxBufferSize: number;
  /** Set on first publish, immutable for the topic's lifetime */
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
  private meta: TopicMeta | null = null;
  private alarmScheduled: boolean | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sessions = new Map();

    // Load cached state once per DO wake-up, before any requests are processed.
    // This eliminates per-request storage reads for meta and alarm state.
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<TopicMeta>("meta");
      this.meta = stored ?? {
        generation: "",
        nextSeq: 0,
        oldestSeq: 0,
        maxBufferSize: DEFAULT_MAX_BUFFER_SIZE,
        messageTtlMs: DEFAULT_MESSAGE_TTL_MS,
      };
      const alarm = await ctx.storage.getAlarm();
      this.alarmScheduled = alarm !== null;
    });

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
    if (path.endsWith("/batch-publish")) {
      return this.handleBatchPublish(request);
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

    // Load or initialize metadata
    const meta = await this.getMeta();

    // Topic config is set once on first publish, immutable after
    if (meta.generation === "") {
      meta.generation = crypto.randomUUID();
      meta.messageTtlMs = (body.ttl ?? DEFAULT_MESSAGE_TTL_MS / 1000) * 1000;
      meta.maxBufferSize = body.max_buffer ?? DEFAULT_MAX_BUFFER_SIZE;
    }

    const seq = meta.nextSeq;
    meta.nextSeq = seq + 1;

    const storedMessage: StoredMessage = {
      seq,
      data: body.message,
      encoding,
      timestamp: Date.now(),
    };

    // Compute prune keys before writing (mutates meta.oldestSeq)
    const pruneKeys = this.computePruneKeys(meta);

    // Coalesce message + metadata into a single storage write
    await this.ctx.storage.put({
      [`msg:${seq}`]: storedMessage,
      "meta": meta,
    });
    if (pruneKeys.length > 0) {
      await this.ctx.storage.delete(pruneKeys);
    }

    // Set alarm for TTL-based cleanup — only if none is pending (cached)
    await this.ensureAlarm(meta.messageTtlMs);

    // Broadcast to all connected WebSockets
    const envelope = JSON.stringify({
      generation: meta.generation,
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
      generation: meta.generation,
      connections: sockets.length,
    });
  }

  // -------------------------------------------------------------------------
  // /batch-publish — Process multiple messages in a single request
  // -------------------------------------------------------------------------

  private async handleBatchPublish(request: Request): Promise<Response> {
    let body: {
      messages?: Array<{
        message?: string;
        encoding?: "text" | "base64";
      }>;
      ttl?: number;
      max_buffer?: number;
    };
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      return Response.json(
        { error: "Missing or empty 'messages' array" },
        { status: 400 }
      );
    }

    // Validate all messages before processing any
    for (let i = 0; i < body.messages.length; i++) {
      if (typeof body.messages[i]!.message !== "string") {
        return Response.json(
          { error: `Invalid message at index ${i}: missing or invalid 'message' field` },
          { status: 400 }
        );
      }
    }

    // Load metadata once for the entire batch
    const meta = await this.getMeta();

    // Topic config is set once on first publish, immutable after
    if (meta.generation === "") {
      meta.generation = crypto.randomUUID();
      meta.messageTtlMs = (body.ttl ?? DEFAULT_MESSAGE_TTL_MS / 1000) * 1000;
      meta.maxBufferSize = body.max_buffer ?? DEFAULT_MAX_BUFFER_SIZE;
    }

    const now = Date.now();
    const storedMessages: StoredMessage[] = [];
    const storageEntries: Record<string, StoredMessage> = {};

    // Assign sequence numbers and prepare storage entries
    for (const item of body.messages) {
      const seq = meta.nextSeq;
      meta.nextSeq = seq + 1;

      const stored: StoredMessage = {
        seq,
        data: item.message!,
        encoding: item.encoding === "base64" ? "base64" : "text",
        timestamp: now,
      };

      storedMessages.push(stored);
      storageEntries[`msg:${seq}`] = stored;
    }

    // Compute prune keys before writing (mutates meta.oldestSeq)
    const pruneKeys = this.computePruneKeys(meta);

    // Coalesce all messages + metadata into a single storage write
    (storageEntries as Record<string, unknown>)["meta"] = meta;
    await this.ctx.storage.put(storageEntries);
    if (pruneKeys.length > 0) {
      await this.ctx.storage.delete(pruneKeys);
    }

    // Set alarm once if needed (cached)
    await this.ensureAlarm(meta.messageTtlMs);

    // Broadcast all messages to connected WebSockets
    const sockets = this.ctx.getWebSockets();
    for (const msg of storedMessages) {
      const envelope = JSON.stringify({
        generation: meta.generation,
        seq: msg.seq,
        data: msg.data,
        encoding: msg.encoding,
        timestamp: msg.timestamp,
      });
      for (const ws of sockets) {
        try {
          ws.send(envelope);
        } catch {
          // Socket may have died — will be cleaned up on close event
        }
      }
    }

    return Response.json({
      topic_id: extractTopicId(new URL(request.url)),
      generation: meta.generation,
      messages_published: storedMessages.length,
      first_seq: storedMessages[0]!.seq,
      last_seq: storedMessages[storedMessages.length - 1]!.seq,
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
    this.meta = null;
    this.alarmScheduled = false;

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

    // Batch-read all remaining messages in a single storage operation
    const allKeys: string[] = [];
    for (let seq = meta.oldestSeq; seq < meta.nextSeq; seq++) {
      allKeys.push(`msg:${seq}`);
    }

    const keysToDelete: string[] = [];
    let firstSurvivorTimestamp: number | null = null;

    if (allKeys.length > 0) {
      const messages = await this.ctx.storage.get<StoredMessage>(allKeys);
      for (let seq = meta.oldestSeq; seq < meta.nextSeq; seq++) {
        const msg = messages.get(`msg:${seq}`);
        if (!msg) {
          // Gap — already deleted (e.g. by pruneBuffer), skip
          continue;
        }
        if (msg.timestamp <= cutoff) {
          keysToDelete.push(`msg:${seq}`);
        } else {
          firstSurvivorTimestamp = msg.timestamp;
          break; // messages are sequential in time — no more expired
        }
      }
    }

    if (keysToDelete.length > 0) {
      await this.ctx.storage.delete(keysToDelete);
      meta.oldestSeq = meta.oldestSeq + keysToDelete.length;
      await this.ctx.storage.put("meta", meta);
    }

    const remaining = meta.nextSeq - meta.oldestSeq;

    if (remaining > 0 && firstSurvivorTimestamp !== null) {
      // Reschedule alarm for the next message expiry
      const nextAlarmTime = firstSurvivorTimestamp + meta.messageTtlMs;
      await this.ctx.storage.setAlarm(Math.max(nextAlarmTime, now + 1000));
      this.alarmScheduled = true;
    } else {
      this.alarmScheduled = false;
      // No messages left — the server hasn't published anything with a later
      // TTL, so this topic is dead. Close all connections and wipe storage
      // for zero-cost state. The server dictates topic lifecycle.
      const sockets = this.ctx.getWebSockets();
      for (const ws of sockets) {
        try {
          ws.close(1000, "topic expired");
        } catch {
          // Ignore errors on already-closed sockets
        }
      }
      this.sessions.clear();
      await this.ctx.storage.deleteAll();
      this.meta = null;
    }
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private async getMeta(): Promise<TopicMeta> {
    if (this.meta) return this.meta;
    // Fallback — should not normally be reached due to blockConcurrencyWhile
    const stored = await this.ctx.storage.get<TopicMeta>("meta");
    this.meta = stored ?? {
      generation: "",
      nextSeq: 0,
      oldestSeq: 0,
      maxBufferSize: DEFAULT_MAX_BUFFER_SIZE,
      messageTtlMs: DEFAULT_MESSAGE_TTL_MS,
    };
    return this.meta;
  }

  /**
   * Compute which message keys should be pruned to stay within maxBufferSize.
   * Pure arithmetic — no I/O. Mutates meta.oldestSeq.
   */
  private computePruneKeys(meta: TopicMeta): string[] {
    const count = meta.nextSeq - meta.oldestSeq;
    if (count <= meta.maxBufferSize) {
      return []; // No pruning needed — simple arithmetic check
    }

    const newOldest = meta.nextSeq - meta.maxBufferSize;
    const keys: string[] = [];
    for (let seq = meta.oldestSeq; seq < newOldest; seq++) {
      keys.push(`msg:${seq}`);
    }

    meta.oldestSeq = newOldest;
    return keys;
  }

  /**
   * Ensure a TTL alarm is scheduled, using cached state to avoid redundant
   * storage reads after the first check per DO lifetime.
   */
  private async ensureAlarm(ttlMs: number): Promise<void> {
    if (this.alarmScheduled === true) return;
    if (this.alarmScheduled === null) {
      // Unknown state — check storage once
      const existing = await this.ctx.storage.getAlarm();
      if (existing !== null) {
        this.alarmScheduled = true;
        return;
      }
    }
    await this.ctx.storage.setAlarm(Date.now() + ttlMs);
    this.alarmScheduled = true;
  }

  private async replayMessages(ws: WebSocket, cursor: number): Promise<void> {
    const meta = await this.getMeta();
    // Start from the cursor or the oldest available message, whichever is later
    const start = Math.max(cursor, meta.oldestSeq);

    // Batch-read all replay keys in a single storage operation
    const keys: string[] = [];
    for (let seq = start; seq < meta.nextSeq; seq++) {
      keys.push(`msg:${seq}`);
    }
    if (keys.length === 0) return;

    const messages = await this.ctx.storage.get<StoredMessage>(keys);
    for (let seq = start; seq < meta.nextSeq; seq++) {
      const msg = messages.get(`msg:${seq}`);
      if (!msg) continue; // gap from TTL expiry
      try {
        ws.send(
          JSON.stringify({
            generation: meta.generation,
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
