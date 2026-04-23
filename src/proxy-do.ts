import { DurableObject } from "cloudflare:workers";

/**
 * ProxyDO — A hibernation-enabled Durable Object that holds WebSocket
 * connections on behalf of browser clients and broadcasts messages published
 * by the backend.
 *
 * One instance per shard. Manages multiple independent topics with isolated
 * storage, WebSocket sessions, and lifecycles. Uses SQLite-backed storage
 * for message buffering and the Alarms API for TTL-based cleanup.
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
  topicKey: string;
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
  /** Per-topic metadata cache. Key = topicKey */
  private topicMetas: Map<string, TopicMeta> = new Map();
  /** Per-topic WebSocket sessions. Key = topicKey → Set of WebSockets */
  private topicSessions: Map<string, Set<WebSocket>> = new Map();
  /** Global alarm state (shared — only one alarm per DO) */
  private alarmScheduled: boolean | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Check alarm state once per DO wake-up
    ctx.blockConcurrencyWhile(async () => {
      const alarm = await ctx.storage.getAlarm();
      this.alarmScheduled = alarm !== null;
      // Topic metas are loaded lazily per-topic on first access
    });

    // Restore sessions from hibernated WebSockets
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as SessionAttachment | null;
      if (attachment) {
        let set = this.topicSessions.get(attachment.topicKey);
        if (!set) {
          set = new Set();
          this.topicSessions.set(attachment.topicKey, set);
        }
        set.add(ws);
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
    const parts = url.pathname.split("/").filter(Boolean);
    // parts[0] = shard, parts[1] = topicKey or "delete-all" or "batch-publish", parts[2] = action

    if (parts.length === 2 && parts[1] === "delete-all") {
      return this.handleDeleteAll();
    }

    if (parts.length === 2 && parts[1] === "batch-publish") {
      return this.handleBatchPublish(request, parts[0]!);
    }

    if (parts.length < 3) {
      return new Response("Not found", { status: 404 });
    }

    const shardKey = parts[0]!;
    const topicKey = parts[1]!;
    const action = parts[2]!;

    if (action === "connect") {
      return this.handleConnect(request, url, topicKey);
    }
    if (action === "publish") {
      return this.handlePublish(request, shardKey, topicKey);
    }
    if (action === "delete") {
      return this.handleDelete(topicKey);
    }

    return new Response("Not found", { status: 404 });
  }

  // -------------------------------------------------------------------------
  // /connect — WebSocket upgrade
  // -------------------------------------------------------------------------

  private async handleConnect(request: Request, url: URL, topicKey: string): Promise<Response> {
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
    const attachment: SessionAttachment = { id: sessionId, topicKey };
    server.serializeAttachment(attachment);

    let set = this.topicSessions.get(topicKey);
    if (!set) {
      set = new Set();
      this.topicSessions.set(topicKey, set);
    }
    set.add(server);

    // Replay buffered messages if cursor is provided
    const cursorParam = url.searchParams.get("cursor");
    if (cursorParam !== null) {
      const cursor = parseInt(cursorParam, 10);
      if (!isNaN(cursor)) {
        await this.replayMessages(server, cursor, topicKey);
      }
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // -------------------------------------------------------------------------
  // /publish — Broadcast a message to a single topic
  // -------------------------------------------------------------------------

  private async handlePublish(request: Request, shardKey: string, topicKey: string): Promise<Response> {
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

    // Load or initialize metadata for this topic
    const meta = await this.getTopicMeta(topicKey);

    // Topic config is set once on first publish, immutable after
    if (meta.generation === "") {
      meta.generation = crypto.randomUUID();
      meta.messageTtlMs = (body.ttl ?? DEFAULT_MESSAGE_TTL_MS / 1000) * 1000;
      meta.maxBufferSize = body.max_buffer ?? DEFAULT_MAX_BUFFER_SIZE;
      await this.registerTopic(topicKey);
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
    const pruneKeys = this.computePruneKeys(meta, topicKey);

    // Coalesce message + metadata into a single storage write
    await this.ctx.storage.put({
      [`msg:${topicKey}:${seq}`]: storedMessage,
      [`meta:${topicKey}`]: meta,
    });
    if (pruneKeys.length > 0) {
      await this.ctx.storage.delete(pruneKeys);
    }

    // Set alarm for TTL-based cleanup
    await this.ensureAlarm(meta.messageTtlMs);

    // Broadcast ONLY to this topic's connected WebSockets
    const envelope = JSON.stringify({
      generation: meta.generation,
      seq: storedMessage.seq,
      data: storedMessage.data,
      encoding: storedMessage.encoding,
      timestamp: storedMessage.timestamp,
    });

    const topicSockets = this.topicSessions.get(topicKey) ?? new Set();
    for (const ws of topicSockets) {
      try {
        ws.send(envelope);
      } catch {
        // Socket may have died — will be cleaned up on close event
      }
    }

    return Response.json({
      seq,
      topic_id: `${shardKey}/${topicKey}`,
      generation: meta.generation,
      connections: topicSockets.size,
    });
  }

  // -------------------------------------------------------------------------
  // /batch-publish — Process multiple messages across topics in a single DO
  // -------------------------------------------------------------------------

  private async handleBatchPublish(request: Request, shardKey: string): Promise<Response> {
    let body: {
      messages?: Array<{
        topicKey?: string;
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
      if (typeof body.messages[i]!.topicKey !== "string") {
        return Response.json(
          { error: `Invalid message at index ${i}: missing 'topicKey'` },
          { status: 400 }
        );
      }
    }

    // Group by topic key within this DO
    const byTopic = new Map<string, Array<{ message: string; encoding?: "text" | "base64" }>>();
    for (const msg of body.messages) {
      const tk = msg.topicKey!;
      let arr = byTopic.get(tk);
      if (!arr) {
        arr = [];
        byTopic.set(tk, arr);
      }
      arr.push({ message: msg.message!, encoding: msg.encoding });
    }

    const now = Date.now();
    const allEntries: Record<string, unknown> = {};
    const allPruneKeys: string[] = [];
    const results: Array<Record<string, unknown>> = [];

    for (const [topicKey, messages] of byTopic) {
      const meta = await this.getTopicMeta(topicKey);

      // Topic config is set once on first publish, immutable after
      if (meta.generation === "") {
        meta.generation = crypto.randomUUID();
        meta.messageTtlMs = (body.ttl ?? DEFAULT_MESSAGE_TTL_MS / 1000) * 1000;
        meta.maxBufferSize = body.max_buffer ?? DEFAULT_MAX_BUFFER_SIZE;
        await this.registerTopic(topicKey);
      }

      const storedMessages: StoredMessage[] = [];

      // Assign sequence numbers and prepare storage entries
      for (const item of messages) {
        const seq = meta.nextSeq;
        meta.nextSeq = seq + 1;

        const stored: StoredMessage = {
          seq,
          data: item.message,
          encoding: item.encoding === "base64" ? "base64" : "text",
          timestamp: now,
        };

        storedMessages.push(stored);
        allEntries[`msg:${topicKey}:${seq}`] = stored;
      }

      // Compute prune keys (mutates meta.oldestSeq)
      allPruneKeys.push(...this.computePruneKeys(meta, topicKey));

      allEntries[`meta:${topicKey}`] = meta;

      // Broadcast all messages to this topic's connected WebSockets
      const topicSockets = this.topicSessions.get(topicKey) ?? new Set();
      for (const msg of storedMessages) {
        const envelope = JSON.stringify({
          generation: meta.generation,
          seq: msg.seq,
          data: msg.data,
          encoding: msg.encoding,
          timestamp: msg.timestamp,
        });
        for (const ws of topicSockets) {
          try {
            ws.send(envelope);
          } catch {
            // Socket may have died — will be cleaned up on close event
          }
        }
      }

      results.push({
        topic_id: `${shardKey}/${topicKey}`,
        generation: meta.generation,
        status: 200,
        messages_published: storedMessages.length,
        first_seq: storedMessages[0]!.seq,
        last_seq: storedMessages[storedMessages.length - 1]!.seq,
        connections: topicSockets.size,
      });
    }

    // Single coalesced storage write for ALL topics in this DO
    await this.ctx.storage.put(allEntries);
    if (allPruneKeys.length > 0) {
      await this.ctx.storage.delete(allPruneKeys);
    }

    // Set alarm using the shortest TTL across all touched topics
    let shortestTtl = DEFAULT_MESSAGE_TTL_MS;
    for (const topicKey of byTopic.keys()) {
      const meta = this.topicMetas.get(topicKey);
      if (meta && meta.messageTtlMs < shortestTtl) {
        shortestTtl = meta.messageTtlMs;
      }
    }
    await this.ensureAlarm(shortestTtl);

    return Response.json({ results });
  }

  // -------------------------------------------------------------------------
  // /delete — Per-topic teardown
  // -------------------------------------------------------------------------

  private async handleDelete(topicKey: string): Promise<Response> {
    const count = await this.teardownTopic(topicKey);
    return Response.json({ deleted: true, connections_closed: count });
  }

  // -------------------------------------------------------------------------
  // /delete-all — Shard-level teardown
  // -------------------------------------------------------------------------

  private async handleDeleteAll(): Promise<Response> {
    const activeTopics = await this.getActiveTopicKeys();

    // Close all WebSockets across all topics
    const sockets = this.ctx.getWebSockets();
    const totalConnections = sockets.length;
    for (const ws of sockets) {
      try {
        ws.close(1000, "shard deleted");
      } catch {
        // Ignore errors on already-closed sockets
      }
    }

    // Wipe everything
    this.topicSessions.clear();
    this.topicMetas.clear();
    await this.ctx.storage.deleteAll();
    this.alarmScheduled = false;

    return Response.json({
      deleted: true,
      topics_deleted: activeTopics.length,
      connections_closed: totalConnections,
    });
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
    const attachment = ws.deserializeAttachment() as SessionAttachment | null;
    if (attachment) {
      this.topicSessions.get(attachment.topicKey)?.delete(ws);
    }
  }

  async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    ws.close(1011, "WebSocket error");
    const attachment = ws.deserializeAttachment() as SessionAttachment | null;
    if (attachment) {
      this.topicSessions.get(attachment.topicKey)?.delete(ws);
    }
  }

  // -------------------------------------------------------------------------
  // Alarm — TTL-based message cleanup across all topics
  // -------------------------------------------------------------------------

  async alarm(): Promise<void> {
    const now = Date.now();
    let nextAlarmTime: number | null = null;
    const activeTopics = await this.getActiveTopicKeys();

    for (const topicKey of activeTopics) {
      const meta = await this.getTopicMeta(topicKey);
      const cutoff = now - meta.messageTtlMs;

      // Batch-read all remaining messages in a single storage operation
      const allKeys: string[] = [];
      for (let seq = meta.oldestSeq; seq < meta.nextSeq; seq++) {
        allKeys.push(`msg:${topicKey}:${seq}`);
      }

      const keysToDelete: string[] = [];
      let firstSurvivorTimestamp: number | null = null;

      if (allKeys.length > 0) {
        const messages = await this.ctx.storage.get<StoredMessage>(allKeys);
        for (let seq = meta.oldestSeq; seq < meta.nextSeq; seq++) {
          const msg = messages.get(`msg:${topicKey}:${seq}`);
          if (!msg) {
            // Gap — already deleted (e.g. by pruneBuffer), skip
            continue;
          }
          if (msg.timestamp <= cutoff) {
            keysToDelete.push(`msg:${topicKey}:${seq}`);
          } else {
            firstSurvivorTimestamp = msg.timestamp;
            break; // messages are sequential in time — no more expired
          }
        }
      }

      if (keysToDelete.length > 0) {
        await this.ctx.storage.delete(keysToDelete);
        meta.oldestSeq = meta.oldestSeq + keysToDelete.length;
        await this.ctx.storage.put(`meta:${topicKey}`, meta);
      }

      const remaining = meta.nextSeq - meta.oldestSeq;

      if (remaining > 0 && firstSurvivorTimestamp !== null) {
        // Reschedule alarm for the next message expiry
        const topicNextAlarm = firstSurvivorTimestamp + meta.messageTtlMs;
        if (nextAlarmTime === null || topicNextAlarm < nextAlarmTime) {
          nextAlarmTime = topicNextAlarm;
        }
      } else if (remaining === 0) {
        // This topic is dead — clean up its storage and close its sockets
        await this.teardownTopic(topicKey);
      }
    }

    if (nextAlarmTime !== null) {
      await this.ctx.storage.setAlarm(Math.max(nextAlarmTime, now + 1000));
      this.alarmScheduled = true;
    } else {
      this.alarmScheduled = false;
      // All topics gone — full DO cleanup for zero-cost state
      if ((await this.getActiveTopicKeys()).length === 0) {
        await this.ctx.storage.deleteAll();
      }
    }
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private async getTopicMeta(topicKey: string): Promise<TopicMeta> {
    const cached = this.topicMetas.get(topicKey);
    if (cached) return cached;

    const stored = await this.ctx.storage.get<TopicMeta>(`meta:${topicKey}`);
    const meta = stored ?? {
      generation: "",
      nextSeq: 0,
      oldestSeq: 0,
      maxBufferSize: DEFAULT_MAX_BUFFER_SIZE,
      messageTtlMs: DEFAULT_MESSAGE_TTL_MS,
    };
    this.topicMetas.set(topicKey, meta);
    return meta;
  }

  /**
   * Compute which message keys should be pruned to stay within maxBufferSize.
   * Pure arithmetic — no I/O. Mutates meta.oldestSeq.
   */
  private computePruneKeys(meta: TopicMeta, topicKey: string): string[] {
    const count = meta.nextSeq - meta.oldestSeq;
    if (count <= meta.maxBufferSize) {
      return []; // No pruning needed — simple arithmetic check
    }

    const newOldest = meta.nextSeq - meta.maxBufferSize;
    const keys: string[] = [];
    for (let seq = meta.oldestSeq; seq < newOldest; seq++) {
      keys.push(`msg:${topicKey}:${seq}`);
    }

    meta.oldestSeq = newOldest;
    return keys;
  }

  /**
   * Ensure a TTL alarm is scheduled, using cached state to avoid redundant
   * storage reads. Uses min(existing, newTTL) to fire at the earliest expiry.
   */
  private async ensureAlarm(ttlMs: number): Promise<void> {
    if (this.alarmScheduled === true) {
      // Alarm exists — check if we need to reschedule earlier
      const existing = await this.ctx.storage.getAlarm();
      if (existing !== null) {
        const proposed = Date.now() + ttlMs;
        if (proposed < existing) {
          await this.ctx.storage.setAlarm(proposed);
        }
        return;
      }
    }
    if (this.alarmScheduled === null) {
      // Unknown state — check storage once
      const existing = await this.ctx.storage.getAlarm();
      if (existing !== null) {
        this.alarmScheduled = true;
        // Check if proposed is earlier
        const proposed = Date.now() + ttlMs;
        if (proposed < existing) {
          await this.ctx.storage.setAlarm(proposed);
        }
        return;
      }
    }
    await this.ctx.storage.setAlarm(Date.now() + ttlMs);
    this.alarmScheduled = true;
  }

  /**
   * Replay buffered messages for a specific topic starting from a cursor.
   */
  private async replayMessages(ws: WebSocket, cursor: number, topicKey: string): Promise<void> {
    const meta = await this.getTopicMeta(topicKey);
    // Start from the cursor or the oldest available message, whichever is later
    const start = Math.max(cursor, meta.oldestSeq);

    // Batch-read all replay keys in a single storage operation
    const keys: string[] = [];
    for (let seq = start; seq < meta.nextSeq; seq++) {
      keys.push(`msg:${topicKey}:${seq}`);
    }
    if (keys.length === 0) return;

    const messages = await this.ctx.storage.get<StoredMessage>(keys);
    for (let seq = start; seq < meta.nextSeq; seq++) {
      const msg = messages.get(`msg:${topicKey}:${seq}`);
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

  /**
   * Tear down a single topic — close its sockets, delete its storage,
   * and unregister it from the active topic index.
   */
  private async teardownTopic(topicKey: string): Promise<number> {
    const sockets = this.topicSessions.get(topicKey) ?? new Set();
    const count = sockets.size;
    for (const ws of sockets) {
      try {
        ws.close(1000, "topic deleted");
      } catch {
        // Ignore errors on already-closed sockets
      }
    }
    this.topicSessions.delete(topicKey);

    const meta = this.topicMetas.get(topicKey);
    if (meta) {
      const keys: string[] = [`meta:${topicKey}`];
      for (let seq = meta.oldestSeq; seq < meta.nextSeq; seq++) {
        keys.push(`msg:${topicKey}:${seq}`);
      }
      await this.ctx.storage.delete(keys);
      this.topicMetas.delete(topicKey);
    }

    await this.unregisterTopic(topicKey);
    return count;
  }

  // -------------------------------------------------------------------------
  // Active topic index
  // -------------------------------------------------------------------------

  private async registerTopic(topicKey: string): Promise<void> {
    const topics = (await this.ctx.storage.get<string[]>("__topics")) ?? [];
    if (!topics.includes(topicKey)) {
      topics.push(topicKey);
      await this.ctx.storage.put("__topics", topics);
    }
  }

  private async unregisterTopic(topicKey: string): Promise<void> {
    const topics = (await this.ctx.storage.get<string[]>("__topics")) ?? [];
    const updated = topics.filter((t) => t !== topicKey);
    if (updated.length > 0) {
      await this.ctx.storage.put("__topics", updated);
    } else {
      await this.ctx.storage.delete("__topics");
    }
  }

  private async getActiveTopicKeys(): Promise<string[]> {
    return (await this.ctx.storage.get<string[]>("__topics")) ?? [];
  }
}
