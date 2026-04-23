/**
 * cloudflare-ws-proxy — Worker entry point
 *
 * Routes incoming HTTP requests to the appropriate ProxyDO instance.
 * Handles backend authentication (Bearer token) and client authentication
 * (HMAC-signed short-lived tokens).
 */

import { generateToken, verifyToken } from "./auth";
import { ProxyDO } from "./proxy-do";
import type { Env } from "./proxy-do";
import { parseTopicId, parseTopicIdFromString, validateSegment } from "./topic-key";

// Re-export the Durable Object class so Cloudflare can discover it
export { ProxyDO };

// ---------------------------------------------------------------------------
// Worker fetch handler
// ---------------------------------------------------------------------------

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method;

    // -----------------------------------------------------------------------
    // Route: POST /t/:shard/:topic/auth
    // -----------------------------------------------------------------------
    const authMatch = matchRoute(method, pathname, "POST", "auth");
    if (authMatch) {
      return handleAuth(request, env, authMatch.shard, authMatch.topic, url);
    }

    // -----------------------------------------------------------------------
    // Route: POST /t/:shard/:topic/publish
    // -----------------------------------------------------------------------
    const publishMatch = matchRoute(method, pathname, "POST", "publish");
    if (publishMatch) {
      return handlePublish(request, env, publishMatch.shard, publishMatch.topic);
    }

    // -----------------------------------------------------------------------
    // Route: GET /t/:shard/:topic/connect
    // -----------------------------------------------------------------------
    const connectMatch = matchRoute(method, pathname, "GET", "connect");
    if (connectMatch) {
      return handleConnect(request, env, connectMatch.shard, connectMatch.topic, url);
    }

    // -----------------------------------------------------------------------
    // Route: DELETE /t/:shard/:topic
    // -----------------------------------------------------------------------
    const deleteTopicMatch = matchDeleteTopicRoute(method, pathname);
    if (deleteTopicMatch) {
      return handleDeleteTopic(request, env, deleteTopicMatch.shard, deleteTopicMatch.topic);
    }

    // -----------------------------------------------------------------------
    // Route: DELETE /t/:shard
    // -----------------------------------------------------------------------
    const deleteShardMatch = matchDeleteShardRoute(method, pathname);
    if (deleteShardMatch) {
      return handleDeleteShard(request, env, deleteShardMatch);
    }

    // -----------------------------------------------------------------------
    // Route: POST /bulk-publish
    // -----------------------------------------------------------------------
    if (method === "POST" && pathname === "/bulk-publish") {
      return handleBulkPublish(request, env);
    }

    // -----------------------------------------------------------------------
    // Health check
    // -----------------------------------------------------------------------
    if (method === "GET" && (pathname === "/" || pathname === "/health")) {
      return Response.json({
        service: "cloudflare-ws-proxy",
        status: "ok",
      });
    }

    return Response.json({ error: "Not found" }, { status: 404 });
  },
} satisfies ExportedHandler<Env>;

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async function handleAuth(
  request: Request,
  env: Env,
  shard: string,
  topic: string,
  url: URL
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicSegments(shard, topic);
  if (idError) return idError;

  const { fullId } = parseTopicId(shard, topic);

  // Parse optional body
  let tokenTtlSeconds: number | undefined;
  let cursor: number | undefined;

  if (request.headers.get("Content-Type")?.includes("application/json")) {
    try {
      const text = await request.text();
      if (text.length > 0) {
        const body = JSON.parse(text) as {
          token_ttl_seconds?: number;
          cursor?: number;
        };
        if (typeof body.token_ttl_seconds === "number" && body.token_ttl_seconds > 0) {
          tokenTtlSeconds = body.token_ttl_seconds;
        }
        if (typeof body.cursor === "number") {
          cursor = body.cursor;
        }
      }
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }
  }

  const { token, expiresAt } = await generateToken(
    env.BACKEND_SECRET,
    fullId,
    cursor,
    tokenTtlSeconds
  );

  // Build the WebSocket URL
  const wsProtocol = url.protocol === "https:" ? "wss:" : "ws:";
  let connectUrl = `${wsProtocol}//${url.host}/t/${shard}/${topic}/connect?token=${encodeURIComponent(token)}`;
  if (cursor !== undefined) {
    connectUrl += `&cursor=${cursor}`;
  }

  return Response.json({
    url: connectUrl,
    topic_id: fullId,
    expires_at: expiresAt,
  });
}

async function handlePublish(
  request: Request,
  env: Env,
  shard: string,
  topic: string
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicSegments(shard, topic);
  if (idError) return idError;

  const stub = getStub(env, shard);
  const doUrl = new URL(`https://do/${shard}/${topic}/publish`);
  return stub.fetch(
    new Request(doUrl.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: request.body,
    })
  );
}

async function handleBulkPublish(
  request: Request,
  env: Env
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  let body: {
    messages?: Array<{
      topic_id?: string;
      message?: string;
      encoding?: "text" | "base64";
      ttl?: number;
      max_buffer?: number;
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

  // Validate all messages and topic IDs upfront before any DO calls
  for (let i = 0; i < body.messages.length; i++) {
    const item = body.messages[i]!;
    if (typeof item.topic_id !== "string" || item.topic_id.length === 0) {
      return Response.json(
        { error: `Invalid message at index ${i}: missing 'topic_id'` },
        { status: 400 }
      );
    }
    const parsed = parseTopicIdFromString(item.topic_id);
    if (!parsed) {
      return Response.json(
        { error: `Invalid topic ID at index ${i}: '${item.topic_id}'. Must be 'shard/topic' format.` },
        { status: 400 }
      );
    }

    if (typeof item.message !== "string") {
      return Response.json(
        { error: `Invalid message at index ${i}: missing 'message'` },
        { status: 400 }
      );
    }
  }

  // Group messages by shard key — one DO subrequest per unique shard
  const shardGroups = new Map<
    string,
    {
      messages: Array<{ topicKey: string; message: string; encoding?: "text" | "base64" }>;
      ttl?: number;
      max_buffer?: number;
    }
  >();

  for (const item of body.messages) {
    const parsed = parseTopicIdFromString(item.topic_id!)!;
    let group = shardGroups.get(parsed.shardKey);
    if (!group) {
      group = { messages: [], ttl: body.ttl, max_buffer: body.max_buffer };
      shardGroups.set(parsed.shardKey, group);
    }
    group.messages.push({
      topicKey: parsed.topicKey,
      message: item.message!,
      encoding: item.encoding,
    });
    // Per-message overrides — last one for this shard wins
    if (item.ttl !== undefined) group.ttl = item.ttl;
    if (item.max_buffer !== undefined) group.max_buffer = item.max_buffer;
  }

  // Fan out to DOs in parallel — one request per shard
  const results = await Promise.all(
    [...shardGroups.entries()].map(async ([shardKey, group]) => {
      const stub = getStub(env, shardKey);
      const doUrl = new URL(`https://do/${shardKey}/batch-publish`);
      const response = await stub.fetch(
        new Request(doUrl.toString(), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: group.messages,
            ttl: group.ttl,
            max_buffer: group.max_buffer,
          }),
        })
      );

      const result = (await response.json()) as {
        results?: Array<Record<string, unknown>>;
      };
      return { shard: shardKey, status: response.status, results: result.results };
    })
  );

  // Flatten per-shard results into per-topic results
  const topicResults: Array<Record<string, unknown>> = [];
  for (const shardResult of results) {
    const perTopic = shardResult.results;
    if (perTopic) {
      topicResults.push(...perTopic);
    }
  }

  const allOk = results.every((r: { status: number }) => r.status === 200);
  return Response.json(
    {
      topics: topicResults.length,
      messages: body.messages.length,
      results: topicResults,
    },
    { status: allOk ? 200 : 207 }
  );
}

async function handleConnect(
  request: Request,
  env: Env,
  shard: string,
  topic: string,
  url: URL
): Promise<Response> {
  const idError = validateTopicSegments(shard, topic);
  if (idError) return idError;

  const { fullId } = parseTopicId(shard, topic);

  // Validate HMAC token
  const tokenParam = url.searchParams.get("token");
  if (!tokenParam) {
    return Response.json({ error: "Missing token parameter" }, { status: 401 });
  }

  const payload = await verifyToken(env.BACKEND_SECRET, tokenParam);
  if (!payload) {
    return Response.json({ error: "Invalid or expired token" }, { status: 401 });
  }

  if (payload.topicId !== fullId) {
    return Response.json(
      { error: "Token does not match requested topic" },
      { status: 403 }
    );
  }

  // Forward upgrade request to the DO
  const stub = getStub(env, shard);
  const doUrl = new URL(`https://do/${shard}/${topic}/connect`);

  // Forward cursor param (from URL or token)
  const cursor = url.searchParams.get("cursor") ?? payload.cursor?.toString();
  if (cursor !== undefined && cursor !== null) {
    doUrl.searchParams.set("cursor", cursor);
  }

  return stub.fetch(
    new Request(doUrl.toString(), {
      method: "GET",
      headers: request.headers,
    })
  );
}

async function handleDeleteTopic(
  request: Request,
  env: Env,
  shard: string,
  topic: string
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicSegments(shard, topic);
  if (idError) return idError;

  const { fullId } = parseTopicId(shard, topic);

  const stub = getStub(env, shard);
  const doUrl = new URL(`https://do/${shard}/${topic}/delete`);
  const response = await stub.fetch(
    new Request(doUrl.toString(), { method: "POST" })
  );

  const result = (await response.json()) as { deleted: boolean; connections_closed: number };
  return Response.json({
    ...result,
    topic_id: fullId,
  });
}

async function handleDeleteShard(
  request: Request,
  env: Env,
  shard: string
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  if (!validateSegment(shard)) {
    return Response.json({ error: "Invalid shard key" }, { status: 400 });
  }

  const stub = getStub(env, shard);
  const doUrl = new URL(`https://do/${shard}/delete-all`);
  const response = await stub.fetch(
    new Request(doUrl.toString(), { method: "POST" })
  );

  const result = (await response.json()) as {
    deleted: boolean;
    topics_deleted: number;
    connections_closed: number;
  };
  return Response.json({
    ...result,
    shard,
  });
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function validateBackendAuth(request: Request, env: Env): Response | null {
  const authHeader = request.headers.get("Authorization");
  if (!authHeader) {
    return Response.json({ error: "Missing Authorization header" }, { status: 401 });
  }

  const parts = authHeader.split(" ");
  if (parts.length !== 2 || parts[0] !== "Bearer" || parts[1] !== env.BACKEND_SECRET) {
    return Response.json({ error: "Invalid authorization" }, { status: 401 });
  }

  return null;
}

function validateTopicSegments(shard: string, topic: string): Response | null {
  if (!validateSegment(shard)) {
    return Response.json(
      {
        error: "Invalid shard key. Must match /^[a-zA-Z0-9_.:~-]{1,128}$/",
      },
      { status: 400 }
    );
  }
  if (!validateSegment(topic)) {
    return Response.json(
      {
        error: "Invalid topic key. Must match /^[a-zA-Z0-9_.:~-]{1,128}$/",
      },
      { status: 400 }
    );
  }
  return null;
}

// ---------------------------------------------------------------------------
// Routing helpers
// ---------------------------------------------------------------------------

/**
 * Match routes of the form: `<method> /t/<shard>/<topic>/<action>`
 * Returns the extracted shard and topic, or null.
 */
function matchRoute(
  method: string,
  pathname: string,
  expectedMethod: string,
  action: string
): { shard: string; topic: string } | null {
  if (method !== expectedMethod) return null;
  if (!pathname.startsWith("/t/") || !pathname.endsWith(`/${action}`)) return null;

  // Strip "/t/" prefix and "/<action>" suffix
  const inner = pathname.slice(3, pathname.length - action.length - 1);
  const slashIndex = inner.indexOf("/");
  if (slashIndex === -1 || slashIndex === 0 || slashIndex === inner.length - 1) return null;

  // Ensure exactly two segments (no extra slashes)
  const shard = inner.slice(0, slashIndex);
  const topic = inner.slice(slashIndex + 1);
  if (topic.includes("/")) return null;

  return { shard, topic };
}

/**
 * Match DELETE /t/:shard/:topic
 */
function matchDeleteTopicRoute(method: string, pathname: string): { shard: string; topic: string } | null {
  if (method !== "DELETE") return null;
  if (!pathname.startsWith("/t/")) return null;

  const inner = pathname.slice(3); // strip "/t/"
  const slashIndex = inner.indexOf("/");
  if (slashIndex === -1 || slashIndex === 0 || slashIndex === inner.length - 1) return null;

  const shard = inner.slice(0, slashIndex);
  const topic = inner.slice(slashIndex + 1);
  if (topic.includes("/")) return null;

  return { shard, topic };
}

/**
 * Match DELETE /t/:shard
 */
function matchDeleteShardRoute(method: string, pathname: string): string | null {
  if (method !== "DELETE") return null;
  if (!pathname.startsWith("/t/")) return null;

  const shard = pathname.slice(3); // strip "/t/"
  if (shard.length === 0 || shard.includes("/")) return null;

  return shard;
}

// ---------------------------------------------------------------------------
// DO stub helper
// ---------------------------------------------------------------------------

function getStub(env: Env, shard: string): DurableObjectStub<ProxyDO> {
  const id = env.PROXY_DO.idFromName(shard);
  return env.PROXY_DO.get(id);
}
