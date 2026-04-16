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

// Re-export the Durable Object class so Cloudflare can discover it
export { ProxyDO };

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TOPIC_ID_REGEX = /^[a-zA-Z0-9_-]{1,128}$/;

// ---------------------------------------------------------------------------
// Worker fetch handler
// ---------------------------------------------------------------------------

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method;

    // -----------------------------------------------------------------------
    // Route: POST /topic/:id/auth
    // -----------------------------------------------------------------------
    const authMatch = matchRoute(method, pathname, "POST", "/topic/", "/auth");
    if (authMatch) {
      return handleAuth(request, env, authMatch, url);
    }

    // -----------------------------------------------------------------------
    // Route: POST /topic/:id/publish
    // -----------------------------------------------------------------------
    const publishMatch = matchRoute(method, pathname, "POST", "/topic/", "/publish");
    if (publishMatch) {
      return handlePublish(request, env, publishMatch);
    }

    // -----------------------------------------------------------------------
    // Route: GET /topic/:id/connect
    // -----------------------------------------------------------------------
    const connectMatch = matchRoute(method, pathname, "GET", "/topic/", "/connect");
    if (connectMatch) {
      return handleConnect(request, env, connectMatch, url);
    }

    // -----------------------------------------------------------------------
    // Route: DELETE /topic/:id
    // -----------------------------------------------------------------------
    const deleteMatch = matchDeleteRoute(method, pathname);
    if (deleteMatch) {
      return handleDelete(request, env, deleteMatch);
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
  topicId: string,
  url: URL
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicId(topicId);
  if (idError) return idError;

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
    topicId,
    cursor,
    tokenTtlSeconds
  );

  // Build the WebSocket URL
  const wsProtocol = url.protocol === "https:" ? "wss:" : "ws:";
  let connectUrl = `${wsProtocol}//${url.host}/topic/${topicId}/connect?token=${encodeURIComponent(token)}`;
  if (cursor !== undefined) {
    connectUrl += `&cursor=${cursor}`;
  }

  return Response.json({
    url: connectUrl,
    topic_id: topicId,
    expires_at: expiresAt,
  });
}

async function handlePublish(
  request: Request,
  env: Env,
  topicId: string
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicId(topicId);
  if (idError) return idError;

  const stub = getStub(env, topicId);
  const doUrl = new URL(`https://do/topic/${topicId}/publish`);
  return stub.fetch(
    new Request(doUrl.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: request.body,
    })
  );
}

async function handleConnect(
  request: Request,
  env: Env,
  topicId: string,
  url: URL
): Promise<Response> {
  const idError = validateTopicId(topicId);
  if (idError) return idError;

  // Validate HMAC token
  const tokenParam = url.searchParams.get("token");
  if (!tokenParam) {
    return Response.json({ error: "Missing token parameter" }, { status: 401 });
  }

  const payload = await verifyToken(env.BACKEND_SECRET, tokenParam);
  if (!payload) {
    return Response.json({ error: "Invalid or expired token" }, { status: 401 });
  }

  if (payload.topicId !== topicId) {
    return Response.json(
      { error: "Token does not match requested topic" },
      { status: 403 }
    );
  }

  // Forward upgrade request to the DO
  const stub = getStub(env, topicId);
  const doUrl = new URL(`https://do/topic/${topicId}/connect`);

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

async function handleDelete(
  request: Request,
  env: Env,
  topicId: string
): Promise<Response> {
  const authError = validateBackendAuth(request, env);
  if (authError) return authError;

  const idError = validateTopicId(topicId);
  if (idError) return idError;

  const stub = getStub(env, topicId);
  const doUrl = new URL(`https://do/topic/${topicId}/delete`);
  const response = await stub.fetch(
    new Request(doUrl.toString(), { method: "POST" })
  );

  const result = (await response.json()) as { deleted: boolean; connections_closed: number };
  return Response.json({
    ...result,
    topic_id: topicId,
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

function validateTopicId(topicId: string): Response | null {
  if (!TOPIC_ID_REGEX.test(topicId)) {
    return Response.json(
      {
        error: "Invalid topic ID. Must match /^[a-zA-Z0-9_-]{1,128}$/",
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
 * Match routes of the form: `<method> <prefix><topicId><suffix>`
 * Returns the extracted topicId or null.
 */
function matchRoute(
  method: string,
  pathname: string,
  expectedMethod: string,
  prefix: string,
  suffix: string
): string | null {
  if (method !== expectedMethod) return null;
  if (!pathname.startsWith(prefix) || !pathname.endsWith(suffix)) return null;

  const topicId = pathname.slice(prefix.length, pathname.length - suffix.length);
  if (topicId.length === 0) return null;
  return topicId;
}

/**
 * Match DELETE /topic/:id
 */
function matchDeleteRoute(method: string, pathname: string): string | null {
  if (method !== "DELETE") return null;
  const prefix = "/topic/";
  if (!pathname.startsWith(prefix)) return null;

  const topicId = pathname.slice(prefix.length);
  // Reject if there are extra path segments
  if (topicId.includes("/") || topicId.length === 0) return null;
  return topicId;
}

// ---------------------------------------------------------------------------
// DO stub helper
// ---------------------------------------------------------------------------

function getStub(env: Env, topicId: string): DurableObjectStub<ProxyDO> {
  const id = env.PROXY_DO.idFromName(topicId);
  return env.PROXY_DO.get(id);
}
