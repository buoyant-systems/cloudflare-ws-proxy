import {
  env,
  SELF,
} from "cloudflare:test";
import { describe, it, expect } from "vitest";

// The BACKEND_SECRET must match what is set in .dev.vars or wrangler.jsonc [vars]
// For tests, we rely on the vitest-pool-workers config to pick up the test env.
const BACKEND_SECRET = (env as Record<string, string>).BACKEND_SECRET ?? "test-secret";

function authHeaders(): HeadersInit {
  return {
    Authorization: `Bearer ${BACKEND_SECRET}`,
    "Content-Type": "application/json",
  };
}

/** Generate a unique topic ID per test to avoid cross-contamination (isolated storage is off). */
function uniqueTopic(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

describe("Worker — health check", () => {
  it("returns 200 on GET /", async () => {
    const response = await SELF.fetch("https://proxy/");
    expect(response.status).toBe(200);
    const body = await response.json<{ service: string; status: string }>();
    expect(body.service).toBe("cloudflare-ws-proxy");
    expect(body.status).toBe("ok");
  });
});

// ---------------------------------------------------------------------------
// Auth — backend authentication
// ---------------------------------------------------------------------------

describe("Worker — backend auth", () => {
  it("rejects requests without Authorization header", async () => {
    const topic = uniqueTopic("auth-noheader");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
      method: "POST",
    });
    expect(response.status).toBe(401);
  });

  it("rejects requests with wrong secret", async () => {
    const topic = uniqueTopic("auth-wrong");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
      method: "POST",
      headers: { Authorization: "Bearer wrong-secret" },
    });
    expect(response.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Topic ID validation
// ---------------------------------------------------------------------------

describe("Worker — topic ID validation", () => {
  it("rejects topic IDs with invalid characters", async () => {
    const response = await SELF.fetch("https://proxy/topic/bad%20id/auth", {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(400);
  });

  it("rejects topic IDs longer than 128 characters", async () => {
    const longId = "a".repeat(129);
    const response = await SELF.fetch(`https://proxy/topic/${longId}/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(400);
  });

  it("accepts valid topic IDs", async () => {
    const topic = uniqueTopic("valid-Topic_123");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(200);
  });

  it("accepts topic IDs with dots, colons, and tildes", async () => {
    for (const topic of ["user:123", "chat.room.5", "org~team"]) {
      const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
        method: "POST",
        headers: authHeaders(),
      });
      expect(response.status).toBe(200);
    }
  });
});

// ---------------------------------------------------------------------------
// Auth endpoint
// ---------------------------------------------------------------------------

describe("Worker — POST /topic/:id/auth", () => {
  it("returns a WebSocket URL with a signed token", async () => {
    const topic = uniqueTopic("auth-url");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      url: string;
      topic_id: string;
      expires_at: number;
    }>();
    expect(body.topic_id).toBe(topic);
    expect(body.url).toContain(`/topic/${topic}/connect?token=`);
    expect(body.expires_at).toBeGreaterThan(Date.now());
  });

  it("respects custom token_ttl_seconds", async () => {
    const topic = uniqueTopic("auth-ttl");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/auth`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ token_ttl_seconds: 60 }),
    });
    const body = await response.json<{ expires_at: number }>();
    // Should expire roughly 60s from now
    expect(body.expires_at).toBeLessThan(Date.now() + 70 * 1000);
    expect(body.expires_at).toBeGreaterThan(Date.now() + 50 * 1000);
  });
});

// ---------------------------------------------------------------------------
// Publish endpoint
// ---------------------------------------------------------------------------

describe("Worker — POST /topic/:id/publish", () => {
  it("publishes a text message and returns a sequence number", async () => {
    const topic = uniqueTopic("pub-text");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "hello" }),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      seq: number;
      topic_id: string;
      connections: number;
    }>();
    expect(body.seq).toBeGreaterThanOrEqual(0);
    expect(body.topic_id).toBe(topic);
    expect(typeof body.connections).toBe("number");
  });

  it("publishes a base64-encoded message", async () => {
    const topic = uniqueTopic("pub-b64");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        message: btoa("binary data"),
        encoding: "base64",
      }),
    });
    expect(response.status).toBe(200);
  });

  it("rejects publish without a message field", async () => {
    const topic = uniqueTopic("pub-nomsg");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(400);
  });

  it("returns incrementing sequence numbers", async () => {
    const topic = uniqueTopic("pub-seq");

    const r1 = await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "first" }),
    });
    const b1 = await r1.json<{ seq: number }>();

    const r2 = await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "second" }),
    });
    const b2 = await r2.json<{ seq: number }>();

    expect(b2.seq).toBe(b1.seq + 1);
  });
});

// ---------------------------------------------------------------------------
// Delete endpoint
// ---------------------------------------------------------------------------

describe("Worker — DELETE /topic/:id", () => {
  it("deletes a topic and returns success", async () => {
    const topic = uniqueTopic("del");

    // First publish a message to ensure the DO exists
    await SELF.fetch(`https://proxy/topic/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "to be deleted" }),
    });

    const response = await SELF.fetch(`https://proxy/topic/${topic}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${BACKEND_SECRET}` },
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      deleted: boolean;
      topic_id: string;
      connections_closed: number;
    }>();
    expect(body.deleted).toBe(true);
    expect(body.topic_id).toBe(topic);
  });
});

// ---------------------------------------------------------------------------
// Connect endpoint — auth validation
// ---------------------------------------------------------------------------

describe("Worker — GET /topic/:id/connect", () => {
  it("rejects connect without a token", async () => {
    const topic = uniqueTopic("conn-notoken");
    const response = await SELF.fetch(`https://proxy/topic/${topic}/connect`, {
      method: "GET",
    });
    expect(response.status).toBe(401);
  });

  it("rejects connect with an invalid token", async () => {
    const topic = uniqueTopic("conn-badtoken");
    const response = await SELF.fetch(
      `https://proxy/topic/${topic}/connect?token=invalid.token`,
      { method: "GET" }
    );
    expect(response.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// 404 handling
// ---------------------------------------------------------------------------

describe("Worker — unknown routes", () => {
  it("returns 404 for unknown paths", async () => {
    const response = await SELF.fetch("https://proxy/unknown/path");
    expect(response.status).toBe(404);
  });
});
