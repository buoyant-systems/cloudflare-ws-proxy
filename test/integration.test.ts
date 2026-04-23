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

/** Generate a unique topic ID (shard/topic) per test to avoid cross-contamination. */
function uniqueTopic(prefix: string): { shard: string; topic: string; fullId: string } {
  const uid = crypto.randomUUID().slice(0, 8);
  const shard = `${prefix}-${uid}`;
  const topic = "t";
  return { shard, topic, fullId: `${shard}/${topic}` };
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
    const { shard, topic } = uniqueTopic("auth-noheader");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/auth`, {
      method: "POST",
    });
    expect(response.status).toBe(401);
  });

  it("rejects requests with wrong secret", async () => {
    const { shard, topic } = uniqueTopic("auth-wrong");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/auth`, {
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
  it("rejects shard keys with invalid characters", async () => {
    const response = await SELF.fetch("https://proxy/t/bad%20id/topic/auth", {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(400);
  });

  it("rejects shard keys longer than 128 characters", async () => {
    const longId = "a".repeat(129);
    const response = await SELF.fetch(`https://proxy/t/${longId}/topic/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(400);
  });

  it("accepts valid shard/topic IDs", async () => {
    const { shard, topic } = uniqueTopic("valid-Topic_123");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(200);
  });

  it("accepts shard keys with dots, colons, and tildes", async () => {
    for (const shard of ["user:123", "chat.room.5", "org~team"]) {
      const response = await SELF.fetch(`https://proxy/t/${shard}/default/auth`, {
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

describe("Worker — POST /t/:shard/:topic/auth", () => {
  it("returns a WebSocket URL with a signed token", async () => {
    const { shard, topic, fullId } = uniqueTopic("auth-url");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/auth`, {
      method: "POST",
      headers: authHeaders(),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      url: string;
      topic_id: string;
      expires_at: number;
    }>();
    expect(body.topic_id).toBe(fullId);
    expect(body.url).toContain(`/t/${shard}/${topic}/connect?token=`);
    expect(body.expires_at).toBeGreaterThan(Date.now());
  });

  it("respects custom token_ttl_seconds", async () => {
    const { shard, topic } = uniqueTopic("auth-ttl");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/auth`, {
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

describe("Worker — POST /t/:shard/:topic/publish", () => {
  it("publishes a text message and returns a sequence number", async () => {
    const { shard, topic, fullId } = uniqueTopic("pub-text");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
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
    expect(body.topic_id).toBe(fullId);
    expect(typeof body.connections).toBe("number");
  });

  it("publishes a base64-encoded message", async () => {
    const { shard, topic } = uniqueTopic("pub-b64");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
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
    const { shard, topic } = uniqueTopic("pub-nomsg");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(400);
  });

  it("returns incrementing sequence numbers", async () => {
    const { shard, topic } = uniqueTopic("pub-seq");

    const r1 = await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "first" }),
    });
    const b1 = await r1.json<{ seq: number }>();

    const r2 = await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "second" }),
    });
    const b2 = await r2.json<{ seq: number }>();

    expect(b2.seq).toBe(b1.seq + 1);
  });
});

// ---------------------------------------------------------------------------
// Delete topic endpoint
// ---------------------------------------------------------------------------

describe("Worker — DELETE /t/:shard/:topic", () => {
  it("deletes a topic and returns success", async () => {
    const { shard, topic, fullId } = uniqueTopic("del");

    // First publish a message to ensure the DO exists
    await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "to be deleted" }),
    });

    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}`, {
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
    expect(body.topic_id).toBe(fullId);
  });
});

// ---------------------------------------------------------------------------
// Delete shard endpoint
// ---------------------------------------------------------------------------

describe("Worker — DELETE /t/:shard", () => {
  it("deletes all topics in a shard and returns success", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `shard-del-${uid}`;

    // Publish to two topics in the same shard
    await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "a" }),
    });
    await SELF.fetch(`https://proxy/t/${shard}/topicB/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "b" }),
    });

    const response = await SELF.fetch(`https://proxy/t/${shard}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${BACKEND_SECRET}` },
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      deleted: boolean;
      shard: string;
      topics_deleted: number;
      connections_closed: number;
    }>();
    expect(body.deleted).toBe(true);
    expect(body.shard).toBe(shard);
    expect(body.topics_deleted).toBe(2);
  });

  it("sequences restart after shard delete", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `shard-seq-${uid}`;

    await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "before" }),
    });

    await SELF.fetch(`https://proxy/t/${shard}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${BACKEND_SECRET}` },
    });

    const response = await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "after" }),
    });
    const body = await response.json<{ seq: number }>();
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Connect endpoint — auth validation
// ---------------------------------------------------------------------------

describe("Worker — GET /t/:shard/:topic/connect", () => {
  it("rejects connect without a token", async () => {
    const { shard, topic } = uniqueTopic("conn-notoken");
    const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}/connect`, {
      method: "GET",
    });
    expect(response.status).toBe(401);
  });

  it("rejects connect with an invalid token", async () => {
    const { shard, topic } = uniqueTopic("conn-badtoken");
    const response = await SELF.fetch(
      `https://proxy/t/${shard}/${topic}/connect?token=invalid.token`,
      { method: "GET" }
    );
    expect(response.status).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Bulk publish endpoint
// ---------------------------------------------------------------------------

describe("Worker — POST /bulk-publish", () => {
  it("rejects bulk publish without auth", async () => {
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages: [{ topic_id: "shard/topic", message: "hello" }],
      }),
    });
    expect(response.status).toBe(401);
  });

  it("rejects empty messages array", async () => {
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ messages: [] }),
    });
    expect(response.status).toBe(400);
  });

  it("rejects messages with missing topic_id", async () => {
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [{ message: "hello" }],
      }),
    });
    expect(response.status).toBe(400);
  });

  it("rejects messages with invalid topic_id format", async () => {
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [{ topic_id: "no-slash", message: "hello" }],
      }),
    });
    expect(response.status).toBe(400);
  });

  it("rejects messages with missing message field", async () => {
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [{ topic_id: "shard/topic" }],
      }),
    });
    expect(response.status).toBe(400);
  });

  it("publishes multiple messages to a single topic", async () => {
    const { fullId } = uniqueTopic("bulk-single");
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [
          { topic_id: fullId, message: "first" },
          { topic_id: fullId, message: "second" },
          { topic_id: fullId, message: "third" },
        ],
      }),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      topics: number;
      messages: number;
      results: Array<{
        topic_id: string;
        messages_published: number;
        first_seq: number;
        last_seq: number;
      }>;
    }>();
    expect(body.topics).toBe(1);
    expect(body.messages).toBe(3);
    expect(body.results).toHaveLength(1);
    expect(body.results[0]!.topic_id).toBe(fullId);
    expect(body.results[0]!.messages_published).toBe(3);
    expect(body.results[0]!.last_seq - body.results[0]!.first_seq).toBe(2);
  });

  it("publishes messages across multiple topics in the same shard", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `bulk-same-${uid}`;
    const topicA = `${shard}/topicA`;
    const topicB = `${shard}/topicB`;

    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [
          { topic_id: topicA, message: "a1" },
          { topic_id: topicB, message: "b1" },
          { topic_id: topicA, message: "a2" },
          { topic_id: topicB, message: "b2" },
        ],
      }),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      topics: number;
      messages: number;
      results: Array<{
        topic_id: string;
        messages_published: number;
      }>;
    }>();
    expect(body.topics).toBe(2);
    expect(body.messages).toBe(4);

    const resultA = body.results.find((r) => r.topic_id === topicA);
    const resultB = body.results.find((r) => r.topic_id === topicB);
    expect(resultA!.messages_published).toBe(2);
    expect(resultB!.messages_published).toBe(2);
  });

  it("publishes messages across different shards", async () => {
    const uidA = crypto.randomUUID().slice(0, 8);
    const uidB = crypto.randomUUID().slice(0, 8);
    const topicA = `shard-a-${uidA}/topic`;
    const topicB = `shard-b-${uidB}/topic`;

    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [
          { topic_id: topicA, message: "a1" },
          { topic_id: topicB, message: "b1" },
        ],
      }),
    });
    expect(response.status).toBe(200);

    const body = await response.json<{
      topics: number;
      results: Array<{ topic_id: string }>;
    }>();
    expect(body.topics).toBe(2);
  });

  it("assigns incrementing sequence numbers within a batch", async () => {
    const { shard, topic, fullId } = uniqueTopic("bulk-seq");

    // First, publish a single message to establish seq 0
    await SELF.fetch(`https://proxy/t/${shard}/${topic}/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "seed" }),
    });

    // Then bulk publish — sequences should continue from 1
    const response = await SELF.fetch("https://proxy/bulk-publish", {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        messages: [
          { topic_id: fullId, message: "batch1" },
          { topic_id: fullId, message: "batch2" },
        ],
      }),
    });
    const body = await response.json<{
      results: Array<{ first_seq: number; last_seq: number }>;
    }>();
    expect(body.results[0]!.first_seq).toBe(1);
    expect(body.results[0]!.last_seq).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Multi-topic isolation
// ---------------------------------------------------------------------------

describe("Worker — multi-topic isolation", () => {
  it("topics in the same shard have independent sequences", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `iso-seq-${uid}`;

    // Publish 3 to topicA
    for (let i = 0; i < 3; i++) {
      const r = await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ message: `a-${i}` }),
      });
      const body = await r.json<{ seq: number }>();
      expect(body.seq).toBe(i);
    }

    // topicB starts from 0 independently
    const r = await SELF.fetch(`https://proxy/t/${shard}/topicB/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "b-0" }),
    });
    const body = await r.json<{ seq: number }>();
    expect(body.seq).toBe(0);
  });

  it("deleting one topic doesn't affect another in the same shard", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `iso-del-${uid}`;

    // Publish to both topics
    await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "a" }),
    });
    await SELF.fetch(`https://proxy/t/${shard}/topicB/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "b" }),
    });

    // Delete only topicA
    await SELF.fetch(`https://proxy/t/${shard}/topicA`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${BACKEND_SECRET}` },
    });

    // topicA restarts from 0
    const rA = await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "a-new" }),
    });
    const bodyA = await rA.json<{ seq: number }>();
    expect(bodyA.seq).toBe(0);

    // topicB continues from 1
    const rB = await SELF.fetch(`https://proxy/t/${shard}/topicB/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "b-2" }),
    });
    const bodyB = await rB.json<{ seq: number }>();
    expect(bodyB.seq).toBe(1);
  });

  it("topics in the same shard have independent generations", async () => {
    const uid = crypto.randomUUID().slice(0, 8);
    const shard = `iso-gen-${uid}`;

    const rA = await SELF.fetch(`https://proxy/t/${shard}/topicA/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "a" }),
    });
    const bodyA = await rA.json<{ generation: string }>();

    const rB = await SELF.fetch(`https://proxy/t/${shard}/topicB/publish`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message: "b" }),
    });
    const bodyB = await rB.json<{ generation: string }>();

    expect(bodyA.generation).not.toBe(bodyB.generation);
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
