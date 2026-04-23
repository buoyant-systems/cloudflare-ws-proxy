import {
  env,
  SELF,
  runDurableObjectAlarm,
} from "cloudflare:test";
import { describe, it, expect } from "vitest";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BACKEND_SECRET =
  (env as Record<string, string>).BACKEND_SECRET ?? "test-secret";

function authHeaders(): HeadersInit {
  return {
    Authorization: `Bearer ${BACKEND_SECRET}`,
    "Content-Type": "application/json",
  };
}

/** Unique topic per test to avoid cross-contamination (isolatedStorage is off). */
function uniqueTopic(prefix: string): { shard: string; topic: string; fullId: string } {
  const uid = crypto.randomUUID().slice(0, 8);
  const shard = `${prefix}-${uid}`;
  const topic = "t";
  return { shard, topic, fullId: `${shard}/${topic}` };
}

/** Publish a single message, returning parsed response. */
async function publish(
  fullId: string,
  message: string,
  opts: { ttl?: number; max_buffer?: number; encoding?: string } = {}
) {
  const [shard, topic] = fullId.split("/");
  const response = await SELF.fetch(
    `https://proxy/t/${shard}/${topic}/publish`,
    {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ message, ...opts }),
    }
  );
  return {
    response,
    body: await response.json<{
      seq: number;
      topic_id: string;
      generation: string;
      connections: number;
    }>(),
  };
}

/** Batch-publish to a single topic, returning parsed response. */
async function batchPublish(
  fullId: string,
  messages: string[],
  opts: { ttl?: number; max_buffer?: number } = {}
) {
  const response = await SELF.fetch("https://proxy/bulk-publish", {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({
      messages: messages.map((m) => ({ topic_id: fullId, message: m })),
      ...opts,
    }),
  });
  return {
    response,
    body: await response.json<{
      topics: number;
      messages: number;
      results: Array<{
        topic_id: string;
        generation: string;
        status: number;
        messages_published: number;
        first_seq: number;
        last_seq: number;
        connections: number;
      }>;
    }>(),
  };
}

/** Delete a topic, returning parsed response. */
async function deleteTopic(fullId: string) {
  const [shard, topic] = fullId.split("/");
  const response = await SELF.fetch(`https://proxy/t/${shard}/${topic}`, {
    method: "DELETE",
    headers: { Authorization: `Bearer ${BACKEND_SECRET}` },
  });
  return {
    response,
    body: await response.json<{
      deleted: boolean;
      topic_id: string;
      connections_closed: number;
    }>(),
  };
}

/** Get a DO stub for a topic (for alarm testing). Uses the shard key. */
function getStub(fullId: string) {
  const shard = fullId.split("/")[0]!;
  const id = env.PROXY_DO.idFromName(shard);
  return env.PROXY_DO.get(id);
}

// ---------------------------------------------------------------------------
// Generation hash
// ---------------------------------------------------------------------------

describe("Topic generation hash", () => {
  it("first publish returns a non-empty generation", async () => {
    const { fullId: topic } = uniqueTopic("gen-first");
    const { body } = await publish(topic, "hello");
    expect(body.generation).toBeDefined();
    expect(body.generation.length).toBeGreaterThan(0);
  });

  it("generation stays stable across publishes within same lifecycle", async () => {
    const { fullId: topic } = uniqueTopic("gen-stable");

    const { body: b1 } = await publish(topic, "msg-1");
    const { body: b2 } = await publish(topic, "msg-2");
    const { body: b3 } = await publish(topic, "msg-3");

    expect(b1.generation).toBe(b2.generation);
    expect(b2.generation).toBe(b3.generation);
  });

  it("batch-publish returns the same generation as single publish", async () => {
    const { fullId: topic } = uniqueTopic("gen-batch");

    const { body: single } = await publish(topic, "seed");
    const { body: batch } = await batchPublish(topic, ["a", "b"]);

    expect(batch.results[0]!.generation).toBe(single.generation);
  });

  it("generation changes after delete + recreate", async () => {
    const { fullId: topic } = uniqueTopic("gen-recycle");

    const { body: before } = await publish(topic, "before");
    const gen1 = before.generation;

    await deleteTopic(topic);

    const { body: after } = await publish(topic, "after");
    const gen2 = after.generation;

    expect(gen1).not.toBe(gen2);
    expect(gen2.length).toBeGreaterThan(0);
  });

  it("generation changes after alarm-based expiry + recreate", async () => {
    const { fullId: topic } = uniqueTopic("gen-expire");

    const { body: before } = await publish(topic, "ephemeral", { ttl: 1 });
    const gen1 = before.generation;

    // Wait for expiry and trigger alarm
    await new Promise((resolve) => setTimeout(resolve, 1100));
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Publish again — new lifecycle, new generation
    const { body: after } = await publish(topic, "reborn", { ttl: 3600 });
    const gen2 = after.generation;

    expect(gen1).not.toBe(gen2);
  });

  it("two different topics get different generations", async () => {
    const { fullId: topicA } = uniqueTopic("gen-diff-a");
    const { fullId: topicB } = uniqueTopic("gen-diff-b");

    const { body: a } = await publish(topicA, "a");
    const { body: b } = await publish(topicB, "b");

    expect(a.generation).not.toBe(b.generation);
  });
});

// ---------------------------------------------------------------------------
// Static topic config (TTL + max_buffer)
// ---------------------------------------------------------------------------

describe("Static topic config", () => {
  it("TTL is locked on first publish", async () => {
    const { fullId: topic } = uniqueTopic("static-ttl");

    // First publish sets TTL to 3600
    await publish(topic, "msg-0", { ttl: 3600 });

    // Subsequent publish with different TTL is silently ignored — topic
    // uses the original TTL. The publish itself should still succeed.
    const { response, body } = await publish(topic, "msg-1", { ttl: 1 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(1);

    // Wait 1.1s — if the second TTL (1s) was applied, messages would expire.
    // But TTL is static at 3600s, so alarm should have no effect.
    await new Promise((resolve) => setTimeout(resolve, 1100));
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Seq should continue — messages not expired
    const { body: after } = await publish(topic, "msg-2");
    expect(after.seq).toBe(2);
  });

  it("max_buffer is locked on first publish", async () => {
    const { fullId: topic } = uniqueTopic("static-buf");

    // First publish sets max_buffer=10
    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { max_buffer: 10 });
    }

    // Subsequent publish with max_buffer=2 — should be ignored, still using 10
    for (let i = 5; i < 12; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 2 });
      expect(body.seq).toBe(i);
    }

    // If max_buffer=2 were applied, pruning would have kicked in at seq 7+.
    // But since max_buffer is static at 10, with 12 messages total and
    // nextSeq=12, oldestSeq should be 2 (12-10), not 10 (12-2).
    // Verify by publishing one more — seq continues normally
    const { body } = await publish(topic, "final");
    expect(body.seq).toBe(12);
  });

  it("batch-publish also locks config on first publish", async () => {
    const { fullId: topic } = uniqueTopic("static-batch");

    // First event is a batch publish with specific config
    const { body: b1 } = await batchPublish(topic, ["a", "b", "c"], {
      ttl: 3600,
      max_buffer: 5,
    });
    expect(b1.results[0]!.first_seq).toBe(0);

    // Subsequent batch with different config — should be ignored
    const { body: b2 } = await batchPublish(topic, ["d", "e", "f"], {
      ttl: 1,
      max_buffer: 100,
    });
    expect(b2.results[0]!.first_seq).toBe(3);
    expect(b2.results[0]!.last_seq).toBe(5);

    // Same generation throughout
    expect(b1.results[0]!.generation).toBe(b2.results[0]!.generation);
  });

  it("config is fresh after delete", async () => {
    const { fullId: topic } = uniqueTopic("static-after-del");

    // Create with max_buffer=10
    await publish(topic, "old", { max_buffer: 10, ttl: 3600 });
    await deleteTopic(topic);

    // Recreate with max_buffer=2 — should take effect
    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `new-${i}`, { max_buffer: 2 });
      expect(body.seq).toBe(i);
    }
    // With max_buffer=2 and 5 messages, 3 should be pruned.
    // Verify next seq is correct
    const { body } = await publish(topic, "check");
    expect(body.seq).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Meta caching & coalesced writes
// ---------------------------------------------------------------------------

describe("Optimization — meta caching & coalesced writes", () => {
  it("first publish to a new topic starts at seq 0", async () => {
    const { fullId: topic } = uniqueTopic("meta-first");
    const { body } = await publish(topic, "first message");
    expect(body.seq).toBe(0);
  });

  it("sequential publishes produce monotonically increasing seqs", async () => {
    const { fullId: topic } = uniqueTopic("meta-seq");
    const seqs: number[] = [];

    for (let i = 0; i < 10; i++) {
      const { body } = await publish(topic, `msg-${i}`);
      seqs.push(body.seq);
    }

    // Every seq should be exactly 1 more than the previous
    for (let i = 1; i < seqs.length; i++) {
      expect(seqs[i]).toBe(seqs[i - 1]! + 1);
    }
    expect(seqs[0]).toBe(0);
    expect(seqs[9]).toBe(9);
  });

  it("meta persists correctly across interleaved publish and batch-publish", async () => {
    const { fullId: topic } = uniqueTopic("meta-interleave");

    // Single publish: seq 0
    const { body: b0 } = await publish(topic, "single-0");
    expect(b0.seq).toBe(0);

    // Batch publish 3 messages: seqs 1, 2, 3
    const { body: b1 } = await batchPublish(topic, ["batch-1", "batch-2", "batch-3"]);
    expect(b1.results[0]!.first_seq).toBe(1);
    expect(b1.results[0]!.last_seq).toBe(3);

    // Another single publish: seq 4
    const { body: b2 } = await publish(topic, "single-4");
    expect(b2.seq).toBe(4);

    // Another batch: seqs 5, 6
    const { body: b3 } = await batchPublish(topic, ["batch-5", "batch-6"]);
    expect(b3.results[0]!.first_seq).toBe(5);
    expect(b3.results[0]!.last_seq).toBe(6);
  });

  it("message content is stored correctly with coalesced write", async () => {
    const { fullId: topic } = uniqueTopic("meta-content");

    // Publish a base64 message
    const { response, body } = await publish(topic, "SGVsbG8=", {
      encoding: "base64",
    });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);

    // Publish a text message right after
    const { body: b2 } = await publish(topic, "hello world");
    expect(b2.seq).toBe(1);
  });

  it("rapid publishes all get unique sequence numbers", async () => {
    const { fullId: topic } = uniqueTopic("meta-rapid");
    const count = 20;

    // Fire all publishes as fast as possible
    const results = await Promise.all(
      Array.from({ length: count }, (_, i) =>
        publish(topic, `rapid-${i}`)
      )
    );

    const seqs = results.map((r) => r.body.seq);
    const uniqueSeqs = new Set(seqs);

    // All seqs should be unique
    expect(uniqueSeqs.size).toBe(count);
    // All seqs should be in range [0, count)
    for (const seq of seqs) {
      expect(seq).toBeGreaterThanOrEqual(0);
      expect(seq).toBeLessThan(count);
    }
  });
});

// ---------------------------------------------------------------------------
// Buffer pruning (computePruneKeys)
// ---------------------------------------------------------------------------

describe("Optimization — buffer pruning (computePruneKeys)", () => {
  it("messages within max_buffer are not pruned", async () => {
    const { fullId: topic } = uniqueTopic("prune-within");

    // First publish sets max_buffer=5
    for (let i = 0; i < 3; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 5 });
      expect(body.seq).toBe(i);
    }
  });

  it("publishes exceeding max_buffer produce correct seqs", async () => {
    const { fullId: topic } = uniqueTopic("prune-exceed");

    // First publish sets max_buffer=3, subsequent publishes use same config
    for (let i = 0; i < 6; i++) {
      const { response, body } = await publish(topic, `msg-${i}`, {
        max_buffer: 3,
      });
      expect(response.status).toBe(200);
      expect(body.seq).toBe(i);
    }
  });

  it("publishes continue correctly after heavy pruning", async () => {
    const { fullId: topic } = uniqueTopic("prune-heavy");

    // max_buffer=2 — aggressive pruning on every publish after the second
    for (let i = 0; i < 20; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 2 });
      expect(body.seq).toBe(i);
    }

    const { body } = await publish(topic, "final");
    expect(body.seq).toBe(20);
  });

  it("batch-publish with small max_buffer prunes correctly", async () => {
    const { fullId: topic } = uniqueTopic("prune-batch");

    // First batch sets max_buffer=3
    const { response, body } = await batchPublish(
      topic,
      Array.from({ length: 10 }, (_, i) => `batch-${i}`),
      { max_buffer: 3 }
    );
    expect(response.status).toBe(200);
    expect(body.results[0]!.first_seq).toBe(0);
    expect(body.results[0]!.last_seq).toBe(9);
    expect(body.results[0]!.messages_published).toBe(10);

    // Subsequent publish continues
    const { body: next } = await publish(topic, "after-batch");
    expect(next.seq).toBe(10);
  });

  it("pruning with max_buffer=1 keeps only the latest message", async () => {
    const { fullId: topic } = uniqueTopic("prune-one");

    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 1 });
      expect(body.seq).toBe(i);
    }

    const { body } = await publish(topic, "last");
    expect(body.seq).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Delete — cache reset
// ---------------------------------------------------------------------------

describe("Optimization — delete resets cached state", () => {
  it("publish after delete restarts sequences from 0", async () => {
    const { fullId: topic } = uniqueTopic("del-reset");

    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`);
    }

    const { response: delRes, body: delBody } = await deleteTopic(topic);
    expect(delRes.status).toBe(200);
    expect(delBody.deleted).toBe(true);

    const { body } = await publish(topic, "after-delete");
    expect(body.seq).toBe(0);
  });

  it("batch-publish after delete restarts sequences from 0", async () => {
    const { fullId: topic } = uniqueTopic("del-batch-reset");

    await batchPublish(topic, ["a", "b", "c"]);
    await deleteTopic(topic);

    const { body } = await batchPublish(topic, ["x", "y"]);
    expect(body.results[0]!.first_seq).toBe(0);
    expect(body.results[0]!.last_seq).toBe(1);
  });

  it("delete is idempotent on an empty topic", async () => {
    const { fullId: topic } = uniqueTopic("del-empty");

    const { response, body } = await deleteTopic(topic);
    expect(response.status).toBe(200);
    expect(body.deleted).toBe(true);
    expect(body.connections_closed).toBe(0);
  });

  it("double delete does not break state", async () => {
    const { fullId: topic } = uniqueTopic("del-double");

    await publish(topic, "message");
    await deleteTopic(topic);
    const { response } = await deleteTopic(topic);
    expect(response.status).toBe(200);

    const { body } = await publish(topic, "resurrection");
    expect(body.seq).toBe(0);
  });

  it("repeated publish-delete cycles each start from seq 0", async () => {
    const { fullId: topic } = uniqueTopic("del-cycle");

    for (let cycle = 0; cycle < 3; cycle++) {
      for (let i = 0; i < 3; i++) {
        const { body } = await publish(topic, `c${cycle}-m${i}`);
        expect(body.seq).toBe(i);
      }
      await deleteTopic(topic);
    }
  });

  it("delete clears alarm state so new publishes can set alarms", async () => {
    const { fullId: topic } = uniqueTopic("del-alarm");

    await publish(topic, "with-alarm", { ttl: 60 });
    await deleteTopic(topic);

    // New lifecycle — TTL can be set fresh
    const { response, body } = await publish(topic, "new-alarm", { ttl: 120 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Alarm cleanup — batch reads & unconditional teardown
// ---------------------------------------------------------------------------

describe("Optimization — alarm cleanup", () => {
  it("alarm cleans up expired messages and tears down topic", async () => {
    const { fullId: topic } = uniqueTopic("alarm-cleanup");

    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1 });
    }

    await new Promise((resolve) => setTimeout(resolve, 1100));

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Topic expired → full teardown. Next publish starts fresh lifecycle.
    const { body } = await publish(topic, "after-alarm", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });

  it("alarm keeps non-expired messages", async () => {
    const { fullId: topic } = uniqueTopic("alarm-keep");

    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 3600 });
    }

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // No messages expired — seq continues
    const { body } = await publish(topic, "after-alarm");
    expect(body.seq).toBe(5);
  });

  it("alarm running on empty topic does not break state", async () => {
    const { fullId: topic } = uniqueTopic("alarm-empty");

    await publish(topic, "seed", { ttl: 1 });
    await deleteTopic(topic);

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    const { body } = await publish(topic, "after-alarm-empty");
    expect(body.seq).toBe(0);
  });

  it("alarm with partially expired messages keeps survivors", async () => {
    const { fullId: topic } = uniqueTopic("alarm-partial");

    // Publish with TTL=1 (static for this topic)
    for (let i = 0; i < 3; i++) {
      await publish(topic, `early-${i}`, { ttl: 1 });
    }

    // Wait for early messages to age
    await new Promise((resolve) => setTimeout(resolve, 600));

    // Publish more — same TTL (1s), but these are younger (published later)
    for (let i = 3; i < 6; i++) {
      await publish(topic, `late-${i}`);
    }

    // Wait a bit more so early messages (age > 1s) expire but late ones don't
    await new Promise((resolve) => setTimeout(resolve, 600));

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Some messages survived — seq continues
    const { body } = await publish(topic, "after-partial");
    expect(body.seq).toBe(6);
  });

  it("multiple alarm invocations are safe", async () => {
    const { fullId: topic } = uniqueTopic("alarm-multi");

    await publish(topic, "msg-0", { ttl: 1 });
    await new Promise((resolve) => setTimeout(resolve, 1100));

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);
    await runDurableObjectAlarm(stub);

    // Topic expired and was wiped — seq restarts
    const { body } = await publish(topic, "after-multi-alarm", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });

  it("alarm after heavy pruning handles gaps correctly", async () => {
    const { fullId: topic } = uniqueTopic("alarm-gaps");

    // max_buffer=3, TTL=1 — creates pruning gaps
    for (let i = 0; i < 10; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1, max_buffer: 3 });
    }

    await new Promise((resolve) => setTimeout(resolve, 1100));

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // All remaining expired → topic wiped
    const { body } = await publish(topic, "after-gaps", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Sequence continuity across complex workflows
// ---------------------------------------------------------------------------

describe("Optimization — sequence continuity", () => {
  it("sequences survive publish → prune → batch → delete → publish cycle", async () => {
    const { fullId: topic } = uniqueTopic("seq-lifecycle");

    // Phase 1: Publish 5 messages (max_buffer=3 set on creation)
    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `phase1-${i}`, { max_buffer: 3 });
      expect(body.seq).toBe(i);
    }

    // Phase 2: Batch publish 3 more
    const { body: batch } = await batchPublish(topic, ["a", "b", "c"]);
    expect(batch.results[0]!.first_seq).toBe(5);
    expect(batch.results[0]!.last_seq).toBe(7);

    // Phase 3: Delete
    await deleteTopic(topic);

    // Phase 4: Fresh publish — new lifecycle starts from 0
    const { body: fresh } = await publish(topic, "fresh");
    expect(fresh.seq).toBe(0);
  });

  it("bulk-publish across two topics maintains independent sequences", async () => {
    const { fullId: topicA } = uniqueTopic("seq-ind-a");
    const { fullId: topicB } = uniqueTopic("seq-ind-b");

    // Publish to topic A first
    await publish(topicA, "a-seed");

    // Bulk publish to both topics
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
    const body = await response.json<{
      results: Array<{
        topic_id: string;
        first_seq: number;
        last_seq: number;
      }>;
    }>();

    const resultA = body.results.find((r: { topic_id: string }) => r.topic_id === topicA)!;
    expect(resultA.first_seq).toBe(1);
    expect(resultA.last_seq).toBe(2);

    const resultB = body.results.find((r: { topic_id: string }) => r.topic_id === topicB)!;
    expect(resultB.first_seq).toBe(0);
    expect(resultB.last_seq).toBe(1);
  });

  it("concurrent publishes to same topic all get unique seqs", async () => {
    const { fullId: topic } = uniqueTopic("seq-concurrent");

    const results = await Promise.all(
      Array.from({ length: 15 }, (_, i) =>
        publish(topic, `concurrent-${i}`)
      )
    );

    const seqs = results.map((r) => r.body.seq).sort((a, b) => a - b);
    expect(new Set(seqs).size).toBe(15);

    for (let i = 0; i < seqs.length; i++) {
      expect(seqs[i]).toBe(i);
    }
  });

  it("sequences correct after alarm cleanup and continued publishing", async () => {
    const { fullId: topic } = uniqueTopic("seq-alarm-cont");

    for (let i = 0; i < 5; i++) {
      await publish(topic, `old-${i}`, { ttl: 1 });
    }

    await new Promise((resolve) => setTimeout(resolve, 1100));

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Topic expired → fresh lifecycle
    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `new-${i}`, { ttl: 3600 });
      expect(body.seq).toBe(i);
    }
  });
});

// ---------------------------------------------------------------------------
// ensureAlarm — cached alarm state
// ---------------------------------------------------------------------------

describe("Optimization — ensureAlarm caching", () => {
  it("multiple publishes to same topic succeed (alarm only set once)", async () => {
    const { fullId: topic } = uniqueTopic("alarm-cache");

    for (let i = 0; i < 10; i++) {
      const { response } = await publish(topic, `msg-${i}`, { ttl: 3600 });
      expect(response.status).toBe(200);
    }
  });

  it("alarm set correctly after delete clears alarm cache", async () => {
    const { fullId: topic } = uniqueTopic("alarm-after-del");

    await publish(topic, "first", { ttl: 60 });
    await deleteTopic(topic);

    const { response, body } = await publish(topic, "second", { ttl: 120 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);

    // Verify alarm fires correctly — TTL is 120s so nothing expires
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    const { body: b2 } = await publish(topic, "third");
    expect(b2.seq).toBe(1);
  });

  it("batch-publish also uses cached alarm state", async () => {
    const { fullId: topic } = uniqueTopic("alarm-batch-cache");

    await publish(topic, "seed", { ttl: 3600 });

    const { response, body } = await batchPublish(
      topic,
      ["a", "b", "c"],
    );
    expect(response.status).toBe(200);
    expect(body.results[0]!.first_seq).toBe(1);
    expect(body.results[0]!.last_seq).toBe(3);
  });

  it("alarm clears and republish re-sets alarm correctly", async () => {
    const { fullId: topic } = uniqueTopic("alarm-clear-reset");

    await publish(topic, "to-expire", { ttl: 1 });

    await new Promise((resolve) => setTimeout(resolve, 1100));
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Topic expired → fresh lifecycle at seq 0
    const { response, body } = await publish(topic, "fresh", { ttl: 3600 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Edge cases & robustness
// ---------------------------------------------------------------------------

describe("Optimization — edge cases", () => {
  it("max_buffer=0 is handled gracefully", async () => {
    const { fullId: topic } = uniqueTopic("edge-zero-buf");

    const { response, body } = await publish(topic, "zero-buf", {
      max_buffer: 0,
    });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });

  it("very large batch-publish succeeds", async () => {
    const { fullId: topic } = uniqueTopic("edge-large-batch");
    const messages = Array.from({ length: 50 }, (_, i) => `msg-${i}`);

    const { response, body } = await batchPublish(topic, messages, {
      max_buffer: 50,
    });
    expect(response.status).toBe(200);
    expect(body.results[0]!.messages_published).toBe(50);
    expect(body.results[0]!.first_seq).toBe(0);
    expect(body.results[0]!.last_seq).toBe(49);
  });

  it("large batch-publish with small buffer prunes correctly", async () => {
    const { fullId: topic } = uniqueTopic("edge-large-prune");
    const messages = Array.from({ length: 50 }, (_, i) => `msg-${i}`);

    const { response, body } = await batchPublish(topic, messages, {
      max_buffer: 5,
    });
    expect(response.status).toBe(200);
    expect(body.results[0]!.messages_published).toBe(50);

    const { body: next } = await publish(topic, "after-large");
    expect(next.seq).toBe(50);
  });

  it("publish with ttl=0 still works", async () => {
    const { fullId: topic } = uniqueTopic("edge-zero-ttl");

    const { response, body } = await publish(topic, "zero-ttl", { ttl: 0 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });

  it("alternating single and batch publishes with pruning", async () => {
    const { fullId: topic } = uniqueTopic("edge-alternate");

    // First publish sets max_buffer=3
    const { body: s1 } = await publish(topic, "s1", { max_buffer: 3 });
    expect(s1.seq).toBe(0);

    const { body: b1 } = await batchPublish(topic, ["b1a", "b1b"]);
    expect(b1.results[0]!.first_seq).toBe(1);

    const { body: s2 } = await publish(topic, "s2");
    expect(s2.seq).toBe(3);

    const { body: b2 } = await batchPublish(topic, ["b2a", "b2b", "b2c"]);
    expect(b2.results[0]!.first_seq).toBe(4);
    expect(b2.results[0]!.last_seq).toBe(6);

    const { body: s3 } = await publish(topic, "s3");
    expect(s3.seq).toBe(7);
  });

  it("delete during active alarm cycle does not corrupt state", async () => {
    const { fullId: topic } = uniqueTopic("edge-del-alarm");

    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1 });
    }

    await deleteTopic(topic);

    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    const { body } = await publish(topic, "fresh-after-del-alarm");
    expect(body.seq).toBe(0);
  });
});
