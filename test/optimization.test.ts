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
function uniqueTopic(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}

/** Publish a single message, returning parsed response. */
async function publish(
  topic: string,
  message: string,
  opts: { ttl?: number; max_buffer?: number; encoding?: string } = {}
) {
  const response = await SELF.fetch(
    `https://proxy/topic/${topic}/publish`,
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
      connections: number;
    }>(),
  };
}

/** Batch-publish to a single topic, returning parsed response. */
async function batchPublish(
  topic: string,
  messages: string[],
  opts: { ttl?: number; max_buffer?: number } = {}
) {
  const response = await SELF.fetch("https://proxy/bulk-publish", {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({
      messages: messages.map((m) => ({ topic_id: topic, message: m })),
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
async function deleteTopic(topic: string) {
  const response = await SELF.fetch(`https://proxy/topic/${topic}`, {
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

/** Get a DO stub for a topic (for alarm testing). */
function getStub(topic: string) {
  const id = env.PROXY_DO.idFromName(topic);
  return env.PROXY_DO.get(id);
}

// ---------------------------------------------------------------------------
// Meta caching & coalesced writes
// ---------------------------------------------------------------------------

describe("Optimization — meta caching & coalesced writes", () => {
  it("first publish to a new topic starts at seq 0", async () => {
    const topic = uniqueTopic("meta-first");
    const { body } = await publish(topic, "first message");
    expect(body.seq).toBe(0);
  });

  it("sequential publishes produce monotonically increasing seqs", async () => {
    const topic = uniqueTopic("meta-seq");
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
    const topic = uniqueTopic("meta-interleave");

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
    const topic = uniqueTopic("meta-content");

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
    const topic = uniqueTopic("meta-rapid");
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
    const topic = uniqueTopic("prune-within");

    // Publish 3 messages with max_buffer=5 — no pruning expected
    for (let i = 0; i < 3; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 5 });
      expect(body.seq).toBe(i);
    }
  });

  it("publishes exceeding max_buffer produce correct seqs", async () => {
    const topic = uniqueTopic("prune-exceed");

    // Publish 6 messages with max_buffer=3
    // After msg 3 (seq=3), pruning kicks in — oldestSeq moves forward
    for (let i = 0; i < 6; i++) {
      const { response, body } = await publish(topic, `msg-${i}`, {
        max_buffer: 3,
      });
      expect(response.status).toBe(200);
      expect(body.seq).toBe(i);
    }
  });

  it("publishes continue correctly after heavy pruning", async () => {
    const topic = uniqueTopic("prune-heavy");

    // Fill buffer with max_buffer=2 — each publish prunes aggressively
    for (let i = 0; i < 20; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 2 });
      expect(body.seq).toBe(i);
    }

    // After 20 publishes with max_buffer=2, the meta should reflect
    // nextSeq=20, oldestSeq=18 — verify by checking next seq
    const { body } = await publish(topic, "final", { max_buffer: 2 });
    expect(body.seq).toBe(20);
  });

  it("batch-publish with small max_buffer prunes correctly", async () => {
    const topic = uniqueTopic("prune-batch");

    // Batch publish 10 messages with max_buffer=3
    const { response, body } = await batchPublish(
      topic,
      Array.from({ length: 10 }, (_, i) => `batch-${i}`),
      { max_buffer: 3 }
    );
    expect(response.status).toBe(200);
    expect(body.results[0]!.first_seq).toBe(0);
    expect(body.results[0]!.last_seq).toBe(9);
    expect(body.results[0]!.messages_published).toBe(10);

    // Subsequent publish should continue at seq 10
    const { body: next } = await publish(topic, "after-batch", {
      max_buffer: 3,
    });
    expect(next.seq).toBe(10);
  });

  it("pruning with max_buffer=1 keeps only the latest message", async () => {
    const topic = uniqueTopic("prune-one");

    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `msg-${i}`, { max_buffer: 1 });
      expect(body.seq).toBe(i);
    }

    // After 5 publishes with max_buffer=1, only seq=4 should remain
    // Verify meta is correct by checking next publish
    const { body } = await publish(topic, "last", { max_buffer: 1 });
    expect(body.seq).toBe(5);
  });

  it("single publish after batch-publish prunes correctly", async () => {
    const topic = uniqueTopic("prune-single-after-batch");

    // Batch publish 5 messages with large buffer
    await batchPublish(
      topic,
      ["a", "b", "c", "d", "e"],
      { max_buffer: 100 }
    );

    // Now single publish with max_buffer=2 — should prune old messages
    const { body } = await publish(topic, "f", { max_buffer: 2 });
    expect(body.seq).toBe(5);

    // Next publish should still work
    const { body: body2 } = await publish(topic, "g", { max_buffer: 2 });
    expect(body2.seq).toBe(6);
  });

  it("changing max_buffer dynamically adjusts pruning", async () => {
    const topic = uniqueTopic("prune-dynamic");

    // Publish 10 messages with large buffer
    for (let i = 0; i < 10; i++) {
      await publish(topic, `msg-${i}`, { max_buffer: 100 });
    }

    // Now publish with tiny buffer — should prune down to 2
    const { body } = await publish(topic, "trigger-prune", { max_buffer: 2 });
    expect(body.seq).toBe(10);

    // Next publish should continue at 11
    const { body: next } = await publish(topic, "after-prune", {
      max_buffer: 2,
    });
    expect(next.seq).toBe(11);
  });
});

// ---------------------------------------------------------------------------
// Delete — cache reset
// ---------------------------------------------------------------------------

describe("Optimization — delete resets cached state", () => {
  it("publish after delete restarts sequences from 0", async () => {
    const topic = uniqueTopic("del-reset");

    // Publish some messages
    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`);
    }

    // Delete the topic
    const { response: delRes, body: delBody } = await deleteTopic(topic);
    expect(delRes.status).toBe(200);
    expect(delBody.deleted).toBe(true);

    // Publish again — should start from seq 0
    const { body } = await publish(topic, "after-delete");
    expect(body.seq).toBe(0);
  });

  it("batch-publish after delete restarts sequences from 0", async () => {
    const topic = uniqueTopic("del-batch-reset");

    // Publish messages then delete
    await batchPublish(topic, ["a", "b", "c"]);
    await deleteTopic(topic);

    // Batch publish again
    const { body } = await batchPublish(topic, ["x", "y"]);
    expect(body.results[0]!.first_seq).toBe(0);
    expect(body.results[0]!.last_seq).toBe(1);
  });

  it("delete is idempotent on an empty topic", async () => {
    const topic = uniqueTopic("del-empty");

    // Delete a topic that was never published to
    const { response, body } = await deleteTopic(topic);
    expect(response.status).toBe(200);
    expect(body.deleted).toBe(true);
    expect(body.connections_closed).toBe(0);
  });

  it("double delete does not break state", async () => {
    const topic = uniqueTopic("del-double");

    await publish(topic, "message");
    await deleteTopic(topic);
    const { response } = await deleteTopic(topic);
    expect(response.status).toBe(200);

    // Publish should still work after double delete
    const { body } = await publish(topic, "resurrection");
    expect(body.seq).toBe(0);
  });

  it("repeated publish-delete cycles each start from seq 0", async () => {
    const topic = uniqueTopic("del-cycle");

    for (let cycle = 0; cycle < 3; cycle++) {
      // Publish a few messages
      for (let i = 0; i < 3; i++) {
        const { body } = await publish(topic, `c${cycle}-m${i}`);
        expect(body.seq).toBe(i);
      }
      // Delete
      await deleteTopic(topic);
    }
  });

  it("delete clears alarm state so new publishes can set alarms", async () => {
    const topic = uniqueTopic("del-alarm");

    // Publish with a TTL (this sets an alarm)
    await publish(topic, "with-alarm", { ttl: 60 });

    // Delete (should clear alarm cache)
    await deleteTopic(topic);

    // Publish again with different TTL — should succeed without issues
    const { response, body } = await publish(topic, "new-alarm", { ttl: 120 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Alarm cleanup — batch reads
// ---------------------------------------------------------------------------

describe("Optimization — alarm cleanup (batch reads)", () => {
  it("alarm cleans up expired messages", async () => {
    const topic = uniqueTopic("alarm-cleanup");

    // Publish messages with a very short TTL (1 second)
    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1 });
    }

    // Wait for messages to expire
    await new Promise((resolve) => setTimeout(resolve, 1100));

    // Trigger the alarm
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // All messages expired → topic is dead (server-dictated lifecycle).
    // Storage wiped, connections closed. Next publish starts fresh.
    const { body } = await publish(topic, "after-alarm", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });

  it("alarm keeps non-expired messages", async () => {
    const topic = uniqueTopic("alarm-keep");

    // Publish messages with a long TTL
    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 3600 });
    }

    // Trigger alarm immediately (no messages should be expired)
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // seq should continue normally — no messages were deleted
    const { body } = await publish(topic, "after-alarm");
    expect(body.seq).toBe(5);
  });

  it("alarm running on empty topic does not break state", async () => {
    const topic = uniqueTopic("alarm-empty");

    // Create the topic by publishing then deleting
    await publish(topic, "seed", { ttl: 1 });
    await deleteTopic(topic);

    // Running alarm on a wiped topic should be a no-op
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Should still be able to publish
    const { body } = await publish(topic, "after-alarm-empty");
    expect(body.seq).toBe(0);
  });

  it("alarm correctly handles mixed expired and live messages", async () => {
    const topic = uniqueTopic("alarm-mixed");

    // Publish 3 messages with 1s TTL
    for (let i = 0; i < 3; i++) {
      await publish(topic, `short-lived-${i}`, { ttl: 1 });
    }

    // Wait for those to expire
    await new Promise((resolve) => setTimeout(resolve, 1100));

    // Publish 3 more with long TTL (these should survive the alarm)
    for (let i = 3; i < 6; i++) {
      await publish(topic, `long-lived-${i}`, { ttl: 3600 });
    }

    // Trigger alarm — should only clean up the first 3
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Publishing should continue at seq 6
    const { body } = await publish(topic, "after-mixed-alarm", { ttl: 3600 });
    expect(body.seq).toBe(6);
  });

  it("multiple alarm invocations are safe", async () => {
    const topic = uniqueTopic("alarm-multi");

    await publish(topic, "msg-0", { ttl: 1 });
    await new Promise((resolve) => setTimeout(resolve, 1100));

    const stub = getStub(topic);

    // Run alarm multiple times — should be idempotent after first cleanup
    await runDurableObjectAlarm(stub);
    await runDurableObjectAlarm(stub);

    // Topic expired and was wiped — seq restarts
    const { body } = await publish(topic, "after-multi-alarm", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });

  it("alarm after heavy pruning handles gaps correctly", async () => {
    const topic = uniqueTopic("alarm-gaps");

    // Publish 10 messages with small buffer — creates pruning gaps
    for (let i = 0; i < 10; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1, max_buffer: 3 });
    }

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 1100));

    // Trigger alarm — should handle gaps from pruned messages
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // All remaining messages expired → topic wiped, seq restarts
    const { body } = await publish(topic, "after-gaps", { ttl: 3600 });
    expect(body.seq).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Sequence continuity across complex workflows
// ---------------------------------------------------------------------------

describe("Optimization — sequence continuity", () => {
  it("sequences survive publish → prune → publish → delete → publish cycle", async () => {
    const topic = uniqueTopic("seq-lifecycle");

    // Phase 1: Publish 5 messages with buffer=3 (triggers pruning)
    for (let i = 0; i < 5; i++) {
      const { body } = await publish(topic, `phase1-${i}`, { max_buffer: 3 });
      expect(body.seq).toBe(i);
    }

    // Phase 2: Batch publish 3 more
    const { body: batch } = await batchPublish(topic, ["a", "b", "c"], {
      max_buffer: 3,
    });
    expect(batch.results[0]!.first_seq).toBe(5);
    expect(batch.results[0]!.last_seq).toBe(7);

    // Phase 3: Delete
    await deleteTopic(topic);

    // Phase 4: Fresh publish — should restart from 0
    const { body: fresh } = await publish(topic, "fresh");
    expect(fresh.seq).toBe(0);
  });

  it("bulk-publish across two topics maintains independent sequences", async () => {
    const topicA = uniqueTopic("seq-ind-a");
    const topicB = uniqueTopic("seq-ind-b");

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

    // Topic A had a seed (seq 0), so bulk seqs should be 1-2
    const resultA = body.results.find((r: { topic_id: string }) => r.topic_id === topicA)!;
    expect(resultA.first_seq).toBe(1);
    expect(resultA.last_seq).toBe(2);

    // Topic B is fresh, seqs should be 0-1
    const resultB = body.results.find((r: { topic_id: string }) => r.topic_id === topicB)!;
    expect(resultB.first_seq).toBe(0);
    expect(resultB.last_seq).toBe(1);
  });

  it("concurrent publishes to same topic all get unique seqs", async () => {
    const topic = uniqueTopic("seq-concurrent");

    const results = await Promise.all(
      Array.from({ length: 15 }, (_, i) =>
        publish(topic, `concurrent-${i}`)
      )
    );

    const seqs = results.map((r) => r.body.seq).sort((a, b) => a - b);

    // All should be unique
    expect(new Set(seqs).size).toBe(15);

    // Should be a contiguous range
    for (let i = 0; i < seqs.length; i++) {
      expect(seqs[i]).toBe(i);
    }
  });

  it("sequences correct after alarm cleanup and continued publishing", async () => {
    const topic = uniqueTopic("seq-alarm-cont");

    // Publish 5 messages with short TTL
    for (let i = 0; i < 5; i++) {
      await publish(topic, `old-${i}`, { ttl: 1 });
    }

    // Wait for expiry
    await new Promise((resolve) => setTimeout(resolve, 1100));

    // Run alarm
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // All messages expired → topic wiped. Seq restarts from 0.
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
    const topic = uniqueTopic("alarm-cache");

    // All these publishes should work even though only the first sets the alarm
    for (let i = 0; i < 10; i++) {
      const { response } = await publish(topic, `msg-${i}`, { ttl: 3600 });
      expect(response.status).toBe(200);
    }
  });

  it("alarm set correctly after delete clears alarm cache", async () => {
    const topic = uniqueTopic("alarm-after-del");

    // Publish (sets alarm + caches alarmScheduled=true)
    await publish(topic, "first", { ttl: 60 });

    // Delete (clears cache: alarmScheduled=false)
    await deleteTopic(topic);

    // Publish again — should set a new alarm (alarmScheduled was false)
    const { response, body } = await publish(topic, "second", { ttl: 120 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);

    // Verify alarm fires correctly by running it
    const stub = getStub(topic);
    // The alarm shouldn't delete messages since TTL is long
    await runDurableObjectAlarm(stub);

    // Should still publish at seq 1
    const { body: b2 } = await publish(topic, "third", { ttl: 120 });
    expect(b2.seq).toBe(1);
  });

  it("batch-publish also uses cached alarm state", async () => {
    const topic = uniqueTopic("alarm-batch-cache");

    // Single publish sets alarm
    await publish(topic, "seed", { ttl: 3600 });

    // Batch publish should reuse cached alarm state
    const { response, body } = await batchPublish(
      topic,
      ["a", "b", "c"],
      { ttl: 3600 }
    );
    expect(response.status).toBe(200);
    expect(body.results[0]!.first_seq).toBe(1);
    expect(body.results[0]!.last_seq).toBe(3);
  });

  it("alarm clears and republish re-sets alarm correctly", async () => {
    const topic = uniqueTopic("alarm-clear-reset");

    // Publish with short TTL
    await publish(topic, "to-expire", { ttl: 1 });

    // Wait and run alarm — cleans up, clears alarm
    await new Promise((resolve) => setTimeout(resolve, 1100));
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Topic expired and was wiped — fresh publish at seq 0 with new alarm
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
    const topic = uniqueTopic("edge-zero-buf");

    // max_buffer=0 means all messages get pruned immediately after storage
    // The publish should still succeed
    const { response, body } = await publish(topic, "zero-buf", {
      max_buffer: 0,
    });
    // Should still get a valid response
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });

  it("very large batch-publish succeeds", async () => {
    const topic = uniqueTopic("edge-large-batch");
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
    const topic = uniqueTopic("edge-large-prune");
    const messages = Array.from({ length: 50 }, (_, i) => `msg-${i}`);

    const { response, body } = await batchPublish(topic, messages, {
      max_buffer: 5,
    });
    expect(response.status).toBe(200);
    expect(body.results[0]!.messages_published).toBe(50);

    // Next publish should start at seq 50
    const { body: next } = await publish(topic, "after-large", {
      max_buffer: 5,
    });
    expect(next.seq).toBe(50);
  });

  it("publish with ttl=0 still works", async () => {
    const topic = uniqueTopic("edge-zero-ttl");

    // TTL of 0 means messages expire immediately — but publish should succeed
    const { response, body } = await publish(topic, "zero-ttl", { ttl: 0 });
    expect(response.status).toBe(200);
    expect(body.seq).toBe(0);
  });

  it("alternating single and batch publishes with pruning", async () => {
    const topic = uniqueTopic("edge-alternate");

    // Single → batch → single → batch, all with max_buffer=3
    const { body: s1 } = await publish(topic, "s1", { max_buffer: 3 });
    expect(s1.seq).toBe(0);

    const { body: b1 } = await batchPublish(topic, ["b1a", "b1b"], {
      max_buffer: 3,
    });
    expect(b1.results[0]!.first_seq).toBe(1);

    const { body: s2 } = await publish(topic, "s2", { max_buffer: 3 });
    expect(s2.seq).toBe(3);

    const { body: b2 } = await batchPublish(topic, ["b2a", "b2b", "b2c"], {
      max_buffer: 3,
    });
    expect(b2.results[0]!.first_seq).toBe(4);
    expect(b2.results[0]!.last_seq).toBe(6);

    // Final single publish
    const { body: s3 } = await publish(topic, "s3", { max_buffer: 3 });
    expect(s3.seq).toBe(7);
  });

  it("delete during active alarm cycle does not corrupt state", async () => {
    const topic = uniqueTopic("edge-del-alarm");

    // Publish with TTL
    for (let i = 0; i < 5; i++) {
      await publish(topic, `msg-${i}`, { ttl: 1 });
    }

    // Delete before alarm fires
    await deleteTopic(topic);

    // Run alarm on the deleted (empty) topic
    const stub = getStub(topic);
    await runDurableObjectAlarm(stub);

    // Publish fresh — should be clean
    const { body } = await publish(topic, "fresh-after-del-alarm");
    expect(body.seq).toBe(0);
  });
});
