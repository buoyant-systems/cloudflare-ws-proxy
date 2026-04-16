import { describe, it, expect } from "vitest";
import { generateToken, verifyToken } from "../src/auth";

const TEST_SECRET = "test-secret-do-not-use-in-production";

describe("auth — generateToken", () => {
  it("generates a token with two dot-separated base64url segments", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic");
    const parts = token.split(".");
    expect(parts.length).toBe(2);
    // base64url characters only
    for (const part of parts) {
      expect(part).toMatch(/^[A-Za-z0-9_-]+$/);
    }
  });

  it("returns an expiresAt timestamp in the future", async () => {
    const before = Date.now();
    const { expiresAt } = await generateToken(TEST_SECRET, "my-topic", undefined, 60);
    const after = Date.now();
    expect(expiresAt).toBeGreaterThanOrEqual(before + 60 * 1000);
    expect(expiresAt).toBeLessThanOrEqual(after + 60 * 1000);
  });

  it("includes cursor in the payload when provided", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic", 42);
    const payload = await verifyToken(TEST_SECRET, token);
    expect(payload).not.toBeNull();
    expect(payload!.cursor).toBe(42);
  });

  it("omits cursor from the payload when not provided", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic");
    const payload = await verifyToken(TEST_SECRET, token);
    expect(payload).not.toBeNull();
    expect(payload!.cursor).toBeUndefined();
  });
});

describe("auth — verifyToken", () => {
  it("verifies a valid token and returns the payload", async () => {
    const { token } = await generateToken(TEST_SECRET, "test-topic", undefined, 300);
    const payload = await verifyToken(TEST_SECRET, token);

    expect(payload).not.toBeNull();
    expect(payload!.topicId).toBe("test-topic");
    expect(payload!.exp).toBeGreaterThan(Date.now());
  });

  it("rejects a token signed with a different secret", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic");
    const payload = await verifyToken("wrong-secret", token);
    expect(payload).toBeNull();
  });

  it("rejects a tampered token", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic");
    // Flip a character in the payload segment
    const tampered = "X" + token.slice(1);
    const payload = await verifyToken(TEST_SECRET, tampered);
    expect(payload).toBeNull();
  });

  it("rejects a malformed token (missing segments)", async () => {
    expect(await verifyToken(TEST_SECRET, "just-one-part")).toBeNull();
    expect(await verifyToken(TEST_SECRET, "a.b.c")).toBeNull();
    expect(await verifyToken(TEST_SECRET, "")).toBeNull();
  });

  it("rejects an expired token", async () => {
    // Generate a token that expired 1 second ago (ttl = -1)
    // We do this by generating normally then manually checking
    const { token } = await generateToken(TEST_SECRET, "my-topic", undefined, 0);
    // Wait a tiny bit to ensure it's expired
    await new Promise((resolve) => setTimeout(resolve, 10));
    const payload = await verifyToken(TEST_SECRET, token);
    expect(payload).toBeNull();
  });

  it("respects the custom TTL", async () => {
    const { token } = await generateToken(TEST_SECRET, "my-topic", undefined, 600);
    const payload = await verifyToken(TEST_SECRET, token);
    expect(payload).not.toBeNull();
    // Should expire ~600s from now
    const expectedMin = Date.now() + 590 * 1000;
    const expectedMax = Date.now() + 610 * 1000;
    expect(payload!.exp).toBeGreaterThan(expectedMin);
    expect(payload!.exp).toBeLessThan(expectedMax);
  });
});
