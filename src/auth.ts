/**
 * Lightweight HMAC-SHA256 token generation and verification using the Web Crypto API.
 *
 * Token format: <base64url(payload)>.<base64url(signature)>
 *
 * Zero external dependencies — uses only `crypto.subtle` available in the
 * Cloudflare Workers runtime.
 */

const DEFAULT_TOKEN_TTL_SECONDS = 300; // 5 minutes
const ALGORITHM = { name: "HMAC", hash: "SHA-256" } as const;

export interface TokenPayload {
  /** The topic this token grants access to */
  topicId: string;
  /** Optional cursor — replay buffered messages from this sequence number */
  cursor?: number;
  /** Expiry timestamp in unix milliseconds */
  exp: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function base64UrlEncode(data: ArrayBuffer | Uint8Array): string {
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlDecode(str: string): ArrayBuffer {
  const padded = str.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes.buffer;
}

async function importKey(secret: string): Promise<CryptoKey> {
  const encoder = new TextEncoder();
  return crypto.subtle.importKey("raw", encoder.encode(secret), ALGORITHM, false, [
    "sign",
    "verify",
  ]);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Generate a short-lived HMAC-SHA256 signed token for WebSocket authentication.
 *
 * @param secret   - The shared BACKEND_SECRET
 * @param topicId  - Topic the token authorizes access to
 * @param cursor   - Optional sequence number for message replay
 * @param ttlSeconds - Token validity duration (default: 300s / 5 min)
 * @returns The signed token string
 */
export async function generateToken(
  secret: string,
  topicId: string,
  cursor?: number,
  ttlSeconds: number = DEFAULT_TOKEN_TTL_SECONDS
): Promise<{ token: string; expiresAt: number }> {
  const exp = Date.now() + ttlSeconds * 1000;

  const payload: TokenPayload = { topicId, exp };
  if (cursor !== undefined) {
    payload.cursor = cursor;
  }

  const encoder = new TextEncoder();
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));

  const key = await importKey(secret);
  const signature = await crypto.subtle.sign(ALGORITHM, key, encoder.encode(payloadB64));
  const signatureB64 = base64UrlEncode(signature);

  return { token: `${payloadB64}.${signatureB64}`, expiresAt: exp };
}

/**
 * Verify and decode an HMAC-SHA256 signed token.
 *
 * @param secret - The shared BACKEND_SECRET
 * @param token  - The token string to verify
 * @returns The decoded payload, or `null` if the token is invalid or expired
 */
export async function verifyToken(
  secret: string,
  token: string
): Promise<TokenPayload | null> {
  const parts = token.split(".");
  if (parts.length !== 2) {
    return null;
  }

  const [payloadB64, signatureB64] = parts;

  try {
    const key = await importKey(secret);
    const encoder = new TextEncoder();
    const signatureBytes = base64UrlDecode(signatureB64!);

    const valid = await crypto.subtle.verify(
      ALGORITHM,
      key,
      signatureBytes,
      encoder.encode(payloadB64)
    );
    if (!valid) {
      return null;
    }

    const payloadBytes = base64UrlDecode(payloadB64!);
    const decoder = new TextDecoder();
    const payload: TokenPayload = JSON.parse(decoder.decode(payloadBytes));

    // Check expiry
    if (payload.exp < Date.now()) {
      return null;
    }

    return payload;
  } catch {
    return null;
  }
}
