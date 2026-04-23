/**
 * Topic ID parsing utilities.
 *
 * All topic IDs are bi-keyed: `shard/topic`. The shard determines which
 * Durable Object handles the request; the topic identifies an independent
 * channel within that DO.
 */

const SEGMENT_REGEX = /^[a-zA-Z0-9_.:~-]{1,128}$/;

export interface TopicAddress {
  /** Used for idFromName() — DO resolution */
  shardKey: string;
  /** Used internally within the DO to namespace storage/sessions */
  topicKey: string;
  /** The original full topic ID: "shard/topic" */
  fullId: string;
}

/**
 * Construct a TopicAddress from already-extracted path segments.
 * Used by the Worker router where shard and topic come from URL path parts.
 */
export function parseTopicId(shard: string, topic: string): TopicAddress {
  return { shardKey: shard, topicKey: topic, fullId: `${shard}/${topic}` };
}

/**
 * Parse a full topic ID string ("shard/topic") into its components.
 * Used by bulk-publish where topic_id comes from the request body.
 * Returns null if the format is invalid.
 */
export function parseTopicIdFromString(fullId: string): TopicAddress | null {
  const slashIndex = fullId.indexOf("/");
  if (slashIndex === -1 || slashIndex === 0 || slashIndex === fullId.length - 1) {
    return null;
  }
  const shard = fullId.slice(0, slashIndex);
  const topic = fullId.slice(slashIndex + 1);
  // Reject if topic contains additional slashes
  if (topic.includes("/")) return null;
  if (!validateSegment(shard) || !validateSegment(topic)) return null;
  return { shardKey: shard, topicKey: topic, fullId };
}

/**
 * Validate a single segment (shard or topic) of a topic ID.
 */
export function validateSegment(segment: string): boolean {
  return SEGMENT_REGEX.test(segment);
}
