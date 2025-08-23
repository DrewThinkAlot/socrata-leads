import { logger } from '../util/logger.js';

export interface RawRecordMsg {
  id: string;
  city: string;
  dataset: string;
  watermark: string;
  payload: any;
}

export interface QueuePublisher {
  publishRawBatch(batch: RawRecordMsg[]): Promise<void>;
  close(): Promise<void>;
}

class NoopQueuePublisher implements QueuePublisher {
  async publishRawBatch(batch: RawRecordMsg[]): Promise<void> {
    logger.info('No REDIS_URL configured. Dropping batch (noop publisher).', { size: batch.length });
  }
  async close(): Promise<void> {}
}

/**
 * Simple Redis list-based publisher. Phase 1 keeps it minimal.
 * Key format: RAW_QUEUE_KEY (default: "socrata:raw")
 */
class RedisListPublisher implements QueuePublisher {
  private redis: any;
  private key: string;

  constructor(redis: any, key: string) {
    this.redis = redis;
    this.key = key;
  }

  async publishRawBatch(batch: RawRecordMsg[]): Promise<void> {
    // push one JSON string per record to keep consumer simple
    const payloads = batch.map((r) => JSON.stringify(r));
    await this.redis.rpush(this.key, ...payloads);
    logger.debug('Published batch to Redis list', { key: this.key, size: batch.length });
  }

  async close(): Promise<void> {
    if (this.redis) {
      try {
        await this.redis.quit();
      } catch (e) {
        // ignore
      }
    }
  }
}

export async function createQueuePublisher(): Promise<QueuePublisher> {
  const url = process.env.REDIS_URL;
  const key = process.env.RAW_QUEUE_KEY || 'socrata:raw';

  if (!url) {
    logger.warn('REDIS_URL not set. Using NoopQueuePublisher.');
    return new NoopQueuePublisher();
  }

  // Lazy import ioredis to avoid hard dep if unused
  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, {
    maxRetriesPerRequest: 2,
    enableReadyCheck: true,
  });

  redis.on('error', (err: any) => logger.error('Redis error', { error: String(err) }));
  redis.on('connect', () => logger.info('Connected to Redis'));

  return new RedisListPublisher(redis, key);
}
