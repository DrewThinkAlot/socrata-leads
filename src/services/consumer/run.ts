#!/usr/bin/env node

/**
 * Phase 2: Queue consumer service
 * - Reads raw record messages from Redis list and persists to storage.raw
 * - Retries with backoff and sends to DLQ after max retries
 * - Batches writes and supports graceful shutdown
 */

import { config as loadEnv } from 'dotenv';
import { logger } from '../../util/logger.js';
import { createStorage } from '../../storage/index.js';

loadEnv();

// Minimal RawRecord shape aligned with Storage.upsertRaw
interface RawRecordMsg {
  id: string;
  city: string;
  dataset: string;
  watermark: string;
  payload: any;
}

interface Envelope {
  record: RawRecordMsg;
  retryCount?: number;
  lastError?: string;
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

async function main() {
  const RAW_QUEUE_KEY = process.env.RAW_QUEUE_KEY || 'socrata:raw';
  const RAW_DLQ_KEY = process.env.RAW_DLQ_KEY || 'socrata:raw:dlq';
  const BATCH_SIZE = envInt('CONSUMER_BATCH_SIZE', 500);
  const POLL_TIMEOUT_SEC = envInt('CONSUMER_POLL_TIMEOUT_SEC', 5);
  const MAX_RETRIES = envInt('CONSUMER_MAX_RETRIES', 3);
  const NORMALIZE_QUEUE_KEY = process.env.NORMALIZE_QUEUE_KEY || 'socrata:normalize';

  const url = process.env.REDIS_URL;
  if (!url) {
    logger.error('REDIS_URL is required for consumer service');
    process.exit(1);
  }

  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, { maxRetriesPerRequest: 2, enableReadyCheck: true });

  const storage = await createStorage();
  let running = true;

  const shutdown = async () => {
    if (!running) return;
    running = false;
    logger.info('Shutting down consumer...');
    try {
      await storage.close();
    } catch {}
    try {
      await redis.quit();
    } catch {}
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('Consumer starting', {
    RAW_QUEUE_KEY,
    RAW_DLQ_KEY,
    NORMALIZE_QUEUE_KEY,
    BATCH_SIZE,
    POLL_TIMEOUT_SEC,
    MAX_RETRIES,
  });

  while (running) {
    try {
      // Block for one item (BLPOP), then drain additional items non-blocking up to BATCH_SIZE
      const blpop = (await redis.blpop(RAW_QUEUE_KEY, POLL_TIMEOUT_SEC)) as [string, string] | null;
      if (!blpop) {
        continue; // timeout, loop again
      }

      const batch: Envelope[] = [];
      const first = blpop[1];
      batch.push(parseEnvelope(first));

      // Drain up to batch size - 1
      for (let i = 1; i < BATCH_SIZE; i++) {
        const v = await redis.lpop(RAW_QUEUE_KEY);
        if (!v) break;
        batch.push(parseEnvelope(v));
      }

      // Process batch: per-record upsert with individual retry handling
      for (const env of batch) {
        await handleRecord(env, storage, redis, RAW_QUEUE_KEY, RAW_DLQ_KEY, MAX_RETRIES);
      }

      logger.info('Processed batch', { size: batch.length });
    } catch (err: any) {
      logger.error('Consumer loop error', { error: String(err) });
    }
  }
}

function parseEnvelope(jsonStr: string): Envelope {
  try {
    const parsed = JSON.parse(jsonStr);
    if ('record' in parsed) return parsed as Envelope;
    // Backward compatibility: older publisher sends raw record directly
    return { record: parsed as RawRecordMsg, retryCount: 0 };
  } catch (e: any) {
    // If message is malformed, put into DLQ format immediately
    return { record: JSON.parse('{}') as any, retryCount: 999, lastError: 'Malformed JSON' };
  }
}

async function handleRecord(
  env: Envelope,
  storage: import('../../types.js').Storage,
  redis: any,
  RAW_QUEUE_KEY: string,
  RAW_DLQ_KEY: string,
  MAX_RETRIES: number
) {
  // Malformed guard
  if (!env.record || !env.record.id) {
    await redis.rpush(RAW_DLQ_KEY, JSON.stringify({ ...env, reason: 'invalid_record' }));
    return;
  }

  const retry = env.retryCount ?? 0;
  try {
    await storage.upsertRaw({
      id: env.record.id,
      city: env.record.city,
      dataset: env.record.dataset,
      watermark: env.record.watermark,
      payload: env.record.payload,
    });
    // On success, publish to normalization queue with the raw record
    const normalizeEnvelope = { raw: env.record };
    const normalizeKey = process.env.NORMALIZE_QUEUE_KEY || 'socrata:normalize';
    await redis.rpush(normalizeKey, JSON.stringify(normalizeEnvelope));
  } catch (e: any) {
    const nextRetry = retry + 1;
    const envelope: Envelope = {
      record: env.record,
      retryCount: nextRetry,
      lastError: e?.message || String(e),
    };

    if (nextRetry > MAX_RETRIES) {
      await redis.rpush(RAW_DLQ_KEY, JSON.stringify(envelope));
      logger.warn('Record sent to DLQ', { id: env.record.id, retries: nextRetry });
    } else {
      // Exponential backoff via delay key (simple sleep here)
      const delayMs = Math.min(30000, Math.pow(2, nextRetry) * 250);
      await sleep(delayMs);
      await redis.rpush(RAW_QUEUE_KEY, JSON.stringify(envelope));
      logger.warn('Record requeued for retry', { id: env.record.id, retry: nextRetry });
    }
  }
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
