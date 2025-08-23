#!/usr/bin/env node

/**
 * Phase 5: Streaming fusion service
 * - Consumes scored items from FUSE_QUEUE_KEY
 * - Performs lightweight deduplication using Redis keys
 * - Publishes de-duplicated items to EXPORT_QUEUE_KEY
 */

import { config as loadEnv } from 'dotenv';
import { logger } from '../../util/logger.js';
import { createHash } from 'crypto';
import type { Event, Lead } from '../../types.js';

loadEnv();

interface FuseEnvelopeV1 {
  event: Event;
  lead: Omit<Lead, 'created_at'>; // created_at added by storage later
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

function fusionKey(e: Event): string {
  const base = `${e.city}|${(e.name || '').toLowerCase()}|${(e.address || '').toLowerCase()}|${e.predicted_open_week}`;
  return createHash('sha1').update(base).digest('hex');
}

async function main() {
  const FUSE_QUEUE_KEY = process.env.FUSE_QUEUE_KEY || 'socrata:fuse';
  const FUSE_DLQ_KEY = process.env.FUSE_DLQ_KEY || 'socrata:fuse:dlq';
  const EXPORT_QUEUE_KEY = process.env.EXPORT_QUEUE_KEY || 'socrata:export';
  const BATCH_SIZE = envInt('FUSER_BATCH_SIZE', 500);
  const POLL_TIMEOUT_SEC = envInt('FUSER_POLL_TIMEOUT_SEC', 5);
  const MAX_RETRIES = envInt('FUSER_MAX_RETRIES', 2);
  const DEDUPE_TTL_SEC = envInt('FUSER_DEDUPE_TTL_SEC', 7 * 24 * 3600);

  const url = process.env.REDIS_URL;
  if (!url) {
    logger.error('REDIS_URL is required for fusion service');
    process.exit(1);
  }

  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, { maxRetriesPerRequest: 2, enableReadyCheck: true });

  let running = true;
  const shutdown = async () => {
    if (!running) return;
    running = false;
    logger.info('Shutting down fusion...');
    try { await redis.quit(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('Fusion starting', { FUSE_QUEUE_KEY, FUSE_DLQ_KEY, EXPORT_QUEUE_KEY, BATCH_SIZE, POLL_TIMEOUT_SEC, MAX_RETRIES, DEDUPE_TTL_SEC });

  while (running) {
    try {
      const blpop = (await redis.blpop(FUSE_QUEUE_KEY, POLL_TIMEOUT_SEC)) as [string, string] | null;
      if (!blpop) continue;

      const batch: FuseEnvelopeV1[] = [];
      batch.push(JSON.parse(blpop[1]!));
      for (let i = 1; i < BATCH_SIZE; i++) {
        const v = await redis.lpop(FUSE_QUEUE_KEY);
        if (!v) break;
        batch.push(JSON.parse(v));
      }

      let forwarded = 0;
      for (const env of batch) {
        let retries = 0;
        while (true) {
          try {
            const e = env?.event as Event;
            const k = fusionKey(e);
            const redisKey = `socrata:fuse:seen:${k}`;
            // Deduplicate using SET NX with TTL
            const set = await redis.set(redisKey, '1', 'EX', DEDUPE_TTL_SEC, 'NX');
            if (!set) {
              // already seen, drop
              break;
            }
            // Publish to export queue
            await redis.rpush(EXPORT_QUEUE_KEY, JSON.stringify({ lead: env.lead, event: e }));
            forwarded++;
            break;
          } catch (e: any) {
            retries++;
            if (retries > MAX_RETRIES) {
              await redis.rpush(FUSE_DLQ_KEY, JSON.stringify({ env, error: String(e) }));
              logger.warn('Fusion DLQ', { err: String(e) });
              break;
            }
            await sleep(Math.min(20000, Math.pow(2, retries) * 200));
          }
        }
      }

      logger.info('Fused batch', { size: batch.length, forwarded });
    } catch (err: any) {
      logger.error('Fusion loop error', { error: String(err) });
    }
  }
}

function sleep(ms: number) { return new Promise((res) => setTimeout(res, ms)); }

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
