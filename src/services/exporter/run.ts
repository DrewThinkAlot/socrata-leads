#!/usr/bin/env node

/**
 * Phase 6: Streaming exporter service
 * - Consumes fused items from EXPORT_QUEUE_KEY
 * - Writes NDJSON artifacts partitioned by city and date
 */

import { config as loadEnv } from 'dotenv';
import { logger } from '../../util/logger.js';
import fs from 'fs';
import path from 'path';
import type { Event, Lead } from '../../types.js';

loadEnv();

interface ExportEnvelopeV1 {
  event: Event;
  lead: Omit<Lead, 'created_at'> | Lead;
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

function ensureDir(dir: string) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function todayStr(): string {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function main() {
  const EXPORT_QUEUE_KEY = process.env.EXPORT_QUEUE_KEY || 'socrata:export';
  const EXPORT_DIR = process.env.EXPORT_DIR || path.join('data', 'exports');
  const EXPORTER_BATCH_SIZE = envInt('EXPORTER_BATCH_SIZE', 1000);
  const POLL_TIMEOUT_SEC = envInt('EXPORTER_POLL_TIMEOUT_SEC', 5);
  const MAX_RETRIES = envInt('EXPORTER_MAX_RETRIES', 2);

  const url = process.env.REDIS_URL;
  if (!url) {
    logger.error('REDIS_URL is required for exporter service');
    process.exit(1);
  }

  ensureDir(EXPORT_DIR);

  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, { maxRetriesPerRequest: 2, enableReadyCheck: true });

  let running = true;
  const shutdown = async () => {
    if (!running) return;
    running = false;
    logger.info('Shutting down exporter...');
    try { await redis.quit(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('Exporter starting', { EXPORT_QUEUE_KEY, EXPORT_DIR, EXPORTER_BATCH_SIZE, POLL_TIMEOUT_SEC, MAX_RETRIES });

  while (running) {
    try {
      const blpop = (await redis.blpop(EXPORT_QUEUE_KEY, POLL_TIMEOUT_SEC)) as [string, string] | null;
      if (!blpop) continue;

      const batch: ExportEnvelopeV1[] = [];
      batch.push(JSON.parse(blpop[1]!));
      for (let i = 1; i < EXPORTER_BATCH_SIZE; i++) {
        const v = await redis.lpop(EXPORT_QUEUE_KEY);
        if (!v) break;
        batch.push(JSON.parse(v));
      }

      let written = 0;
      for (const env of batch) {
        const e = env?.event as Event | undefined;
        const l = env?.lead as Lead | Omit<Lead, 'created_at'> | undefined;
        if (!e || !e.city) continue;

        const citySlug = e.city.toLowerCase().replace(/\s+/g, '-');
        const dir = path.join(EXPORT_DIR, citySlug);
        ensureDir(dir);
        const file = path.join(dir, `${todayStr()}.ndjson`);

        const record = { event: e, lead: l };
        fs.appendFileSync(file, JSON.stringify(record) + '\n', 'utf-8');
        written++;
      }

      logger.info('Exporter wrote batch', { size: batch.length, written });
    } catch (err: any) {
      logger.error('Exporter loop error', { error: String(err) });
    }
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
