#!/usr/bin/env node

/**
 * Phase 4: Streaming scoring service
 * - Consumes normalized records from SCORE_QUEUE_KEY
 * - Creates Events and simple Leads and persists to storage
 */

import { config as loadEnv } from 'dotenv';
import { logger } from '../../util/logger.js';
import { createStorage } from '../../storage/index.js';
import { randomUUID } from 'crypto';
import type { Event, NormalizedRecord, Lead } from '../../types.js';
import { analyzeSpotOnIntelligence } from '../../filters/spoton.js';

loadEnv();

interface ScoreEnvelopeV1 {
  normalized: NormalizedRecord;
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

function inferSignalStrength(n: NormalizedRecord): number {
  const type = (n.type || '').toLowerCase();
  const desc = (n.description || '').toLowerCase();
  // Very rough heuristics; refined fusion happens later
  if (type.includes('food') && (desc.includes('pass') || desc.includes('approved'))) return 85;
  if (type.includes('final inspection') || desc.includes('final inspection')) return 80;
  if (type.includes('utility') || desc.includes('utility')) return 78;
  if (type.includes('equipment') || desc.includes('hood')) return 75;
  if (type.includes('liquor') || desc.includes('liquor')) return 70;
  if (type.includes('building') || desc.includes('permit')) return 60;
  return 50;
}

function simpleScoreFromEvent(e: Event): number {
  const s = e.signal_strength;
  let score = Math.min(100, Math.max(30, Math.round(s)));
  const text = `${e.description || ''} ${e.name || ''}`.toLowerCase();
  if (text.includes('liquor')) score += 5;
  if (text.includes('equipment') || text.includes('utility')) score += 5;
  return Math.min(100, score);
}

async function main() {
  const SCORE_QUEUE_KEY = process.env.SCORE_QUEUE_KEY || 'socrata:score';
  const SCORE_DLQ_KEY = process.env.SCORE_DLQ_KEY || 'socrata:score:dlq';
  const BATCH_SIZE = envInt('SCORER_BATCH_SIZE', 200);
  const POLL_TIMEOUT_SEC = envInt('SCORER_POLL_TIMEOUT_SEC', 5);
  const MAX_RETRIES = envInt('SCORER_MAX_RETRIES', 2);
  const FUSE_QUEUE_KEY = process.env.FUSE_QUEUE_KEY || 'socrata:fuse';

  const url = process.env.REDIS_URL;
  if (!url) {
    logger.error('REDIS_URL is required for scorer service');
    process.exit(1);
  }

  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, { maxRetriesPerRequest: 2, enableReadyCheck: true });
  const storage = await createStorage();

  let running = true;
  const shutdown = async () => {
    if (!running) return;
    running = false;
    logger.info('Shutting down scorer...');
    try { await storage.close(); } catch {}
    try { await redis.quit(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('Scorer starting', { SCORE_QUEUE_KEY, SCORE_DLQ_KEY, FUSE_QUEUE_KEY, BATCH_SIZE, POLL_TIMEOUT_SEC, MAX_RETRIES });

  while (running) {
    try {
      const blpop = (await redis.blpop(SCORE_QUEUE_KEY, POLL_TIMEOUT_SEC)) as [string, string] | null;
      if (!blpop) continue;

      const batch: ScoreEnvelopeV1[] = [];
      batch.push(JSON.parse(blpop[1]!));
      for (let i = 1; i < BATCH_SIZE; i++) {
        const v = await redis.lpop(SCORE_QUEUE_KEY);
        if (!v) break;
        batch.push(JSON.parse(v));
      }

      let ok = 0;
      for (const env of batch) {
        const n = env?.normalized as NormalizedRecord;
        if (!n || !n.raw_id) {
          await redis.rpush(SCORE_DLQ_KEY, JSON.stringify({ env, reason: 'invalid_normalized' }));
          continue;
        }

        let retries = 0;
        while (true) {
          try {
            // Build Event from single normalized record (later: fuse multiple signals)
            const eventBase: Omit<Event, 'created_at'> = {
              event_id: randomUUID(),
              city: n.city,
              address: n.address || '',
              name: n.business_name,
              predicted_open_week: (n.event_date || new Date().toISOString()).slice(0, 10),
              signal_strength: inferSignalStrength(n),
              evidence: [n],
            };
            if (n.description) {
              (eventBase as any).description = n.description;
            }

            await storage.insertEvent(eventBase);

            // Materialize Event with created_at for downstream use
            const eventObj: Event = { ...eventBase, created_at: new Date().toISOString() } as Event;

            // Create a simple lead immediately (iterative improvement later with fusion)
            const spoton = await analyzeSpotOnIntelligence([eventObj]);
            const leadBase: Omit<Lead, 'created_at'> = {
              lead_id: randomUUID(),
              city: n.city,
              name: eventObj.name,
              address: eventObj.address,
              phone: undefined,
              email: undefined,
              score: eventObj.signal_strength,
              spoton_intelligence: spoton,
              evidence: [eventObj],
            };

            await storage.insertLead(leadBase);

            // Publish to fusion queue
            const envelope = { event: eventObj, lead: { ...leadBase, created_at: new Date().toISOString() } };
            await redis.rpush(FUSE_QUEUE_KEY, JSON.stringify(envelope));

            ok++;
            break;
          } catch (e: any) {
            logger.error('Failed to score record', { 
              id: n.raw_id, 
              error: e.message, 
              stack: e.stack,
              normalized: JSON.stringify(n)
            });
            retries++;
            if (retries > MAX_RETRIES) {
              await redis.rpush(SCORE_DLQ_KEY, JSON.stringify({ env, error: String(e) }));
              logger.warn('Score DLQ', { id: n.raw_id });
              break;
            }
            await sleep(Math.min(20000, Math.pow(2, retries) * 200));
          }
        }
      }

      logger.info('Scored batch', { size: batch.length, ok });
    } catch (err: any) {
      logger.error('Scorer loop error', { error: String(err) });
    }
  }
}

function sleep(ms: number) { return new Promise((res) => setTimeout(res, ms)); }

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
