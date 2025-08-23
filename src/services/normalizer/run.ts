#!/usr/bin/env node

/**
 * Phase 3: Streaming normalization service
 * - Consumes raw records from NORMALIZE_QUEUE_KEY
 * - Normalizes to canonical schema and writes to storage.normalized
 */

import { config as loadEnv } from 'dotenv';
import { logger } from '../../util/logger.js';
import { loadCityConfig } from '../../config/index.js';
import { createStorage } from '../../storage/index.js';
import { parseDate, formatDateTimeISO } from '../../util/dates.js';
import { normalizeAddress, parseCoordinate, validateCoordinates } from '../../util/address.js';

loadEnv();

interface NormalizeEnvelopeV1 {
  raw: {
    id: string;
    city: string;
    dataset: string;
    watermark: string;
    payload: any;
  };
}

function envInt(name: string, def: number): number {
  const v = process.env[name];
  if (!v) return def;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
}

function isRestaurantRecord(raw: any): boolean {
  const payload = raw.payload || {};
  const fields = [
    payload.business_type,
    payload.description,
    payload.business_name,
    payload.type,
  ]
    .map((x: any) => (typeof x === 'string' ? x.toLowerCase() : ''))
    .join(' ');
  const keywords = ['restaurant', 'cafe', 'bar', 'grill', 'kitchen', 'diner', 'bistro', 'pub', 'tavern', 'bakery', 'pizzeria', 'deli', 'sandwich', 'food', 'eatery'];
  return keywords.some((k) => fields.includes(k));
}

function evaluateConcat(expression: string, payload: any): string {
  const match = expression.match(/CONCAT\(([^)]+)\)/);
  if (!match) return '';
  const parts = match[1]!.split(',').map((p) => p.trim());
  const values = parts.map((part) => {
    if (part.startsWith("'") && part.endsWith("'")) return part.slice(1, -1);
    if (part.startsWith('COALESCE(')) return evaluateCoalesce(part, payload) || '';
    return payload[part] || '';
  });
  return values.join('');
}

function evaluateCoalesce(expression: string, payload: any): string | null {
  const match = expression.match(/COALESCE\(([^)]+)\)/);
  if (!match) return null;
  const fields = match[1]!.split(',').map((f) => f.trim());
  for (const f of fields) {
    const v = payload[f];
    if (v !== null && v !== undefined && v !== '') return v;
  }
  return null;
}

async function normalizeRecord(rawRecord: any): Promise<any> {
  const cityConfig = loadCityConfig(rawRecord.city);
  const datasetConfig = cityConfig.datasets[rawRecord.dataset];
  if (!datasetConfig) {
    throw new Error(`No dataset config for ${rawRecord.city}/${rawRecord.dataset}`);
  }

  const payload = rawRecord.payload;
  const mapping = datasetConfig.map;
  const normalized: any = {
    uid: (await import('crypto')).randomUUID(),
    city: rawRecord.city,
    dataset: rawRecord.dataset,
    raw_id: rawRecord.id,
  };

  for (const [canonicalField, expression] of Object.entries(mapping)) {
    let value: any = null;
    if (typeof expression === 'string') {
      if (expression === ':self') {
        value = `${cityConfig.base_url}/resource/${datasetConfig.id}/${payload[':id'] || rawRecord.id}`;
      } else if (expression.startsWith('CONCAT(')) {
        value = evaluateConcat(expression, payload);
      } else if (expression.startsWith('COALESCE(')) {
        value = evaluateCoalesce(expression, payload);
      } else {
        value = payload[expression];
      }
    }

    switch (canonicalField) {
      case 'address':
        normalized.address = normalizeAddress(value);
        break;
      case 'lat':
        {
          const lat = parseCoordinate(value);
          const coords = validateCoordinates(lat, undefined);
          normalized.lat = coords.lat;
        }
        break;
      case 'lon':
        {
          const lon = parseCoordinate(value);
          const coords2 = validateCoordinates(undefined, lon);
          normalized.lon = coords2.lon;
        }
        break;
      case 'event_date':
        {
          const d = parseDate(value);
          normalized.event_date = d ? formatDateTimeISO(d) : null;
        }
        break;
      default:
        normalized[canonicalField] = value;
    }
  }

  // Optional LLM enhancement gate; keep disabled by default for streaming
  const LLM_ENABLED = process.env.LLM_ENABLED === 'true';
  if (LLM_ENABLED && isRestaurantRecord(rawRecord)) {
    try {
      const { categorizeBusinessType, analyzeDescription } = await import('../../util/llm.js');
      const businessType = await categorizeBusinessType(normalized.description || '', normalized.business_name);
      if (!normalized.type) normalized.type = businessType.category;
      if (normalized.description) {
        const analysis = await analyzeDescription(normalized.description, normalized.business_name);
        normalized.business_category = analysis.businessType;
        normalized.confidence_score = analysis.confidence;
      }
    } catch (e) {
      logger.warn('LLM enrichment failed; continuing', { id: rawRecord.id, error: String(e) });
    }
  }

  return normalized;
}

async function main() {
  const NORMALIZE_QUEUE_KEY = process.env.NORMALIZE_QUEUE_KEY || 'socrata:normalize';
  const NORMALIZE_DLQ_KEY = process.env.NORMALIZE_DLQ_KEY || 'socrata:normalize:dlq';
  const SCORE_QUEUE_KEY = process.env.SCORE_QUEUE_KEY || 'socrata:score';
  const BATCH_SIZE = envInt('NORMALIZER_BATCH_SIZE', 200);
  const POLL_TIMEOUT_SEC = envInt('NORMALIZER_POLL_TIMEOUT_SEC', 5);
  const MAX_RETRIES = envInt('NORMALIZER_MAX_RETRIES', 2);

  const url = process.env.REDIS_URL;
  if (!url) {
    logger.error('REDIS_URL is required for normalizer service');
    process.exit(1);
  }

  const { default: IORedis } = await import('ioredis');
  const redis = new IORedis(url, { maxRetriesPerRequest: 2, enableReadyCheck: true });
  const storage = await createStorage();

  let running = true;
  const shutdown = async () => {
    if (!running) return;
    running = false;
    logger.info('Shutting down normalizer...');
    try { await storage.close(); } catch {}
    try { await redis.quit(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  logger.info('Normalizer starting', { NORMALIZE_QUEUE_KEY, NORMALIZE_DLQ_KEY, SCORE_QUEUE_KEY, BATCH_SIZE, POLL_TIMEOUT_SEC, MAX_RETRIES });

  while (running) {
    try {
      const blpop = (await redis.blpop(NORMALIZE_QUEUE_KEY, POLL_TIMEOUT_SEC)) as [string, string] | null;
      if (!blpop) continue;

      const batch: NormalizeEnvelopeV1[] = [];
      batch.push(JSON.parse(blpop[1]!));
      for (let i = 1; i < BATCH_SIZE; i++) {
        const v = await redis.lpop(NORMALIZE_QUEUE_KEY);
        if (!v) break;
        batch.push(JSON.parse(v));
      }

      let ok = 0;
      for (const env of batch) {
        const rec = env?.raw;
        if (!rec || !rec.id) {
          await redis.rpush(NORMALIZE_DLQ_KEY, JSON.stringify({ env, reason: 'invalid_raw' }));
          continue;
        }
        let retries = 0;
        while (true) {
          try {
            const normalized = await normalizeRecord(rec);
            await storage.insertNormalized(normalized);
            // Publish to score queue for Phase 4
            await redis.rpush(SCORE_QUEUE_KEY, JSON.stringify({ normalized }));
            ok++;
            break;
          } catch (e: any) {
            retries++;
            if (retries > MAX_RETRIES) {
              await redis.rpush(NORMALIZE_DLQ_KEY, JSON.stringify({ env, error: String(e) }));
              logger.warn('Normalize DLQ', { id: rec.id });
              break;
            }
            await sleep(Math.min(20000, Math.pow(2, retries) * 200));
          }
        }
      }

      logger.info('Normalized batch', { size: batch.length, ok });
    } catch (err: any) {
      logger.error('Normalizer loop error', { error: String(err) });
    }
  }
}

function sleep(ms: number) { return new Promise((res) => setTimeout(res, ms)); }

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
