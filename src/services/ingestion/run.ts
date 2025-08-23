#!/usr/bin/env node

/**
 * Phase 1: Ingestion service
 * - Extract Socrata data and publish raw records to a queue (Redis list)
 * - Still updates checkpoints in the existing storage layer
 */

import { config as loadEnv } from 'dotenv';
import { loadCityConfig } from '../../config/index.js';
import { createStorage } from '../../storage/index.js';
import { createSocrataAdapter } from '../../adapters/socrata.js';
import { createQueuePublisher } from '../../queue/index.js';
import { logger } from '../../util/logger.js';
import { parseCliArgs, CLI_CONFIGS } from '../../util/cli.js';

loadEnv();

interface IngestArgs {
  city: string;
  dataset?: string;
  since?: string;
  limit?: string;
}

function parseArgs(): IngestArgs {
  const values = parseCliArgs(CLI_CONFIGS.extract); // reuse extract CLI shape
  return values as IngestArgs;
}

async function main() {
  const args = parseArgs();
  const storage = await createStorage();
  const queue = await createQueuePublisher();
  const cityConfig = loadCityConfig(args.city);

  try {
    logger.info('Starting ingestion service', {
      city: args.city,
      dataset: args.dataset,
      since: args.since,
      limit: args.limit,
      redis: Boolean(process.env.REDIS_URL),
      queueKey: process.env.RAW_QUEUE_KEY || 'socrata:raw',
    });

    const adapter = createSocrataAdapter(cityConfig, storage, {
      sink: async (batch) => {
        await queue.publishRawBatch(batch);
      },
    });

    const sinceDate = args.since ? new Date(args.since) : undefined;
    const maxRecords = args.limit ? parseInt(String(args.limit), 10) : undefined;

    // Build options with only defined fields to satisfy exactOptionalPropertyTypes
    const opts: { sinceDate?: Date; maxRecords?: number } = {};
    if (sinceDate !== undefined) opts.sinceDate = sinceDate;
    if (maxRecords !== undefined) opts.maxRecords = maxRecords;

    if (args.dataset) {
      await adapter.extractDataset(args.dataset, opts);
    } else {
      await adapter.extractAllDatasets(opts);
    }

    logger.info('Ingestion service completed');
    process.exit(0);
  } catch (error: any) {
    logger.error('Ingestion service failed', { error: error?.message || String(error) });
    process.exit(1);
  } finally {
    await queue.close();
    await storage.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
