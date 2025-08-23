#!/usr/bin/env node

/**
 * Data extraction pipeline runner
 */

import { config } from 'dotenv';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { extractJobPostings, DEFAULT_JOB_CONFIG } from '../adapters/job_postings.js';
import { logger } from '../util/logger.js';
import { parseDate } from '../util/dates.js';
import { parseCliArgs as parseSharedCliArgs, CLI_CONFIGS } from '../util/cli.js';
import type { ExtractArgs } from '../types.js';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs(): ExtractArgs {
  const values = parseSharedCliArgs(CLI_CONFIGS.extract);
  
  const result: ExtractArgs = {
    city: values.city as string,
  };
  
  if (values.dataset) {
    result.dataset = values.dataset as string;
  }
  
  if (values.since) {
    result.since = values.since as string;
  }
  
  if (values.limit) {
    result.maxRecords = parseInt(values.limit as string, 10);
  }
  
  return result;
}

/**
 * Main extraction function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting data extraction', args);

    const runOpts: { city: string; since?: string; maxRecords?: number; optimized?: boolean } = { city: args.city };
    if (args.since !== undefined) runOpts.since = args.since;
    if (args.maxRecords !== undefined) runOpts.maxRecords = args.maxRecords;
    if (args.optimized !== undefined) runOpts.optimized = args.optimized;
    await runExtraction(runOpts);

    logger.info('Data extraction completed successfully', {
      city: args.city,
    });
    
    process.exit(0);
    
  } catch (error) {
    if (error instanceof Error) {
      logger.error('Data extraction failed', {
        error: error.message,
        stack: error.stack,
        name: error.constructor.name,
        args
      });
    } else {
      logger.error('Data extraction failed', { error, args });
    }
    process.exit(1);
  }
}

async function runExtraction(options: {
  city: string;
  since?: string;
  maxRecords?: number;
  optimized?: boolean;
}): Promise<void> {
  const storage = await createStorage();
  const cityConfig = loadCityConfig(options.city);
  
  try {
    // Extract Socrata datasets using optimized adapter
    const adapter = createSocrataAdapter(cityConfig, storage);

    const sinceDate = options.since ? new Date(options.since) : undefined;
    
    logger.info('Starting data extraction', {
      city: options.city,
      optimized: options.optimized,
      since: sinceDate?.toISOString(),
      maxRecords: options.maxRecords
    });

    const extractOpts: { sinceDate?: Date; maxRecords?: number } = {};
    if (sinceDate !== undefined) extractOpts.sinceDate = sinceDate;
    if (options.maxRecords !== undefined) extractOpts.maxRecords = options.maxRecords;
    const results = await adapter.extractAllDatasets(extractOpts);


    logger.info('Data extraction completed', {
      city: options.city,
      results,
    });

  } catch (error) {
    logger.error('Data extraction failed', { error });
    throw error;
  } finally {
    await storage.close();
  }
}


// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', { promise, reason });
  process.exit(1);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', { error: error.message, stack: error.stack });
  process.exit(1);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
