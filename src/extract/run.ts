#!/usr/bin/env node

/**
 * Data extraction pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { createOptimizedSocrataAdapter } from '../adapters/socrata_optimized.js';
import { extractJobPostings, DEFAULT_JOB_CONFIG } from '../adapters/job_postings.js';
import { logger } from '../util/logger.js';
import { parseDate } from '../util/dates.js';
import type { ExtractArgs } from '../types.js';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs(): ExtractArgs {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: {
        type: 'string',
        short: 'c',
      },
      dataset: {
        type: 'string',
        short: 'd',
      },
      since: {
        type: 'string',
        short: 's',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
      limit: {
        type: 'string',
        short: 'l',
      },
      optimized: {
        type: 'boolean',
        short: 'o',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run extract -- --city <city> [--dataset <dataset>] [--since <date>] [--optimized] [--include-occupancy]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to extract (optional)
  -s, --since <date>       Extract records since this date (optional)
  -h, --help              Show this help message
  -o, --optimized         Use optimized Socrata adapter (optional)
  -i, --include-occupancy  Include occupancy and inspection data extraction (optional)
  -l, --limit <limit>      Maximum number of records to extract (optional)

Examples:
  npm run extract -- --city chicago
  npm run extract -- --city chicago --dataset building_permits
  npm run extract -- --city chicago --since 2024-01-01
  npm run extract -- --city chicago --optimized
  npm run extract -- --city chicago --include-occupancy
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

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
  
  if (values.optimized) {
    result.optimized = true;
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
    // Extract traditional Socrata datasets
    const adapter = options.optimized 
      ? createOptimizedSocrataAdapter(cityConfig, storage)
      : createSocrataAdapter(cityConfig, storage);

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
