#!/usr/bin/env node

/**
 * Data extraction pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
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
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run extract -- --city <city> [--dataset <dataset>] [--since <date>]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to extract (optional)
  -s, --since <date>       Extract records since this date (optional)
  -h, --help              Show this help message

Examples:
  npm run extract -- --city chicago
  npm run extract -- --city chicago --dataset building_permits
  npm run extract -- --city chicago --since 2024-01-01
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
  
  return result;
}

/**
 * Main extraction function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting data extraction', args);

    // Load city configuration
    const cityConfig = loadCityConfig(args.city);
    
    // Create storage instance
    const storage = await createStorage();
    
    // Create Socrata adapter
    const adapter = createSocrataAdapter(cityConfig, storage);
    
    // Test connection
    const isConnected = await adapter.testConnection();
    if (!isConnected) {
      throw new Error('Failed to connect to Socrata API');
    }
    
    // Parse since date if provided
    let sinceDate: Date | undefined;
    if (args.since) {
      sinceDate = parseDate(args.since) || undefined;
      if (!sinceDate) {
        throw new Error(`Invalid date format: ${args.since}`);
      }
    }
    
    // Extract data
    let results: Record<string, { recordCount: number; lastWatermark: string | null }>;
    
    if (args.dataset) {
      // Extract specific dataset
      if (!cityConfig.datasets[args.dataset]) {
        throw new Error(`Dataset '${args.dataset}' not found in city '${args.city}' configuration`);
      }
      
      const options: any = {};
      if (sinceDate) {
        options.sinceDate = sinceDate;
      }
      if (args.maxRecords) {
        options.maxRecords = args.maxRecords;
      }
      
      const result = await adapter.extractDataset(args.dataset, options);
      results = { [args.dataset]: result };
    } else {
      // Extract all datasets
      const options: any = {};
      if (sinceDate) {
        options.sinceDate = sinceDate;
      }
      
      results = await adapter.extractAllDatasets(options);
    }
    
    // Skip job postings for production accuracy (avoid mock inflating signals)
    
    // Log results
    let totalRecords = 0;
    for (const [datasetName, result] of Object.entries(results)) {
      logger.info('Dataset extraction completed', {
        dataset: datasetName,
        recordCount: result.recordCount,
        lastWatermark: result.lastWatermark,
      });
      totalRecords += result.recordCount;
    }
    
    logger.info('Data extraction completed successfully', {
      city: args.city,
      totalRecords,
      datasetsProcessed: Object.keys(results).length,
    });
    
    // Close storage
    await storage.close();
    
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
