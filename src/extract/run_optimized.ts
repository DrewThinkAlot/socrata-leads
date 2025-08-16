#!/usr/bin/env node

/**
 * Optimized data extraction pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createOptimizedSocrataAdapter } from '../adapters/socrata_optimized.js';
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
      parallel: {
        type: 'string',
        short: 'p',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run extract:optimized -- --city <city> [options]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to extract (optional)
  -s, --since <date>       Extract records since this date (optional)
  -p, --parallel <number>  Max concurrent requests (default: 5)
  -h, --help              Show this help message

Examples:
  npm run extract:optimized -- --city chicago
  npm run extract:optimized -- --city chicago --dataset building_permits
  npm run extract:optimized -- --city chicago --since 2024-01-01 --parallel 10
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
 * Main optimized extraction function
 */
async function main() {
  const args = parseCliArgs();
  const startTime = Date.now();
  
  try {
    logger.info('Starting optimized data extraction', args);

    // Load city configuration
    const cityConfig = loadCityConfig(args.city);
    
    // Create storage instance
    const storage = await createStorage();
    
    // Create optimized Socrata adapter
    const adapter = createOptimizedSocrataAdapter(cityConfig, storage);
    
    // Optimize database for bulk operations if using SQLite
    if ('optimizeForBulkOperations' in storage) {
      await (storage as any).optimizeForBulkOperations();
    }
    
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
      // Extract all datasets in parallel
      const options: any = {};
      if (sinceDate) {
        options.sinceDate = sinceDate;
      }
      
      results = await adapter.extractAllDatasets(options);
    }
    
    // Calculate performance metrics
    const endTime = Date.now();
    const durationSeconds = (endTime - startTime) / 1000;
    
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
    
    logger.info('Optimized data extraction completed successfully', {
      city: args.city,
      totalRecords,
      datasetsProcessed: Object.keys(results).length,
      durationSeconds,
      recordsPerSecond: Math.round(totalRecords / durationSeconds),
    });
    
    // Close storage
    await storage.close();
    
    process.exit(0);
    
  } catch (error) {
    if (error instanceof Error) {
      logger.error('Optimized data extraction failed', {
        error: error.message,
        stack: error.stack,
        name: error.constructor.name,
        args
      });
    } else {
      logger.error('Optimized data extraction failed', { error, args });
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
