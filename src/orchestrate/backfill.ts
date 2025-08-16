#!/usr/bin/env node

/**
 * Backfill pipeline orchestration runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { logger } from '../util/logger.js';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { getDaysAgo } from '../util/dates.js';
import { exec } from 'child_process';
import { promisify } from 'util';
import type { BackfillArgs } from '../types.js';

const execAsync = promisify(exec);

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs(): BackfillArgs {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: {
        type: 'string',
        short: 'c',
      },
      days: {
        type: 'string',
        short: 'd',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run backfill -- --city <city> --days <number>

Options:
  -c, --city <city>    City name (required)
  -d, --days <number>  Number of days to backfill (default: 120)
  -h, --help          Show this help message

Examples:
  npm run backfill -- --city chicago --days 120
  npm run backfill -- --city seattle --days 90
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  const days = values.days ? parseInt(values.days, 10) : 120;
  if (isNaN(days) || days <= 0) {
    console.error('Error: --days must be a positive number');
    process.exit(1);
  }

  return {
    city: values.city,
    days,
  };
}

/**
 * Run a pipeline command
 */
async function runCommand(command: string, description: string): Promise<void> {
  logger.info(`Starting: ${description}`);
  
  try {
    const { stdout, stderr } = await execAsync(command, {
      cwd: process.cwd(),
      env: process.env,
    });
    
    if (stdout) {
      logger.debug(`${description} stdout:`, { output: stdout.trim() });
    }
    
    if (stderr) {
      logger.warn(`${description} stderr:`, { output: stderr.trim() });
    }
    
    logger.info(`Completed: ${description}`);
  } catch (error) {
    logger.error(`Failed: ${description}`, { error });
    throw error;
  }
}

/**
 * Main backfill function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting backfill pipeline', args);
    
    // Load city configuration to validate
    const cityConfig = loadCityConfig(args.city);
    logger.info(`Loaded configuration for ${args.city}`, {
      datasets: Object.keys(cityConfig.datasets),
      baseUrl: cityConfig.base_url,
    });
    
    // Calculate since date
    const sinceDate = getDaysAgo(args.days);
    const sinceDateStr = sinceDate.toISOString().split('T')[0]; // YYYY-MM-DD format
    
    logger.info(`Backfilling data since ${sinceDateStr} (${args.days} days ago)`);
    
    const startTime = Date.now();
    
    // 1. Extract historical data
    await runCommand(
      `npx tsx src/extract/run.ts --city ${args.city} --since ${sinceDateStr}`,
      `Extract historical data for ${args.city}`
    );
    
    // 2. Normalize all data
    await runCommand(
      `npx tsx src/normalize/run.ts --city ${args.city}`,
      `Normalize data for ${args.city}`
    );
    
    // 3. Fuse signals
    await runCommand(
      `npx tsx src/fuse/run.ts --city ${args.city}`,
      `Fuse signals for ${args.city}`
    );
    
    // 4. Score leads
    await runCommand(
      `npx tsx src/score/run.ts --city ${args.city}`,
      `Score leads for ${args.city}`
    );
    
    // 5. Export top 12 leads
    await runCommand(
      `npx tsx src/export/run.ts --city ${args.city} --limit 12 --out out/${args.city}-backfill-drop.csv`,
      `Export backfill leads for ${args.city}`
    );
    
    const duration = Date.now() - startTime;
    
    // Get final statistics
    const storage = await createStorage();
    const { getDatabaseStats } = await import('../storage/index.js');
    const stats = await getDatabaseStats(storage);
    await storage.close();
    
    logger.info('Backfill pipeline completed successfully', {
      city: args.city,
      days: args.days,
      sinceDate: sinceDateStr,
      durationMs: duration,
      durationMin: Math.round(duration / 60000),
      finalStats: stats,
    });
    
    // Display summary
    console.log('\n🎉 Backfill Complete!');
    console.log(`City: ${args.city}`);
    console.log(`Period: ${args.days} days (since ${sinceDateStr})`);
    console.log(`Duration: ${Math.round(duration / 60000)} minutes`);
    console.log(`Output: out/${args.city}-backfill-drop.csv`);
    
    if (stats.raw) {
      console.log(`\n📊 Data Summary:`);
      console.log(`Raw records: ${stats.raw || 0}`);
      console.log(`Normalized records: ${stats.normalized || 0}`);
      console.log(`Events generated: ${stats.events || 0}`);
      console.log(`Leads scored: ${stats.leads || 0}`);
    }
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Backfill pipeline failed', { error, args });
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