#!/usr/bin/env node

/**
 * Daily pipeline orchestration runner
 */

import { config } from 'dotenv';
import { logger } from '../util/logger.js';
import { getAvailableCities } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { loadCityConfig } from '../config/index.js';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

// Load environment variables
config();

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
 * Run full pipeline for a city
 */
async function runCityPipeline(city: string): Promise<void> {
  logger.info(`Starting daily pipeline for city: ${city}`);
  
  const startTime = Date.now();
  
  try {
    // 1. Extract new data
    await runCommand(
      `npx tsx src/extract/run.ts --city ${city}`,
      `Extract data for ${city}`
    );
    
    // 2. Normalize data
    await runCommand(
      `npx tsx src/normalize/run.ts --city ${city}`,
      `Normalize data for ${city}`
    );
    
    // 3. Fuse signals
    await runCommand(
      `npx tsx src/fuse/run.ts --city ${city}`,
      `Fuse signals for ${city}`
    );
    
    // 4. Score leads
    await runCommand(
      `npx tsx src/score/run.ts --city ${city}`,
      `Score leads for ${city}`
    );
    
    // 5. Export top 12 leads
    await runCommand(
      `npx tsx src/export/run.ts --city ${city} --limit 12 --out out/${city}-drop.csv`,
      `Export leads for ${city}`
    );
    
    const duration = Date.now() - startTime;
    logger.info(`Daily pipeline completed for ${city}`, {
      city,
      durationMs: duration,
      durationMin: Math.round(duration / 60000),
    });
    
  } catch (error) {
    logger.error(`Daily pipeline failed for ${city}`, { city, error });
    throw error;
  }
}

/**
 * Main daily orchestration function
 */
async function main() {
  try {
    logger.info('Starting daily pipeline orchestration');
    
    // Get all available cities
    const cities = getAvailableCities();
    
    if (cities.length === 0) {
      logger.warn('No city configurations found');
      process.exit(0);
    }
    
    logger.info(`Found ${cities.length} cities to process: ${cities.join(', ')}`);
    
    const results: Array<{ city: string; success: boolean; error?: any }> = [];
    
    // Process each city
    for (const city of cities) {
      try {
        await runCityPipeline(city);
        results.push({ city, success: true });
      } catch (error) {
        results.push({ city, success: false, error });
      }
    }
    
    // Summary
    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);
    
    logger.info('Daily pipeline orchestration completed', {
      totalCities: cities.length,
      successful: successful.length,
      failed: failed.length,
      successfulCities: successful.map(r => r.city),
      failedCities: failed.map(r => r.city),
    });
    
    if (failed.length > 0) {
      logger.error('Some cities failed processing', {
        failures: failed.map(r => ({ city: r.city, error: r.error?.message })),
      });
      process.exit(1);
    }
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Daily pipeline orchestration failed', { error });
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