#!/usr/bin/env node

/**
 * Evaluation pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { GroundTruthCollector } from './ground-truth.js';
import { EvaluationMetrics } from './metrics.js';
import { logger } from '../util/logger.js';
import { subDays, subMonths } from 'date-fns';
import type { CityConfig } from '../types.js';

// Load environment variables
config();

interface EvaluationArgs {
  city: string;
  mode: 'ground-truth' | 'evaluate' | 'full';
  startDate?: string;
  endDate?: string;
  days?: number;
  months?: number;
  restaurantOnly?: boolean;
  help?: boolean;
}

/**
 * Parse command line arguments
 */
function parseCliArgs(): EvaluationArgs {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: {
        type: 'string',
        short: 'c',
      },
      mode: {
        type: 'string',
        short: 'm',
      },
      'start-date': {
        type: 'string',
        short: 's',
      },
      'end-date': {
        type: 'string',
        short: 'e',
      },
      days: {
        type: 'string',
        short: 'd',
      },
      months: {
        type: 'string',
      },
      'restaurant-only': {
        type: 'boolean',
        short: 'r',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run evaluate -- --city <city> --mode <mode> [options]

Modes:
  ground-truth    Collect ground truth data from city licenses/inspections
  evaluate        Run evaluation metrics on existing data
  full            Run both ground truth collection and evaluation

Options:
  -c, --city <city>           City name (required)
  -m, --mode <mode>           Evaluation mode (required)
  -s, --start-date <date>     Start date (YYYY-MM-DD)
  -e, --end-date <date>       End date (YYYY-MM-DD)
  -d, --days <number>         Look back N days from today
  --months <number>           Look back N months from today
  -r, --restaurant-only       Filter to restaurant-related licenses only
  -h, --help                  Show this help message

Examples:
  npm run evaluate -- --city chicago --mode ground-truth --months 6 --restaurant-only
  npm run evaluate -- --city chicago --mode evaluate --start-date 2024-01-01 --end-date 2024-06-30
  npm run evaluate -- --city chicago --mode full --days 90
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  if (!values.mode || !['ground-truth', 'evaluate', 'full'].includes(values.mode as string)) {
    console.error('Error: --mode must be one of: ground-truth, evaluate, full');
    process.exit(1);
  }

  const result: EvaluationArgs = {
    city: values.city as string,
    mode: values.mode as 'ground-truth' | 'evaluate' | 'full',
  };

  if (values['start-date']) {
    result.startDate = values['start-date'] as string;
  }

  if (values['end-date']) {
    result.endDate = values['end-date'] as string;
  }

  if (values.days) {
    result.days = parseInt(values.days as string, 10);
  }

  if (values.months) {
    result.months = parseInt(values.months as string, 10);
  }

  if (values['restaurant-only']) {
    result.restaurantOnly = true;
  }

  return result;
}

/**
 * Calculate date range from arguments
 */
function calculateDateRange(args: EvaluationArgs): { startDate: Date; endDate: Date } {
  const now = new Date();
  let startDate: Date;
  let endDate: Date;

  if (args.startDate && args.endDate) {
    startDate = new Date(args.startDate);
    endDate = new Date(args.endDate);
  } else if (args.days) {
    startDate = subDays(now, args.days);
    endDate = now;
  } else if (args.months) {
    startDate = subMonths(now, args.months);
    endDate = now;
  } else {
    // Default to last 6 months
    startDate = subMonths(now, 6);
    endDate = now;
  }

  return { startDate, endDate };
}

/**
 * Run ground truth collection
 */
async function runGroundTruthCollection(args: EvaluationArgs): Promise<void> {
  const storage = await createStorage();
  const cityConfig = loadCityConfig(args.city) as CityConfig;
  const { startDate, endDate } = calculateDateRange(args);

  const collector = new GroundTruthCollector(cityConfig, storage);

  try {
    logger.info('Starting ground truth collection', {
      city: args.city,
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString(),
      restaurantOnly: args.restaurantOnly
    });

    const collectOptions: {
      startDate: Date;
      endDate: Date;
      restaurantOnly?: boolean;
    } = { startDate, endDate };
    if (args.restaurantOnly === true) {
      collectOptions.restaurantOnly = true;
    }
    const records = await collector.collectGroundTruth(collectOptions);

    logger.info('Ground truth collection completed', {
      city: args.city,
      recordsCollected: records.length
    });

  } finally {
    await storage.close();
  }
}

/**
 * Run evaluation metrics
 */
async function runEvaluation(args: EvaluationArgs): Promise<void> {
  const storage = await createStorage();
  const { startDate, endDate } = calculateDateRange(args);

  const metrics = new EvaluationMetrics(storage);

  try {
    logger.info('Starting evaluation', {
      city: args.city,
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString()
    });

    const result = await metrics.evaluatePeriod({
      city: args.city,
      periodStart: startDate,
      periodEnd: endDate
    });

    // Log key metrics
    logger.info('Evaluation completed', {
      city: args.city,
      evaluationId: result.evaluation_id,
      metrics: {
        totalGroundTruth: result.total_ground_truth,
        totalPredictions: result.total_predictions,
        precisionAt50: result.precision_at_50,
        precisionAt100: result.precision_at_100,
        recall: result.recall,
        medianLeadTimeDays: result.median_lead_time_days,
        costPerVerifiedLead: result.cost_per_verified_lead
      }
    });

    // Print summary to console
    console.log('\n=== EVALUATION RESULTS ===');
    console.log(`City: ${result.city}`);
    console.log(`Period: ${result.period_start} to ${result.period_end}`);
    console.log(`Total Ground Truth: ${result.total_ground_truth}`);
    console.log(`Total Predictions: ${result.total_predictions}`);
    console.log(`Precision @ 50: ${(result.precision_at_50 * 100).toFixed(1)}%`);
    console.log(`Precision @ 100: ${(result.precision_at_100 * 100).toFixed(1)}%`);
    console.log(`Recall: ${(result.recall * 100).toFixed(1)}%`);
    console.log(`Median Lead Time: ${result.median_lead_time_days} days`);
    console.log(`Cost per Verified Lead: ${result.cost_per_verified_lead.toFixed(2)}`);

  } finally {
    await storage.close();
  }
}

/**
 * Run signal ablation analysis
 */
async function runSignalAblation(args: EvaluationArgs): Promise<void> {
  const storage = await createStorage();
  const { startDate, endDate } = calculateDateRange(args);

  const metrics = new EvaluationMetrics(storage);

  try {
    logger.info('Starting signal ablation analysis', {
      city: args.city,
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString()
    });

    const signalTypes = ['building_permits', 'business_licenses', 'food_inspections', 'zoning'];
    const results = await metrics.runSignalAblation({
      city: args.city,
      periodStart: startDate,
      periodEnd: endDate,
      signalTypes
    });

    console.log('\n=== SIGNAL ABLATION RESULTS ===');
    for (const result of results) {
      console.log(`Signal: ${result.signal_type}`);
      console.log(`  Precision Impact: ${result.precision_impact.toFixed(3)}`);
      console.log(`  Recall Impact: ${result.recall_impact.toFixed(3)}`);
      console.log(`  Lead Time Impact: ${result.lead_time_impact.toFixed(1)} days`);
    }

  } finally {
    await storage.close();
  }
}

/**
 * Main function
 */
async function main() {
  const args = parseCliArgs();

  try {
    switch (args.mode) {
      case 'ground-truth':
        await runGroundTruthCollection(args);
        break;

      case 'evaluate':
        await runEvaluation(args);
        break;

      case 'full':
        await runGroundTruthCollection(args);
        await runEvaluation(args);
        await runSignalAblation(args);
        break;
    }

    logger.info('Evaluation pipeline completed successfully', {
      city: args.city,
      mode: args.mode
    });

    process.exit(0);

  } catch (error) {
    if (error instanceof Error) {
      logger.error('Evaluation pipeline failed', {
        error: error.message,
        stack: error.stack,
        name: error.constructor.name,
        args
      });
    } else {
      logger.error('Evaluation pipeline failed', { error, args });
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
