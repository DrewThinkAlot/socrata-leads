/**
 * Shared CLI argument parsing utilities
 */

import { parseArgs } from 'util';

export interface CliOptions {
  [key: string]: any;
}

export interface CliConfig {
  options: any; // Simplified to avoid Node.js version compatibility issues
  help?: string;
  required?: readonly string[];
}

/**
 * Parse CLI arguments with shared configuration
 */
export function parseCliArgs(config: CliConfig): CliOptions {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: config.options,
  }) as { values: CliOptions };

  if (values.help) {
    console.log(config.help || 'No help available');
    process.exit(0);
  }

  // Check required arguments
  if (config.required) {
    for (const req of config.required) {
      if (!values[req]) {
        console.error(`Error: --${req} is required`);
        process.exit(1);
      }
    }
  }

  return values;
}

/**
 * Common CLI argument configurations
 */
export const CLI_CONFIGS = {
  extract: {
    options: {
      city: {
        type: 'string',
        short: 'c',
        description: 'City name (required)',
      },
      dataset: {
        type: 'string',
        short: 'd',
        description: 'Specific dataset to extract (optional)',
      },
      since: {
        type: 'string',
        short: 's',
        description: 'Extract records since this date (optional)',
      },
      limit: {
        type: 'string',
        short: 'l',
        description: 'Maximum number of records to extract (optional)',
      },
      help: {
        type: 'boolean',
        short: 'h',
        description: 'Show help message',
      },
    },
    required: ['city'],
    help: `
Usage: npm run extract -- --city <city> [--dataset <dataset>] [--since <date>] [--limit <limit>]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to extract (optional)
  -s, --since <date>       Extract records since this date (optional)
  -l, --limit <limit>      Maximum number of records to extract (optional)
  -h, --help              Show this help message

Examples:
  npm run extract -- --city chicago
  npm run extract -- --city chicago --dataset building_permits
  npm run extract -- --city chicago --since 2024-01-01
    `,
  },

  normalize: {
    options: {
      city: {
        type: 'string',
        short: 'c',
        description: 'City name (required)',
      },
      dataset: {
        type: 'string',
        short: 'd',
        description: 'Specific dataset to normalize (optional)',
      },
      fast: {
        type: 'boolean',
        short: 'f',
        description: 'Skip LLM processing for non-restaurant records',
      },
      workers: {
        type: 'string',
        short: 'w',
        description: 'Number of worker processes (default: 2)',
      },
      batchSize: {
        type: 'string',
        short: 'b',
        description: 'Batch size for processing (default: 5000)',
      },
      resume: {
        type: 'boolean',
        short: 'r',
        description: 'Resume from last checkpoint',
      },
      help: {
        type: 'boolean',
        short: 'h',
        description: 'Show help message',
      },
    },
    required: ['city'],
    help: `
Usage: npm run normalize -- --city <city> [--dataset <dataset>] [--fast] [--workers <n>] [--batch-size <n>] [--resume]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to normalize (optional)
  -f, --fast              Skip LLM for non-restaurant records
  -w, --workers <n>       Number of worker processes (default: 2)
  -b, --batch-size <n>    Batch size for processing (default: 5000)
  -r, --resume            Resume from last checkpoint
  -h, --help              Show help message

Examples:
  npm run normalize -- --city chicago
  npm run normalize -- --city chicago --fast --workers 4
    `,
  },

  score: {
    options: {
      city: {
        type: 'string',
        short: 'c',
        description: 'City name (required)',
      },
      help: {
        type: 'boolean',
        short: 'h',
        description: 'Show help message',
      },
    },
    required: ['city'],
    help: `
Usage: npm run score -- --city <city>

Options:
  -c, --city <city>  City name (required)
  -h, --help        Show this help message

Examples:
  npm run score -- --city chicago
    `,
  },

  fuse: {
    options: {
      city: {
        type: 'string',
        short: 'c',
        description: 'City name (required)',
      },
      help: {
        type: 'boolean',
        short: 'h',
        description: 'Show help message',
      },
    },
    required: ['city'],
    help: `
Usage: npm run fuse -- --city <city>

Options:
  -c, --city <city>  City name (required)
  -h, --help        Show this help message

Examples:
  npm run fuse -- --city chicago
    `,
  },

  export: {
    options: {
      city: {
        type: 'string',
        short: 'c',
        description: 'City name (required)',
      },
      format: {
        type: 'string',
        short: 'f',
        description: 'Export format (csv|json|xlsx)',
      },
      limit: {
        type: 'string',
        short: 'l',
        description: 'Maximum records to export',
      },
      help: {
        type: 'boolean',
        short: 'h',
        description: 'Show help message',
      },
    },
    required: ['city'],
    help: `
Usage: npm run export -- --city <city> [--format <format>] [--limit <limit>]

Options:
  -c, --city <city>    City name (required)
  -f, --format <format> Export format: csv, json, xlsx (default: csv)
  -l, --limit <limit>  Maximum records to export
  -h, --help          Show help message

Examples:
  npm run export -- --city chicago
  npm run export -- --city chicago --format json --limit 1000
    `,
  },
} as const;
