#!/usr/bin/env node

/**
 * Main entry point for the socrata-leads pipeline
 */

import { config } from 'dotenv';
import { logger } from './util/logger.js';
import { initializeLLM } from './util/llm.js';

// Load environment variables
config();

async function main() {
  logger.info('Starting socrata-leads pipeline');
  
  // Initialize LLM utilities
  initializeLLM();
  
  // This is the main entry point - for now just log
  // Individual commands are handled by their respective run.ts files
  logger.info('Use specific commands like npm run extract, npm run daily, etc.');
  
  process.exit(0);
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
  main().catch((error) => {
    logger.error('Main process error:', { error: error.message, stack: error.stack });
    process.exit(1);
  });
}