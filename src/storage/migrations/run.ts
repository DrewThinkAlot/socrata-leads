#!/usr/bin/env node

/**
 * Database migration runner
 */

import { config } from 'dotenv';
import { createStorage, runMigrations, testConnection } from '../index.js';
import { logger } from '../../util/logger.js';

// Load environment variables
config();

async function main() {
  try {
    logger.info('Starting database migration');
    
    // Create storage instance
    const storage = await createStorage();
    
    // Test connection
    const isConnected = await testConnection(storage);
    if (!isConnected) {
      throw new Error('Database connection failed');
    }
    
    // Run migrations
    await runMigrations(storage);
    
    // Close connection
    await storage.close();
    
    logger.info('Database migration completed successfully');
    process.exit(0);
    
  } catch (error) {
    logger.error('Database migration failed', { error });
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}