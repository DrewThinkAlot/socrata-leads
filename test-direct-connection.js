#!/usr/bin/env node

/**
 * Test direct connection to Supabase using our application's storage layer
 */

import { createStorage, testConnection } from './src/storage/index.js';
import { logger } from './src/util/logger.js';

async function testDirectConnection() {
  try {
    logger.info('Testing direct Supabase connection...');
    
    // Create storage instance (should use PostgresStorage based on DATABASE_URL)
    const storage = await createStorage();
    logger.info('✅ Storage instance created successfully');
    
    // Test connection
    const isConnected = await testConnection(storage);
    if (!isConnected) {
      throw new Error('Connection test failed');
    }
    logger.info('✅ Connection test passed');
    
    // Test a simple query - get checkpoints
    const checkpoints = await storage.getLastCheckpoint('chicago', 'business_licenses');
    logger.info('✅ Successfully queried checkpoints', { checkpoints });
    
    // Test record count
    if (storage.getRawRecordCount) {
      const count = await storage.getRawRecordCount();
      logger.info('✅ Raw records in database:', { count });
    }
    
    logger.info('🎉 All tests passed! Pipeline can run directly against Supabase');
    
  } catch (error) {
    logger.error('❌ Connection test failed:', { error: error.message });
    process.exit(1);
  }
}

testDirectConnection();
