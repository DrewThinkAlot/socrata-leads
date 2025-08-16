#!/usr/bin/env tsx

/**
 * Test Supabase connection and basic operations
 * Validates that your Supabase setup is working correctly
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../src/storage/index.js';
import { logger } from '../src/util/logger.js';
import { randomUUID } from 'crypto';

config();

function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      help: { type: 'boolean', short: 'h' },
    },
  });

  if (values.help) {
    console.log(`
Usage: tsx scripts/test-supabase-connection.ts

This script tests your Supabase connection and performs basic operations
to ensure your migration setup is working correctly.

Environment variables required:
  DATABASE_URL - Your Supabase PostgreSQL connection string
    `);
    process.exit(0);
  }

  return {};
}

async function main() {
  const args = parseCliArgs();

  try {
    logger.info('Testing Supabase connection and operations');

    const databaseUrl = process.env.DATABASE_URL;
    if (!databaseUrl) {
      throw new Error('DATABASE_URL environment variable not set');
    }

    if (!databaseUrl.startsWith('postgres://') && !databaseUrl.startsWith('postgresql://')) {
      throw new Error('DATABASE_URL must be a PostgreSQL connection string for Supabase');
    }

    logger.info('Creating storage instance...');
    const storage = await createStorage();

    // Test 1: Basic connection
    logger.info('Test 1: Testing basic connection...');
    if ('testConnection' in storage && typeof storage.testConnection === 'function') {
      const connected = await (storage as any).testConnection();
      if (!connected) {
        throw new Error('Connection test failed');
      }
      logger.info('✓ Connection test passed');
    } else {
      // Fallback test
      await storage.getLastCheckpoint('test', 'test');
      logger.info('✓ Basic connection working');
    }

    // Test 2: Database migrations
    logger.info('Test 2: Running database migrations...');
    if ('runMigrations' in storage && typeof storage.runMigrations === 'function') {
      await (storage as any).runMigrations();
      logger.info('✓ Database migrations completed');
    } else {
      logger.warn('Storage implementation does not support migrations');
    }

    // Test 3: Insert and retrieve test data
    logger.info('Test 3: Testing data operations...');
    
    const testId = randomUUID();
    const testRaw = {
      id: testId,
      city: 'test-city',
      dataset: 'test-dataset',
      watermark: new Date().toISOString(),
      payload: { test: true, timestamp: Date.now() }
    };

    // Insert raw record
    await storage.upsertRaw(testRaw);
    logger.info('✓ Raw record inserted');

    // Retrieve raw records
    const rawRecords = await storage.getRawByCity('test-city');
    const foundRaw = rawRecords.find(r => r.id === testId);
    if (!foundRaw) {
      throw new Error('Failed to retrieve inserted raw record');
    }
    logger.info('✓ Raw record retrieved');

    // Test normalized record
    const testNormalized = {
      uid: randomUUID(),
      city: 'test-city',
      dataset: 'test-dataset',
      business_name: 'Test Business',
      address: '123 Test St',
      lat: 41.8781,
      lon: -87.6298,
      status: 'ACTIVE',
      event_date: new Date().toISOString(),
      type: 'test-permit',
      description: 'Test permit description',
      source_link: 'https://test.example.com',
      raw_id: testId
    };

    await storage.insertNormalized(testNormalized);
    logger.info('✓ Normalized record inserted');

    const normalizedRecords = await storage.getNormalizedByCity('test-city');
    const foundNormalized = normalizedRecords.find(r => r.uid === testNormalized.uid);
    if (!foundNormalized) {
      throw new Error('Failed to retrieve inserted normalized record');
    }
    logger.info('✓ Normalized record retrieved');

    // Test checkpoint operations
    logger.info('Test 4: Testing checkpoint operations...');
    const testWatermark = new Date().toISOString();
    await storage.setCheckpoint('test-city', 'test-dataset', testWatermark);
    logger.info('✓ Checkpoint set');

    const retrievedWatermark = await storage.getLastCheckpoint('test-city', 'test-dataset');
    if (retrievedWatermark !== testWatermark) {
      throw new Error(`Checkpoint mismatch: expected ${testWatermark}, got ${retrievedWatermark}`);
    }
    logger.info('✓ Checkpoint retrieved');

    // Test 5: Performance check with batch operations
    logger.info('Test 5: Testing batch performance...');
    const startTime = Date.now();
    
    const batchSize = 100;
    const batchPromises: Promise<void>[] = [];
    
    for (let i = 0; i < batchSize; i++) {
      const batchRecord = {
        uid: randomUUID(),
        city: 'test-city-batch',
        dataset: 'test-dataset-batch',
        business_name: `Test Business ${i}`,
        address: `${i} Test St`,
        raw_id: testId
      };
      batchPromises.push(storage.insertNormalized(batchRecord));
    }

    await Promise.all(batchPromises);
    const duration = Date.now() - startTime;
    const recordsPerSecond = Math.round((batchSize / duration) * 1000);
    
    logger.info(`✓ Batch insert completed: ${batchSize} records in ${duration}ms (${recordsPerSecond} records/sec)`);

    // Test 6: Database statistics
    if ('getStats' in storage && typeof storage.getStats === 'function') {
      logger.info('Test 6: Getting database statistics...');
      const stats = await (storage as any).getStats();
      logger.info('✓ Database statistics:', stats);
    }

    // Cleanup test data
    logger.info('Cleaning up test data...');
    // Note: We don't have delete methods in the interface, so test data will remain
    // This is fine for testing purposes

    await storage.close();

    console.log('\n🎉 ALL TESTS PASSED!');
    console.log('\nYour Supabase setup is working correctly.');
    console.log('You can now run your pipeline with confidence.');
    console.log('\nNext steps:');
    console.log('1. Run data migration: npm run migrate:data');
    console.log('2. Start using your pipeline: npm run extract -- --city chicago');

  } catch (error) {
    logger.error('Supabase connection test failed', { error, args });
    console.log('\n❌ TEST FAILED');
    console.log('\nTroubleshooting:');
    console.log('1. Check your DATABASE_URL is correct');
    console.log('2. Verify your Supabase project is active');
    console.log('3. Ensure your database password is correct');
    console.log('4. Check network connectivity to Supabase');
    process.exit(1);
  }
}

main().catch(console.error);
