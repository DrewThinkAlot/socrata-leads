#!/usr/bin/env tsx

/**
 * Data migration script from SQLite to Supabase
 * Migrates existing data while preserving relationships
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../src/storage/index.js';
import { logger } from '../src/util/logger.js';
import { SqliteStorage } from '../src/storage/sqlite.js';
import { PostgresStorage } from '../src/storage/postgres.js';
import { resolve } from 'path';

config();

interface MigrationStats {
  raw: number;
  normalized: number;
  events: number;
  leads: number;
  checkpoints: number;
  groundTruth?: number;
  evaluationResults?: number;
  leadEvaluations?: number;
}

function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      sourceDb: { type: 'string', short: 's' },
      targetUrl: { type: 'string', short: 't' },
      dryRun: { type: 'boolean', short: 'd' },
      batchSize: { type: 'string', short: 'b' },
      help: { type: 'boolean', short: 'h' },
    },
  });

  if (values.help) {
    console.log(`
Usage: tsx scripts/migrate-to-supabase.ts [options]

Options:
  -s, --sourceDb <path>     Source SQLite database path (default: ./data/pipeline.db)
  -t, --targetUrl <url>     Target Supabase connection string (default: from DATABASE_URL)
  -d, --dryRun             Run migration without actually inserting data
  -b, --batchSize <size>   Records per batch (default: 1000)
  -h, --help               Show this help message

Examples:
  # Migrate from default SQLite to Supabase (using DATABASE_URL)
  tsx scripts/migrate-to-supabase.ts

  # Specify custom source database
  tsx scripts/migrate-to-supabase.ts --sourceDb ./backup/pipeline.db

  # Dry run to see what would be migrated
  tsx scripts/migrate-to-supabase.ts --dryRun
    `);
    process.exit(0);
  }

  return {
    sourceDb: values.sourceDb || './data/pipeline.db',
    targetUrl: values.targetUrl || process.env.DATABASE_URL,
    dryRun: values.dryRun || false,
    batchSize: parseInt(values.batchSize as string) || 1000,
  };
}

async function migrateTable<T>(
  tableName: string,
  sourceStorage: SqliteStorage,
  targetStorage: PostgresStorage,
  getSourceData: () => Promise<T[]>,
  insertTargetData: (item: T) => Promise<void>,
  batchSize: number,
  dryRun: boolean
): Promise<number> {
  logger.info(`Starting migration for ${tableName}`);
  
  const sourceData = await getSourceData();
  const totalRecords = sourceData.length;
  
  if (totalRecords === 0) {
    logger.info(`No records found in ${tableName}`);
    return 0;
  }

  logger.info(`Found ${totalRecords} records in ${tableName}`);

  if (dryRun) {
    logger.info(`[DRY RUN] Would migrate ${totalRecords} records from ${tableName}`);
    return totalRecords;
  }

  let migrated = 0;
  for (let i = 0; i < sourceData.length; i += batchSize) {
    const batch = sourceData.slice(i, i + batchSize);
    
    for (const item of batch) {
      try {
        await insertTargetData(item);
        migrated++;
      } catch (error) {
        logger.warn(`Failed to migrate record in ${tableName}`, { 
          error: error instanceof Error ? error.message : error,
          record: item 
        });
      }
    }

    const progress = ((migrated / totalRecords) * 100).toFixed(1);
    logger.info(`${tableName}: ${migrated}/${totalRecords} (${progress}%)`);
  }

  logger.info(`Completed migration for ${tableName}: ${migrated}/${totalRecords} records`);
  return migrated;
}

async function main() {
  const args = parseCliArgs();

  try {
    logger.info('Starting Supabase migration', args);

    if (!args.targetUrl) {
      throw new Error('Target database URL not provided. Set DATABASE_URL or use --targetUrl');
    }

    if (!args.targetUrl.startsWith('postgres://') && !args.targetUrl.startsWith('postgresql://')) {
      throw new Error('Target URL must be a PostgreSQL connection string');
    }

    // Create source (SQLite) storage
    const sourcePath = resolve(process.cwd(), args.sourceDb);
    const sourceStorage = new SqliteStorage(sourcePath);
    
    // Create target (Supabase) storage
    const targetStorage = new PostgresStorage(args.targetUrl);

    // Test connections
    logger.info('Testing database connections...');
    
    try {
      await sourceStorage.getLastCheckpoint('test', 'test');
      logger.info('✓ Source SQLite connection successful');
    } catch (error) {
      throw new Error(`Failed to connect to source SQLite database: ${error}`);
    }

    try {
      if ('testConnection' in targetStorage) {
        const connected = await (targetStorage as any).testConnection();
        if (!connected) {
          throw new Error('Connection test failed');
        }
      }
      logger.info('✓ Target Supabase connection successful');
    } catch (error) {
      throw new Error(`Failed to connect to target Supabase database: ${error}`);
    }

    // Run migrations on target database
    if (!args.dryRun) {
      logger.info('Running database migrations on target...');
      if ('runMigrations' in targetStorage) {
        await (targetStorage as any).runMigrations();
        logger.info('✓ Database migrations completed');
      }
    }

    const stats: MigrationStats = {
      raw: 0,
      normalized: 0,
      events: 0,
      leads: 0,
      checkpoints: 0,
    };

    // Migrate in dependency order
    
    // 1. Raw data (no dependencies)
    stats.raw = await migrateTable(
      'raw',
      sourceStorage,
      targetStorage,
      () => sourceStorage.getRawByCity(''), // Get all cities
      (item) => targetStorage.upsertRaw(item),
      args.batchSize,
      args.dryRun
    );

    // 2. Normalized data (depends on raw)
    stats.normalized = await migrateTable(
      'normalized',
      sourceStorage,
      targetStorage,
      () => sourceStorage.getNormalizedByCity(''), // Get all cities
      (item) => targetStorage.insertNormalized(item),
      args.batchSize,
      args.dryRun
    );

    // 3. Events (independent)
    stats.events = await migrateTable(
      'events',
      sourceStorage,
      targetStorage,
      () => sourceStorage.getEventsByCity(''), // Get all cities
      (item) => targetStorage.insertEvent(item),
      args.batchSize,
      args.dryRun
    );

    // 4. Leads (independent)
    stats.leads = await migrateTable(
      'leads',
      sourceStorage,
      targetStorage,
      () => sourceStorage.getLeadsByCity(''), // Get all cities
      (item) => targetStorage.insertLead(item),
      args.batchSize,
      args.dryRun
    );

    // 5. Checkpoints (independent)
    if (!args.dryRun) {
      // For checkpoints, we need to handle them specially since there's no bulk get method
      logger.info('Migrating checkpoints...');
      // This would require extending the storage interface or using direct SQL
      logger.warn('Checkpoint migration not implemented - you may need to re-run extractions');
    }

    // 6. Evaluation data (if exists)
    try {
      if ('getGroundTruthByPeriod' in sourceStorage) {
        // Migration for evaluation tables would go here
        logger.info('Evaluation data migration not implemented yet');
      }
    } catch (error) {
      logger.info('No evaluation data found to migrate');
    }

    // Close connections
    await sourceStorage.close();
    await targetStorage.close();

    // Summary
    const totalMigrated = stats.raw + stats.normalized + stats.events + stats.leads;
    
    logger.info('Migration completed successfully!', {
      dryRun: args.dryRun,
      stats,
      totalRecords: totalMigrated
    });

    if (args.dryRun) {
      console.log('\n🔍 DRY RUN SUMMARY:');
      console.log(`  Raw records: ${stats.raw}`);
      console.log(`  Normalized records: ${stats.normalized}`);
      console.log(`  Events: ${stats.events}`);
      console.log(`  Leads: ${stats.leads}`);
      console.log(`  Total: ${totalMigrated} records would be migrated`);
      console.log('\nRun without --dryRun to perform actual migration.');
    } else {
      console.log('\n✅ MIGRATION COMPLETE:');
      console.log(`  Raw records: ${stats.raw}`);
      console.log(`  Normalized records: ${stats.normalized}`);
      console.log(`  Events: ${stats.events}`);
      console.log(`  Leads: ${stats.leads}`);
      console.log(`  Total: ${totalMigrated} records migrated`);
      console.log('\nYour pipeline is now ready to use Supabase!');
      console.log('Update your DATABASE_URL to point to Supabase and run your pipeline.');
    }

  } catch (error) {
    logger.error('Migration failed', { error, args });
    process.exit(1);
  }
}

main().catch(console.error);
