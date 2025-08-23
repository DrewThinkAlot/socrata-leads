#!/usr/bin/env tsx

/**
 * Custom migration script using MCP server for Supabase data migration
 * Handles large datasets by batching and using direct SQL execution
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { SqliteStorage } from '../src/storage/sqlite.js';
import { logger } from '../src/util/logger.js';
import { resolve } from 'path';

config();

interface MigrationProgress {
  table: string;
  migrated: number;
  total: number;
  batchSize: number;
  completed: boolean;
}

function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      sourceDb: { type: 'string', short: 's' },
      batchSize: { type: 'string', short: 'b' },
      table: { type: 'string', short: 't' },
      help: { type: 'boolean', short: 'h' },
    },
  });

  if (values.help) {
    console.log(`
Usage: tsx scripts/migrate-via-mcp.ts [options]

Options:
  -s, --sourceDb <path>     Source SQLite database path (default: ./data/pipeline.db)
  -b, --batchSize <size>   Records per batch (default: 1000)
  -t, --table <name>       Migrate specific table only (raw, normalized, events, leads)
  -h, --help               Show this help message

This script exports SQLite data to CSV files that can be imported into Supabase.
    `);
    process.exit(0);
  }

  return {
    sourceDb: values.sourceDb || './data/pipeline.db',
    batchSize: parseInt(values.batchSize || '1000'),
    table: values.table,
  };
}

async function exportTableToCSV(storage: SqliteStorage, tableName: string, batchSize: number): Promise<void> {
  logger.info(`Starting export for table: ${tableName}`);
  
  // Get total count
  const countQuery = `SELECT COUNT(*) as count FROM ${tableName}`;
  const countResult = await (storage as any).db.get(countQuery);
  const totalRecords = countResult.count;
  
  if (totalRecords === 0) {
    logger.info(`Table ${tableName} is empty, skipping`);
    return;
  }
  
  logger.info(`Exporting ${totalRecords} records from ${tableName} in batches of ${batchSize}`);
  
  // Export in batches to CSV files
  let offset = 0;
  let batchNum = 0;
  
  while (offset < totalRecords) {
    const selectQuery = `SELECT * FROM ${tableName} LIMIT ${batchSize} OFFSET ${offset}`;
    const records = await (storage as any).db.all(selectQuery);
    
    if (records.length === 0) break;
    
    // Write batch to CSV file
    const csvFile = `./data/migration_${tableName}_batch_${batchNum}.csv`;
    const csvContent = convertToCSV(records);
    
    const fs = await import('fs');
    fs.writeFileSync(csvFile, csvContent);
    
    batchNum++;
    offset += batchSize;
    
    const progress = Math.round((offset / totalRecords) * 100);
    logger.info(`${tableName}: ${offset}/${totalRecords} (${progress}%) - Batch ${batchNum} exported to ${csvFile}`);
  }
  
  logger.info(`✓ Export completed for ${tableName}: ${batchNum} batch files created`);
}

function convertToCSV(records: any[]): string {
  if (records.length === 0) return '';
  
  const headers = Object.keys(records[0]);
  const csvRows = [headers.join(',')];
  
  for (const record of records) {
    const values = headers.map(header => {
      const value = record[header];
      if (value === null || value === undefined) return '';
      if (typeof value === 'string') {
        // Escape quotes and wrap in quotes if contains comma or quote
        const escaped = value.replace(/"/g, '""');
        return escaped.includes(',') || escaped.includes('"') || escaped.includes('\n') 
          ? `"${escaped}"` 
          : escaped;
      }
      return String(value);
    });
    csvRows.push(values.join(','));
  }
  
  return csvRows.join('\n');
}

async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting SQLite to CSV export for Supabase migration', args);
    
    const sourceDbPath = resolve(process.cwd(), args.sourceDb);
    const storage = new SqliteStorage(sourceDbPath);
    
    // Tables to migrate in order (dependencies first)
    const tables = args.table ? [args.table] : ['raw', 'normalized', 'events', 'leads'];
    
    for (const table of tables) {
      await exportTableToCSV(storage, table, args.batchSize);
    }
    
    await storage.close();
    
    console.log('\n🎉 CSV EXPORT COMPLETED!');
    console.log('\nNext steps:');
    console.log('1. Use Supabase dashboard to import the CSV files');
    console.log('2. Or use the generated CSV files with COPY commands');
    console.log('3. Verify data integrity after import');
    
  } catch (error) {
    logger.error('CSV export failed', { error, args });
    console.log('\n❌ EXPORT FAILED');
    process.exit(1);
  }
}

main().catch(console.error);
