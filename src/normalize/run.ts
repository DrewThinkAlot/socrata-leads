#!/usr/bin/env node

/**
 * Data normalization pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { normalizeAddress, parseCoordinate, validateCoordinates } from '../util/address.js';
import { parseDate, formatDateTimeISO } from '../util/dates.js';
import { categorizeBusinessType, analyzeDescription } from '../util/llm.js';
import { randomUUID } from 'crypto';
import pLimit from 'p-limit';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: {
        type: 'string',
        short: 'c',
      },
      dataset: {
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
Usage: npm run normalize -- --city <city> [--dataset <dataset>]

Options:
  -c, --city <city>        City name (required)
  -d, --dataset <dataset>  Specific dataset to normalize (optional)
  -h, --help              Show this help message

Examples:
  npm run normalize -- --city chicago
  npm run normalize -- --city chicago --dataset building_permits
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  return {
    city: values.city as string,
    dataset: values.dataset as string | undefined,
  };
}

/**
 * Normalize a single raw record to canonical schema
 */
async function normalizeRecord(
  rawRecord: any,
  cityConfig: any,
  datasetConfig: any
): Promise<any> {
  const payload = rawRecord.payload;
  const mapping = datasetConfig.map;
  
  // Apply field mappings
  const normalized: any = {
    uid: randomUUID(),
    city: rawRecord.city,
    dataset: rawRecord.dataset,
    raw_id: rawRecord.id,
  };

  // Map each canonical field
  for (const [canonicalField, expression] of Object.entries(mapping)) {
    let value = null;
    
    if (typeof expression === 'string') {
      if (expression === ':self') {
        // Special case for source links
        value = `${cityConfig.base_url}/resource/${datasetConfig.id}/${payload[':id'] || rawRecord.id}`;
      } else if (expression.startsWith('CONCAT(')) {
        // Handle CONCAT expressions
        value = evaluateConcat(expression, payload);
      } else if (expression.startsWith('COALESCE(')) {
        // Handle COALESCE expressions
        value = evaluateCoalesce(expression, payload);
      } else {
        // Direct field mapping
        value = payload[expression];
      }
    }
    
    // Apply field-specific transformations
    switch (canonicalField) {
      case 'address':
        normalized.address = normalizeAddress(value);
        break;
      case 'lat':
        const lat = parseCoordinate(value);
        const coords = validateCoordinates(lat, undefined);
        normalized.lat = coords.lat;
        break;
      case 'lon':
        const lon = parseCoordinate(value);
        const coords2 = validateCoordinates(undefined, lon);
        normalized.lon = coords2.lon;
        break;
      case 'event_date':
        const date = parseDate(value);
        normalized.event_date = date ? formatDateTimeISO(date) : null;
        break;
      default:
        normalized[canonicalField] = value;
    }
  }
  
  // Apply LLM-enhanced processing if we have business-related fields
  if (normalized.business_name || normalized.description || normalized.type) {
    try {
      // Enhanced business categorization
      const businessType = await categorizeBusinessType(
        normalized.description || '',
        normalized.business_name
      );
      
      // If we don't have a type field, use the LLM-generated category
      if (!normalized.type) {
        normalized.type = businessType.category;
      }
      
      // Enhanced description analysis
      if (normalized.description) {
        const analysis = await analyzeDescription(
          normalized.description,
          normalized.business_name
        );
        
        // Add analysis results as additional fields
        normalized.business_category = analysis.businessType;
        normalized.confidence_score = analysis.confidence;
      }
    } catch (error) {
      logger.warn('LLM processing failed for record, continuing with basic normalization', {
        recordId: rawRecord.id,
        error: error instanceof Error ? error.message : error,
      });
    }
  }
  
  return normalized;
}

/**
 * Evaluate CONCAT expression
 */
function evaluateConcat(expression: string, payload: any): string {
  // Simple CONCAT parser - handles CONCAT(field1,' ',field2)
  const match = expression.match(/CONCAT\(([^)]+)\)/);
  if (!match) return '';
  
  const parts = match[1]!.split(',').map(part => part.trim());
  const values = parts.map(part => {
    if (part.startsWith("'") && part.endsWith("'")) {
      // String literal
      return part.slice(1, -1);
    } else if (part.startsWith('COALESCE(')) {
      // Nested COALESCE
      return evaluateCoalesce(part, payload) || '';
    } else {
      // Field reference
      return payload[part] || '';
    }
  });
  
  return values.join('');
}

/**
 * Evaluate COALESCE expression
 */
function evaluateCoalesce(expression: string, payload: any): string | null {
  // Simple COALESCE parser - handles COALESCE(field1, field2)
  const match = expression.match(/COALESCE\(([^)]+)\)/);
  if (!match) return null;
  
  const fields = match[1]!.split(',').map(f => f.trim());
  for (const field of fields) {
    const value = payload[field];
    if (value !== null && value !== undefined && value !== '') {
      return value;
    }
  }
  
  return null;
}

/**
 * Main normalization function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting data normalization', args);

    // Load city configuration
    const cityConfig = loadCityConfig(args.city);
    
    // Create storage instance
    const storage = await createStorage();
    
    // Get raw records to normalize
    const rawRecords = await storage.getRawByCity(args.city, args.dataset);
    
    if (rawRecords.length === 0) {
      logger.info('No raw records found to normalize', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Found ${rawRecords.length} raw records to normalize`);
    
    // Limit concurrency to prevent API overload
    const limit = pLimit(2); // Max 2 concurrent LLM calls to avoid rate limits
    const batchSize = 100;
    let normalizedCount = 0;
    let errors = 0;
    const startTime = Date.now();
    
    // Process records in batches
    for (let i = 0; i < rawRecords.length; i += batchSize) {
      const batch = rawRecords.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(rawRecords.length / batchSize);
      
      logger.info(`Processing batch ${batchNumber}/${totalBatches} (${batch.length} records)`);
      
      const batchStartTime = Date.now();
      
      // Process batch with concurrency limit
      const results = await Promise.allSettled(
        batch.map(rawRecord => 
          limit(async () => {
            try {
              const datasetConfig = cityConfig.datasets[rawRecord.dataset];
              if (!datasetConfig) {
                logger.warn(`No configuration found for dataset: ${rawRecord.dataset}`);
                return null;
              }
              
              // Normalize the record
              const normalized = await normalizeRecord(rawRecord, cityConfig, datasetConfig);
              
              // Store normalized record
              await storage.insertNormalized(normalized);
              return normalized;
            } catch (error) {
              logger.error('Failed to normalize record', {
                rawId: rawRecord.id,
                error: error instanceof Error ? error.message : error,
              });
              throw error;
            }
          })
        )
      );
      
      // Count results
      const batchNormalized = results.filter(r => r.status === 'fulfilled' && r.value).length;
      const batchErrors = results.filter(r => r.status === 'rejected').length;
      
      normalizedCount += batchNormalized;
      errors += batchErrors;
      
      const batchDuration = Date.now() - batchStartTime;
      const totalDuration = Date.now() - startTime;
      const recordsPerSecond = Math.round(normalizedCount / (totalDuration / 1000));
      const eta = normalizedCount > 0 ? 
        Math.round(((rawRecords.length - normalizedCount) / recordsPerSecond) / 60) : 
        'unknown';
      
      logger.info(`Batch ${batchNumber} completed`, {
        batchNormalized,
        batchErrors,
        batchDurationMs: batchDuration,
        totalNormalized: normalizedCount,
        totalErrors: errors,
        progress: `${normalizedCount}/${rawRecords.length} (${Math.round(normalizedCount / rawRecords.length * 100)}%)`,
        recordsPerSecond,
        etaMinutes: eta,
      });
    }
    
    const totalDuration = Date.now() - startTime;
    
    logger.info('Data normalization completed successfully', {
      city: args.city,
      totalRecords: rawRecords.length,
      normalizedCount,
      errors,
      durationMs: totalDuration,
      durationMinutes: Math.round(totalDuration / 60000),
      recordsPerSecond: Math.round(normalizedCount / (totalDuration / 1000)),
    });
    
    // Close storage
    await storage.close();
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Data normalization failed', { error, args });
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