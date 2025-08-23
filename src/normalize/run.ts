#!/usr/bin/env node

/**
 * Data normalization pipeline runner
 */

import { config } from 'dotenv';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { normalizeAddress, parseCoordinate, validateCoordinates } from '../util/address.js';
import { parseDate, formatDateTimeISO } from '../util/dates.js';
import { categorizeBusinessType, analyzeDescription } from '../util/llm.js';
import { randomUUID } from 'crypto';
import pLimit from 'p-limit';
import type { Storage } from '../storage/index.js';
import { parseCliArgs as parseSharedCliArgs, CLI_CONFIGS } from '../util/cli.js';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  const values = parseSharedCliArgs(CLI_CONFIGS.normalize);
  
  return {
    city: values.city as string,
    dataset: values.dataset as string,
    fast: values.fast as boolean,
    workers: values.workers ? parseInt(values.workers as string, 10) : undefined,
    batchSize: values.batchSize ? parseInt(values.batchSize as string, 10) : undefined,
    resume: values.resume as boolean,
  };
}

/**
 * Check if record is restaurant-related for selective LLM processing
 */
function isRestaurantRecord(rawRecord: any): boolean {
  const businessType = rawRecord.business_type?.toLowerCase() || '';
  const description = rawRecord.description?.toLowerCase() || '';
  const name = rawRecord.business_name?.toLowerCase() || '';
  
  const restaurantKeywords = [
    'restaurant', 'cafe', 'bar', 'grill', 'kitchen', 'diner', 'bistro', 'pub',
    'tavern', 'bakery', 'pizzeria', 'deli', 'sandwich', 'food', 'eatery'
  ];
  
  const combinedText = `${businessType} ${description} ${name}`;
  return restaurantKeywords.some(keyword => combinedText.includes(keyword));
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
  let storage: Storage | null = null;
  try {
    logger.info('Starting data normalization', args);

    // Load city configuration
    const cityConfig = loadCityConfig(args.city);
    
    // Create storage instance
    storage = await createStorage();
    
    // Get raw records to normalize
    const rawRecords = await storage.getRawByCity(args.city, args.dataset);
    
    if (rawRecords.length === 0) {
      logger.info('No raw records found to normalize', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Found ${rawRecords.length} raw records to normalize`);
    
    // Environment configuration
    const LLM_ENABLED = process.env.LLM_ENABLED === 'true';
    
    // Log configuration for debugging
    logger.info('Configuration', {
      fast: args.fast,
      workers: args.workers,
      batchSize: args.batchSize,
      LLM_ENABLED
    });
    // Dynamic configuration based on mode
    const useLLM = !args.fast && LLM_ENABLED;
    const workers = args.workers ?? 2;
    const batchSize = args.batchSize ?? 5000;
    
    // Separate limits for LLM vs non-LLM operations
    const llmLimit = pLimit(useLLM ? 2 : 1); // LLM calls (minimum 1)
    const cpuLimit = pLimit(Math.max(1, workers)); // CPU-bound operations (minimum 1)
    
    let normalizedCount = 0;
    let errors = 0;
    const startTime = Date.now();
    
    // Process records in batches with streaming for large datasets
    const totalRecords = rawRecords.length;
    
    let startOffset = 0;
    
    for (let i = startOffset; i < totalRecords; i += batchSize) {
      const batch = rawRecords.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(totalRecords / batchSize);
      
      logger.info(`Processing batch ${batchNumber}/${totalBatches} (${batch.length} records)`);
      
      const batchStartTime = Date.now();
      
      // Process batch with optimized concurrency
      const results = await Promise.allSettled(
        batch.map(rawRecord => 
          cpuLimit(async () => {
            try {
              const datasetConfig = cityConfig.datasets[rawRecord.dataset];
              if (!datasetConfig) {
                logger.warn(`No configuration found for dataset: ${rawRecord.dataset}`);
                return null;
              }
              
              // Skip LLM processing in fast mode
              let normalized;
              if (useLLM && isRestaurantRecord(rawRecord)) {
                normalized = await llmLimit(async () => 
                  normalizeRecord(rawRecord, cityConfig, datasetConfig)
                );
              } else {
                // Fast mode - skip LLM calls
                normalized = await normalizeRecord(rawRecord, cityConfig, datasetConfig);
              }
              
              // Store normalized record
              if (storage) {
                await storage.insertNormalized(normalized);
              }
              
              // Update progress checkpoint
              if ((i + batch.indexOf(rawRecord)) % 1000 === 0) {
                logger.info(`Progress checkpoint: ${i + batch.indexOf(rawRecord)}/${totalRecords}`);
              }
              
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
    logger.error('Data normalization failed', { 
      error: error instanceof Error ? {
        message: error.message,
        stack: error.stack,
        name: error.name
      } : error,
      args 
    });
    // Best-effort close handled in finally section below
    process.exit(1);
  } finally {
    try { if (storage) { await storage.close(); } } catch {}
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