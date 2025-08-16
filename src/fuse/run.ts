#!/usr/bin/env node

/**
 * Signal fusion pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { applyFusionRules, validateFusionRules } from './rules.js';
import { analyzeDescription } from '../util/llm.js';
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
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run fuse -- --city <city>

Options:
  -c, --city <city>  City name (required)
  -h, --help        Show this help message

Examples:
  npm run fuse -- --city chicago
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  return {
    city: values.city as string,
  };
}

/**
 * Apply fusion rules with LLM enhancement
 */
async function applyFusionRulesWithLLM(
  normalizedRecords: any[],
  city: string
): Promise<any[]> {
  // First apply the existing rules
  const events = applyFusionRules(normalizedRecords, city);
  
  logger.info(`Generated ${events.length} events, enhancing with LLM analysis`);
  
  // Enhance events with LLM analysis using concurrency control
  const limit = pLimit(2); // Max 2 concurrent LLM calls
  const batchSize = 50;
  let enhancedCount = 0;
  const startTime = Date.now();
  
  // Process events in batches
  for (let i = 0; i < events.length; i += batchSize) {
    const batch = events.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(events.length / batchSize);
    
    logger.info(`Enhancing batch ${batchNumber}/${totalBatches} (${batch.length} events)`);
    
    // Process batch with concurrency limit
    const results = await Promise.allSettled(
      batch.map(event => 
        limit(async () => {
          try {
            // Analyze the evidence to get more detailed business insights
            if (event.evidence && event.evidence.length > 0) {
              const primaryEvidence = event.evidence[0];
              if (primaryEvidence && (primaryEvidence.description || primaryEvidence.business_name)) {
                const analysis = await analyzeDescription(
                  primaryEvidence.description || '',
                  primaryEvidence.business_name
                );
                
                // Add enhanced insights to the event (as metadata)
                (event as any).business_insights = {
                  category: analysis.businessType,
                  key_features: analysis.keyFeatures,
                  confidence: analysis.confidence
                };
              }
            }
            return event;
          } catch (error) {
            logger.warn('LLM analysis failed for event, continuing with basic fusion', {
              eventId: event.event_id,
              error: error instanceof Error ? error.message : error,
            });
            return event;
          }
        })
      )
    );
    
    const batchEnhanced = results.filter(r => r.status === 'fulfilled').length;
    enhancedCount += batchEnhanced;
    
    const progress = Math.round(enhancedCount / events.length * 100);
    logger.info(`Batch ${batchNumber} enhanced: ${batchEnhanced}/${batch.length} events (${progress}% total progress)`);
  }
  
  const totalDuration = Date.now() - startTime;
  logger.info('LLM enhancement completed', {
    eventsEnhanced: enhancedCount,
    durationMs: totalDuration,
    durationSeconds: Math.round(totalDuration / 1000),
  });
  
  return events;
}

/**
 * Main fusion function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting signal fusion', args);

    // Validate fusion rules
    if (!validateFusionRules()) {
      throw new Error('Fusion rules validation failed');
    }
    
    // Create storage instance
    const storage = await createStorage();
    
    // Get normalized records to fuse
    const normalizedRecords = await storage.getNormalizedByCity(args.city);
    
    if (normalizedRecords.length === 0) {
      logger.info('No normalized records found to fuse', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Found ${normalizedRecords.length} normalized records to fuse`);
    
    // Apply fusion rules
    const events = await applyFusionRulesWithLLM(normalizedRecords, args.city);
    
    logger.info(`Generated ${events.length} events from fusion rules`);
    
    // Store events
    let storedCount = 0;
    for (const event of events) {
      try {
        await storage.insertEvent(event);
        storedCount++;
        
        if (storedCount % 10 === 0) {
          logger.info(`Stored ${storedCount} events`);
        }
        
      } catch (error) {
        logger.error('Failed to store event', {
          eventId: event.event_id,
          error: error instanceof Error ? error.message : error,
        });
      }
    }
    
    logger.info('Signal fusion completed successfully', {
      city: args.city,
      normalizedRecords: normalizedRecords.length,
      eventsGenerated: events.length,
      eventsStored: storedCount,
    });
    
    // Close storage
    await storage.close();
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Signal fusion failed', { error, args });
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