#!/usr/bin/env node

/**
 * Optimized Lead scoring pipeline with streaming and chunking
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { getAgeInDays, parseDate } from '../util/dates.js';
import type { Event, Lead, NormalizedRecord } from '../types.js';
import { randomUUID } from 'crypto';
import pLimit from 'p-limit';

config();

function parseCliArgs() {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      city: { type: 'string', short: 'c' },
      chunkSize: { type: 'string', short: 's' },
      workers: { type: 'string', short: 'w' },
      help: { type: 'boolean', short: 'h' },
    },
  });

  if (values.help) {
    console.log(`
Usage: npm run score:optimized -- --city <city>

Options:
  -c, --city <city>        City name (required)
  -s, --chunkSize <size>   Records per chunk (default: 10000)
  -w, --workers <count>    Parallel workers (default: 4)
  -h, --help              Show this help message
    `);
    process.exit(0);
  }

  if (!values.city) {
    console.error('Error: --city is required');
    process.exit(1);
  }

  return {
    city: values.city as string,
    chunkSize: parseInt(values.chunkSize as string) || 10000,
    workers: parseInt(values.workers as string) || 4,
  };
}

/**
 * Stream normalized records in chunks to avoid memory overload
 */
async function* streamNormalizedRecords(storage: any, city: string, chunkSize: number) {
  let offset = 0;
  
  while (true) {
    // Use storage interface instead of direct DB access
    const allRecords = await storage.getNormalizedByCity(city);
    const chunk = allRecords.slice(offset, offset + chunkSize);
    
    if (chunk.length === 0) break;
    
    yield chunk;
    offset += chunkSize;
    
    // Memory cleanup
    if (global.gc) global.gc();
    
    // If we got less than chunkSize, we're done
    if (chunk.length < chunkSize) break;
  }
}

/**
 * Group records by address for lead generation
 */
function groupRecordsByAddress(records: NormalizedRecord[]): Map<string, NormalizedRecord[]> {
  const addressGroups = new Map<string, NormalizedRecord[]>();
  
  for (const record of records) {
    if (!record.address) continue;
    
    const key = record.address.trim().toLowerCase();
    if (!addressGroups.has(key)) {
      addressGroups.set(key, []);
    }
    addressGroups.get(key)!.push(record);
  }
  
  return addressGroups;
}

/**
 * Fast restaurant detection without LLM
 */
function isRestaurantRelated(records: NormalizedRecord[]): boolean {
  const restaurantKeywords = [
    'restaurant', 'cafe', 'bar', 'grill', 'kitchen', 'diner', 'bistro', 'pub',
    'tavern', 'bakery', 'pizzeria', 'deli', 'sandwich', 'food', 'eatery',
    'liquor', 'wine', 'beer', 'alcohol'
  ];
  
  const combinedText = records.map(r => 
    `${r.business_name || ''} ${r.description || ''} ${r.type || ''}`
  ).join(' ').toLowerCase();
  
  return restaurantKeywords.some(keyword => combinedText.includes(keyword));
}

/**
 * Calculate basic lead score without expensive LLM calls
 */
function calculateBasicScore(records: NormalizedRecord[]): number {
  let score = 0;
  
  // Recency scoring (0-40 points)
  const dates = records
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => b.getTime() - a.getTime());
  
  if (dates.length > 0) {
    const mostRecent = dates[0];
    if (!mostRecent) return score;
    const ageInDays = getAgeInDays(mostRecent);
    
    if (ageInDays <= 30) score += 40;
    else if (ageInDays <= 60) score += 30;
    else if (ageInDays <= 90) score += 20;
    else score += 10;
  }
  
  // Signal strength (0-30 points)
  const hasLiquorLicense = records.some(r => 
    r.type?.toLowerCase().includes('liquor') && r.status === 'AAI'
  );
  const hasFoodInspection = records.some(r => 
    r.type?.toLowerCase().includes('food') && r.status === 'PASS'
  );
  const hasBuildingPermit = records.some(r => 
    r.type?.toLowerCase().includes('building')
  );
  
  if (hasLiquorLicense) score += 15;
  if (hasFoodInspection) score += 10;
  if (hasBuildingPermit) score += 5;
  
  // Multi-signal bonus (0-20 points)
  const uniqueTypes = new Set(records.map(r => r.type)).size;
  score += Math.min(uniqueTypes * 3, 20);
  
  // Business complexity (0-10 points)
  const hasComplexPermits = records.some(r => {
    const desc = r.description?.toLowerCase() || '';
    return desc.includes('hood') || desc.includes('fire suppression') || 
           desc.includes('grease trap');
  });
  if (hasComplexPermits) score += 10;
  
  return Math.min(score, 100);
}

/**
 * Create lead from address group
 */
function createBasicLead(address: string, records: NormalizedRecord[], city: string): Lead {
  const primaryRecord = records.sort((a, b) => {
    const dateA = parseDate(a.event_date);
    const dateB = parseDate(b.event_date);
    if (!dateA || !dateB) return 0;
    return dateB.getTime() - dateA.getTime();
  })[0];
  
  if (!primaryRecord) {
    throw new Error('No primary record found for address: ' + address);
  }
  
  const score = calculateBasicScore(records);
  
  return {
    lead_id: randomUUID(),
    city,
    name: primaryRecord.business_name || 'Unknown Business',
    address,
    score,
    evidence: records.map(r => ({
      event_id: r.uid || randomUUID(),
      city: r.city,
      name: r.business_name || '',
      address: r.address || '',
      description: r.description || '',
      signal_strength: 70, // Default strength
      evidence: [r]
    })),
    created_at: new Date().toISOString(),
  } as Lead;
}

/**
 * Main optimized scoring function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting optimized lead scoring', args);
    
    const storage = await createStorage();
    const limit = pLimit(args.workers);
    
    // Get total count for progress tracking
    const allRecords = await storage.getNormalizedByCity(args.city);
    const totalCount = { count: allRecords.length };
    
    logger.info(`Processing ${totalCount.count} records in chunks of ${args.chunkSize}`);
    
    let processedCount = 0;
    let leadsGenerated = 0;
    const leads: Lead[] = [];
    const startTime = Date.now();
    
    // Stream and process chunks
    for await (const chunk of streamNormalizedRecords(storage, args.city, args.chunkSize)) {
      const chunkStartTime = Date.now();
      
      // Group by address
      const addressGroups = groupRecordsByAddress(chunk);
      
      // Process addresses in parallel
      const chunkLeads = await Promise.all(
        Array.from(addressGroups.entries()).map(([address, records]) =>
          limit(async () => {
            // Filter for restaurant-related only
            if (!isRestaurantRelated(records)) return null;
            
            // Basic lead scoring
            return createBasicLead(address, records, args.city);
          })
        )
      );
      
      // Collect valid leads
      const validLeads = chunkLeads.filter((lead): lead is Lead => 
        lead !== null && lead.score >= 30
      );
      
      leads.push(...validLeads);
      leadsGenerated += validLeads.length;
      processedCount += chunk.length;
      
      const chunkDuration = Date.now() - chunkStartTime;
      const progress = ((processedCount / totalCount.count) * 100).toFixed(1);
      const recordsPerSecond = Math.round(chunk.length / (chunkDuration / 1000));
      
      logger.info(`Chunk processed`, {
        processed: processedCount,
        total: totalCount.count,
        progress: `${progress}%`,
        leadsInChunk: validLeads.length,
        totalLeads: leadsGenerated,
        recordsPerSecond,
        chunkDurationMs: chunkDuration
      });
    }
    
    // Sort leads by score
    leads.sort((a, b) => b.score - a.score);
    
    // Store leads
    for (const lead of leads) {
      await storage.insertLead(lead);
    }
    
    const totalDuration = Date.now() - startTime;
    
    logger.info('Optimized scoring completed', {
      totalRecords: processedCount,
      totalLeads: leadsGenerated,
      durationMs: totalDuration,
      durationMinutes: Math.round(totalDuration / 60000),
      recordsPerSecond: Math.round(processedCount / (totalDuration / 1000)),
      avgScore: leads.length > 0 ? Math.round(leads.reduce((sum, l) => sum + l.score, 0) / leads.length) : 0
    });
    
    await storage.close();
    process.exit(0);
    
  } catch (error) {
    logger.error('Optimized scoring failed', { error, args });
    process.exit(1);
  }
}

main().catch(console.error);
