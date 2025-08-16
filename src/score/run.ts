#!/usr/bin/env node

/**
 * Lead scoring pipeline runner
 */

import { config } from 'dotenv';
import { parseArgs } from 'util';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { getAgeInDays, parseDate } from '../util/dates.js';
import { analyzeDescription, categorizeBusinessType } from '../util/llm.js';
import type { Event, Lead, NormalizedRecord } from '../types.js';
import { randomUUID } from 'crypto';
import { analyzeSpotOnIntelligence } from '../filters/spoton.js';
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
Usage: npm run score -- --city <city>

Options:
  -c, --city <city>  City name (required)
  -h, --help        Show this help message

Examples:
  npm run score -- --city chicago
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
 * Opening timeline patterns (days before typical opening)
 */
const OPENING_TIMELINE = {
  'building_permit': -90,          // 90 days before
  'liquor_license': -45,           // 45 days before  
  'food_inspection_pass': -14,     // 2 weeks before (STRONG signal)
  'building_inspection_passed': -21, // 3 weeks before
  'food_inspection_fail': -30,     // 30 days before (needs fixes)
};

/**
 * Calculate lead score based on weighted factors and opening progression
 */
async function calculateLeadScore(events: Event[]): Promise<number> {
  if (events.length === 0) return 0;
  
  let score = 0;
  
  // Get the primary event (highest signal strength)
  const primaryEvent = events.reduce((prev, current) =>
    current.signal_strength > prev.signal_strength ? current : prev
  );
  
  // 1. Enhanced Recency Score with Opening Timeline (0-30 points)
  const allEvidence = events.flatMap(e => e.evidence);
  const mostRecentDate = allEvidence
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => b.getTime() - a.getTime())[0];
  
  if (mostRecentDate) {
    const ageInDays = getAgeInDays(mostRecentDate);
    
    // Check if we have high-confidence opening signals
    const hasRecentFoodPass = allEvidence.some(r => {
      const isFood = r.type?.toLowerCase().includes('food inspection') ?? false;
      const isLicense = /license/i.test(r.type || '') || /license/i.test(r.description || '');
      const passed = r.status?.toUpperCase() === 'PASS';
      const d = parseDate(r.event_date);
      return isFood && isLicense && passed && d && getAgeInDays(d) <= 14;
    });
    
    const hasRecentBuildingPass = allEvidence.some(r => {
      const isBld = r.type?.toLowerCase().includes('building inspection') ?? false;
      const passed = r.status?.toUpperCase() === 'PASSED';
      const d = parseDate(r.event_date);
      return isBld && passed && d && getAgeInDays(d) <= 21;
    });
    
    // Boost score for strong opening indicators
    if (hasRecentFoodPass) {
      score += 20; // Licensing inspection pass is strong but not definitive
    } else if (hasRecentBuildingPass) {
      score += 15; // Building pass alone should not dominate
    } else if (ageInDays <= 30) {
      score += 30;
    } else if (ageInDays <= 60) {
      score += 25;
    } else if (ageInDays <= 90) {
      score += 20;
    } else if (ageInDays <= 120) {
      score += 15;
    } else {
      score += 10;
    }
  }
  
  // 2. Permit/License Type Weight (0-30 points)
  const hasRestaurantSignals = events.some(e =>
    e.evidence.some(r => {
      const type = r.type?.toLowerCase() || '';
      const desc = r.description?.toLowerCase() || '';
      return type.includes('restaurant') ||
             type.includes('liquor') ||
             desc.includes('restaurant') ||
             desc.includes('kitchen');
    })
  );
  
  const hasRetailSignals = events.some(e =>
    e.evidence.some(r => {
      const type = r.type?.toLowerCase() || '';
      const desc = r.description?.toLowerCase() || '';
      return type.includes('retail') ||
             type.includes('commercial') ||
             desc.includes('retail') ||
             desc.includes('store');
    })
  );
  
  if (hasRestaurantSignals) {
    score += 30; // Restaurants are high-value leads
  } else if (hasRetailSignals) {
    score += 25; // Retail is also valuable
  } else {
    score += 15; // Other commercial activities
  }
  
  // 3. Contact Information Presence (0-20 points)
  const hasContactInfo = events.some(e =>
    e.evidence.some(r => {
      // Check if raw payload has contact information
      const payload = (r as any).payload || {};
      return payload.phone ||
             payload.email ||
             payload.contact_1_phone ||
             payload.ContactPhone ||
             payload.ApplicantName;
    })
  );
  
  if (hasContactInfo) {
    score += 20;
  }
  
  // 4. Multi-signal Bonus (0-20 points)
  if (events.length > 1) {
    score += 20; // Multiple signals indicate higher confidence
  } else if (primaryEvent.signal_strength >= 80) {
    score += 15; // High-strength single signal
  } else if (primaryEvent.signal_strength >= 70) {
    score += 10;
  }
  
  // 5. LLM-Enhanced Business Potential (0-20 points)
  try {
    const llmScore = await calculateLLMBusinessPotential(events);
    score += llmScore;
  } catch (error) {
    logger.warn('LLM business potential scoring failed, continuing with basic scoring', { error });
  }
  
  // Cap at 120 (20 points extra from LLM)
  return Math.min(score, 120);
}

/**
 * Calculate LLM-based business potential score with enhanced opening detection
 */
async function calculateLLMBusinessPotential(events: Event[]): Promise<number> {
  let llmScore = 0;
  let openingVotes = 0;
  let nonOpeningVotes = 0;
  let totalRecords = 0;
  
  // Enhanced patterns for detecting operational vs new businesses
  const openingPatterns = [
    /grand opening/i,
    /opening soon/i,
    /new location/i,
    /build[-\s]?out/i,
    /tenant improvement/i,
    /new restaurant/i,
    /coming soon/i,
    /under construction/i,
    /now hiring/i,
    /help wanted/i,
    /soft opening/i
  ];
  
  const operationalPatterns = [
    /renewal/i,
    /transfer/i,
    /change of ownership/i,
    /re[-\s]?inspection/i,
    /maintenance/i,
    /repair/i,
    /remodel/i,
    /renovation/i,
    /update/i,
    /annual/i,
    /routine/i,
    /existing/i,
    /current/i,
    /established/i
  ];
  
  for (const event of events) {
    for (const record of event.evidence) {
      try {
        totalRecords++;
        
        // Analyze business description and intent
        const textToAnalyze = `${record.type || ''} ${record.description || ''} ${record.business_name || ''}`;
        const analysis = await analyzeDescription(
          record.description || '',
          record.business_name
        );
        
        // Score based on confidence and business type
        const baseScore = Math.min(analysis.confidence / 100 * 20, 20);
        
        // Enhanced business category scoring
        const highValueCategories = [
          'Restaurant/Food Service',
          'Bar/Nightlife',
          'Retail/Store',
          'Professional Services',
          'Healthcare/Medical'
        ];
        
        let categoryMultiplier = 1.0;
        if (highValueCategories.includes(analysis.businessType)) {
          categoryMultiplier = 1.3; // 30% bonus for high-value categories
        }
        
        // Enhanced intent detection
        const text = textToAnalyze.toLowerCase();
        
        // Check for strong opening indicators
        const hasOpeningSignal = openingPatterns.some(pattern => pattern.test(text));
        const hasOperationalSignal = operationalPatterns.some(pattern => pattern.test(text));
        
        // Timeline-based intent detection
        const recordDate = parseDate(record.event_date);
        let timelineMultiplier = 1.0;
        
        if (recordDate) {
          const ageInDays = getAgeInDays(recordDate);
          
          // Very recent activity gets boost, old activity gets penalty
          if (ageInDays <= 30) {
            timelineMultiplier = 1.2;
          } else if (ageInDays > 180) {
            timelineMultiplier = 0.5; // Strong penalty for old activity
          }
        }
        
        // Apply scoring with multipliers
        const finalScore = baseScore * categoryMultiplier * timelineMultiplier;
        
        // Track intent votes
        if (hasOpeningSignal) openingVotes += 2; // Strong opening signal
        if (hasOperationalSignal) nonOpeningVotes += 2; // Strong operational signal
        
        // Additional context-based scoring
        if (text.includes('new') && !text.includes('renew')) openingVotes++;
        if (text.includes('first') || text.includes('initial')) openingVotes++;
        if (text.includes('existing') || text.includes('current')) nonOpeningVotes++;
        
        llmScore += finalScore;
        
      } catch (error) {
        logger.warn('LLM analysis failed for record, skipping', {
          recordId: record.uid,
          error: error instanceof Error ? error.message : error
        });
      }
    }
  }
  
  // Enhanced intent adjustment
  let intentFactor = 1.0;
  
  if (nonOpeningVotes > openingVotes * 2) {
    intentFactor = 0.1; // Strong penalty for operational businesses
  } else if (nonOpeningVotes > openingVotes) {
    intentFactor = 0.3; // Moderate penalty
  } else if (openingVotes > nonOpeningVotes * 2) {
    intentFactor = 1.5; // Strong bonus for new openings
  } else if (openingVotes > nonOpeningVotes) {
    intentFactor = 1.2; // Moderate bonus
  }
  
  // Apply final intent adjustment
  const adjusted = (llmScore / Math.max(totalRecords, 1)) * intentFactor;
  
  // Cap score based on intent confidence
  const maxScore = Math.min(20, Math.max(5, openingVotes * 3));
  return Math.min(adjusted, maxScore);
}

/**
 * Extract contact information from events
 */
function extractContactInfo(events: Event[]): { phone?: string; email?: string } {
  const contact: { phone?: string; email?: string } = {};
  
  for (const event of events) {
    for (const record of event.evidence) {
      const payload = (record as any).payload || {};
      
      if (!contact.phone) {
        contact.phone = payload.phone ||
                      payload.contact_1_phone ||
                      payload.ContactPhone ||
                      payload.Phone;
      }
      
      if (!contact.email) {
        contact.email = payload.email ||
                       payload.Email;
      }
      
      if (contact.phone && contact.email) {
        break;
      }
    }
    
    if (contact.phone && contact.email) {
      break;
    }
  }
  
  return contact;
}

/**
 * Analyze an address to determine if it represents a new business opportunity
 */
export async function analyzeAddressForNewBusiness(
  address: string,
  events: Event[],
  allLicenses: NormalizedRecord[],
  businessLicenses: NormalizedRecord[],
  inspections: NormalizedRecord[],
  now: Date
): Promise<boolean> {
  // Get all records for this address
  const addressLicenses = allLicenses.filter(l => l.address === address);
  const addressBusinessLicenses = businessLicenses.filter(l => l.address === address);
  const addressInspections = inspections.filter(i => i.address === address);
  
  // FILTER 1: Check for operational signals, but allow licensing inspections 30-60 days before opening
  const hasOperationalInspections = addressInspections.some(inspection => {
    const inspectionType = inspection.type?.toLowerCase() || '';
    const description = inspection.description?.toLowerCase() || '';
    
    // Allow licensing inspections as they indicate pre-opening phase
    const isLicensingInspection = inspectionType.includes('license') || 
                                 inspectionType.includes('permit') ||
                                 description.includes('license') ||
                                 description.includes('permit');
    
    if (isLicensingInspection) return false;
    
    // Only mark as operational for non-licensing inspections within last 90 days
    const inspectionDate = parseDate(inspection.event_date);
    if (!inspectionDate) return false;
    
    const ageInDays = getAgeInDays(inspectionDate);
    return ageInDays <= 90; // Non-licensing inspection within last 90 days = operational
  });
  
  if (hasOperationalInspections) {
    return false;
  }
  
  // STRICT FILTER 2: Check for active business operations
  const hasActiveBusinessLicense = addressBusinessLicenses.some(lic => {
    const status = lic.status?.toUpperCase();
    const issueDate = parseDate(lic.event_date);
    if (!issueDate) return false;
    
    const ageInDays = getAgeInDays(issueDate);
    
    // Active business license issued > 90 days ago = likely operational
    return status === 'AAC' && ageInDays > 90;
  });
  
  if (hasActiveBusinessLicense) {
    return false;
  }
  
  // STRICT FILTER 3: Check for long license history (indicates established business)
  const licenseHistory = addressLicenses
    .map(lic => parseDate(lic.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => a.getTime() - b.getTime());
  
  if (licenseHistory.length >= 3) {
    const oldestLicense = licenseHistory[0];
    if (oldestLicense) {
      const ageInDays = getAgeInDays(oldestLicense);
      if (ageInDays > 365) {
        return false; // Established business with multi-year history
      }
    }
  }
  
  // REQUIREMENT: Must have liquor license with AAI status
  const liquorLicenses = addressLicenses.filter(lic => {
    const type = lic.type?.toLowerCase() || '';
    return type.includes('liquor') && lic.status?.toUpperCase() === 'AAI';
  });
  
  if (liquorLicenses.length === 0) {
    return false;
  }
  
  // REASONABLE TIMELINE: Accept activity within 120 days for new openings
  const recentActivity = events
    .flatMap(e => e.evidence)
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null);
  
  if (recentActivity.length === 0) {
    return false;
  }
  
  const mostRecentActivity = recentActivity.sort((a, b) => b.getTime() - a.getTime())[0];
  if (!mostRecentActivity) {
    return false;
  }
  
  const daysSinceActivity = getAgeInDays(mostRecentActivity);
  
  // Accept businesses with activity in the last 120 days
  if (daysSinceActivity > 120 || daysSinceActivity < 0) {
    return false;
  }
  
  // Always return true for new openings with valid timeline and liquor license
  return true;
}

/**
 * Create a lead from events
 */
async function createLead(events: Event[], city: string): Promise<Lead> {
  const primaryEvent = events.reduce((prev, current) =>
    current.signal_strength > prev.signal_strength ? current : prev
  );
  
  const contact = extractContactInfo(events);
  const score = await calculateLeadScore(events);
  const spoton_intelligence = await analyzeSpotOnIntelligence(events);
  
  return {
    lead_id: randomUUID(),
    city,
    name: primaryEvent.name,
    address: primaryEvent.address,
    phone: contact.phone,
    email: contact.email,
    score,
    spoton_intelligence,
    evidence: events,
    created_at: new Date().toISOString(),
  };
}

/**
 * Main scoring function
 */
async function main() {
  const args = parseCliArgs();
  
  try {
    logger.info('Starting lead scoring', args);
    
    // Create storage instance
    const storage = await createStorage();
    
    // Load normalized records for comprehensive business analysis
    const normalized: NormalizedRecord[] = await storage.getNormalizedByCity(args.city);
    const inspections = normalized.filter(r => (r.dataset || '').toLowerCase() === 'food_inspections');
    const licensesAll = normalized.filter(r => (r.dataset || '').toLowerCase() === 'liquor_licenses');
    const businessLicenses = normalized.filter(r => (r.dataset || '').toLowerCase() === 'business_licenses');
    
    // Enhanced operational address detection - AGGRESSIVE FILTERING
    const operationalAddresses = new Set<string>();
    const now = new Date();
    
    // Method 1: Food inspection analysis - allow licensing inspections for 30-60 day detection
    for (const inspection of inspections) {
      if (!inspection.address) continue;
      
      const inspectionType = inspection.type?.toLowerCase() || '';
      const description = inspection.description?.toLowerCase() || '';
      
      // Allow licensing inspections as they indicate pre-opening phase
      const isLicensingInspection = inspectionType.includes('license') || 
                                   inspectionType.includes('permit') ||
                                   description.includes('license') ||
                                   description.includes('permit');
      
      if (!isLicensingInspection) {
        const inspectionDate = parseDate(inspection.event_date);
        if (inspectionDate) {
          const ageInDays = getAgeInDays(inspectionDate);
          if (ageInDays <= 90) { // Only mark as operational for non-licensing inspections within 90 days
            operationalAddresses.add(inspection.address);
          }
        }
      }
    }
    
    // Method 2: Active business license detection - ANY active license = operational
    for (const license of businessLicenses) {
      if (!license.address) continue;
      const status = license.status?.toUpperCase();
      const issueDate = parseDate(license.event_date);
      
      if (status === 'AAC' || status === 'ACTIVE') {
        // Active business license issued > 30 days ago = operational
        if (issueDate) {
          const ageInDays = getAgeInDays(issueDate);
          if (ageInDays > 30) {
            operationalAddresses.add(license.address);
          }
        } else {
          operationalAddresses.add(license.address);
        }
      }
    }
    
    // Method 3: License history analysis - established patterns
    const licenseByAddress = new Map<string, NormalizedRecord[]>();
    for (const lic of licensesAll) {
      if (!lic.address) continue;
      const arr = licenseByAddress.get(lic.address) || [];
      arr.push(lic);
      licenseByAddress.set(lic.address, arr);
    }
    
    for (const [addr, licenses] of licenseByAddress) {
      // Multiple licenses (3+) = established business
      if (licenses.length >= 3) {
        operationalAddresses.add(addr);
      }
      
      // Any license > 1 year old = established business
      const hasOldLicense = licenses.some(lic => {
        const d = parseDate(lic.event_date);
        return d && (now.getTime() - d.getTime()) > (365 * 24 * 60 * 60 * 1000);
      });
      
      if (hasOldLicense) {
        operationalAddresses.add(addr);
      }
      
      // License renewal pattern (multiple AAI -> AAC transitions)
      const licenseStatuses = licenses.map(l => l.status?.toUpperCase()).filter(Boolean);
      const hasRenewalPattern = licenseStatuses.filter(s => s === 'AAI').length >= 2;
      if (hasRenewalPattern) {
        operationalAddresses.add(addr);
      }
    }
    
    // Method 4: Recent food safety inspections (strong operational signal)
    for (const inspection of inspections) {
      if (!inspection.address) continue;
      
      const inspectionDate = parseDate(inspection.event_date);
      if (!inspectionDate) continue;
      
      const ageInDays = getAgeInDays(inspectionDate);
      
      // Any food safety inspection within 90 days = operational
      const isFoodSafety = inspection.type?.toLowerCase().includes('food') || 
                          inspection.description?.toLowerCase().includes('food');
      
      if (isFoodSafety && ageInDays <= 90) {
        operationalAddresses.add(inspection.address);
      }
    }

    // Get events to score
    const events = await storage.getEventsByCity(args.city);
    
    if (events.length === 0) {
      logger.info('No events found to score', args);
      await storage.close();
      process.exit(0);
    }
    
    logger.info(`Found ${events.length} events to score`);
    
    // Group events by address for lead creation
    const addressGroups = new Map<string, Event[]>();
    
    for (const event of events) {
      const existing = addressGroups.get(event.address) || [];
      existing.push(event);
      addressGroups.set(event.address, existing);
    }
    
    logger.info(`Grouped into ${addressGroups.size} unique addresses`);
    
    // Create and score leads with enhanced filtering and concurrency control
    const leads: Lead[] = [];
    const limit = pLimit(5); // Limit concurrent LLM calls
    const batchSize = 10;
    
    let operationalFiltered = 0;
    let qualificationFiltered = 0;
    const startTime = Date.now();
    
    // Convert to array for batch processing
    const addressArray = Array.from(addressGroups.entries());
    
    // Process addresses in batches
    for (let i = 0; i < addressArray.length; i += batchSize) {
      const batch = addressArray.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(addressArray.length / batchSize);
      
      logger.info(`Processing address batch ${batchNumber}/${totalBatches} (${batch.length} addresses)`);
      
      // Process batch with concurrency limit
      const results = await Promise.allSettled(
        batch.map(([address, addressEvents]) => 
          limit(async () => {
            // Skip obviously operational addresses
            if (operationalAddresses.has(address)) {
              return { type: 'operational', address };
            }
            
            // Analyze the address for new business indicators
            const isQualified = await analyzeAddressForNewBusiness(
              address, 
              addressEvents, 
              licensesAll, 
              businessLicenses,
              inspections,
              now
            );
            
            if (isQualified) {
              const lead = await createLead(addressEvents, args.city);
              return { type: 'qualified', address, lead };
            } else {
              return { type: 'filtered', address };
            }
          })
        )
      );
      
      // Process results
      for (const result of results) {
        if (result.status === 'fulfilled') {
          const { type } = result.value;
          if (type === 'operational') {
            operationalFiltered++;
          } else if (type === 'qualified' && result.value.lead) {
            leads.push(result.value.lead);
          } else if (type === 'filtered') {
            qualificationFiltered++;
          }
        } else {
          logger.error('Failed to process address', { error: result.reason });
          qualificationFiltered++;
        }
      }
      
      const progress = Math.round(((i + batch.length) / addressArray.length) * 100);
      logger.info(`Batch ${batchNumber} completed: ${leads.length} leads found so far (${progress}% progress)`);
    }
    
    logger.info('Filtering results', {
      totalAddresses: addressGroups.size,
      operationalFiltered,
      qualificationFiltered,
      qualified: leads.length
    });
    
    // Sort leads by score (highest first)
    leads.sort((a, b) => b.score - a.score);
    
    // Store leads
    let storedCount = 0;
    for (const lead of leads) {
      try {
        await storage.insertLead(lead);
        storedCount++;
        
        if (storedCount % 10 === 0) {
          logger.info(`Stored ${storedCount} leads`);
        }
        
      } catch (error) {
        logger.error('Failed to store lead', {
          leadId: lead.lead_id,
          error: error instanceof Error ? error.message : error,
        });
      }
    }
    
    // Log score distribution
    const scoreRanges = {
      '90-100': leads.filter(l => l.score >= 90).length,
      '80-89': leads.filter(l => l.score >= 80 && l.score < 90).length,
      '70-79': leads.filter(l => l.score >= 70 && l.score < 80).length,
      '60-69': leads.filter(l => l.score >= 60 && l.score < 70).length,
      '50-59': leads.filter(l => l.score >= 50 && l.score < 60).length,
      '<50': leads.filter(l => l.score < 50).length,
    };
    
    logger.info('Lead scoring completed successfully', {
      city: args.city,
      eventsProcessed: events.length,
      leadsGenerated: leads.length,
      leadsStored: storedCount,
      scoreDistribution: scoreRanges,
      topScore: leads[0]?.score || 0,
    });
    
    // Close storage
    await storage.close();
    
    process.exit(0);
    
  } catch (error) {
    logger.error('Lead scoring failed', { error, args });
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
