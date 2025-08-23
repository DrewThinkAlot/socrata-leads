#!/usr/bin/env node

/**
 * Lead scoring pipeline runner
 */

import { config } from 'dotenv';
import { loadCityConfig } from '../config/index.js';
import { createStorage } from '../storage/index.js';
import { logger } from '../util/logger.js';
import { parseCliArgs as parseSharedCliArgs, CLI_CONFIGS } from '../util/cli.js';
import { analyzeDescription, categorizeBusinessType, classifyProjectStage, estimateDaysRemaining, detectOperationalStatus, resolveBusinessEntity, extractContactInfoLLM, calculateDynamicLeadScore } from '../util/llm.js';
import type { Event, Lead, NormalizedRecord } from '../types.js';
import { randomUUID } from 'crypto';
import { analyzeSpotOnIntelligence } from '../filters/spoton.js';
import pLimit from 'p-limit';
import { parseDate, getAgeInDays } from '../util/dates.js';

// Load environment variables
config();

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  const values = parseSharedCliArgs(CLI_CONFIGS.score);
  
  return {
    city: values.city as string,
  };
}

/**
 * Restaurant type definitions
 */
type RestaurantType = 'fast-food' | 'fast-casual' | 'full-service' | 'unknown';

/**
 * Restaurant-specific opening timeline patterns (days before typical opening)
 * Based on industry research: fast-food (8-10 weeks), fast-casual (10-14 weeks), full-service (20+ weeks)
 */
const RESTAURANT_OPENING_TIMELINES = {
  'fast-food': {
    'building_permit': -63,          // 9 weeks average
    'liquor_license': -35,           // 5 weeks
    'food_inspection_pass': -10,     // 1.5 weeks (STRONG signal)
    'building_inspection_passed': -14, // 2 weeks
    'food_inspection_fail': -21,     // 3 weeks (needs fixes)
    'equipment_installation': -7,    // 1 week
    'utility_hookups': -5,           // 5 days (very late stage)
    'final_inspection': -7,          // 1 week
  },
  'fast-casual': {
    'building_permit': -84,          // 12 weeks average
    'liquor_license': -42,           // 6 weeks
    'food_inspection_pass': -15,     // 2+ weeks
    'building_inspection_passed': -21, // 3 weeks
    'food_inspection_fail': -28,     // 4 weeks
    'equipment_installation': -14,   // 2 weeks
    'utility_hookups': -10,          // 1.5 weeks
    'final_inspection': -14,         // 2 weeks
  },
  'full-service': {
    'building_permit': -154,         // 22 weeks average
    'liquor_license': -56,           // 8 weeks
    'food_inspection_pass': -21,     // 3 weeks
    'building_inspection_passed': -28, // 4 weeks
    'food_inspection_fail': -42,     // 6 weeks
    'equipment_installation': -21,   // 3 weeks
    'utility_hookups': -14,          // 2 weeks
    'final_inspection': -21,         // 3 weeks
  },
  'unknown': {
    'building_permit': -90,          // Default fallback
    'liquor_license': -45,
    'food_inspection_pass': -14,
    'building_inspection_passed': -21,
    'food_inspection_fail': -30,
    'equipment_installation': -14,
    'utility_hookups': -10,
    'final_inspection': -14,
  }
};

/**
 * Detect restaurant type based on business description and permit types
 */
function detectRestaurantType(events: Event[]): RestaurantType {
  const allEvidence = events.flatMap(e => e.evidence);
  const businessNames = allEvidence.map(r => r.business_name?.toLowerCase() || '').join(' ');
  const descriptions = allEvidence.map(r => r.description?.toLowerCase() || '').join(' ');
  const types = allEvidence.map(r => r.type?.toLowerCase() || '').join(' ');
  
  const combinedText = `${businessNames} ${descriptions} ${types}`;
  
  // Fast-food indicators
  const fastFoodKeywords = [
    'mcdonald', 'burger king', 'kfc', 'taco bell', 'subway', 'pizza hut', 'domino',
    'fast food', 'quick service', 'drive thru', 'drive-thru', 'counter service',
    'takeout only', 'delivery only', 'grab and go', 'express'
  ];
  
  // Fast-casual indicators
  const fastCasualKeywords = [
    'chipotle', 'panera', 'shake shack', 'five guys', 'qdoba', 'panda express',
    'fast casual', 'counter order', 'casual dining', 'fresh', 'build your own',
    'made to order', 'artisan', 'craft', 'gourmet fast'
  ];
  
  // Full-service indicators
  const fullServiceKeywords = [
    'fine dining', 'full service', 'table service', 'waiter', 'waitress', 'server',
    'reservation', 'wine list', 'sommelier', 'chef', 'tasting menu', 'prix fixe',
    'upscale', 'bistro', 'brasserie', 'steakhouse', 'seafood restaurant'
  ];
  
  // Check for liquor license complexity (full-service indicator)
  const hasComplexLiquor = allEvidence.some(r => {
    const desc = r.description?.toLowerCase() || '';
    const type = r.type?.toLowerCase() || '';
    return desc.includes('full bar') || desc.includes('wine') || 
           type.includes('liquor') || type.includes('tavern');
  });
  
  // Check for equipment complexity
  const hasComplexEquipment = combinedText.includes('hood') || 
                             combinedText.includes('ventilation') ||
                             combinedText.includes('grease trap') ||
                             combinedText.includes('fire suppression');
  
  // Classification logic
  if (fastFoodKeywords.some(keyword => combinedText.includes(keyword))) {
    return 'fast-food';
  }
  
  if (fullServiceKeywords.some(keyword => combinedText.includes(keyword)) || 
      (hasComplexLiquor && hasComplexEquipment)) {
    return 'full-service';
  }
  
  if (fastCasualKeywords.some(keyword => combinedText.includes(keyword))) {
    return 'fast-casual';
  }
  
  // Default classification based on permit complexity
  const permitCount = new Set(allEvidence.map(r => r.type)).size;
  if (permitCount >= 4 && hasComplexLiquor) {
    return 'full-service';
  } else if (permitCount >= 2) {
    return 'fast-casual';
  }
  
  return 'unknown';
}

/**
 * Get seasonal adjustment factor based on current date
 */
function getSeasonalAdjustment(targetDate?: Date): number {
  const date = targetDate || new Date();
  const month = date.getMonth() + 1; // 1-12
  
  // Q1 (Jan-Mar): 1.3x timeline extension
  if (month >= 1 && month <= 3) return 1.3;
  
  // Q2 (Apr-Jun): 0.9x (prime opening season)
  if (month >= 4 && month <= 6) return 0.9;
  
  // Q3 (Jul-Sep): 1.0x baseline
  if (month >= 7 && month <= 9) return 1.0;
  
  // Q4 (Oct-Dec): 1.4x (holiday avoidance)
  return 1.4;
}

/**
 * Get city-specific permit processing adjustments
 */
function getCityProcessingAdjustment(city: string, permitTypes: string[]): number {
  const permits = permitTypes.join(' ').toLowerCase();
  
  switch (city.toLowerCase()) {
    case 'seattle':
      // Seattle has faster processing for bundled permits
      if (permits.includes('construction') && (permits.includes('electrical') || permits.includes('mechanical'))) {
        return 0.8; // 20% faster for bundled permits
      }
      // Food service plan review is standardized at 14 days
      if (permits.includes('food') || permits.includes('health')) {
        return 0.9;
      }
      return 1.0;
    
    case 'chicago':
      // Chicago has electronic submission requirements that can speed processing
      if (permits.includes('building') || permits.includes('construction')) {
        return 0.9; // Slightly faster with electronic systems
      }
      // Complex liquor licenses may take longer
      if (permits.includes('liquor') && permits.includes('tavern')) {
        return 1.2;
      }
      return 1.0;
    
    default:
      return 1.0; // No adjustment for unknown cities
  }
}

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
  
  // 1. Enhanced Recency Score with Dynamic Opening Timeline (0-35 points)
  const allEvidence = events.flatMap(e => e.evidence);
  
  // Detect restaurant type for dynamic timeline calculation
  const restaurantType = detectRestaurantType(events);
  const timeline = RESTAURANT_OPENING_TIMELINES[restaurantType];
  const seasonalFactor = getSeasonalAdjustment();
  
  // Get city-specific processing adjustments
  const permitTypes = [...new Set(allEvidence.map(r => r.type).filter(Boolean))] as string[];
  const cityFactor = getCityProcessingAdjustment(events[0]?.city || 'unknown', permitTypes);
  const mostRecentDate = allEvidence
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => b.getTime() - a.getTime())[0];
  
  if (mostRecentDate) {
    const ageInDays = getAgeInDays(mostRecentDate);
    
    // Check for high-confidence opening signals with restaurant-specific timelines
    const hasRecentFoodPass = allEvidence.some(r => {
      const isFood = r.type?.toLowerCase().includes('food inspection') ?? false;
      const isLicense = /license/i.test(r.type || '') || /license/i.test(r.description || '');
      const passed = r.status?.toUpperCase() === 'PASS';
      const d = parseDate(r.event_date);
      const expectedDays = Math.abs(timeline.food_inspection_pass) * seasonalFactor * cityFactor;
      return isFood && isLicense && passed && d && getAgeInDays(d) <= expectedDays;
    });
    
    const hasRecentBuildingPass = allEvidence.some(r => {
      const isBld = r.type?.toLowerCase().includes('building inspection') ?? false;
      const passed = r.status?.toUpperCase() === 'PASSED';
      const d = parseDate(r.event_date);
      const expectedDays = Math.abs(timeline.building_inspection_passed) * seasonalFactor * cityFactor;
      return isBld && passed && d && getAgeInDays(d) <= expectedDays;
    });
    
    // Check for equipment installation signals (very strong late-stage indicator)
    const hasEquipmentSignals = allEvidence.some(r => {
      const desc = r.description?.toLowerCase() || '';
      const type = r.type?.toLowerCase() || '';
      return desc.includes('equipment') || desc.includes('kitchen') || 
             desc.includes('hood') || desc.includes('installation') ||
             type.includes('equipment');
    });
    
    // Check for utility hookup signals (very strong late-stage indicator)
    const hasUtilityHookups = allEvidence.some(r => {
      const desc = r.description?.toLowerCase() || '';
      const type = r.type?.toLowerCase() || '';
      return desc.includes('water service') || desc.includes('gas connection') ||
             desc.includes('electrical service') || desc.includes('utility hookup') ||
             desc.includes('water permit') || desc.includes('gas permit') ||
             type.includes('water service') || type.includes('electrical') ||
             (type.includes('plumbing') && desc.includes('commercial')) ||
             (desc.includes('utility') && (desc.includes('connect') || desc.includes('install')));
    });
    
    // Dynamic scoring based on restaurant type and timeline position
    if (hasUtilityHookups) {
      score += 32; // Strongest available signal - utilities being connected
    } else if (hasEquipmentSignals) {
      score += 30; // Very strong - equipment installation phase
    } else if (hasRecentFoodPass) {
      score += 25; // Strong signal with restaurant-specific timing
    } else if (hasRecentBuildingPass) {
      score += 20; // Good signal but earlier in timeline
    } else {
      // Standard recency scoring with restaurant-type awareness
      const buildingPermitWindow = Math.abs(timeline.building_permit) * seasonalFactor * cityFactor;
      const liquorLicenseWindow = Math.abs(timeline.liquor_license) * seasonalFactor * cityFactor;
      
      if (ageInDays <= 30) {
        score += 30;
      } else if (ageInDays <= 60) {
        score += 25;
      } else if (ageInDays <= liquorLicenseWindow) {
        score += 20; // Within liquor license window
      } else if (ageInDays <= buildingPermitWindow) {
        score += 15; // Within building permit window
      } else {
        score += 10;
      }
    }
  }
  
  // 2. Restaurant Type & Complexity Weight (0-35 points)
  const restaurantTypeScore = {
    'full-service': 35,  // Highest value - complex operations
    'fast-casual': 30,   // High value - growing segment
    'fast-food': 25,     // Good value - predictable model
    'unknown': 20        // Default scoring
  };
  
  score += restaurantTypeScore[restaurantType];
  
  // Additional complexity bonuses
  const hasLiquorLicense = allEvidence.some(r => {
    const type = r.type?.toLowerCase() || '';
    const desc = r.description?.toLowerCase() || '';
    return type.includes('liquor') || desc.includes('liquor') || desc.includes('alcohol');
  });
  
  const hasComplexPermits = allEvidence.some(r => {
    const desc = r.description?.toLowerCase() || '';
    return desc.includes('hood') || desc.includes('fire suppression') || 
           desc.includes('grease trap') || desc.includes('ventilation');
  });
  
  if (hasLiquorLicense) score += 5;
  if (hasComplexPermits) score += 5;
  
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
  
  // 6. Dynamic LLM Scoring Enhancement (if enabled)
  const useDynamicScoring = process.env.LLM_DYNAMIC_SCORING === 'true';
  if (useDynamicScoring) {
    try {
      const dynamicAnalysis = await calculateDynamicLeadScore(events, score);
      
      // Use LLM-adjusted score if confidence is high
      if (dynamicAnalysis.source === 'llm') {
        logger.debug('Applied dynamic LLM scoring', {
          originalScore: score,
          adjustedScore: dynamicAnalysis.score,
          adjustments: dynamicAnalysis.adjustments
        });
        score = dynamicAnalysis.score;
      }
    } catch (error) {
      logger.warn('Dynamic LLM scoring failed, using static score', { error });
    }
  }
  
  // Cap at 130 (increased due to enhanced scoring)
  return Math.min(score, 130);
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
 * Extract contact information from events with enhanced LLM extraction
 */
async function extractContactInfo(events: Event[]): Promise<{ phone?: string; email?: string; website?: string; contactPerson?: string }> {
  const contact: { phone?: string; email?: string; website?: string; contactPerson?: string } = {};
  
  // First try traditional payload extraction
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
  
  // Enhanced LLM contact extraction if enabled and missing info
  const useEnhancedExtraction = process.env.LLM_CONTACT_EXTRACTION === 'true';
  
  if (useEnhancedExtraction && (!contact.phone || !contact.email)) {
    try {
      // Combine all event descriptions for LLM analysis
      const combinedText = events.flatMap(e => e.evidence)
        .map(r => `${r.description || ''} ${r.business_name || ''}`)
        .join(' ')
        .substring(0, 1000); // Limit text length
      
      const primaryEvent = events.reduce((prev, current) =>
        current.signal_strength > prev.signal_strength ? current : prev
      );
      
      const llmContact = await extractContactInfoLLM(combinedText, primaryEvent.name);
      
      // Merge LLM results with existing contact info (prefer existing)
      if (!contact.phone && llmContact.phone) contact.phone = llmContact.phone;
      if (!contact.email && llmContact.email) contact.email = llmContact.email;
      if (llmContact.website) contact.website = llmContact.website;
      if (llmContact.contactPerson) contact.contactPerson = llmContact.contactPerson;
      
      if (llmContact.phone || llmContact.email) {
        logger.debug('LLM extracted additional contact info', {
          address: primaryEvent.address,
          extractedFields: Object.keys(llmContact).filter(k => k !== 'source' && llmContact[k as keyof typeof llmContact])
        });
      }
    } catch (error) {
      logger.warn('LLM contact extraction failed', { error });
    }
  }
  
  return contact;
}

/**
 * Analyze an address to determine if it represents a new business opportunity
 * Enhanced with LLM-based operational detection
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
  
  // Enhanced LLM-based operational detection if enabled
  const useEnhancedFiltering = process.env.LLM_ENHANCED_FILTERING === 'true';
  
  if (useEnhancedFiltering && events.length > 0) {
    try {
      // Use LLM to analyze the primary event for operational status
      const primaryEvent = events.reduce((prev, current) =>
        current.signal_strength > prev.signal_strength ? current : prev
      );
      
      const allEvidence = events.flatMap(e => e.evidence);
      const permitTypes = [...new Set(allEvidence.map(r => r.type).filter(Boolean))] as string[];
      const mostRecentDate = allEvidence
        .map(r => parseDate(r.event_date))
        .filter((d): d is Date => d !== null)
        .sort((a, b) => b.getTime() - a.getTime())[0];
      
      const operationalAnalysis = await detectOperationalStatus(
        primaryEvent.description || '',
        permitTypes,
        primaryEvent.name,
        mostRecentDate?.toISOString()
      );
      
      // If LLM has high confidence that business is operational, filter it out
      if (operationalAnalysis.isOperational && operationalAnalysis.confidence > 75) {
        logger.debug('LLM detected operational business', {
          address,
          confidence: operationalAnalysis.confidence,
          businessName: primaryEvent.name
        });
        return false;
      }
    } catch (error) {
      logger.warn('LLM operational detection failed, falling back to rule-based', { error });
    }
  }
  
  // Fallback to original rule-based detection
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
 * Create a lead from events with project stage classification
 */
async function createLead(events: Event[], city: string): Promise<Lead> {
  const primaryEvent = events.reduce((prev, current) =>
    current.signal_strength > prev.signal_strength ? current : prev
  );
  
  const contact = await extractContactInfo(events);
  const score = await calculateLeadScore(events);
  const spoton_intelligence = await analyzeSpotOnIntelligence(events);
  
  // Extract permit types and issue date for stage classification
  const allEvidence = events.flatMap(e => e.evidence);
  const permitTypes = [...new Set(allEvidence.map(r => r.type).filter(Boolean))] as string[];
  const issueDate = allEvidence
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => b.getTime() - a.getTime())[0]?.toISOString();
  
  // Get project stage classification
  const stageAnalysis = await classifyProjectStage(
    primaryEvent.description || '',
    primaryEvent.name,
    permitTypes,
    issueDate
  );
  
  // Get days remaining estimation
  const daysAnalysis = await estimateDaysRemaining(
    stageAnalysis.stage,
    primaryEvent.description || '',
    permitTypes,
    issueDate,
    primaryEvent.name
  );
  
  return {
    lead_id: randomUUID(),
    city,
    name: primaryEvent.name,
    address: primaryEvent.address,
    phone: contact.phone,
    email: contact.email,
    score,
    spoton_intelligence,
    project_stage: stageAnalysis.stage,
    days_remaining: daysAnalysis.daysRemaining,
    stage_confidence: String(stageAnalysis.confidence),
    days_confidence: String(daysAnalysis.confidence),
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
    
    // Method 1: Food inspection analysis with signal expiration
    for (const inspection of inspections) {
      if (!inspection.address) continue;
      
      const inspectionType = inspection.type?.toLowerCase() || '';
      const description = inspection.description?.toLowerCase() || '';
      
      // Skip expired signals (older than 180 days)
      const inspectionDate = parseDate(inspection.event_date);
      if (!inspectionDate) continue;
      
      const ageInDays = getAgeInDays(inspectionDate);
      if (ageInDays > 180) {
        continue; // Skip expired signals
      }
      
      // Allow licensing inspections as they indicate pre-opening phase
      const isLicensingInspection = inspectionType.includes('license') || 
                                   inspectionType.includes('permit') ||
                                   description.includes('license') ||
                                   description.includes('permit');
      
      if (!isLicensingInspection) {
        if (ageInDays <= 90) { // Only mark as operational for non-licensing inspections within 90 days
          operationalAddresses.add(inspection.address);
        }
      }
    }
    
    // Method 2: Active business license detection with expiration tracking
    for (const license of businessLicenses) {
      if (!license.address) continue;
      const status = license.status?.toUpperCase();
      const issueDate = parseDate(license.event_date);
      
      // Skip expired signals
      if (issueDate) {
        const ageInDays = getAgeInDays(issueDate);
        if (ageInDays > 180) {
          continue; // Skip expired signals
        }
      }
      
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
    
    // Method 3: License history analysis with expiration tracking
    const licenseByAddress = new Map<string, NormalizedRecord[]>();
    for (const lic of licensesAll) {
      if (!lic.address) continue;
      
      // Skip expired signals
      const issueDate = parseDate(lic.event_date);
      if (issueDate) {
        const ageInDays = getAgeInDays(issueDate);
        if (ageInDays > 180) {
          continue; // Skip expired signals
        }
      }
      
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
    
    // Method 4: Recent food safety inspections with expiration tracking
    for (const inspection of inspections) {
      if (!inspection.address) continue;
      
      const inspectionDate = parseDate(inspection.event_date);
      if (!inspectionDate) continue;
      
      // Skip expired signals
      const ageInDays = getAgeInDays(inspectionDate);
      if (ageInDays > 180) {
        continue; // Skip expired signals
      }
      
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
    const batchSize = 1000;
    
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
    
    // Enhanced duplicate detection if enabled
    const useDuplicateDetection = process.env.LLM_DUPLICATE_DETECTION === 'true';
    let deduplicatedLeads = leads;
    
    if (useDuplicateDetection && leads.length > 1) {
      logger.info('Starting LLM-based duplicate detection...');
      const duplicateGroups = new Map<number, number[]>();
      const processedPairs = new Set<string>();
      
      // Compare all pairs of leads
      for (let i = 0; i < leads.length; i++) {
        for (let j = i + 1; j < leads.length; j++) {
          const pairKey = `${i}-${j}`;
          if (processedPairs.has(pairKey)) continue;
          processedPairs.add(pairKey);
          
          try {
            const lead1 = leads[i];
            const lead2 = leads[j];
            
            if (!lead1 || !lead2 || !lead1.name || !lead2.name) continue;
            
            const duplicateAnalysis = await resolveBusinessEntity(
              lead1.address,
              lead1.name,
              lead2.address,
              lead2.name
            );
            
            // If high confidence duplicate, group them
            if (duplicateAnalysis.isSameBusiness && duplicateAnalysis.confidence > 80) {
              logger.debug('Detected duplicate leads', {
                lead1: { address: lead1.address, name: lead1.name },
                lead2: { address: lead2.address, name: lead2.name },
                confidence: duplicateAnalysis.confidence
              });
              
              // Group duplicates (keep the higher scoring lead)
              if (!duplicateGroups.has(i) && !duplicateGroups.has(j)) {
                duplicateGroups.set(i, [j]);
              } else if (duplicateGroups.has(i)) {
                duplicateGroups.get(i)!.push(j);
              } else if (duplicateGroups.has(j)) {
                duplicateGroups.get(j)!.push(i);
              }
            }
          } catch (error) {
            logger.warn('Duplicate detection failed for pair', { i, j, error });
          }
        }
      }
      
      // Remove duplicates (keep highest scoring lead from each group)
      const indicesToRemove = new Set<number>();
      for (const [primaryIndex, duplicateIndices] of duplicateGroups) {
        for (const dupIndex of duplicateIndices) {
          const primaryLead = leads[primaryIndex];
          const dupLead = leads[dupIndex];
          
          if (!primaryLead || !dupLead) continue;
          
          // Keep the lead with higher score
          if (primaryLead.score >= dupLead.score) {
            indicesToRemove.add(dupIndex);
          } else {
            indicesToRemove.add(primaryIndex);
          }
        }
      }
      
      deduplicatedLeads = leads.filter((_, index) => !indicesToRemove.has(index));
      
      if (indicesToRemove.size > 0) {
        logger.info(`Removed ${indicesToRemove.size} duplicate leads via LLM analysis`);
      }
    }
    
    // Sort leads by score (highest first)
    deduplicatedLeads.sort((a, b) => b.score - a.score);
    
    // Store leads
    let storedCount = 0;
    for (const lead of deduplicatedLeads) {
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
