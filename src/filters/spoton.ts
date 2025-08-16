/**
 * SpotOn-specific business intelligence filters
 * Implements the 8 priority signals for sales rep targeting
 */

import type { Event, NormalizedRecord, SpotOnBusinessIntelligence, SpotOnFilterMatch } from '../types.js';
import { analyzeDescription } from '../util/llm.js';
import { parseDate } from '../util/dates.js';

/**
 * Configuration for SpotOn filters
 */
export interface SpotOnFilterConfig {
  min_seat_capacity: number;
  min_square_footage: number;
  preferred_business_types: string[];
  reservation_platforms: string[];
  liquor_license_priority: string[];
  timeline_window_days: [number, number];
  service_model_weights: Record<string, number>;
  operator_type_weights: Record<string, number>;
}

/**
 * Default SpotOn filter configuration
 */
export const DEFAULT_SPOTON_CONFIG: SpotOnFilterConfig = {
  min_seat_capacity: 25,
  min_square_footage: 1500,
  preferred_business_types: [
    'Restaurant', 'Bar', 'Fast Casual', 'Full Service', 'Casual Dining',
    'Fine Dining', 'Brewery', 'Winery', 'Cocktail Bar', 'Sports Bar'
  ],
  reservation_platforms: [
    'OpenTable', 'Resy', 'SevenRooms', 'Yelp Waitlist', 'Tock',
    'Reservations', 'Waitlist', 'Booking', 'Table Management'
  ],
  liquor_license_priority: [
    'Full Bar License', 'Tavern License', 'Restaurant License',
    'Beer and Wine License', 'Catering License'
  ],
  timeline_window_days: [30, 60],
  service_model_weights: {
    'full-service': 1.5,
    'fast-casual': 1.3,
    'takeout-only': 0.5,
    'delivery-first': 0.3,
    'unknown': 1.0
  },
  operator_type_weights: {
    'chain-expansion': 1.4,
    'existing-operator': 1.2,
    'new-operator': 1.0,
    'unknown': 0.8
  }
};

/**
 * Analyze events for SpotOn business intelligence
 */
export async function analyzeSpotOnIntelligence(
  events: Event[],
  config: SpotOnFilterConfig = DEFAULT_SPOTON_CONFIG
): Promise<SpotOnBusinessIntelligence> {
  const allEvidence = events.flatMap(e => e.evidence);
  
  // Analyze each signal
  const serviceModel = await detectServiceModel(allEvidence);
  const seatCapacity = await estimateCapacity(allEvidence);
  const squareFootage = await estimateSquareFootage(allEvidence);
  const liquorType = await detectLiquorLicenseType(allEvidence);
  const reservationSystems = await detectReservationSystems(allEvidence);
  const kitchenComplexity = await assessKitchenComplexity(allEvidence);
  const operatorType = await detectOperatorType(allEvidence);
  const timelineDays = await estimateTimeline(allEvidence);
  
  // Generate filter matches
  const intelligenceInput: Partial<SpotOnBusinessIntelligence> = {
    service_model: serviceModel,
    reservation_systems: reservationSystems,
    kitchen_complexity: kitchenComplexity,
    operator_type: operatorType
  };
  if (liquorType !== undefined) intelligenceInput.liquor_license_type = liquorType;
  if (seatCapacity !== undefined) intelligenceInput.seat_capacity = seatCapacity;
  if (squareFootage !== undefined) intelligenceInput.square_footage = squareFootage;
  if (timelineDays !== undefined) intelligenceInput.opening_timeline_days = timelineDays;

  const filterMatches = await generateFilterMatches(intelligenceInput, config);
  
  // Calculate SpotOn score
  const spotonScore = calculateSpotOnScore(filterMatches, config);
  
  return {
    business_category: determineBusinessCategory(allEvidence),
    service_model: serviceModel,
    ...(seatCapacity !== undefined ? { seat_capacity: seatCapacity } : {}),
    ...(squareFootage !== undefined ? { square_footage: squareFootage } : {}),
    ...(liquorType !== undefined ? { liquor_license_type: liquorType } : {}),
    reservation_systems: reservationSystems,
    kitchen_complexity: kitchenComplexity,
    operator_type: operatorType,
    ...(timelineDays !== undefined ? { opening_timeline_days: timelineDays } : {}),
    has_type_i_hood: await detectTypeIHood(allEvidence),
    has_multiple_cook_lines: await detectMultipleCookLines(allEvidence),
    has_hot_cold_stations: await detectHotColdStations(allEvidence),
    has_multiple_printers: await detectMultiplePrinters(allEvidence),
    spoton_score: spotonScore,
    filter_matches: filterMatches
  };
}

/**
 * Detect service model (full-service, fast-casual, etc.)
 */
async function detectServiceModel(records: NormalizedRecord[]): Promise<SpotOnBusinessIntelligence['service_model']> {
  const descriptions = records.map(r => `${r.business_name || ''} ${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  
  // Check for delivery-first indicators
  if (descriptions.includes('ghost kitchen') || 
      descriptions.includes('delivery only') || 
      descriptions.includes('cloud kitchen') ||
      descriptions.includes('virtual restaurant')) {
    return 'delivery-first';
  }
  
  // Check for takeout-only
  if (descriptions.includes('takeout only') || 
      descriptions.includes('pickup only') ||
      descriptions.includes('counter service only')) {
    return 'takeout-only';
  }
  
  // Check for full-service indicators
  if (descriptions.includes('full service') || 
      descriptions.includes('table service') || 
      descriptions.includes('dine-in') ||
      descriptions.includes('waitstaff') ||
      descriptions.includes('server')) {
    return 'full-service';
  }
  
  // Check for fast-casual
  if (descriptions.includes('fast casual') || 
      descriptions.includes('quick service') || 
      descriptions.includes('counter service') ||
      descriptions.includes('fast food')) {
    return 'fast-casual';
  }
  
  // Use LLM for ambiguous cases
  const combinedText = records.slice(0, 3).map(r => r.description || '').join(' ');
  if (combinedText.length > 10) {
    const analysis = await analyzeDescription(combinedText);
    const businessType = analysis.businessType.toLowerCase();
    
    if (businessType.includes('restaurant') && businessType.includes('full')) return 'full-service';
    if (businessType.includes('fast') || businessType.includes('quick')) return 'fast-casual';
    if (businessType.includes('bar') && !businessType.includes('restaurant')) return 'full-service';
  }
  
  return 'unknown';
}

/**
 * Estimate seating capacity from descriptions
 */
async function estimateCapacity(records: NormalizedRecord[]): Promise<number | undefined> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ');
  
  // Look for explicit capacity mentions
  const capacityMatch = descriptions.match(/(\d+)\s*(?:seat|seats|seating|capacity|person)/i);
  if (capacityMatch) {
    const group = capacityMatch[1];
    if (group !== undefined) {
      return parseInt(group, 10);
    }
  }
  
  // Look for square footage to estimate capacity
  const sqftMatch = descriptions.match(/(\d+)\s*(?:sq|square|ft|foot)/i);
  if (sqftMatch) {
    const group = sqftMatch[1];
    if (group !== undefined) {
      const sqft = parseInt(group, 10);
      // Rough estimate: 15-20 sq ft per seat
      return Math.floor(sqft / 18);
    }
  }
  
  // Look for restaurant size descriptors
  if (descriptions.includes('small') || descriptions.includes('cafe')) return 15;
  if (descriptions.includes('medium') || descriptions.includes('bistro')) return 40;
  if (descriptions.includes('large') || descriptions.includes('fine dining')) return 80;
  
  return undefined;
}

/**
 * Estimate square footage from descriptions
 */
async function estimateSquareFootage(records: NormalizedRecord[]): Promise<number | undefined> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ');
  
  const sqftMatch = descriptions.match(/(\d+)\s*(?:sq|square|ft|foot)/i);
  if (sqftMatch) {
    const group = sqftMatch[1];
    if (group !== undefined) {
      return parseInt(group, 10);
    }
  }
  
  return undefined;
}

/**
 * Detect liquor license type
 */
async function detectLiquorLicenseType(records: NormalizedRecord[]): Promise<SpotOnBusinessIntelligence['liquor_license_type']> {
  const liquorRecords = records.filter(r => 
    r.type?.toLowerCase().includes('liquor') || 
    r.description?.toLowerCase().includes('liquor') ||
    r.type?.toLowerCase().includes('alcohol')
  );
  
  if (liquorRecords.length === 0) return 'unknown';
  
  const descriptions = liquorRecords.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  
  if (descriptions.includes('tavern') || descriptions.includes('bar')) return 'full-bar';
  if (descriptions.includes('restaurant') && descriptions.includes('liquor')) return 'restaurant';
  if (descriptions.includes('beer') && descriptions.includes('wine')) return 'beer-wine';
  if (descriptions.includes('tavern')) return 'tavern';
  
  return 'unknown';
}

/**
 * Detect reservation systems
 */
async function detectReservationSystems(records: NormalizedRecord[]): Promise<string[]> {
  const descriptions = records.map(r => `${r.business_name || ''} ${r.description || ''}`).join(' ').toLowerCase();
  const systems: string[] = [];
  
  const platforms = [
    'opentable', 'resy', 'sevenrooms', 'yelp waitlist', 'tock',
    'reservations', 'waitlist', 'booking system', 'table management'
  ];
  
  for (const platform of platforms) {
    if (descriptions.includes(platform.toLowerCase())) {
      systems.push(platform);
    }
  }
  
  return systems;
}

/**
 * Assess kitchen complexity
 */
async function assessKitchenComplexity(records: NormalizedRecord[]): Promise<SpotOnBusinessIntelligence['kitchen_complexity']> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  
  // Check for complex kitchen indicators
  if (descriptions.includes('commercial kitchen') || 
      descriptions.includes('professional kitchen') ||
      descriptions.includes('full kitchen')) {
    return 'complex';
  }
  
  // Check for multi-station indicators
  if (descriptions.includes('multiple stations') || 
      descriptions.includes('cook line') ||
      descriptions.includes('prep station') ||
      descriptions.includes('hot line') ||
      descriptions.includes('cold line')) {
    return 'multi-station';
  }
  
  // Check for moderate complexity
  if (descriptions.includes('kitchen equipment') || 
      descriptions.includes('cooking equipment') ||
      descriptions.includes('food prep')) {
    return 'moderate';
  }
  
  // Check for simple setup
  if (descriptions.includes('basic kitchen') || 
      descriptions.includes('small kitchen') ||
      descriptions.includes('limited kitchen')) {
    return 'simple';
  }
  
  return 'unknown';
}

/**
 * Detect operator type
 */
async function detectOperatorType(records: NormalizedRecord[]): Promise<SpotOnBusinessIntelligence['operator_type']> {
  const descriptions = records.map(r => `${r.business_name || ''} ${r.description || ''}`).join(' ');
  
  // Check for chain expansion
  if (descriptions.includes('new location') || 
      descriptions.includes('second location') || 
      descriptions.includes('expansion') ||
      descriptions.includes('franchise')) {
    return 'chain-expansion';
  }
  
  // Check for existing operator
  if (descriptions.includes('experienced operator') || 
      descriptions.includes('established operator') ||
      descriptions.includes('restaurateur')) {
    return 'existing-operator';
  }
  
  // Check for new operator indicators
  if (descriptions.includes('first restaurant') || 
      descriptions.includes('new business') ||
      descriptions.includes('startup')) {
    return 'new-operator';
  }
  
  return 'unknown';
}

/**
 * Estimate opening timeline in days (positive = days until opening, negative = already opened)
 */
async function estimateTimeline(records: NormalizedRecord[]): Promise<number | undefined> {
  const futureDates = records
    .map(r => parseDate(r.future_date))
    .filter((d): d is Date => d !== null)
    .filter(d => d > new Date()) // Only future dates
    .sort((a, b) => a.getTime() - b.getTime());
  
  if (futureDates.length === 0) return undefined;
  
  const earliestFuture = futureDates[0];
  if (!earliestFuture) return undefined;
  
  const now = new Date();
  const daysUntilOpening = Math.ceil((earliestFuture.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
  
  return daysUntilOpening;
}

/**
 * Detect Type I hood requirements
 */
async function detectTypeIHood(records: NormalizedRecord[]): Promise<boolean> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  return descriptions.includes('type i') || descriptions.includes('type 1') || descriptions.includes('commercial hood');
}

/**
 * Detect multiple cook lines
 */
async function detectMultipleCookLines(records: NormalizedRecord[]): Promise<boolean> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  return descriptions.includes('multiple cook lines') || descriptions.includes('cook stations') || descriptions.includes('cooking lines');
}

/**
 * Detect hot/cold stations
 */
async function detectHotColdStations(records: NormalizedRecord[]): Promise<boolean> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  return descriptions.includes('hot station') || descriptions.includes('cold station') || descriptions.includes('temperature zones');
}

/**
 * Detect multiple printers
 */
async function detectMultiplePrinters(records: NormalizedRecord[]): Promise<boolean> {
  const descriptions = records.map(r => `${r.description || ''} ${r.type || ''}`).join(' ').toLowerCase();
  return descriptions.includes('multiple printers') || descriptions.includes('kitchen printers') || descriptions.includes('station printers');
}

/**
 * Determine business category
 */
function determineBusinessCategory(records: NormalizedRecord[]): string {
  const descriptions = records.map(r => `${r.business_name || ''} ${r.description || ''} ${r.type || ''}`).join(' ');
  
  if (descriptions.toLowerCase().includes('restaurant')) return 'Restaurant';
  if (descriptions.toLowerCase().includes('bar')) return 'Bar';
  if (descriptions.toLowerCase().includes('cafe')) return 'Cafe';
  if (descriptions.toLowerCase().includes('brewery')) return 'Brewery';
  if (descriptions.toLowerCase().includes('winery')) return 'Winery';
  
  return 'Other';
}

/**
 * Generate filter matches
 */
async function generateFilterMatches(
  intelligence: Partial<SpotOnBusinessIntelligence>,
  config: SpotOnFilterConfig
): Promise<SpotOnFilterMatch[]> {
  const matches: SpotOnFilterMatch[] = [];
  
  // Service model filter
  matches.push({
    filter_name: 'service_model',
    matched: intelligence.service_model === 'full-service' || intelligence.service_model === 'fast-casual',
    value: intelligence.service_model,
    confidence: 0.8
  });
  
  // Seat capacity filter
  matches.push({
    filter_name: 'seat_capacity',
    matched: intelligence.seat_capacity ? intelligence.seat_capacity >= config.min_seat_capacity : false,
    value: intelligence.seat_capacity,
    confidence: 0.7
  });
  
  // Square footage filter
  matches.push({
    filter_name: 'square_footage',
    matched: intelligence.square_footage ? intelligence.square_footage >= config.min_square_footage : false,
    value: intelligence.square_footage,
    confidence: 0.6
  });
  
  // Liquor license filter
  matches.push({
    filter_name: 'liquor_license',
    matched: intelligence.liquor_license_type !== 'unknown' && intelligence.liquor_license_type !== undefined,
    value: intelligence.liquor_license_type,
    confidence: 0.9
  });
  
  // Reservation systems filter
  matches.push({
    filter_name: 'reservation_systems',
    matched: Array.isArray(intelligence.reservation_systems) && intelligence.reservation_systems.length > 0,
    value: intelligence.reservation_systems,
    confidence: 0.8
  });
  
  // Timeline filter
  const timelineMatch = intelligence.opening_timeline_days 
    ? intelligence.opening_timeline_days >= config.timeline_window_days[0] && 
      intelligence.opening_timeline_days <= config.timeline_window_days[1]
    : false;
  
  matches.push({
    filter_name: 'timeline_window',
    matched: timelineMatch,
    value: intelligence.opening_timeline_days,
    confidence: 0.7
  });
  
  // Operator type filter
  matches.push({
    filter_name: 'operator_type',
    matched: intelligence.operator_type !== 'unknown',
    value: intelligence.operator_type,
    confidence: 0.75
  });
  
  return matches;
}

/**
 * Calculate SpotOn-specific score
 */
function calculateSpotOnScore(
  filterMatches: SpotOnFilterMatch[],
  config: SpotOnFilterConfig
): number {
  let score = 0;
  let maxScore = 0;
  
  for (const match of filterMatches) {
    const baseWeight = 10;
    let weight = baseWeight;
    
    // Apply specific weights based on filter type
    switch (match.filter_name) {
      case 'service_model':
        {
          const key = String(match.value ?? 'unknown');
          const multiplier = config.service_model_weights?.[key] ?? 1;
          weight = multiplier * baseWeight || baseWeight;
        }
        break;
      case 'operator_type':
        {
          const key = String(match.value ?? 'unknown');
          const multiplier = config.operator_type_weights?.[key] ?? 1;
          weight = multiplier * baseWeight || baseWeight;
        }
        break;
      case 'liquor_license':
        weight = 15; // High priority for liquor licenses
        break;
      case 'seat_capacity':
        weight = 12; // Important for SpotOn fit
        break;
      case 'timeline_window':
        weight = 20; // Critical for sales timing
        break;
    }
    
    maxScore += weight;
    if (match.matched) {
      score += weight * match.confidence;
    }
  }
  
  // Normalize to 100-point scale
  return maxScore > 0 ? Math.round((score / maxScore) * 100) : 0;
}
