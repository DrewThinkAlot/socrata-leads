/**
 * Signal fusion rules for identifying business opening opportunities
 */

import { logger } from '../util/logger.js';
import { addressesMatch } from '../util/address.js';
import { parseDate, isWithinDays, getISOWeek, getPredictedOpenWeek } from '../util/dates.js';
import type { NormalizedRecord, Event, FusionRule } from '../types.js';
import { randomUUID } from 'crypto';

/**
 * Rule A: Building permit + liquor license within 120 days, same address → 80 points
 */
export const ruleA: FusionRule = {
  name: 'Permit + License Combo',
  description: 'Building permit and liquor license within 120 days at same address',
  signal_strength: 80,
  match: (records: NormalizedRecord[]): boolean => {
    const permits = records.filter(r => r.type?.toLowerCase().includes('permit'));
    const licenses = records.filter(r => r.type?.toLowerCase().includes('license') && r.status?.toUpperCase() === 'AAI');
    
    if (permits.length === 0 || licenses.length === 0) {
      return false;
    }
    
    // Check for permits and licenses within 120 days at same address
    for (const permit of permits) {
      const permitDate = parseDate(permit.event_date);
      if (!permitDate) continue;
      
      for (const license of licenses) {
        const licenseDate = parseDate(license.event_date);
        if (!licenseDate) continue;
        
        // Check if within 120 days
        const daysDiff = Math.abs(permitDate.getTime() - licenseDate.getTime()) / (1000 * 60 * 60 * 24);
        if (daysDiff > 120) continue;
        
        // Check if same address
        if (addressesMatch(
          permit.address, permit.lat, permit.lon,
          license.address, license.lat, license.lon,
          100 // 100 meter tolerance
        )) {
          return true;
        }
      }
    }
    
    return false;
  }
};

/**
 * Rule B: Large commercial permit alone within 60 days → 60 points
 */
export const ruleB: FusionRule = {
  name: 'Large Commercial Permit',
  description: 'Large commercial permit indicating build-out within last 60 days',
  signal_strength: 60,
  match: (records: NormalizedRecord[]): boolean => {
    const permits = records.filter(r => r.type?.toLowerCase().includes('permit'));
    
    for (const permit of permits) {
      const permitDate = parseDate(permit.event_date);
      if (!permitDate) continue;
      
      // Check if within last 60 days
      if (!isWithinDays(permitDate, 60)) continue;
      
      // Check if it's a large commercial permit
      const type = permit.type?.toLowerCase() || '';
      const description = permit.description?.toLowerCase() || '';
      
      const isLargeCommercial = 
        type.includes('commercial') ||
        type.includes('restaurant') ||
        type.includes('retail') ||
        description.includes('build-out') ||
        description.includes('tenant improvement') ||
        description.includes('commercial kitchen') ||
        description.includes('restaurant');
      
      if (isLargeCommercial) {
        return true;
      }
    }
    
    return false;
  }
};

/**
 * Rule C: License status AAI/ACT + future start date → 70 points
 */
export const ruleC: FusionRule = {
  name: 'Active License Future Start',
  description: 'License with AAI/ACT status and future start date',
  signal_strength: 70,
  match: (records: NormalizedRecord[]): boolean => {
    const licenses = records.filter(r => r.type?.toLowerCase().includes('license'));
    
    for (const license of licenses) {
      const status = license.status?.toUpperCase();
      if (!status || !['AAI', 'ACT', 'ACTIVE', 'ISSUED'].includes(status)) {
        continue;
      }
      
      const eventDate = parseDate(license.event_date);
      if (!eventDate) continue;
      
      // Check if start date is in the future (within next 90 days)
      const now = new Date();
      const daysDiff = (eventDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24);
      
      if (daysDiff > 0 && daysDiff <= 90) {
        return true;
      }
    }
    
    return false;
  }
};

/**
 * Rule D: Recent AAI status records (for liquor licenses without type field) → 50 points
 */
export const ruleD: FusionRule = {
  name: 'Recent AAI Status Record',
  description: 'Recent record with AAI status indicating approved for issuance',
  signal_strength: 50,
  match: (records: NormalizedRecord[]): boolean => {
    for (const record of records) {
      const status = record.status?.toUpperCase();
      if (status === 'AAI') {
        const eventDate = parseDate(record.event_date);
        if (!eventDate) continue;
        
        // Check if the record is from the last 90 days (recent approvals)
        const now = new Date();
        const daysDiff = (now.getTime() - eventDate.getTime()) / (1000 * 60 * 60 * 24);
        
        if (daysDiff >= 0 && daysDiff <= 90) { // Only last 90 days
          return true;
        }
      }
    }
    
    return false;
  }
};

/**
 * Rule E: Food inspection PASS within 30 days → 90 points (very strong opening signal)
 */
export const ruleE: FusionRule = {
  name: 'Recent Food Inspection Pass',
  description: 'Recent licensing food inspection with PASS within 30 days',
  signal_strength: 75,
  match: (records: NormalizedRecord[]): boolean => {
    const inspections = records.filter(r => {
      const isFood = r.type?.toLowerCase().includes('food inspection') ?? false;
      const isLicense = /license/i.test(r.type || '') || /license/i.test(r.description || '');
      const passed = r.status?.toUpperCase() === 'PASS';
      return isFood && isLicense && passed;
    });
    
    for (const inspection of inspections) {
      const inspectionDate = parseDate(inspection.event_date);
      if (!inspectionDate) continue;
      
      // Check if within last 30 days
      if (isWithinDays(inspectionDate, 30)) {
        return true;
      }
    }
    
    return false;
  }
};

/**
 * Rule F: Building inspection PASSED + liquor license within 60 days → 85 points
 */
export const ruleF: FusionRule = {
  name: 'Building Pass + License Combo',
  description: 'Building inspection PASSED and AAI license within 60 days',
  signal_strength: 70,
  match: (records: NormalizedRecord[]): boolean => {
    const buildingPassed = records.filter(r => 
      r.type?.toLowerCase().includes('building inspection') &&
      r.status?.toUpperCase() === 'PASSED'
    );
    const licenses = records.filter(r => r.type?.toLowerCase().includes('license') && (r.status?.toUpperCase() === 'AAI'));
    
    if (buildingPassed.length === 0 || licenses.length === 0) {
      return false;
    }
    
    for (const building of buildingPassed) {
      const buildingDate = parseDate(building.event_date);
      if (!buildingDate) continue;
      
      for (const license of licenses) {
        const licenseDate = parseDate(license.event_date);
        if (!licenseDate) continue;
        
        // Check if within 60 days
        const daysDiff = Math.abs(buildingDate.getTime() - licenseDate.getTime()) / (1000 * 60 * 60 * 24);
        if (daysDiff <= 60) {
          return true;
        }
      }
    }
    
    return false;
  }
};

/**
 * Rule G: Multi-signal progression (permit → inspection → license) → 95 points
 */
export const ruleG: FusionRule = {
  name: 'Opening Progression Sequence',
  description: 'Complete opening sequence: permit, then inspection, then license approval',
  signal_strength: 85,
  match: (records: NormalizedRecord[]): boolean => {
    const permits = records.filter(r => r.type?.toLowerCase().includes('permit'));
    const inspections = records.filter(r => r.type?.toLowerCase().includes('inspection'));
    const licenses = records.filter(r => r.type?.toLowerCase().includes('license') && (r.status?.toUpperCase() === 'AAI'));
    
    if (permits.length === 0 || inspections.length === 0 || licenses.length === 0) {
      return false;
    }
    
    // Find the most recent of each type
    const getLatestDate = (records: NormalizedRecord[]) => {
      return records
        .map(r => parseDate(r.event_date))
        .filter((d): d is Date => d !== null)
        .sort((a, b) => b.getTime() - a.getTime())[0];
    };
    
    const latestPermit = getLatestDate(permits);
    const latestInspection = getLatestDate(inspections);
    const latestLicense = getLatestDate(licenses);
    
    if (!latestPermit || !latestInspection || !latestLicense) {
      return false;
    }
    
    // Check logical progression: permit → inspection → license (within 180 days total)
    const permitToInspection = (latestInspection.getTime() - latestPermit.getTime()) / (1000 * 60 * 60 * 24);
    const inspectionToLicense = (latestLicense.getTime() - latestInspection.getTime()) / (1000 * 60 * 60 * 24);
    const totalSpan = (latestLicense.getTime() - latestPermit.getTime()) / (1000 * 60 * 60 * 24);
    
    // Require AAI license to be the last step, and recency caps
    const now = new Date();
    const licenseIsFuture = latestLicense.getTime() > now.getTime();

    return permitToInspection >= 0 &&
           inspectionToLicense >= 0 &&
           totalSpan <= 180 &&
           permitToInspection <= 120 &&
           inspectionToLicense <= 60 &&
           licenseIsFuture;
  }
};

/**
 * Rule H: Recent job postings + license/permit within 45 days → 75 points
 */
export const ruleH: FusionRule = {
  name: 'Hiring + License Combo',
  description: 'Recent job postings combined with permits or licenses within 45 days',
  signal_strength: 75,
  match: (records: NormalizedRecord[]): boolean => {
    const jobPostings = records.filter(r => 
      r.type?.toLowerCase().includes('job posting') &&
      r.status?.toUpperCase() === 'HIRING'
    );
    const permits = records.filter(r => 
      r.type?.toLowerCase().includes('permit') || 
      r.type?.toLowerCase().includes('license')
    );
    
    if (jobPostings.length === 0 || permits.length === 0) {
      return false;
    }
    
    for (const job of jobPostings) {
      const jobDate = parseDate(job.event_date);
      if (!jobDate || !isWithinDays(jobDate, 30)) continue; // Recent job postings only
      
      for (const permit of permits) {
        const permitDate = parseDate(permit.event_date);
        if (!permitDate) continue;
        
        // Check if permit/license is within 45 days of job posting
        const daysDiff = Math.abs(jobDate.getTime() - permitDate.getTime()) / (1000 * 60 * 60 * 24);
        if (daysDiff <= 45) {
          return true;
        }
      }
    }
    
    return false;
  }
};

/**
 * All fusion rules (ordered by signal strength, highest first)
 */
export const fusionRules: FusionRule[] = [ruleG, ruleF, ruleE, ruleA, ruleH, ruleC, ruleB, ruleD];

/**
 * Apply fusion rules to a group of normalized records
 */
export function applyFusionRules(
  records: NormalizedRecord[],
  city: string
): Event[] {
  if (records.length === 0) {
    return [];
  }
  
  const events: Event[] = [];
  
  // Group records by address for fusion
  const addressGroups = groupRecordsByAddress(records);
  
  for (const [address, addressRecords] of addressGroups) {
    // Try each fusion rule
    for (const rule of fusionRules) {
      if (rule.match(addressRecords)) {
        const event = createEvent(addressRecords, rule, city, address);
        events.push(event);
        
        logger.debug('Fusion rule matched', {
          rule: rule.name,
          address,
          recordCount: addressRecords.length,
          signalStrength: rule.signal_strength,
        });
        
        // Only apply the highest-scoring rule per address
        break;
      }
    }
  }
  
  return events;
}

/**
 * Group records by normalized address
 */
function groupRecordsByAddress(records: NormalizedRecord[]): Map<string, NormalizedRecord[]> {
  const groups = new Map<string, NormalizedRecord[]>();
  
  for (const record of records) {
    if (!record.address) continue;
    
    // Find existing group with matching address
    let matchingKey: string | null = null;
    
    for (const [existingAddress, existingRecords] of groups) {
      const firstRecord = existingRecords[0];
      if (firstRecord && addressesMatch(
        record.address, record.lat, record.lon,
        firstRecord.address, firstRecord.lat, firstRecord.lon,
        100 // 100 meter tolerance
      )) {
        matchingKey = existingAddress;
        break;
      }
    }
    
    if (matchingKey) {
      groups.get(matchingKey)!.push(record);
    } else {
      groups.set(record.address, [record]);
    }
  }
  
  return groups;
}

/**
 * Create an event from matched records and rule
 */
function createEvent(
  records: NormalizedRecord[],
  rule: FusionRule,
  city: string,
  address: string
): Event {
  // Find the most recent event date for prediction
  const dates = records
    .map(r => parseDate(r.event_date))
    .filter((d): d is Date => d !== null)
    .sort((a, b) => b.getTime() - a.getTime());
  
  const mostRecentDate = dates[0] || new Date();
  
  // Determine business name from records
  const businessName = records
    .map(r => r.business_name)
    .filter(name => name && name.trim().length > 0)
    .find(name => name && !name.toLowerCase().includes('permit')) || undefined;
  
  // Predict opening week based on most recent event and type
  const mostRecentRecord = records.find(r => {
    const recordDate = parseDate(r.event_date);
    return recordDate && recordDate.getTime() === mostRecentDate.getTime();
  });
  
  const predictedOpenWeek = getPredictedOpenWeek(
    mostRecentDate,
    mostRecentRecord?.type || 'permit'
  );
  
  return {
    event_id: randomUUID(),
    city,
    address,
    name: businessName,
    predicted_open_week: predictedOpenWeek,
    signal_strength: rule.signal_strength,
    evidence: records,
    created_at: new Date().toISOString(),
  };
}

/**
 * Get fusion rule by name
 */
export function getFusionRule(name: string): FusionRule | undefined {
  return fusionRules.find(rule => rule.name === name);
}

/**
 * Validate fusion rules
 */
export function validateFusionRules(): boolean {
  for (const rule of fusionRules) {
    if (!rule.name || !rule.description || !rule.match) {
      logger.error('Invalid fusion rule', { rule });
      return false;
    }
    
    if (rule.signal_strength < 0 || rule.signal_strength > 100) {
      logger.error('Invalid signal strength', { rule: rule.name, strength: rule.signal_strength });
      return false;
    }
  }
  
  return true;
}