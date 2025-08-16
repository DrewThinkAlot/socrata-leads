#!/usr/bin/env node

/**
 * Business Detection Script - Identify truly new vs existing businesses
 * 
 * This script analyzes all normalized records to build comprehensive business profiles
 * and determine which addresses represent new business openings vs existing operations.
 */

import { config } from 'dotenv';
import { createStorage } from './storage/index.js';
import { logger } from './util/logger.js';
import { parseDate, getAgeInDays } from './util/dates.js';
import { analyzeDescription } from './util/llm.js';
import type { NormalizedRecord } from './types.js';

// Load environment variables
config();

/**
 * Comprehensive business profile for an address
 */
interface BusinessProfile {
  address: string;
  names: Set<string>;
  firstSeen: Date;
  lastSeen: Date;
  signalCount: number;
  hasOperationalHistory: boolean;
  isLikelyNew: boolean;
  datasets: Set<string>;
  inspectionCount: number;
  licenseCount: number;
  permitCount: number;
  businessLicenseStatus: string | null;
  oldestInspection: Date | null;
  recentActivity: NormalizedRecord[];
}

/**
 * Detect if businesses are truly new or existing based on comprehensive analysis
 */
export async function detectNewBusinesses(city: string): Promise<Map<string, BusinessProfile>> {
  const storage = await createStorage();
  const normalized = await storage.getNormalizedByCity(city);
  
  logger.info('Analyzing business profiles', {
    city,
    totalRecords: normalized.length
  });
  
  // Build comprehensive business profiles
  const businessProfiles = new Map<string, BusinessProfile>();
  
  for (const record of normalized) {
    if (!record.address) continue;
    
    const profile = businessProfiles.get(record.address) || {
      address: record.address,
      names: new Set(),
      firstSeen: new Date(),
      lastSeen: new Date(0),
      signalCount: 0,
      hasOperationalHistory: false,
      isLikelyNew: true,
      datasets: new Set(),
      inspectionCount: 0,
      licenseCount: 0,
      permitCount: 0,
      businessLicenseStatus: null,
      oldestInspection: null,
      recentActivity: []
    };
    
    // Update profile with record data
    if (record.business_name) {
      profile.names.add(record.business_name);
    }
    
    const eventDate = parseDate(record.event_date);
    if (eventDate) {
      if (eventDate < profile.firstSeen) profile.firstSeen = eventDate;
      if (eventDate > profile.lastSeen) profile.lastSeen = eventDate;
    }
    
    profile.signalCount++;
    profile.datasets.add(record.dataset || 'unknown');
    
    // Track activity by dataset type
    const dataset = record.dataset?.toLowerCase() || '';
    const type = record.type?.toLowerCase() || '';
    const status = record.status?.toLowerCase() || '';
    
    if (dataset === 'food_inspections') {
      profile.inspectionCount++;
      if (!profile.oldestInspection || (eventDate && eventDate < profile.oldestInspection)) {
        profile.oldestInspection = eventDate;
      }
      
      // Operational inspections (not licensing) indicate existing business
      if (!type.includes('license')) {
        profile.hasOperationalHistory = true;
      }
    }
    
    if (dataset === 'liquor_licenses') {
      profile.licenseCount++;
    }
    
    if (dataset === 'building_permits') {
      profile.permitCount++;
    }
    
    if (dataset === 'business_licenses') {
      profile.businessLicenseStatus = record.status || null;
      // Active business license = existing business
      if (status === 'aac' || status === 'active') {
        profile.hasOperationalHistory = true;
      }
    }
    
    // Keep recent activity for analysis
    if (getAgeInDays(eventDate || new Date()) <= 90) {
      profile.recentActivity.push(record);
    }
    
    businessProfiles.set(record.address, profile);
  }
  
  // Analyze each business profile to determine if truly new
  for (const [address, profile] of businessProfiles) {
    profile.isLikelyNew = await analyzeBusinessProfile(profile);
  }
  
  await storage.close();
  return businessProfiles;
}

/**
 * Analyze a business profile to determine if it's likely a new business
 */
async function analyzeBusinessProfile(profile: BusinessProfile): Promise<boolean> {
  const now = new Date();
  
  // Strong indicators of EXISTING business
  if (profile.hasOperationalHistory) {
    return false;
  }
  
  // Multiple inspections over time = existing
  if (profile.inspectionCount >= 3) {
    const timeSpan = profile.oldestInspection ? 
      (now.getTime() - profile.oldestInspection.getTime()) / (1000 * 60 * 60 * 24) : 0;
    if (timeSpan > 90) {
      return false;
    }
  }
  
  // Long history = existing
  const ageInDays = getAgeInDays(profile.firstSeen);
  if (ageInDays > 365) {
    return false;
  }
  
  // Too many signals over time = likely existing
  if (profile.signalCount > 10 && ageInDays > 180) {
    return false;
  }
  
  // Active business license = existing
  if (profile.businessLicenseStatus === 'AAC' || profile.businessLicenseStatus === 'ACTIVE') {
    return false;
  }
  
  // Analyze recent activity patterns with LLM
  if (profile.recentActivity.length > 0) {
    const hasNewOpeningSignals = await analyzeRecentActivity(profile.recentActivity);
    if (!hasNewOpeningSignals) {
      return false;
    }
  }
  
  // If we get here, likely a new business
  return true;
}

/**
 * Use LLM to analyze recent activity for new opening indicators
 */
async function analyzeRecentActivity(records: NormalizedRecord[]): Promise<boolean> {
  try {
    // Look for opening-related keywords and patterns
    const descriptions = records
      .map(r => `${r.type || ''} ${r.description || ''}`)
      .join(' ')
      .toLowerCase();
    
    // Simple heuristics first
    const openingKeywords = ['grand opening', 'opening soon', 'new location', 'build-out', 'tenant improvement', 'new construction'];
    const renewalKeywords = ['renewal', 'transfer', 'change of ownership', 'reinspection', 're-inspection', 'maintenance', 'repair'];
    
    const hasOpeningSignals = openingKeywords.some(keyword => descriptions.includes(keyword));
    const hasRenewalSignals = renewalKeywords.some(keyword => descriptions.includes(keyword));
    
    // If clear renewal signals, not new
    if (hasRenewalSignals && !hasOpeningSignals) {
      return false;
    }
    
    // If clear opening signals, likely new
    if (hasOpeningSignals) {
      return true;
    }
    
    // For ambiguous cases, use LLM analysis
    for (const record of records.slice(0, 3)) { // Analyze up to 3 recent records
      if (record.description || record.business_name) {
        const analysis = await analyzeDescription(
          record.description || '',
          record.business_name
        );
        
        // If LLM indicates high business potential and recent, likely new
        if (analysis.confidence > 70) {
          return true;
        }
      }
    }
    
    return true; // Default to new if unclear
  } catch (error) {
    logger.warn('LLM analysis failed for recent activity', { error });
    return true; // Default to new on error
  }
}

/**
 * Main function to analyze and report
 */
async function main() {
  const city = process.argv[2] || 'chicago';
  
  logger.info('Starting new business detection', { city });
  
  try {
    const profiles = await detectNewBusinesses(city);
    
    const newBusinesses = Array.from(profiles.values()).filter(p => p.isLikelyNew);
    const existingBusinesses = Array.from(profiles.values()).filter(p => !p.isLikelyNew);
    
    logger.info('Business analysis complete', {
      city,
      totalAddresses: profiles.size,
      newBusinesses: newBusinesses.length,
      existingBusinesses: existingBusinesses.length,
      newBusinessPercentage: Math.round((newBusinesses.length / profiles.size) * 100)
    });
    
    // Log detailed examples
    console.log('\n🆕 LIKELY NEW BUSINESSES:');
    newBusinesses.slice(0, 10).forEach((p, i) => {
      const names = Array.from(p.names).join(', ') || 'Unknown';
      const age = getAgeInDays(p.firstSeen);
      console.log(`${i+1}. ${p.address}`);
      console.log(`   Names: ${names}`);
      console.log(`   Signals: ${p.signalCount} (${Array.from(p.datasets).join(', ')})`);
      console.log(`   Age: ${age} days`);
      console.log(`   Recent activity: ${p.recentActivity.length} records`);
      console.log('');
    });
    
    console.log('\n🏢 LIKELY EXISTING BUSINESSES:');
    existingBusinesses.slice(0, 5).forEach((p, i) => {
      const names = Array.from(p.names).join(', ') || 'Unknown';
      const age = getAgeInDays(p.firstSeen);
      console.log(`${i+1}. ${p.address}`);
      console.log(`   Names: ${names}`);
      console.log(`   Signals: ${p.signalCount} (${Array.from(p.datasets).join(', ')})`);
      console.log(`   Age: ${age} days`);
      console.log(`   Inspections: ${p.inspectionCount}, Licenses: ${p.licenseCount}`);
      console.log(`   Status: ${p.businessLicenseStatus || 'Unknown'}`);
      console.log('');
    });
    
  } catch (error) {
    logger.error('Business detection failed', { error, city });
    process.exit(1);
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    logger.error('Failed to detect new businesses', { error });
    process.exit(1);
  });
}
