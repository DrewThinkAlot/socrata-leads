/**
 * CSV export utilities
 */

import { stringify } from 'csv-stringify';
import type { Lead } from '../types.js';

export interface CsvExportOptions {
  headers?: boolean;
  delimiter?: string;
  quote?: string;
}

/**
 * Convert leads to CSV format
 */
export async function leadsToCSV(
  leads: Lead[],
  options: CsvExportOptions = {}
): Promise<string> {
  const {
    headers = true,
    delimiter = ',',
    quote = '"',
  } = options;

  const records = leads.map(lead => {
    // Extract restaurant type from evidence
    const allEvidence = lead.evidence.flatMap(e => e.evidence);
    const businessNames = allEvidence.map(r => r.business_name?.toLowerCase() || '').join(' ');
    const descriptions = allEvidence.map(r => r.description?.toLowerCase() || '').join(' ');
    const types = allEvidence.map(r => r.type?.toLowerCase() || '').join(' ');
    const combinedText = `${businessNames} ${descriptions} ${types}`;
    
    let restaurantType = 'unknown';
    if (['mcdonald', 'burger king', 'kfc', 'fast food', 'drive thru', 'quick service'].some(k => combinedText.includes(k))) {
      restaurantType = 'fast-food';
    } else if (['fine dining', 'full service', 'wine list', 'sommelier', 'upscale'].some(k => combinedText.includes(k))) {
      restaurantType = 'full-service';
    } else if (['chipotle', 'panera', 'fast casual', 'counter order', 'artisan'].some(k => combinedText.includes(k))) {
      restaurantType = 'fast-casual';
    }
    
    // Calculate timeline confidence based on signal strength
    const avgSignalStrength = lead.evidence.reduce((sum, e) => sum + e.signal_strength, 0) / lead.evidence.length;
    const timelineConfidence = Math.round(avgSignalStrength * 0.8 + 20); // Convert to percentage
    
    return {
      name: lead.name || '',
      address: lead.address,
      restaurant_type: restaurantType,
      predicted_open_week: lead.evidence[0]?.predicted_open_week || '',
      score: lead.score,
      project_stage: lead.project_stage || '',
      days_remaining: lead.days_remaining || '',
      stage_confidence: lead.stage_confidence || '',
      days_confidence: lead.days_confidence || '',
      timeline_confidence: timelineConfidence,
      evidence_count: lead.evidence.length,
      permit_types: [...new Set(allEvidence.map(r => r.type).filter(Boolean))].join('; '),
      evidence_links: allEvidence
        .map(record => record.source_link)
        .filter(link => link && link !== ':self')
        .join('; '),
      // SpotOn Intelligence Fields
      spoton_score: lead.spoton_intelligence?.spoton_score || 0,
      spoton_service_model: lead.spoton_intelligence?.service_model || 'unknown',
      spoton_operator_type: lead.spoton_intelligence?.operator_type || 'unknown',
      spoton_liquor_license_type: lead.spoton_intelligence?.liquor_license_type || 'unknown',
      spoton_seat_capacity: lead.spoton_intelligence?.seat_capacity || 0,
      spoton_square_footage: lead.spoton_intelligence?.square_footage || 0,
      spoton_kitchen_complexity: lead.spoton_intelligence?.kitchen_complexity || 'unknown',
      spoton_reservation_systems: Array.isArray(lead.spoton_intelligence?.reservation_systems) 
        ? lead.spoton_intelligence.reservation_systems.join('; ') 
        : '',
      spoton_is_pop_up_vendor: lead.spoton_intelligence?.is_pop_up_vendor || false,
      spoton_opening_timeline_days: lead.spoton_intelligence?.opening_timeline_days || 0,
      spoton_filter_matches: Array.isArray(lead.spoton_intelligence?.filter_matches)
        ? lead.spoton_intelligence.filter_matches.map(m => `${m.filter_name}:${m.matched}`).join('; ')
        : '',
      city: lead.city,
      phone: lead.phone || '',
      email: lead.email || '',
      created_at: lead.created_at,
    };
  });

  return new Promise((resolve, reject) => {
    const options_obj: any = {
      header: headers,
      delimiter,
      quote,
    };
    
    if (headers) {
      options_obj.columns = {
        name: 'Business Name',
        address: 'Address',
        restaurant_type: 'Restaurant Type',
        predicted_open_week: 'Predicted Open Week',
        score: 'Score',
        project_stage: 'Project Stage',
        days_remaining: 'Days Remaining',
        stage_confidence: 'Stage Confidence',
        days_confidence: 'Days Confidence',
        timeline_confidence: 'Timeline Confidence (%)',
        evidence_count: 'Evidence Count',
        permit_types: 'Permit Types',
        evidence_links: 'Evidence Links',
        spoton_score: 'SpotOn Score',
        spoton_service_model: 'SpotOn Service Model',
        spoton_operator_type: 'SpotOn Operator Type',
        spoton_liquor_license_type: 'SpotOn Liquor License Type',
        spoton_seat_capacity: 'SpotOn Seat Capacity',
        spoton_square_footage: 'SpotOn Square Footage',
        spoton_kitchen_complexity: 'SpotOn Kitchen Complexity',
        spoton_reservation_systems: 'SpotOn Reservation Systems',
        spoton_is_pop_up_vendor: 'SpotOn Is Pop-up Vendor',
        spoton_opening_timeline_days: 'SpotOn Opening Timeline Days',
        spoton_filter_matches: 'SpotOn Filter Matches',
        city: 'City',
        phone: 'Phone',
        email: 'Email',
        created_at: 'Created At',
      };
    }
    
    stringify(records, options_obj, (err, output) => {
      if (err) {
        reject(err);
      } else {
        resolve(output);
      }
    });
  });
}

/**
 * Convert any array of objects to CSV
 */
export async function objectsToCSV<T extends Record<string, any>>(
  objects: T[],
  options: CsvExportOptions = {}
): Promise<string> {
  const {
    headers = true,
    delimiter = ',',
    quote = '"',
  } = options;

  return new Promise((resolve, reject) => {
    stringify(objects, {
      header: headers,
      delimiter,
      quote,
    }, (err, output) => {
      if (err) {
        reject(err);
      } else {
        resolve(output);
      }
    });
  });
}

/**
 * Escape CSV field value
 */
export function escapeCsvField(value: any): string {
  if (value === null || value === undefined) {
    return '';
  }
  
  const str = String(value);
  
  // If the field contains comma, quote, or newline, wrap in quotes and escape quotes
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  
  return str;
}

/**
 * Create CSV header row from object keys
 */
export function createCsvHeader(obj: Record<string, any>): string {
  return Object.keys(obj)
    .map(key => escapeCsvField(key))
    .join(',');
}

/**
 * Convert object to CSV row
 */
export function objectToCsvRow(obj: Record<string, any>): string {
  return Object.values(obj)
    .map(value => escapeCsvField(value))
    .join(',');
}
