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

  const records = leads.map(lead => ({
    name: lead.name || '',
    address: lead.address,
    predicted_open_week: lead.evidence[0]?.predicted_open_week || '',
    score: lead.score,
    evidence_links: lead.evidence
      .flatMap(event => event.evidence)
      .map(record => record.source_link)
      .filter(link => link && link !== ':self')
      .join('; '),
    city: lead.city,
    phone: lead.phone || '',
    email: lead.email || '',
    created_at: lead.created_at,
  }));

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
        predicted_open_week: 'Predicted Open Week',
        score: 'Score',
        evidence_links: 'Evidence Links',
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
