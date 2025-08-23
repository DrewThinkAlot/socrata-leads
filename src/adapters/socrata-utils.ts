/**
 * Shared Socrata adapter utilities
 */

import type { DatasetConfig } from '../config/index.js';

/**
 * Generate a stable record ID from the record data
 */
export function generateRecordId(record: any, dataset: DatasetConfig, city: string): string {
  // Try to use Socrata's built-in :id field first
  if (record[':id']) {
    return `${city}-${dataset.id}-${record[':id']}`;
  }

  // Fall back to using the watermark field as ID
  const watermarkValue = record[dataset.watermark_field];
  if (watermarkValue) {
    const hash = simpleHash(JSON.stringify(record));
    return `${city}-${dataset.id}-${watermarkValue}-${hash}`;
  }

  // Last resort: hash the entire record
  const hash = simpleHash(JSON.stringify(record));
  return `${city}-${dataset.id}-${hash}`;
}

/**
 * Extract watermark value from record
 */
export function extractWatermark(record: any, watermarkField: string): string {
  const value = record[watermarkField];
  if (value === null || value === undefined) {
    return new Date().toISOString(); // Fallback to current time
  }
  return String(value);
}

/**
 * Simple hash function for generating IDs
 */
export function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(36);
}
