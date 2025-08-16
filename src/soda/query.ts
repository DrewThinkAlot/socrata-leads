/**
 * SoQL (Socrata Query Language) query builder utilities
 */

import type { SocrataQueryParams } from '../types.js';

/**
 * Build a SoQL SELECT clause
 */
export function buildSelect(fields: string[]): string {
  if (fields.length === 0) {
    return '*';
  }
  
  // Escape field names that might contain spaces or special characters
  const escapedFields = fields.map(field => {
    if (field.includes(' ') || field.includes('(') || field.includes(',')) {
      return field; // Assume it's already a complex expression
    }
    return field;
  });
  
  return escapedFields.join(', ');
}

/**
 * Build a SoQL ORDER BY clause
 */
export function buildOrderBy(field: string, direction: 'ASC' | 'DESC' = 'ASC'): string {
  return `${field} ${direction}`;
}

/**
 * Escape a string value for use in SoQL
 */
export function escapeString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/**
 * Format a date for SoQL queries
 */
export function formatDate(date: Date): string {
  // Format as YYYY-MM-DDTHH:MM:SS without milliseconds and Z suffix using UTC
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  
  return `'${year}-${month}-${day}T${hours}:${minutes}:${seconds}'`;
}

/**
 * Build a WHERE clause from multiple conditions
 */
export function buildWhere(conditions: string[]): string {
  if (conditions.length === 0) {
    return '';
  }
  return conditions.join(' AND ');
}

/**
 * Build a watermark condition for incremental processing
 */
export function buildWatermarkCondition(
  watermarkField: string, 
  lastWatermark: string | null,
  operator: '>' | '>=' = '>'
): string {
  if (!lastWatermark) {
    return '';
  }
  
  // Check if it looks like a valid ISO date string (YYYY-MM-DDTHH:MM:SS)
  const isoDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
  if (isoDateRegex.test(lastWatermark)) {
    const date = new Date(lastWatermark);
    if (!isNaN(date.getTime()) && date.toISOString().startsWith(lastWatermark.substring(0, 19))) {
      return `${watermarkField} ${operator} ${formatDate(date)}`;
    }
  }
  
  // Fall back to string comparison
  return `${watermarkField} ${operator} ${escapeString(lastWatermark)}`;
}

/**
 * Build a date range condition
 */
export function buildDateRange(
  field: string,
  startDate?: Date,
  endDate?: Date
): string {
  const conditions: string[] = [];
  
  if (startDate) {
    conditions.push(`${field} >= ${formatDate(startDate)}`);
  }
  
  if (endDate) {
    conditions.push(`${field} <= ${formatDate(endDate)}`);
  }
  
  return conditions.join(' AND ');
}

/**
 * Build an IN condition for multiple values
 */
export function buildInCondition(field: string, values: string[]): string {
  if (values.length === 0) {
    return '';
  }
  
  const escapedValues = values.map(escapeString).join(', ');
  return `${field} IN (${escapedValues})`;
}

/**
 * Build a LIKE condition for text search
 */
export function buildLikeCondition(field: string, pattern: string): string {
  return `upper(${field}) LIKE upper(${escapeString(pattern)})`;
}

/**
 * Build complete SoQL query parameters
 */
export function buildQueryParams(options: {
  select?: string[];
  where?: string[];
  orderBy?: string;
  limit?: number;
  offset?: number;
  watermarkField?: string;
  lastWatermark?: string | null;
}): SocrataQueryParams {
  const params: SocrataQueryParams = {};
  
  // SELECT clause
  if (options.select && options.select.length > 0) {
    params.$select = buildSelect(options.select);
  }
  
  // WHERE clause
  const whereConditions: string[] = [];
  
  if (options.where) {
    whereConditions.push(...options.where);
  }
  
  if (options.watermarkField && options.lastWatermark) {
    const watermarkCondition = buildWatermarkCondition(
      options.watermarkField,
      options.lastWatermark
    );
    if (watermarkCondition) {
      whereConditions.push(watermarkCondition);
    }
  }
  
  if (whereConditions.length > 0) {
    params.$where = buildWhere(whereConditions);
  }
  
  // ORDER BY clause
  if (options.orderBy) {
    params.$order = options.orderBy;
  }
  
  // LIMIT
  if (options.limit !== undefined) {
    params.$limit = options.limit;
  }
  
  // OFFSET
  if (options.offset !== undefined) {
    params.$offset = options.offset;
  }
  
  return params;
}

/**
 * Build query for incremental data extraction
 */
export function buildIncrementalQuery(
  selectFields: string[],
  whereClause: string | undefined,
  orderByField: string,
  watermarkField: string,
  lastWatermark: string | null,
  limit: number = 1000
): SocrataQueryParams {
  const whereConditions: string[] = [];
  
  if (whereClause) {
    whereConditions.push(whereClause);
  }
  
  return buildQueryParams({
    select: selectFields,
    where: whereConditions,
    orderBy: buildOrderBy(orderByField, 'ASC'),
    watermarkField,
    lastWatermark,
    limit,
  });
}

/**
 * Build query for date-based filtering
 */
export function buildDateFilterQuery(
  selectFields: string[],
  whereClause: string | undefined,
  orderByField: string,
  dateField: string,
  sinceDate?: Date,
  limit: number = 1000
): SocrataQueryParams {
  const whereConditions: string[] = [];
  
  if (whereClause) {
    whereConditions.push(whereClause);
  }
  
  if (sinceDate) {
    whereConditions.push(buildDateRange(dateField, sinceDate));
  }
  
  return buildQueryParams({
    select: selectFields,
    where: whereConditions,
    orderBy: buildOrderBy(orderByField, 'ASC'),
    limit,
  });
}

/**
 * Validate SoQL field name
 */
export function isValidFieldName(fieldName: string): boolean {
  // Basic validation - field names should be alphanumeric with underscores
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(fieldName);
}

/**
 * Sanitize field name for SoQL
 */
export function sanitizeFieldName(fieldName: string): string {
  return fieldName.replace(/[^a-zA-Z0-9_]/g, '_');
}
