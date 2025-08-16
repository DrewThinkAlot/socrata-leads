/**
 * Keyset pagination utilities for Socrata API
 */

import type { SodaClient } from './client.js';
import type { SocrataQueryParams } from '../types.js';
import { buildIncrementalQuery } from './query.js';
import { logger } from '../util/logger.js';

/**
 * Options for paginated data fetching
 */
export interface PaginationOptions {
  selectFields: string[];
  whereClause?: string;
  orderByField: string;
  watermarkField: string;
  lastWatermark?: string | null;
  pageSize?: number;
  maxPages?: number;
}

/**
 * Result from paginated fetch
 */
export interface PaginationResult {
  records: any[];
  lastWatermark: string | null;
  totalRecords: number;
  pagesProcessed: number;
  hasMore: boolean;
}

/**
 * Paginate through Socrata dataset using keyset pagination
 */
export async function* paginateDataset(
  client: SodaClient,
  datasetPath: string,
  options: PaginationOptions
): AsyncGenerator<any[], void, unknown> {
  const {
    selectFields,
    whereClause,
    orderByField,
    watermarkField,
    pageSize = 1000,
    maxPages = Infinity,
  } = options;

  let currentWatermark = options.lastWatermark || null;
  let pagesProcessed = 0;
  let totalRecords = 0;

  logger.info('Starting paginated fetch', {
    datasetPath,
    watermarkField,
    lastWatermark: currentWatermark,
    pageSize,
  });

  while (pagesProcessed < maxPages) {
    // Build query for current page
    const queryParams = buildIncrementalQuery(
      selectFields,
      whereClause,
      orderByField,
      watermarkField,
      currentWatermark,
      pageSize
    );

    logger.debug('Fetching page', {
      page: pagesProcessed + 1,
      watermark: currentWatermark,
      queryParams,
    });

    // Fetch page
    const records = await client.getJson({
      baseUrl: '', // Will be ignored by client
      path: datasetPath,
      params: queryParams,
    });

    // If no records, we're done
    if (records.length === 0) {
      logger.info('No more records found, pagination complete', {
        totalRecords,
        pagesProcessed,
      });
      break;
    }

    // Update watermark to the last record's watermark value
    const lastRecord = records[records.length - 1];
    const newWatermark = extractWatermark(lastRecord, watermarkField);
    
    if (newWatermark === null) {
      logger.warn('Could not extract watermark from last record', {
        watermarkField,
        lastRecord,
      });
      break;
    }

    // Check if we've reached the end (same watermark as previous)
    if (currentWatermark === newWatermark) {
      logger.info('Watermark unchanged, pagination complete', {
        watermark: currentWatermark,
        totalRecords,
        pagesProcessed,
      });
      break;
    }

    currentWatermark = newWatermark;
    totalRecords += records.length;
    pagesProcessed++;

    logger.debug('Page fetched successfully', {
      page: pagesProcessed,
      recordCount: records.length,
      newWatermark: currentWatermark,
      totalRecords,
    });

    // Yield the records
    yield records;

    // If we got fewer records than requested, we're likely at the end
    if (records.length < pageSize) {
      logger.info('Received partial page, pagination likely complete', {
        recordCount: records.length,
        pageSize,
        totalRecords,
        pagesProcessed,
      });
      break;
    }
  }

  logger.info('Pagination completed', {
    totalRecords,
    pagesProcessed,
    finalWatermark: currentWatermark,
  });
}

/**
 * Fetch all records using pagination (non-streaming)
 */
export async function fetchAllRecords(
  client: SodaClient,
  datasetPath: string,
  options: PaginationOptions
): Promise<PaginationResult> {
  const allRecords: any[] = [];
  let lastWatermark: string | null = null;
  let pagesProcessed = 0;

  for await (const records of paginateDataset(client, datasetPath, options)) {
    allRecords.push(...records);
    pagesProcessed++;
    
    // Update watermark from last record
    if (records.length > 0) {
      const lastRecord = records[records.length - 1];
      lastWatermark = extractWatermark(lastRecord, options.watermarkField);
    }
  }

  return {
    records: allRecords,
    lastWatermark,
    totalRecords: allRecords.length,
    pagesProcessed,
    hasMore: false, // We fetched everything
  };
}

/**
 * Fetch records in batches with callback processing
 */
export async function processBatches(
  client: SodaClient,
  datasetPath: string,
  options: PaginationOptions,
  processor: (records: any[], batchInfo: { page: number; watermark: string | null }) => Promise<void>
): Promise<PaginationResult> {
  let totalRecords = 0;
  let pagesProcessed = 0;
  let lastWatermark: string | null = null;

  for await (const records of paginateDataset(client, datasetPath, options)) {
    // Update watermark from last record
    if (records.length > 0) {
      const lastRecord = records[records.length - 1];
      lastWatermark = extractWatermark(lastRecord, options.watermarkField);
    }

    // Process the batch
    await processor(records, {
      page: pagesProcessed + 1,
      watermark: lastWatermark,
    });

    totalRecords += records.length;
    pagesProcessed++;
  }

  return {
    records: [], // Not storing records when processing in batches
    lastWatermark,
    totalRecords,
    pagesProcessed,
    hasMore: false,
  };
}

/**
 * Extract watermark value from a record
 */
function extractWatermark(record: any, watermarkField: string): string | null {
  if (!record || typeof record !== 'object') {
    return null;
  }

  const value = record[watermarkField];
  
  if (value === null || value === undefined) {
    return null;
  }

  return String(value);
}

/**
 * Estimate total records (for progress tracking)
 */
export async function estimateRecordCount(
  client: SodaClient,
  datasetPath: string,
  whereClause?: string
): Promise<number> {
  try {
    const params: SocrataQueryParams = {
      $select: 'count(*) as count',
    };

    if (whereClause) {
      params.$where = whereClause;
    }

    const result = await client.getJson({
      baseUrl: '',
      path: datasetPath,
      params,
    });

    if (result.length > 0 && result[0].count) {
      return parseInt(result[0].count, 10);
    }
  } catch (error) {
    logger.warn('Could not estimate record count', { error });
  }

  return 0;
}

/**
 * Check if more records are available after a given watermark
 */
export async function hasMoreRecords(
  client: SodaClient,
  datasetPath: string,
  options: Pick<PaginationOptions, 'selectFields' | 'whereClause' | 'watermarkField'> & {
    watermark: string;
  }
): Promise<boolean> {
  try {
    const queryParams = buildIncrementalQuery(
      ['*'], // Just check existence
      options.whereClause,
      options.watermarkField,
      options.watermarkField,
      options.watermark,
      1 // Just need one record
    );

    const records = await client.getJson({
      baseUrl: '',
      path: datasetPath,
      params: queryParams,
    });

    return records.length > 0;
  } catch (error) {
    logger.warn('Could not check for more records', { error });
    return false;
  }
}