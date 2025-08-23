/**
 * Optimized Socrata adapter with parallel processing and batch operations
 */

import { createSodaClient } from '../soda/client.js';
import { paginateDataset } from '../soda/paginate.js';
import { buildDatasetUrl, getAppToken, type CityConfig, type DatasetConfig } from '../config/index.js';
import { logger } from '../util/logger.js';
import { generateRecordId, extractWatermark, simpleHash } from './socrata-utils.js';
import type { Storage } from '../types.js';

/**
 * Optimized Socrata data source adapter
 */
export class OptimizedSocrataAdapter {
  private cityConfig: CityConfig;
  private storage: Storage;
  private checkpointCache: Map<string, string> = new Map();
  private readonly MAX_CONCURRENT_REQUESTS = 8;
  private readonly BATCH_SIZE = 5000;
  private readonly DB_BATCH_SIZE = 500;
  // Optional sink for publishing raw batches (e.g., to a queue)
  private sink: ((rawRecords: Array<{ id: string; city: string; dataset: string; watermark: string; payload: any }>) => Promise<void>) | undefined;

  constructor(cityConfig: CityConfig, storage: Storage, options?: { sink?: (rawRecords: Array<{ id: string; city: string; dataset: string; watermark: string; payload: any }>) => Promise<void> }) {
    this.cityConfig = cityConfig;
    this.storage = storage;
    this.sink = options?.sink;
  }

  /**
   * Extract data from a Socrata dataset with optimizations
   */
  async extractDataset(
    datasetName: string,
    options: {
      sinceDate?: Date;
      maxRecords?: number;
    } = {}
  ): Promise<{ recordCount: number; lastWatermark: string | null }> {
    const dataset = this.cityConfig.datasets[datasetName];
    if (!dataset) {
      throw new Error(`Dataset '${datasetName}' not found in city '${this.cityConfig.city}' configuration`);
    }

    logger.info('Starting optimized Socrata data extraction', {
      city: this.cityConfig.city,
      dataset: datasetName,
      datasetId: dataset.id,
      sinceDate: options.sinceDate?.toISOString(),
    });

    const client = createSodaClient(this.cityConfig.base_url, getAppToken(this.cityConfig));
    const lastWatermark = await this.getCachedCheckpoint(this.cityConfig.city, datasetName);
    const datasetPath = `/resource/${dataset.id}.json`;

    let recordCount = 0;
    let finalWatermark: string | null = lastWatermark;

    try {
      const paginationOptions = {
        selectFields: dataset.select,
        orderByField: dataset.order_by,
        watermarkField: dataset.watermark_field,
        lastWatermark,
        pageSize: Math.min(this.BATCH_SIZE, options.maxRecords || this.BATCH_SIZE),
        maxPages: options.maxRecords ? Math.ceil(options.maxRecords / this.BATCH_SIZE) : undefined,
      } as any;

      if (dataset.where) {
        paginationOptions.whereClause = dataset.where;
      }

      const allRecords: any[] = [];
      
      for await (const records of paginateDataset(client, datasetPath, paginationOptions)) {
        allRecords.push(...records);
        recordCount += records.length;

        if (records.length > 0) {
          const lastRecord = records[records.length - 1];
          finalWatermark = extractWatermark(lastRecord, dataset.watermark_field);
        }

        // Process in batches to avoid memory issues
        if (allRecords.length >= this.DB_BATCH_SIZE) {
          await this.processBatch(allRecords, datasetName, dataset);
          allRecords.length = 0; // Clear array
        }

        if (options.maxRecords && recordCount >= options.maxRecords) {
          break;
        }
      }

      // Process remaining records
      if (allRecords.length > 0) {
        await this.processBatch(allRecords, datasetName, dataset);
      }

      // Update checkpoint
      if (finalWatermark && finalWatermark !== lastWatermark) {
        await this.setCachedCheckpoint(this.cityConfig.city, datasetName, finalWatermark);
      }

      logger.info('Optimized Socrata data extraction completed', {
        city: this.cityConfig.city,
        dataset: datasetName,
        recordCount,
        finalWatermark,
      });

      return { recordCount, lastWatermark: finalWatermark };

    } catch (error) {
      logger.error('Optimized Socrata data extraction failed', {
        city: this.cityConfig.city,
        dataset: datasetName,
        error,
        recordCount,
      });
      throw error;
    }
  }

  /**
   * Extract data from all configured datasets in parallel
   */
  async extractAllDatasets(options: {
    sinceDate?: Date;
    maxRecords?: number;
  } = {}): Promise<Record<string, { recordCount: number; lastWatermark: string | null }>> {
    const datasetNames = Object.keys(this.cityConfig.datasets);
    const results: Record<string, { recordCount: number; lastWatermark: string | null }> = {};

    // Process datasets in batches to avoid overwhelming the API
    const batches = this.chunkArray(datasetNames, this.MAX_CONCURRENT_REQUESTS);
    
    for (const batch of batches) {
      const batchPromises = batch.map(datasetName => 
        this.extractDataset(datasetName, options)
          .catch(error => {
            logger.error(`Failed to extract dataset '${datasetName}'`, { error });
            return { recordCount: 0, lastWatermark: null };
          })
      );

      const batchResults = await Promise.allSettled(batchPromises);
      
      batchResults.forEach((result, index) => {
        const datasetName = batch[index]!;
        if (result.status === 'fulfilled') {
          results[datasetName] = result.value;
        } else {
          results[datasetName] = { recordCount: 0, lastWatermark: null };
        }
      });
    }

    return results;
  }

  /**
      await (this.storage as any).executeRaw(`
        INSERT OR REPLACE INTO raw (id, city, dataset, watermark, payload, inserted_at)
        VALUES ${placeholders}
      `, values);
    } catch (error: any) {
      logger.warn('Bulk upsert failed, falling back to smaller batches', { error: error.message, batchSize: rawRecords.length });
      
      // Fallback to smaller batches of 100 records
      for (let i = 0; i < rawRecords.length; i += 100) {
        const smallBatch = rawRecords.slice(i, i + 100);
        const smallPlaceholders = smallBatch.map(() => "(?, ?, ?, ?, ?, datetime('now'))").join(', ');
        const smallValues = smallBatch.flatMap(r => [r.id, r.city, r.dataset, r.watermark, JSON.stringify(r.payload)]);
        
        try {
          await (this.storage as any).executeRaw(`
            INSERT OR REPLACE INTO raw (id, city, dataset, watermark, payload, inserted_at)
            VALUES ${smallPlaceholders}
          `, smallValues);
        } catch (smallError: any) {
          logger.error('Small batch upsert failed, falling back to individual inserts', { error: smallError.message });
          
          // Final fallback: individual inserts
          for (const record of smallBatch) {
            await this.storage.upsertRaw(record);
          }
        }
      }
    }
  }

  /**
   * Get cached checkpoint to reduce database queries
   */
  private async getCachedCheckpoint(city: string, dataset: string): Promise<string | null> {
    const key = `${city}:${dataset}`;
    if (this.checkpointCache.has(key)) {
      return this.checkpointCache.get(key) || null;
    }
    
    const checkpoint = await this.storage.getLastCheckpoint(city, dataset);
    if (checkpoint) {
      this.checkpointCache.set(key, checkpoint);
    }
    return checkpoint;
  }

  /**
   * Set cached checkpoint
   */
  private async setCachedCheckpoint(city: string, dataset: string, watermark: string): Promise<void> {
    const key = `${city}:${dataset}`;
    this.checkpointCache.set(key, watermark);
    await this.storage.setCheckpoint(city, dataset, watermark);
  }

  /**
   * Query a dataset with custom parameters (for evaluation ground truth collection)
   */
  async queryDataset(datasetId: string, params: Record<string, any>): Promise<any[]> {
    try {
      const client = createSodaClient(this.cityConfig.base_url, getAppToken(this.cityConfig));
      
      logger.debug('Querying Socrata dataset', {
        city: this.cityConfig.city,
        datasetId,
        params
      });

      const response = await client.getJson({
        baseUrl: this.cityConfig.base_url,
        path: `/resource/${datasetId}.json`,
        params
      });
      return response;
    } catch (error) {
      logger.error('Socrata dataset query failed', { error, datasetId, params });
      throw error;
    }
  }

  /**
   * Test connection to Socrata API
   */
  async testConnection(): Promise<boolean> {
    try {
      const client = createSodaClient(this.cityConfig.base_url, getAppToken(this.cityConfig));
      return await client.testConnection();
    } catch (error) {
      logger.error('Socrata connection test failed', { error });
      return false;
    }
  }

  /**
   * Utility function to chunk array into smaller arrays
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }

  /**
   * Process a batch of records with bulk database operations
   */
  private async processBatch(records: any[], datasetName: string, dataset: DatasetConfig): Promise<void> {
    const rawRecords = records.map(record => ({
      id: generateRecordId(record, dataset, this.cityConfig.city),
      city: this.cityConfig.city,
      dataset: datasetName,
      watermark: extractWatermark(record, dataset.watermark_field),
      payload: record,
    }));

    // If a sink is provided, publish to sink and return
    if (this.sink) {
      await this.sink(rawRecords);
      return;
    }

    // Bulk upsert with fallback to smaller batches
    try {
      const placeholders = rawRecords.map(() => "(?, ?, ?, ?, ?, datetime('now'))").join(',');
      const values = rawRecords.flatMap(r => [r.id, r.city, r.dataset, r.watermark, JSON.stringify(r.payload)]);
      
      await (this.storage as any).executeRaw(`
        INSERT OR REPLACE INTO raw (id, city, dataset, watermark, payload, inserted_at)
        VALUES ${placeholders}
      `, values);
    } catch (error: any) {
      logger.warn('Bulk upsert failed, falling back to smaller batches', { error: error.message, batchSize: rawRecords.length });
      
      // Fallback to smaller batches of 100 records
      for (let i = 0; i < rawRecords.length; i += 100) {
        const smallBatch = rawRecords.slice(i, i + 100);
        const smallPlaceholders = smallBatch.map(() => "(?, ?, ?, ?, ?, datetime('now'))").join(', ');
        const smallValues = smallBatch.flatMap(r => [r.id, r.city, r.dataset, r.watermark, JSON.stringify(r.payload)]);
        
        try {
          await (this.storage as any).executeRaw(`
            INSERT OR REPLACE INTO raw (id, city, dataset, watermark, payload, inserted_at)
            VALUES ${smallPlaceholders}
          `, smallValues);
        } catch (smallError: any) {
          logger.error('Small batch upsert failed, falling back to individual inserts', { error: smallError.message });
          
          // Final fallback: individual inserts
          for (const record of smallBatch) {
            await this.storage.upsertRaw(record);
          }
        }
      }
    }
  }

}

/**
 * Create optimized Socrata adapter instance
 */
export function createOptimizedSocrataAdapter(cityConfig: CityConfig, storage: Storage, options?: { sink?: (rawRecords: Array<{ id: string; city: string; dataset: string; watermark: string; payload: any }>) => Promise<void> }): OptimizedSocrataAdapter {
  return new OptimizedSocrataAdapter(cityConfig, storage, options);
}
