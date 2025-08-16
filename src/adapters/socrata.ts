/**
 * Socrata adapter implementation
 */

import { createSodaClient } from '../soda/client.js';
import { paginateDataset } from '../soda/paginate.js';
import { buildDatasetUrl, getAppToken, type CityConfig, type DatasetConfig } from '../config/index.js';
import { logger } from '../util/logger.js';
import type { Storage } from '../types.js';

/**
 * Socrata data source adapter
 */
export class SocrataAdapter {
  private cityConfig: CityConfig;
  private storage: Storage;

  constructor(cityConfig: CityConfig, storage: Storage) {
    this.cityConfig = cityConfig;
    this.storage = storage;
  }

  /**
   * Extract data from a Socrata dataset
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

    logger.info('Starting Socrata data extraction', {
      city: this.cityConfig.city,
      dataset: datasetName,
      datasetId: dataset.id,
      sinceDate: options.sinceDate?.toISOString(),
    });

    // Create SODA client
    const client = createSodaClient(this.cityConfig.base_url, getAppToken(this.cityConfig));
    
    // Get last checkpoint
    const lastWatermark = await this.storage.getLastCheckpoint(this.cityConfig.city, datasetName);
    
    // Build dataset URL
    const datasetPath = `/resource/${dataset.id}.json`;
    
    let recordCount = 0;
    let finalWatermark: string | null = lastWatermark;

    try {
      // Paginate through dataset
      const paginationOptions = {
        selectFields: dataset.select,
        orderByField: dataset.order_by,
        watermarkField: dataset.watermark_field,
        lastWatermark,
        pageSize: 1000,
        maxPages: options.maxRecords ? Math.ceil(options.maxRecords / 1000) : undefined,
      } as any;
      
      if (dataset.where) {
        paginationOptions.whereClause = dataset.where;
      }
      
      for await (const records of paginateDataset(client, datasetPath, paginationOptions)) {
        
        // Process each record
        for (const record of records) {
          const rawRecord = {
            id: this.generateRecordId(record, dataset),
            city: this.cityConfig.city,
            dataset: datasetName,
            watermark: this.extractWatermark(record, dataset.watermark_field),
            payload: record,
          };

          await this.storage.upsertRaw(rawRecord);
          recordCount++;

          // Update final watermark
          finalWatermark = rawRecord.watermark;

          // Check max records limit
          if (options.maxRecords && recordCount >= options.maxRecords) {
            break;
          }
        }

        // Update checkpoint after each batch
        if (finalWatermark && finalWatermark !== lastWatermark) {
          await this.storage.setCheckpoint(this.cityConfig.city, datasetName, finalWatermark);
        }

        // Check max records limit
        if (options.maxRecords && recordCount >= options.maxRecords) {
          break;
        }
      }

      logger.info('Socrata data extraction completed', {
        city: this.cityConfig.city,
        dataset: datasetName,
        recordCount,
        finalWatermark,
      });

      return { recordCount, lastWatermark: finalWatermark };

    } catch (error) {
      logger.error('Socrata data extraction failed', {
        city: this.cityConfig.city,
        dataset: datasetName,
        error,
        recordCount,
      });
      throw error;
    }
  }

  /**
   * Extract data from all configured datasets
   */
  async extractAllDatasets(options: {
    sinceDate?: Date;
    maxRecords?: number;
  } = {}): Promise<Record<string, { recordCount: number; lastWatermark: string | null }>> {
    const results: Record<string, { recordCount: number; lastWatermark: string | null }> = {};

    for (const datasetName of Object.keys(this.cityConfig.datasets)) {
      try {
        results[datasetName] = await this.extractDataset(datasetName, options);
      } catch (error) {
        logger.error(`Failed to extract dataset '${datasetName}'`, { error });
        results[datasetName] = { recordCount: 0, lastWatermark: null };
      }
    }

    return results;
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
   * Generate a stable record ID from the record data
   */
  private generateRecordId(record: any, dataset: DatasetConfig): string {
    // Try to use Socrata's built-in :id field first
    if (record[':id']) {
      return `${this.cityConfig.city}-${dataset.id}-${record[':id']}`;
    }

    // Fall back to using the watermark field as ID
    const watermarkValue = record[dataset.watermark_field];
    if (watermarkValue) {
      const hash = this.simpleHash(JSON.stringify(record));
      return `${this.cityConfig.city}-${dataset.id}-${watermarkValue}-${hash}`;
    }

    // Last resort: hash the entire record
    const hash = this.simpleHash(JSON.stringify(record));
    return `${this.cityConfig.city}-${dataset.id}-${hash}`;
  }

  /**
   * Extract watermark value from record
   */
  private extractWatermark(record: any, watermarkField: string): string {
    const value = record[watermarkField];
    if (value === null || value === undefined) {
      return new Date().toISOString(); // Fallback to current time
    }
    return String(value);
  }

  /**
   * Simple hash function for generating IDs
   */
  private simpleHash(str: string): string {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36);
  }
}

/**
 * Create Socrata adapter instance
 */
export function createSocrataAdapter(cityConfig: CityConfig, storage: Storage): SocrataAdapter {
  return new SocrataAdapter(cityConfig, storage);
}