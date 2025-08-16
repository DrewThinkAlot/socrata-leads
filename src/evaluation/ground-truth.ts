#!/usr/bin/env node

/**
 * Ground truth data collection for restaurant openings
 * Collects actual restaurant opening data from city license databases
 */

import { logger } from '../util/logger.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { loadCityConfig } from '../config/index.js';
import type { Storage, GroundTruthRecord, CityConfig } from '../types.js';
import { parseDate } from '../util/dates.js';

export class GroundTruthCollector {
  constructor(
    private cityConfig: CityConfig,
    private storage: Storage
  ) {}

  /**
   * Collect ground truth data for a specific time period
   */
  async collectGroundTruth(options: {
    startDate: Date;
    endDate: Date;
    restaurantOnly?: boolean;
  }): Promise<GroundTruthRecord[]> {
    logger.info('Starting ground truth collection', {
      city: this.cityConfig.city,
      startDate: options.startDate.toISOString(),
      endDate: options.endDate.toISOString()
    });

    const records: GroundTruthRecord[] = [];

    // Collect from business licenses
    const licenseRecords = await this.collectFromLicenses(options);
    records.push(...licenseRecords);

    // Collect from health inspections (if available)
    const inspectionRecords = await this.collectFromInspections(options);
    records.push(...inspectionRecords);

    // Store in database
    for (const record of records) {
      await this.storage.insertGroundTruth(record);
    }

    logger.info('Ground truth collection completed', {
      city: this.cityConfig.city,
      recordsCollected: records.length
    });

    return records;
  }

  /**
   * Collect ground truth from business license data
   */
  private async collectFromLicenses(options: {
    startDate: Date;
    endDate: Date;
    restaurantOnly?: boolean;
  }): Promise<GroundTruthRecord[]> {
    const adapter = createSocrataAdapter(this.cityConfig, this.storage);
    const records: GroundTruthRecord[] = [];

    // Query business licenses issued in the period
    const licenseDataset = this.cityConfig.datasets.business_licenses;
    if (!licenseDataset) {
      logger.warn('No business license dataset configured', { city: this.cityConfig.city });
      return records;
    }

    try {
      const rawLicenses = await adapter.queryDataset(licenseDataset.id, {
        $where: `${licenseDataset.watermark_field} >= '${options.startDate.toISOString()}' AND ${licenseDataset.watermark_field} <= '${options.endDate.toISOString()}'`,
        $order: licenseDataset.order_by,
        $limit: 10000
      });

      for (const license of rawLicenses) {
        // Filter for restaurant-related licenses
        if (options.restaurantOnly === true && !this.isRestaurantLicense(license)) {
          continue;
        }

        const groundTruthRecord: GroundTruthRecord = {
          ground_truth_id: `${this.cityConfig.city}_license_${license.id}`,
          city: this.cityConfig.city,
          business_name: this.extractBusinessName(license),
          address: this.extractAddress(license),
          license_number: this.extractLicenseNumber(license),
          license_issue_date: this.extractIssueDate(license),
          license_type: this.extractLicenseType(license),
          actual_open_date: this.extractIssueDate(license), // Use license date as proxy for opening
          source: 'license',
          verification_status: 'verified',
          created_at: new Date().toISOString()
        };

        records.push(groundTruthRecord);
      }

      logger.info('Collected ground truth from licenses', {
        city: this.cityConfig.city,
        count: records.length
      });

    } catch (error) {
      logger.error('Failed to collect ground truth from licenses', { error });
    }

    return records;
  }

  /**
   * Collect ground truth from health inspection data
   */
  private async collectFromInspections(options: {
    startDate: Date;
    endDate: Date;
    restaurantOnly?: boolean;
  }): Promise<GroundTruthRecord[]> {
    const adapter = createSocrataAdapter(this.cityConfig, this.storage);
    const records: GroundTruthRecord[] = [];

    const inspectionDataset = this.cityConfig.datasets.food_inspections;
    if (!inspectionDataset) {
      return records;
    }

    try {
      // Query for first-time inspections that passed
      const rawInspections = await adapter.queryDataset(inspectionDataset.id, {
        $where: `${inspectionDataset.watermark_field} >= '${options.startDate.toISOString()}' AND ${inspectionDataset.watermark_field} <= '${options.endDate.toISOString()}' AND results = 'Pass'`,
        $order: inspectionDataset.order_by,
        $limit: 10000
      });

      // Group by business and find first passing inspection
      const businessInspections = new Map<string, any>();
      for (const inspection of rawInspections) {
        const businessKey = `${this.extractBusinessName(inspection)}_${this.extractAddress(inspection)}`;
        const inspectionDate = new Date(this.extractInspectionDate(inspection));
        
        if (!businessInspections.has(businessKey) || 
            inspectionDate < new Date(businessInspections.get(businessKey).inspection_date)) {
          businessInspections.set(businessKey, inspection);
        }
      }

      for (const [, inspection] of businessInspections) {
        const groundTruthRecord: GroundTruthRecord = {
          ground_truth_id: `${this.cityConfig.city}_inspection_${inspection.id}`,
          city: this.cityConfig.city,
          business_name: this.extractBusinessName(inspection),
          address: this.extractAddress(inspection),
          license_number: this.extractLicenseNumber(inspection) || 'unknown',
          license_issue_date: this.extractInspectionDate(inspection),
          license_type: 'food_service',
          actual_open_date: this.extractInspectionDate(inspection),
          source: 'inspection',
          verification_status: 'verified',
          created_at: new Date().toISOString()
        };

        records.push(groundTruthRecord);
      }

      logger.info('Collected ground truth from inspections', {
        city: this.cityConfig.city,
        count: records.length
      });

    } catch (error) {
      logger.error('Failed to collect ground truth from inspections', { error });
    }

    return records;
  }

  /**
   * Check if a license record is restaurant-related
   */
  private isRestaurantLicense(record: any): boolean {
    const businessType = (record.business_activity || record.license_description || '').toLowerCase();
    const restaurantKeywords = [
      'restaurant', 'food', 'eating', 'dining', 'cafe', 'coffee',
      'bar', 'tavern', 'brewery', 'bakery', 'catering', 'deli'
    ];
    
    return restaurantKeywords.some(keyword => businessType.includes(keyword));
  }

  // Extraction helpers - these would be customized per city's data format
  private extractBusinessName(record: any): string {
    return record.business_name || record.doing_business_as_name || record.legal_name || 'Unknown';
  }

  private extractAddress(record: any): string {
    return record.address || record.site_address || record.location || 'Unknown';
  }

  private extractLicenseNumber(record: any): string {
    return record.license_number || record.account_number || record.id || 'Unknown';
  }

  private extractIssueDate(record: any): string {
    return record.license_start_date || record.issue_date || record.application_created_date || new Date().toISOString();
  }

  private extractLicenseType(record: any): string {
    return record.license_description || record.business_activity || 'Unknown';
  }

  private extractInspectionDate(record: any): string {
    return record.inspection_date || record.date || new Date().toISOString();
  }
}

/**
 * CLI runner for ground truth collection
 */
export async function runGroundTruthCollection(options: {
  city: string;
  startDate: string;
  endDate: string;
  restaurantOnly?: boolean;
}): Promise<void> {
  const { createStorage } = await import('../storage/index.js');
  const storage = await createStorage();
  const cityConfig = loadCityConfig(options.city) as CityConfig;
  
  const collector = new GroundTruthCollector(cityConfig, storage);
  
  try {
    const collectOptions: {
      startDate: Date;
      endDate: Date;
      restaurantOnly?: boolean;
    } = {
      startDate: new Date(options.startDate),
      endDate: new Date(options.endDate)
    };
    if (options.restaurantOnly === true) {
      collectOptions.restaurantOnly = true;
    }
    await collector.collectGroundTruth(collectOptions);
  } finally {
    await storage.close();
  }
}
