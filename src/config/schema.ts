/**
 * Zod schemas for validating city configuration files
 */

import { z } from 'zod';

/**
 * Schema for dataset field mapping configuration
 */
export const DatasetMapSchema = z.record(z.string(), z.string().nullable()).describe(
  'Mapping from canonical field names to dataset-specific field expressions (null for unavailable fields)'
);

/**
 * Schema for individual dataset configuration
 */
export const DatasetConfigSchema = z.object({
  id: z.string().describe('Socrata dataset identifier (4x4 format)'),
  select: z.array(z.string()).min(1).describe('Fields to select from the dataset'),
  where: z.string().optional().describe('Optional WHERE clause for filtering'),
  order_by: z.string().describe('Field to order results by (for pagination)'),
  watermark_field: z.string().describe('Field used for incremental processing'),
  map: DatasetMapSchema.describe('Field mapping to canonical schema'),
}).strict();

/**
 * Schema for city configuration
 */
export const CityConfigSchema = z.object({
  city: z.string().min(1).describe('City name (lowercase, used as identifier)'),
  base_url: z.string().url().describe('Base URL for the Socrata API'),
  app_token: z.string().optional().describe('Optional Socrata app token for higher rate limits'),
  datasets: z.record(z.string(), DatasetConfigSchema).describe('Dataset configurations'),
}).strict();

/**
 * Validate that required canonical fields are mapped
 */
export function validateFieldMappings(config: CityConfig): string[] {
  const errors: string[] = [];
  const requiredFields = [
    'business_name',
    'address',
    'event_date',
    'type',
  ];

  for (const [datasetName, dataset] of Object.entries(config.datasets)) {
    const mappedFields = Object.keys(dataset.map);
    
    for (const requiredField of requiredFields) {
      if (!mappedFields.includes(requiredField)) {
        errors.push(`Dataset '${datasetName}' missing required field mapping: '${requiredField}'`);
      }
    }

    // Validate that watermark_field is in select list
    if (!dataset.select.includes(dataset.watermark_field)) {
      errors.push(`Dataset '${datasetName}' watermark_field '${dataset.watermark_field}' not in select list`);
    }

    // Validate that order_by field is in select list
    if (!dataset.select.includes(dataset.order_by)) {
      errors.push(`Dataset '${datasetName}' order_by field '${dataset.order_by}' not in select list`);
    }
  }

  return errors;
}

/**
 * Validate dataset name follows conventions
 */
export function validateDatasetNames(config: CityConfig): string[] {
  const errors: string[] = [];
  const validNames = [
    'building_permits', 
    'liquor_licenses', 
    'business_licenses',
    'food_inspections',      // Phase 1 addition
    'building_violations',   // Phase 1 addition
    'job_postings'          // Phase 1 addition
  ];
  
  for (const datasetName of Object.keys(config.datasets)) {
    if (!validNames.includes(datasetName)) {
      errors.push(`Dataset name '${datasetName}' should be one of: ${validNames.join(', ')}`);
    }
  }

  return errors;
}

/**
 * Comprehensive validation of city configuration
 */
export function validateCityConfig(config: unknown): {
  success: boolean;
  data?: CityConfig;
  errors: string[];
} {
  try {
    // First, validate against Zod schema
    const parsed = CityConfigSchema.parse(config);
    
    // Then run additional business logic validations
    const fieldErrors = validateFieldMappings(parsed);
    const nameErrors = validateDatasetNames(parsed);
    
    const allErrors = [...fieldErrors, ...nameErrors];
    
    if (allErrors.length > 0) {
      return {
        success: false,
        errors: allErrors,
      };
    }

    return {
      success: true,
      data: parsed,
      errors: [],
    };
  } catch (error) {
    if (error instanceof z.ZodError) {
      return {
        success: false,
        errors: error.errors.map(e => `${e.path.join('.')}: ${e.message}`),
      };
    }
    
    return {
      success: false,
      errors: [`Validation error: ${error}`],
    };
  }
}

/**
 * Type exports for use in other modules
 */
export type CityConfig = z.infer<typeof CityConfigSchema>;
export type DatasetConfig = z.infer<typeof DatasetConfigSchema>;
export type DatasetMap = z.infer<typeof DatasetMapSchema>;