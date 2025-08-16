/**
 * Configuration loader and manager
 */

import { readFileSync, readdirSync } from 'fs';
import { resolve } from 'path';
import { parse as parseYaml } from 'yaml';
import { validateCityConfig, type CityConfig } from './schema.js';
import { ConfigError } from '../types.js';
import { logger } from '../util/logger.js';

/**
 * Cache for loaded configurations
 */
const configCache = new Map<string, CityConfig>();

/**
 * Load and validate a city configuration from YAML file
 */
export function loadCityConfig(cityName: string): CityConfig {
  // Check cache first
  if (configCache.has(cityName)) {
    return configCache.get(cityName)!;
  }

  const configPath = resolve(process.cwd(), 'configs', `${cityName}.yaml`);
  
  try {
    logger.debug(`Loading config from ${configPath}`);
    
    // Read and parse YAML file
    const yamlContent = readFileSync(configPath, 'utf-8');
    const rawConfig = parseYaml(yamlContent);
    
    // Substitute environment variables
    const processedConfig = substituteEnvVars(rawConfig);
    
    // Validate configuration
    const validation = validateCityConfig(processedConfig);
    
    if (!validation.success) {
      throw new ConfigError(
        `Invalid configuration for city '${cityName}':\n${validation.errors.join('\n')}`
      );
    }
    
    const config = validation.data!;
    
    // Cache the validated config
    configCache.set(cityName, config);
    
    logger.info(`Loaded configuration for city: ${cityName}`, {
      datasets: Object.keys(config.datasets),
      baseUrl: config.base_url,
    });
    
    return config;
    
  } catch (error) {
    if (error instanceof ConfigError) {
      throw error;
    }
    
    if ((error as any).code === 'ENOENT') {
      throw new ConfigError(`Configuration file not found: ${configPath}`);
    }
    
    throw new ConfigError(`Failed to load configuration for '${cityName}': ${error}`);
  }
}

/**
 * Get all available city configurations
 */
export function getAvailableCities(): string[] {
  try {
    const configDir = resolve(process.cwd(), 'configs');
    
    return readdirSync(configDir)
      .filter((file: string) => file.endsWith('.yaml'))
      .map((file: string) => file.replace('.yaml', ''));
  } catch (error) {
    logger.warn('Could not read configs directory', { error });
    return [];
  }
}

/**
 * Validate all city configurations
 */
export function validateAllConfigs(): { valid: string[]; invalid: Array<{ city: string; errors: string[] }> } {
  const cities = getAvailableCities();
  const valid: string[] = [];
  const invalid: Array<{ city: string; errors: string[] }> = [];
  
  for (const city of cities) {
    try {
      loadCityConfig(city);
      valid.push(city);
    } catch (error) {
      if (error instanceof ConfigError) {
        invalid.push({
          city,
          errors: [error.message],
        });
      } else {
        invalid.push({
          city,
          errors: [`Unexpected error: ${error}`],
        });
      }
    }
  }
  
  return { valid, invalid };
}

/**
 * Clear configuration cache
 */
export function clearConfigCache(): void {
  configCache.clear();
  logger.debug('Configuration cache cleared');
}

/**
 * Substitute environment variables in configuration object
 */
function substituteEnvVars(obj: any): any {
  if (typeof obj === 'string') {
    // Replace ${VAR_NAME} patterns with environment variables
    return obj.replace(/\$\{([^}]+)\}/g, (match, varName) => {
      const value = process.env[varName];
      if (value === undefined) {
        logger.warn(`Environment variable not found: ${varName}`);
        return match; // Keep original if not found
      }
      return value;
    });
  }
  
  if (Array.isArray(obj)) {
    return obj.map(item => substituteEnvVars(item));
  }
  
  if (obj && typeof obj === 'object') {
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = substituteEnvVars(value);
    }
    return result;
  }
  
  return obj;
}

/**
 * Get dataset configuration by name
 */
export function getDatasetConfig(cityConfig: CityConfig, datasetName: string) {
  const dataset = cityConfig.datasets[datasetName];
  if (!dataset) {
    throw new ConfigError(`Dataset '${datasetName}' not found in city '${cityConfig.city}' configuration`);
  }
  return dataset;
}

/**
 * Build Socrata API URL for a dataset
 */
export function buildDatasetUrl(cityConfig: CityConfig, datasetName: string): string {
  const dataset = getDatasetConfig(cityConfig, datasetName);
  return `${cityConfig.base_url}/resource/${dataset.id}.json`;
}

/**
 * Get app token for a city (if configured)
 */
export function getAppToken(cityConfig: CityConfig): string | undefined {
  return cityConfig.app_token;
}

/**
 * Export types for use in other modules
 */
export type { CityConfig, DatasetConfig } from './schema.js';