#!/usr/bin/env node

/**
 * Performance benchmark script to compare extraction speeds
 */

import { createStorage } from '../storage/index.js';
import { loadCityConfig } from '../config/index.js';
import { createSocrataAdapter } from '../adapters/socrata.js';
import { createOptimizedSocrataAdapter } from '../adapters/socrata_optimized.js';
import { logger } from './logger.js';

interface BenchmarkResult {
  method: string;
  totalRecords: number;
  durationMs: number;
  recordsPerSecond: number;
  datasetsProcessed: number;
}

async function runBenchmark(city: string, maxRecords: number = 1000): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];
  
  // Test configurations
  const configs = [
    { method: 'Original', adapter: createSocrataAdapter },
    { method: 'Optimized', adapter: createOptimizedSocrataAdapter }
  ];

  for (const config of configs) {
    logger.info(`Starting benchmark: ${config.method}`);
    
    const storage = await createStorage();
    const cityConfig = loadCityConfig(city);
    const adapter = config.adapter(cityConfig, storage);
    
    // Optimize database for bulk operations if using SQLite
    if ('optimizeForBulkOperations' in storage) {
      await (storage as any).optimizeForBulkOperations();
    }
    
    const startTime = Date.now();
    
    try {
      const extractionResults = await adapter.extractAllDatasets({
        maxRecords: Math.floor(maxRecords / Object.keys(cityConfig.datasets).length)
      });
      
      const endTime = Date.now();
      const durationMs = endTime - startTime;
      
      const totalRecords = Object.values(extractionResults)
        .reduce((sum, result) => sum + result.recordCount, 0);
      
      results.push({
        method: config.method,
        totalRecords,
        durationMs,
        recordsPerSecond: Math.round(totalRecords / (durationMs / 1000)),
        datasetsProcessed: Object.keys(extractionResults).length
      });
      
    } catch (error) {
      logger.error(`Benchmark failed for ${config.method}`, { error });
    } finally {
      await storage.close();
    }
  }
  
  return results;
}

async function main() {
  const city = process.argv[2] || 'chicago';
  const maxRecords = parseInt(process.argv[3] || '1000', 10);
  
  console.log(`\n🔍 Running extraction benchmark for ${city} with max ${maxRecords} records...\n`);
  
  const results = await runBenchmark(city, maxRecords);
  
  console.log('\n📊 Benchmark Results:\n');
  console.table(results);
  
  if (results.length === 2) {
    const original = results[0];
    const optimized = results[1];
    if (!original || !optimized) {
      return;
    }

    const speedup = Math.round((original.durationMs / optimized.durationMs) * 100) / 100;
    const throughputIncrease = Math.round(((optimized.recordsPerSecond - original.recordsPerSecond) / original.recordsPerSecond) * 100);

    console.log(`\n⚡ Performance Improvements:`);
    console.log(`   Speedup: ${speedup}x faster`);
    console.log(`   Throughput increase: ${throughputIncrease}%`);
    console.log(`   Records per second: ${original.recordsPerSecond} → ${optimized.recordsPerSecond}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error);
}
