/**
 * Unified Socrata adapter entry. Re-exports the optimized adapter as the default
 * and provides a factory compatible with existing callers.
 */

import type { CityConfig } from '../config/index.js';
import type { Storage } from '../types.js';
import { OptimizedSocrataAdapter, createOptimizedSocrataAdapter } from './socrata_optimized.js';

export { OptimizedSocrataAdapter as SocrataAdapter } from './socrata_optimized.js';

export function createSocrataAdapter(cityConfig: CityConfig, storage: Storage, options?: {
  sink?: (rawRecords: Array<{ id: string; city: string; dataset: string; watermark: string; payload: any }>) => Promise<void>;
}) {
  return createOptimizedSocrataAdapter(cityConfig, storage, options);
}
