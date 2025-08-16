/**
 * Storage layer interface and factory
 */

import { resolve } from 'path';
import { StorageError, type Storage } from '../types.js';
import { logger } from '../util/logger.js';

/**
 * Create storage instance based on DATABASE_URL
 */
export async function createStorage(): Promise<Storage> {
  const databaseUrl = process.env.DATABASE_URL || 'sqlite://./data/pipeline.db';
  
  logger.info('Creating storage instance', { databaseUrl: sanitizeUrl(databaseUrl) });
  
  if (databaseUrl.startsWith('sqlite://')) {
    const { SqliteStorage } = await import('./sqlite.js');
    const dbPath = databaseUrl.replace('sqlite://', '');
    const absolutePath = resolve(process.cwd(), dbPath);
    return new SqliteStorage(absolutePath);
  }
  
  if (databaseUrl.startsWith('postgres://') || databaseUrl.startsWith('postgresql://')) {
    const { PostgresStorage } = await import('./postgres.js');
    return new PostgresStorage(databaseUrl);
  }
  
  throw new StorageError(`Unsupported database URL format: ${databaseUrl}`);
}

/**
 * Sanitize database URL for logging (remove credentials)
 */
function sanitizeUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.username || parsed.password) {
      return `${parsed.protocol}//${parsed.hostname}:${parsed.port}${parsed.pathname}`;
    }
    return url;
  } catch {
    // If URL parsing fails, just hide everything after ://
    const parts = url.split('://');
    if (parts.length > 1) {
      return `${parts[0]}://***`;
    }
    return url;
  }
}

/**
 * Run database migrations
 */
export async function runMigrations(storage: Storage): Promise<void> {
  logger.info('Running database migrations');
  
  if ('runMigrations' in storage && typeof storage.runMigrations === 'function') {
    await (storage as any).runMigrations();
    logger.info('Database migrations completed');
  } else {
    logger.warn('Storage implementation does not support migrations');
  }
}

/**
 * Test database connection
 */
export async function testConnection(storage: Storage): Promise<boolean> {
  try {
    if ('testConnection' in storage && typeof storage.testConnection === 'function') {
      return await (storage as any).testConnection();
    }
    
    // Fallback test - try to get a checkpoint
    await storage.getLastCheckpoint('test', 'test');
    return true;
  } catch (error) {
    logger.error('Database connection test failed', { error });
    return false;
  }
}

/**
 * Get database statistics
 */
export async function getDatabaseStats(storage: Storage): Promise<Record<string, number>> {
  const stats: Record<string, number> = {};
  
  try {
    if ('getStats' in storage && typeof storage.getStats === 'function') {
      return await (storage as any).getStats();
    }
    
    // Fallback - try to count records in each table
    // This is implementation-specific, so we'll return empty stats
    return stats;
  } catch (error) {
    logger.warn('Could not get database statistics', { error });
    return stats;
  }
}

/**
 * Export types
 */
export type { Storage } from '../types.js';