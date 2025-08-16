/**
 * PostgreSQL storage implementation (stub)
 */

import { StorageError } from '../types.js';
import type {
  Storage,
  RawRecord,
  NormalizedRecord,
  Event,
  Lead,
} from '../types.js';

/**
 * PostgreSQL storage implementation (not implemented)
 */
export class PostgresStorage implements Storage {
  private connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = connectionString;
    throw new StorageError('PostgreSQL storage is not yet implemented. Use SQLite for now.');
  }

  // Raw data operations
  async upsertRaw(record: Omit<RawRecord, 'inserted_at'>): Promise<void> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  async getRawByCity(city: string, dataset?: string): Promise<RawRecord[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Normalized data operations
  async insertNormalized(record: Omit<NormalizedRecord, 'created_at'>): Promise<void> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  async getNormalizedByCity(city: string, dataset?: string): Promise<NormalizedRecord[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Event operations
  async insertEvent(event: Omit<Event, 'created_at'>): Promise<void> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  async getEventsByCity(city: string): Promise<Event[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Lead operations
  async insertLead(lead: Omit<Lead, 'created_at'>): Promise<void> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  async getLeadsByCity(city: string, limit?: number): Promise<Lead[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Checkpoint operations
  async getLastCheckpoint(city: string, dataset: string): Promise<string | null> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  async setCheckpoint(city: string, dataset: string, watermark: string): Promise<void> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Query operations for export
  async queryForExport(city: string, limit: number): Promise<Lead[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Query operations for future leads export
  async queryFutureLeads(city: string, limit: number): Promise<Lead[]> {
    throw new StorageError('PostgreSQL storage not implemented');
  }

  // Cleanup operations
  async close(): Promise<void> {
    // No-op for stub
  }
}

/*
TODO: Implement PostgreSQL storage

When implementing PostgreSQL support, consider:

1. Use pg library for database connections
2. Implement connection pooling
3. Use parameterized queries to prevent SQL injection
4. Handle PostgreSQL-specific data types (JSONB for payload/evidence)
5. Implement proper transaction handling
6. Add migration support for PostgreSQL schema
7. Consider using a query builder like Kysely for type safety

Example implementation structure:

```typescript
import { Pool } from 'pg';

export class PostgresStorage implements Storage {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async upsertRaw(record: Omit<RawRecord, 'inserted_at'>): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query(`
        INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (id) DO UPDATE SET
          watermark = EXCLUDED.watermark,
          payload = EXCLUDED.payload,
          inserted_at = EXCLUDED.inserted_at
      `, [record.id, record.city, record.dataset, record.watermark, JSON.stringify(record.payload)]);
    } finally {
      client.release();
    }
  }

  // ... implement other methods
}
```
*/