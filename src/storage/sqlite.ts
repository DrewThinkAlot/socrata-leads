/**
 * SQLite storage implementation using better-sqlite3
 */

import Database from 'better-sqlite3';
import { readFileSync } from 'fs';
import { dirname, resolve } from 'path';
import { mkdirSync } from 'fs';
import { logger } from '../util/logger.js';
import { StorageError } from '../types.js';
import type {
  Storage,
  RawRecord,
  NormalizedRecord,
  Event,
  Lead,
  Checkpoint,
} from '../types.js';

/**
 * SQLite storage implementation
 */
export class SqliteStorage implements Storage {
  private db: Database.Database;
  private dbPath: string;

  constructor(dbPath: string) {
    this.dbPath = dbPath;
    
    // Ensure directory exists
    const dir = dirname(dbPath);
    mkdirSync(dir, { recursive: true });
    
    // Open database
    this.db = new Database(dbPath);
    
    // Enable WAL mode for better concurrency
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('cache_size = 1000');
    this.db.pragma('temp_store = memory');
    
    logger.info('SQLite database initialized', { path: dbPath });
  }

  // Leads by period (for evaluation)
  async getLeadsByPeriod(city: string, periodStart: Date, periodEnd: Date): Promise<Lead[]> {
    try {
      const stmt = this.db.prepare(`
        SELECT * FROM leads
        WHERE city = ?
          AND created_at >= ?
          AND created_at <= ?
        ORDER BY score DESC, created_at DESC
      `);
      const rows = stmt.all(city, periodStart.toISOString(), periodEnd.toISOString()) as any[];
      return rows.map(row => ({
        ...row,
        evidence: JSON.parse(row.evidence),
      }));
    } catch (error) {
      throw new StorageError(`Failed to get leads by period: ${error}`);
    }
  }

  // Evaluation: Ground truth operations
  async insertGroundTruth(record: any): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO ground_truth (
          ground_truth_id, city, business_name, address, license_number,
          license_issue_date, license_type, actual_open_date, source,
          verification_status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      stmt.run(
        record.ground_truth_id,
        record.city,
        record.business_name,
        record.address,
        record.license_number,
        record.license_issue_date,
        record.license_type,
        record.actual_open_date,
        record.source,
        record.verification_status,
        record.created_at || new Date().toISOString()
      );
    } catch (error) {
      throw new StorageError(`Failed to insert ground truth: ${error}`);
    }
  }

  async getGroundTruthByPeriod(city: string, periodStart: Date, periodEnd: Date): Promise<any[]> {
    try {
      const stmt = this.db.prepare(`
        SELECT * FROM ground_truth
        WHERE city = ?
          AND actual_open_date >= ?
          AND actual_open_date <= ?
        ORDER BY actual_open_date ASC
      `);
      const rows = stmt.all(city, periodStart.toISOString(), periodEnd.toISOString()) as any[];
      return rows;
    } catch (error) {
      throw new StorageError(`Failed to get ground truth by period: ${error}`);
    }
  }

  // Evaluation: Results and lead evaluations
  async insertEvaluationResult(result: any): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO evaluation_results (
          evaluation_id, city, evaluation_date, period_start, period_end,
          total_ground_truth, total_predictions, precision_at_50, precision_at_100,
          recall, median_lead_time_days, cost_per_verified_lead,
          signal_ablation_results, geographic_coverage, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      stmt.run(
        result.evaluation_id,
        result.city,
        result.evaluation_date,
        result.period_start,
        result.period_end,
        result.total_ground_truth,
        result.total_predictions,
        result.precision_at_50,
        result.precision_at_100,
        result.recall,
        result.median_lead_time_days,
        result.cost_per_verified_lead,
        JSON.stringify(result.signal_ablation_results || []),
        JSON.stringify(result.geographic_coverage || []),
        result.created_at || new Date().toISOString()
      );
    } catch (error) {
      throw new StorageError(`Failed to insert evaluation result: ${error}`);
    }
  }

  async insertLeadEvaluation(evaluation: any): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO lead_evaluations (
          lead_id, ground_truth_id, is_true_positive, is_false_positive,
          lead_time_days, prediction_date, actual_open_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      stmt.run(
        evaluation.lead_id,
        evaluation.ground_truth_id || null,
        evaluation.is_true_positive ? 1 : 0,
        evaluation.is_false_positive ? 1 : 0,
        evaluation.lead_time_days ?? null,
        evaluation.prediction_date,
        evaluation.actual_open_date ?? null
      );
    } catch (error) {
      throw new StorageError(`Failed to insert lead evaluation: ${error}`);
    }
  }

  /**
   * Run database migrations
   */
  async runMigrations(): Promise<void> {
    try {
      const migrationsDir = resolve(process.cwd(), 'src/storage/migrations');
      // Load all .sql files in alphabetical order
      const fs = await import('fs');
      const files = fs.readdirSync(migrationsDir)
        .filter(f => f.endsWith('.sql'))
        .sort();

      this.db.transaction(() => {
        for (const file of files) {
          const fullPath = resolve(migrationsDir, file);
          const sql = readFileSync(fullPath, 'utf-8');
          const statements = sql
            .split(';')
            .map(stmt => stmt.trim())
            .filter(stmt => stmt.length > 0);
          for (const statement of statements) {
            this.db.exec(statement);
          }
        }
      })();

      logger.info('SQLite migrations completed', { files });
    } catch (error) {
      throw new StorageError(`Migration failed: ${error}`);
    }
  }

  /**
   * Test database connection
   */
  async testConnection(): Promise<boolean> {
    try {
      this.db.prepare('SELECT 1').get();
      return true;
    } catch (error) {
      logger.error('SQLite connection test failed', { error });
      return false;
    }
  }

  /**
   * Get database statistics
   */
  async getStats(): Promise<Record<string, number>> {
    try {
      const tables = ['raw', 'normalized', 'events', 'leads', 'checkpoints', 'ground_truth', 'evaluation_results', 'lead_evaluations'];
      const stats: Record<string, number> = {};
      
      for (const table of tables) {
        const result = this.db.prepare(`SELECT COUNT(*) as count FROM ${table}`).get() as { count: number };
        stats[table] = result.count;
      }
      
      return stats;
    } catch (error) {
      logger.warn('Could not get SQLite statistics', { error });
      return {};
    }
  }

  // Raw data operations
  async upsertRaw(record: Omit<RawRecord, 'inserted_at'>): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO raw (id, city, dataset, watermark, payload, inserted_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
      `);
      
      stmt.run(
        record.id,
        record.city,
        record.dataset,
        record.watermark,
        JSON.stringify(record.payload)
      );
    } catch (error) {
      throw new StorageError(`Failed to upsert raw record: ${error}`);
    }
  }

  async getRawByCity(city: string, dataset?: string): Promise<RawRecord[]> {
    try {
      let query = 'SELECT * FROM raw WHERE city = ?';
      const params: any[] = [city];
      
      if (dataset) {
        query += ' AND dataset = ?';
        params.push(dataset);
      }
      
      query += ' ORDER BY inserted_at DESC';
      
      const stmt = this.db.prepare(query);
      const rows = stmt.all(...params) as any[];
      
      return rows.map(row => ({
        ...row,
        payload: JSON.parse(row.payload),
      }));
    } catch (error) {
      throw new StorageError(`Failed to get raw records: ${error}`);
    }
  }

  // Normalized data operations
  async insertNormalized(record: Omit<NormalizedRecord, 'created_at'>): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO normalized (
          uid, city, dataset, business_name, address, lat, lon, status,
          event_date, type, description, source_link, raw_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      `);
      
      stmt.run(
        record.uid,
        record.city,
        record.dataset,
        record.business_name,
        record.address,
        record.lat,
        record.lon,
        record.status,
        record.event_date,
        record.type,
        record.description,
        record.source_link,
        record.raw_id
      );
    } catch (error) {
      throw new StorageError(`Failed to insert normalized record: ${error}`);
    }
  }

  async getNormalizedByCity(city: string, dataset?: string): Promise<NormalizedRecord[]> {
    try {
      let query = 'SELECT * FROM normalized WHERE city = ?';
      const params: any[] = [city];
      
      if (dataset) {
        query += ' AND dataset = ?';
        params.push(dataset);
      }
      
      query += ' ORDER BY created_at DESC';
      
      const stmt = this.db.prepare(query);
      return stmt.all(...params) as NormalizedRecord[];
    } catch (error) {
      throw new StorageError(`Failed to get normalized records: ${error}`);
    }
  }

  // Event operations
  async insertEvent(event: Omit<Event, 'created_at'>): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO events (
          event_id, city, address, name, predicted_open_week,
          signal_strength, evidence, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
      `);
      
      stmt.run(
        event.event_id,
        event.city,
        event.address,
        event.name,
        event.predicted_open_week,
        event.signal_strength,
        JSON.stringify(event.evidence)
      );
    } catch (error) {
      throw new StorageError(`Failed to insert event: ${error}`);
    }
  }

  async getEventsByCity(city: string): Promise<Event[]> {
    try {
      const stmt = this.db.prepare(`
        SELECT * FROM events WHERE city = ? ORDER BY signal_strength DESC, created_at DESC
      `);
      
      const rows = stmt.all(city) as any[];
      
      return rows.map(row => ({
        ...row,
        evidence: JSON.parse(row.evidence),
      }));
    } catch (error) {
      throw new StorageError(`Failed to get events: ${error}`);
    }
  }

  // Lead operations
  async insertLead(lead: Omit<Lead, 'created_at'>): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT INTO leads (
          lead_id, city, name, address, phone, email, score, evidence, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      `);
      
      stmt.run(
        lead.lead_id,
        lead.city,
        lead.name,
        lead.address,
        lead.phone,
        lead.email,
        lead.score,
        JSON.stringify(lead.evidence)
      );
    } catch (error) {
      throw new StorageError(`Failed to insert lead: ${error}`);
    }
  }

  async getLeadsByCity(city: string, limit?: number): Promise<Lead[]> {
    try {
      let query = 'SELECT * FROM leads WHERE city = ? ORDER BY score DESC, created_at DESC';
      
      if (limit) {
        query += ` LIMIT ${limit}`;
      }
      
      const stmt = this.db.prepare(query);
      const rows = stmt.all(city) as any[];
      
      return rows.map(row => ({
        ...row,
        evidence: JSON.parse(row.evidence),
      }));
    } catch (error) {
      throw new StorageError(`Failed to get leads: ${error}`);
    }
  }

  // Checkpoint operations
  async getLastCheckpoint(city: string, dataset: string): Promise<string | null> {
    try {
      const stmt = this.db.prepare(`
        SELECT watermark FROM checkpoints WHERE city = ? AND dataset = ?
      `);
      
      const result = stmt.get(city, dataset) as { watermark: string } | undefined;
      return result?.watermark || null;
    } catch (error) {
      throw new StorageError(`Failed to get checkpoint: ${error}`);
    }
  }

  async setCheckpoint(city: string, dataset: string, watermark: string): Promise<void> {
    try {
      const stmt = this.db.prepare(`
        INSERT OR REPLACE INTO checkpoints (city, dataset, watermark, updated_at)
        VALUES (?, ?, ?, datetime('now'))
      `);
      
      stmt.run(city, dataset, watermark);
    } catch (error) {
      throw new StorageError(`Failed to set checkpoint: ${error}`);
    }
  }

  // Query operations for export
  async queryForExport(city: string, limit: number): Promise<Lead[]> {
    try {
      const stmt = this.db.prepare(`
        SELECT * FROM leads 
        WHERE city = ? 
        ORDER BY score DESC, created_at DESC 
        LIMIT ?
      `);
      
      const rows = stmt.all(city, limit) as any[];
      
      return rows.map(row => ({
        ...row,
        evidence: JSON.parse(row.evidence),
      }));
    } catch (error) {
      throw new StorageError(`Failed to query for export: ${error}`);
    }
  }

  // Query operations for future leads export (30-90 days out)
  async queryFutureLeads(city: string, limit: number): Promise<Lead[]> {
    try {
      const stmt = this.db.prepare(`
        SELECT l.* FROM leads l
        JOIN events e ON l.address = e.address
        WHERE l.city = ? 
        AND e.predicted_open_week >= (SELECT strftime('%Y-W%W', 'now'))
        AND e.predicted_open_week <= (SELECT strftime('%Y-W%W', date('now', '+90 days')))
        ORDER BY l.score DESC, l.created_at DESC 
        LIMIT ?
      `);
      
      const rows = stmt.all(city, limit) as any[];
      
      return rows.map(row => ({
        ...row,
        evidence: JSON.parse(row.evidence),
      }));
    } catch (error) {
      throw new StorageError(`Failed to query future leads: ${error}`);
    }
  }

  // Cleanup operations
  async close(): Promise<void> {
    try {
      this.db.close();
      logger.info('SQLite database connection closed');
    } catch (error) {
      throw new StorageError(`Failed to close database: ${error}`);
    }
  }

  /**
   * Execute raw SQL (for advanced operations)
   */
  async executeRaw(sql: string, params: any[] = []): Promise<any[]> {
    try {
      const stmt = this.db.prepare(sql);
      // Use run() for non-SELECT statements and all() for SELECT queries
      if (/^\s*select/i.test(sql)) {
        return stmt.all(...params);
      } else {
        const info = stmt.run(...params);
        // Normalize to array return type for API consistency
        return [info as unknown as any];
      }
    } catch (error) {
      throw new StorageError(`Failed to execute raw SQL: ${error}`);
    }
  }

  /**
   * Begin transaction
   */
  beginTransaction(): Database.Transaction {
    return this.db.transaction(() => {});
  }
}
