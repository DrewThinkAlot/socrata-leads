/**
 * PostgreSQL storage implementation
 */

import { Pool } from 'pg';
import { readFileSync, readdirSync } from 'fs';
import { resolve } from 'path';
import { logger } from '../util/logger.js';
import { StorageError } from '../types.js';
import type {
  Storage,
  RawRecord,
  NormalizedRecord,
  Event,
  Lead,
  EvaluationResult,
  LeadEvaluation,
  GroundTruthRecord,
} from '../types.js';

/**
 * PostgreSQL storage implementation (not implemented)
 */
export class PostgresStorage implements Storage {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  // Raw data operations
  async upsertRaw(record: Omit<RawRecord, 'inserted_at'>): Promise<void> {
    const sql = `
      INSERT INTO raw (id, city, dataset, watermark, payload, inserted_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (id) DO UPDATE SET
        city = EXCLUDED.city,
        dataset = EXCLUDED.dataset,
        watermark = EXCLUDED.watermark,
        payload = EXCLUDED.payload,
        inserted_at = EXCLUDED.inserted_at
    `;
    await this.pool.query(sql, [
      record.id,
      record.city,
      record.dataset,
      record.watermark,
      JSON.stringify(record.payload),
    ]);
  }

  async getRawByCity(city: string, dataset?: string): Promise<RawRecord[]> {
    let sql = 'SELECT * FROM raw WHERE city = $1';
    const params: any[] = [city];
    if (dataset) {
      sql += ' AND dataset = $2';
      params.push(dataset);
    }
    sql += ' ORDER BY inserted_at DESC';
    const res = await this.pool.query(sql, params);
    return res.rows as unknown as RawRecord[];
  }

  // Normalized data operations
  async insertNormalized(record: Omit<NormalizedRecord, 'created_at'>): Promise<void> {
    const sql = `
      INSERT INTO normalized (
        uid, city, dataset, business_name, address, lat, lon, status,
        event_date, type, description, source_link, raw_id, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
      ON CONFLICT (uid) DO NOTHING
    `;
    await this.pool.query(sql, [
      record.uid,
      record.city,
      record.dataset,
      record.business_name ?? null,
      record.address ?? null,
      record.lat ?? null,
      record.lon ?? null,
      record.status ?? null,
      record.event_date ?? null,
      record.type ?? null,
      record.description ?? null,
      record.source_link ?? null,
      record.raw_id,
    ]);
  }

  async getNormalizedByCity(city: string, dataset?: string): Promise<NormalizedRecord[]> {
    let sql = 'SELECT * FROM normalized WHERE city = $1';
    const params: any[] = [city];
    if (dataset) {
      sql += ' AND dataset = $2';
      params.push(dataset);
    }
    sql += ' ORDER BY created_at DESC';
    const res = await this.pool.query(sql, params);
    return res.rows as unknown as NormalizedRecord[];
  }

  // Event operations
  async insertEvent(event: Omit<Event, 'created_at'>): Promise<void> {
    const sql = `
      INSERT INTO events (
        event_id, city, address, name, predicted_open_week,
        signal_strength, evidence, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
      ON CONFLICT (event_id) DO NOTHING
    `;
    await this.pool.query(sql, [
      event.event_id,
      event.city,
      event.address,
      event.name ?? null,
      event.predicted_open_week,
      event.signal_strength,
      JSON.stringify(event.evidence),
    ]);
  }

  async getEventsByCity(city: string): Promise<Event[]> {
    const res = await this.pool.query(
      'SELECT * FROM events WHERE city = $1 ORDER BY signal_strength DESC, created_at DESC',
      [city]
    );
    return res.rows as unknown as Event[];
  }

  // Lead operations
  async insertLead(lead: Omit<Lead, 'created_at'>): Promise<void> {
    const sql = `
      INSERT INTO leads (
        lead_id, city, name, address, phone, email, score, evidence, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
      ON CONFLICT (lead_id) DO NOTHING
    `;
    await this.pool.query(sql, [
      lead.lead_id,
      lead.city,
      lead.name ?? null,
      lead.address,
      lead.phone ?? null,
      lead.email ?? null,
      lead.score,
      JSON.stringify(lead.evidence),
    ]);
  }

  async getLeadsByCity(city: string, limit?: number): Promise<Lead[]> {
    let sql = 'SELECT * FROM leads WHERE city = $1 ORDER BY score DESC, created_at DESC';
    const params: any[] = [city];
    if (limit && Number.isFinite(limit)) {
      sql += ' LIMIT $2';
      params.push(limit);
    }
    const res = await this.pool.query(sql, params);
    return res.rows as unknown as Lead[];
  }

  // Leads by period (for evaluation)
  async getLeadsByPeriod(city: string, periodStart: Date, periodEnd: Date): Promise<Lead[]> {
    const sql = `
      SELECT * FROM leads
      WHERE city = $1
        AND created_at >= $2
        AND created_at <= $3
      ORDER BY score DESC, created_at DESC
    `;
    const res = await this.pool.query(sql, [city, periodStart.toISOString(), periodEnd.toISOString()]);
    return res.rows as unknown as Lead[];
  }

  // Evaluation: Ground truth operations
  async insertGroundTruth(record: GroundTruthRecord): Promise<void> {
    const sql = `
      INSERT INTO ground_truth (
        ground_truth_id, city, business_name, address, license_number,
        license_issue_date, license_type, actual_open_date, source,
        verification_status, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
      ON CONFLICT (ground_truth_id) DO UPDATE SET
        city = EXCLUDED.city,
        business_name = EXCLUDED.business_name,
        address = EXCLUDED.address,
        license_number = EXCLUDED.license_number,
        license_issue_date = EXCLUDED.license_issue_date,
        license_type = EXCLUDED.license_type,
        actual_open_date = EXCLUDED.actual_open_date,
        source = EXCLUDED.source,
        verification_status = EXCLUDED.verification_status
    `;
    await this.pool.query(sql, [
      record.ground_truth_id,
      record.city,
      record.business_name,
      record.address,
      record.license_number ?? null,
      record.license_issue_date ?? null,
      record.license_type ?? null,
      record.actual_open_date,
      record.source,
      record.verification_status,
    ]);
  }

  async getGroundTruthByPeriod(city: string, periodStart: Date, periodEnd: Date): Promise<GroundTruthRecord[]> {
    const sql = `
      SELECT * FROM ground_truth
      WHERE city = $1
        AND actual_open_date >= $2
        AND actual_open_date <= $3
      ORDER BY actual_open_date ASC
    `;
    const res = await this.pool.query(sql, [city, periodStart.toISOString(), periodEnd.toISOString()]);
    return res.rows as unknown as GroundTruthRecord[];
  }

  // Evaluation: Results and lead evaluations
  async insertEvaluationResult(result: EvaluationResult): Promise<void> {
    const sql = `
      INSERT INTO evaluation_results (
        evaluation_id, city, evaluation_date, period_start, period_end,
        total_ground_truth, total_predictions, precision_at_50, precision_at_100,
        recall, median_lead_time_days, cost_per_verified_lead,
        signal_ablation_results, geographic_coverage, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
      ON CONFLICT (evaluation_id) DO UPDATE SET
        city = EXCLUDED.city,
        evaluation_date = EXCLUDED.evaluation_date,
        period_start = EXCLUDED.period_start,
        period_end = EXCLUDED.period_end,
        total_ground_truth = EXCLUDED.total_ground_truth,
        total_predictions = EXCLUDED.total_predictions,
        precision_at_50 = EXCLUDED.precision_at_50,
        precision_at_100 = EXCLUDED.precision_at_100,
        recall = EXCLUDED.recall,
        median_lead_time_days = EXCLUDED.median_lead_time_days,
        cost_per_verified_lead = EXCLUDED.cost_per_verified_lead,
        signal_ablation_results = EXCLUDED.signal_ablation_results,
        geographic_coverage = EXCLUDED.geographic_coverage
    `;
    await this.pool.query(sql, [
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
      JSON.stringify(result.signal_ablation_results ?? []),
      JSON.stringify(result.geographic_coverage ?? []),
    ]);
  }

  async insertLeadEvaluation(evaluation: LeadEvaluation): Promise<void> {
    const sql = `
      INSERT INTO lead_evaluations (
        lead_id, ground_truth_id, is_true_positive, is_false_positive,
        lead_time_days, prediction_date, actual_open_date, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    `;
    await this.pool.query(sql, [
      evaluation.lead_id,
      evaluation.ground_truth_id ?? null,
      evaluation.is_true_positive,
      evaluation.is_false_positive,
      evaluation.lead_time_days ?? null,
      evaluation.prediction_date,
      evaluation.actual_open_date ?? null,
    ]);
  }

  // Checkpoint operations
  async getLastCheckpoint(city: string, dataset: string): Promise<string | null> {
    const res = await this.pool.query(
      'SELECT watermark FROM checkpoints WHERE city = $1 AND dataset = $2',
      [city, dataset]
    );
    return res.rows[0]?.watermark ?? null;
  }

  async setCheckpoint(city: string, dataset: string, watermark: string): Promise<void> {
    const sql = `
      INSERT INTO checkpoints (city, dataset, watermark, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (city, dataset) DO UPDATE SET
        watermark = EXCLUDED.watermark,
        updated_at = EXCLUDED.updated_at
    `;
    await this.pool.query(sql, [city, dataset, watermark]);
  }

  // Query operations for export
  async queryForExport(city: string, limit: number): Promise<Lead[]> {
    const res = await this.pool.query(
      `SELECT * FROM leads WHERE city = $1 ORDER BY score DESC, created_at DESC LIMIT $2`,
      [city, limit]
    );
    return res.rows as unknown as Lead[];
  }

  // Query operations for future leads export
  async queryFutureLeads(city: string, limit: number): Promise<Lead[]> {
    const sql = `
      SELECT l.* FROM leads l
      JOIN events e ON l.address = e.address
      WHERE l.city = $1
        AND e.predicted_open_week >= to_char(date_trunc('week', now()), 'IYYY-"W"IW')
        AND e.predicted_open_week <= to_char(date_trunc('week', now() + interval '90 days'), 'IYYY-"W"IW')
      ORDER BY l.score DESC, l.created_at DESC
      LIMIT $2
    `;
    const res = await this.pool.query(sql, [city, limit]);
    return res.rows as unknown as Lead[];
  }

  // Cleanup operations
  async close(): Promise<void> {
    await this.pool.end();
    logger.info('PostgreSQL database connection pool closed');
  }

  // Optional migration runner (used by src/storage/migrations/run.ts)
  async runMigrations(): Promise<void> {
    const client = await this.pool.connect();
    try {
      const migrationsDir = resolve(process.cwd(), 'src/storage/migrations/postgres');
      const files = readdirSync(migrationsDir)
        .filter(f => f.endsWith('.sql'))
        .sort();

      await client.query('BEGIN');
      for (const file of files) {
        const fullPath = resolve(migrationsDir, file);
        const sql = readFileSync(fullPath, 'utf-8');
        // Execute as a single batch; PostgreSQL supports multiple statements
        await client.query(sql);
      }
      await client.query('COMMIT');
      logger.info('PostgreSQL migrations completed', { files });
    } catch (error) {
      await client.query('ROLLBACK');
      throw new StorageError(`PostgreSQL migration failed: ${error}`);
    } finally {
      client.release();
    }
  }

  // Optional connectivity test
  async testConnection(): Promise<boolean> {
    try {
      const res = await this.pool.query('SELECT 1');
      return res.rowCount === 1;
    } catch (error) {
      logger.error('PostgreSQL connection test failed', { error });
      return false;
    }
  }

  // Optional stats helper
  async getStats(): Promise<Record<string, number>> {
    try {
      const tables = ['raw', 'normalized', 'events', 'leads', 'checkpoints', 'ground_truth', 'evaluation_results', 'lead_evaluations'];
      const stats: Record<string, number> = {};
      for (const table of tables) {
        const res = await this.pool.query(`SELECT COUNT(*)::int AS count FROM ${table}`);
        stats[table] = res.rows[0].count as number;
      }
      return stats;
    } catch (error) {
      logger.warn('Could not get PostgreSQL statistics', { error });
      return {};
    }
  }
}

/* End of implementation */