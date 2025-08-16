/**
 * Core type definitions for the socrata-leads pipeline
 */

// Raw data from Socrata API
export interface RawRecord {
  id: string;
  city: string;
  dataset: string;
  watermark: string;
  payload: Record<string, any>;
  inserted_at: string;
}

// Normalized canonical schema
export interface NormalizedRecord {
  uid: string;
  city: string;
  dataset: string;
  business_name?: string;
  address?: string;
  lat?: number;
  lon?: number;
  status?: string;
  event_date?: string;
  type?: string;
  description?: string;
  source_link?: string;
  future_date?: string;
  raw_id: string;
  created_at: string;
}

// Fused event from multiple signals
export interface Event {
  event_id: string;
  city: string;
  address: string;
  name: string | undefined;
  predicted_open_week: string;
  signal_strength: number;
  evidence: NormalizedRecord[];
  created_at: string;
}

// SpotOn-specific business intelligence
export interface SpotOnBusinessIntelligence {
  business_category: string;
  service_model: 'full-service' | 'fast-casual' | 'takeout-only' | 'delivery-first' | 'unknown';
  seat_capacity?: number;
  square_footage?: number;
  liquor_license_type?: 'full-bar' | 'beer-wine' | 'restaurant' | 'tavern' | 'unknown';
  reservation_systems: string[];
  kitchen_complexity: 'simple' | 'moderate' | 'complex' | 'multi-station' | 'unknown';
  operator_type: 'new-operator' | 'existing-operator' | 'chain-expansion' | 'unknown';
  opening_timeline_days?: number;
  has_type_i_hood?: boolean;
  has_multiple_cook_lines?: boolean;
  has_hot_cold_stations?: boolean;
  has_multiple_printers?: boolean;
  spoton_score: number;
  filter_matches: SpotOnFilterMatch[];
}

export interface SpotOnFilterMatch {
  filter_name: string;
  matched: boolean;
  value?: any;
  confidence: number;
}

// Scored business lead with SpotOn intelligence
export interface Lead {
  lead_id: string;
  city: string;
  name: string | undefined;
  address: string;
  phone: string | undefined;
  email: string | undefined;
  score: number;
  spoton_intelligence: SpotOnBusinessIntelligence;
  evidence: Event[];
  created_at: string;
}

// Checkpoint for incremental processing
export interface Checkpoint {
  city: string;
  dataset: string;
  watermark: string;
  updated_at: string;
}

// Configuration types
export interface DatasetConfig {
  id: string;
  select: string[];
  where?: string;
  order_by: string;
  watermark_field: string;
  map: Record<string, string>;
}

export interface SpotOnFilterConfig {
  min_seat_capacity: number;
  min_square_footage: number;
  preferred_business_types: string[];
  reservation_platforms: string[];
  liquor_license_priority: string[];
  timeline_window_days: [number, number];
  service_model_weights: Record<string, number>;
  operator_type_weights: Record<string, number>;
}

export interface CityConfig {
  city: string;
  base_url: string;
  app_token?: string;
  datasets: Record<string, DatasetConfig>;
  spoton_filters?: SpotOnFilterConfig;
}

// Socrata API types
export interface SocrataQueryParams {
  $select?: string;
  $where?: string;
  $order?: string;
  $limit?: number;
  $offset?: number;
}

export interface SocrataClientOptions {
  baseUrl: string;
  path: string;
  params?: SocrataQueryParams;
  appToken?: string;
}

// Fusion rule types
export interface FusionRule {
  name: string;
  description: string;
  signal_strength: number;
  match: (records: NormalizedRecord[]) => boolean;
}

// CLI argument types
export interface ExtractArgs {
  city: string;
  dataset?: string;
  since?: string;
  maxRecords?: number;
}

export interface ExportArgs {
  city: string;
  limit: number;
  out: string;
}

export interface BackfillArgs {
  city: string;
  days: number;
}

// Storage interface
export interface Storage {
  // Raw data operations
  upsertRaw(record: Omit<RawRecord, 'inserted_at'>): Promise<void>;
  getRawByCity(city: string, dataset?: string): Promise<RawRecord[]>;
  
  // Normalized data operations
  insertNormalized(record: Omit<NormalizedRecord, 'created_at'>): Promise<void>;
  getNormalizedByCity(city: string, dataset?: string): Promise<NormalizedRecord[]>;
  
  // Event operations
  insertEvent(event: Omit<Event, 'created_at'>): Promise<void>;
  getEventsByCity(city: string): Promise<Event[]>;
  
  // Lead operations
  insertLead(lead: Omit<Lead, 'created_at'>): Promise<void>;
  getLeadsByCity(city: string, limit?: number): Promise<Lead[]>;
  
  // Checkpoint operations
  getLastCheckpoint(city: string, dataset: string): Promise<string | null>;
  setCheckpoint(city: string, dataset: string, watermark: string): Promise<void>;
  
  // Query operations for export
  queryForExport(city: string, limit: number): Promise<Lead[]>;
  queryFutureLeads(city: string, limit: number): Promise<Lead[]>;
  
  // Cleanup operations
  close(): Promise<void>;
}

// Utility types
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  debug(message: string, meta?: Record<string, any>): void;
  info(message: string, meta?: Record<string, any>): void;
  warn(message: string, meta?: Record<string, any>): void;
  error(message: string, meta?: Record<string, any>): void;
}

// Error types
export class SocrataError extends Error {
  constructor(
    message: string,
    public statusCode?: number,
    public retryAfter?: number
  ) {
    super(message);
    this.name = 'SocrataError';
  }
}

export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigError';
  }
}

export class StorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StorageError';
  }
}
