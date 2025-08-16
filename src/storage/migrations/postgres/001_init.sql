-- Initial PostgreSQL schema for socrata-leads pipeline

-- Raw data table - stores original API responses
CREATE TABLE IF NOT EXISTS raw (
  id TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  dataset TEXT NOT NULL,
  watermark TEXT NOT NULL,
  payload JSONB NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (city, dataset, id)
);

-- Normalized data table - canonical schema for all cities
CREATE TABLE IF NOT EXISTS normalized (
  uid TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  dataset TEXT NOT NULL,
  business_name TEXT,
  address TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  status TEXT,
  event_date TEXT,
  type TEXT,
  description TEXT,
  source_link TEXT,
  raw_id TEXT NOT NULL REFERENCES raw(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Events table - fused signals from multiple datasets
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  address TEXT NOT NULL,
  name TEXT,
  predicted_open_week TEXT NOT NULL,
  signal_strength INTEGER NOT NULL,
  evidence JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Leads table - scored business opening opportunities
CREATE TABLE IF NOT EXISTS leads (
  lead_id TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  name TEXT,
  address TEXT NOT NULL,
  phone TEXT,
  email TEXT,
  score INTEGER NOT NULL,
  evidence JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Checkpoints table - tracks watermarks for incremental processing
CREATE TABLE IF NOT EXISTS checkpoints (
  city TEXT NOT NULL,
  dataset TEXT NOT NULL,
  watermark TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (city, dataset)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_city_dataset ON raw(city, dataset);
CREATE INDEX IF NOT EXISTS idx_raw_watermark ON raw(watermark);
CREATE INDEX IF NOT EXISTS idx_normalized_city_dataset ON normalized(city, dataset);
CREATE INDEX IF NOT EXISTS idx_normalized_event_date ON normalized(event_date);
CREATE INDEX IF NOT EXISTS idx_normalized_address ON normalized(address);
CREATE INDEX IF NOT EXISTS idx_events_city_week ON events(city, predicted_open_week);
CREATE INDEX IF NOT EXISTS idx_events_signal_strength ON events(signal_strength);
CREATE INDEX IF NOT EXISTS idx_leads_city_score ON leads(city, score DESC);
CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at);
