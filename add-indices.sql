-- Add performance indices for the pipeline
CREATE INDEX IF NOT EXISTS idx_raw_city_dataset ON raw_records(city, dataset);
CREATE INDEX IF NOT EXISTS idx_raw_date ON raw_records(event_date);
CREATE INDEX IF NOT EXISTS idx_normalized_city ON normalized_records(city);
CREATE INDEX IF NOT EXISTS idx_normalized_date ON normalized_records(event_date);
CREATE INDEX IF NOT EXISTS idx_normalized_type ON normalized_records(business_type);
CREATE INDEX IF NOT EXISTS idx_normalized_address ON normalized_records(address);
CREATE INDEX IF NOT EXISTS idx_normalized_name ON normalized_records(business_name);
