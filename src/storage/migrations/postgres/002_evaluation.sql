-- Evaluation schema additions for PostgreSQL

-- Ground truth records of actual openings
CREATE TABLE IF NOT EXISTS ground_truth (
  ground_truth_id TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  business_name TEXT NOT NULL,
  address TEXT NOT NULL,
  license_number TEXT,
  license_issue_date TEXT,
  license_type TEXT,
  actual_open_date TEXT NOT NULL,
  source TEXT NOT NULL, -- 'license' | 'inspection' | 'manual'
  verification_status TEXT NOT NULL, -- 'verified' | 'pending' | 'disputed'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Evaluation results summary for a period
CREATE TABLE IF NOT EXISTS evaluation_results (
  evaluation_id TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  evaluation_date TEXT NOT NULL,
  period_start TEXT NOT NULL,
  period_end TEXT NOT NULL,
  total_ground_truth INTEGER NOT NULL,
  total_predictions INTEGER NOT NULL,
  precision_at_50 DOUBLE PRECISION NOT NULL,
  precision_at_100 DOUBLE PRECISION NOT NULL,
  recall DOUBLE PRECISION NOT NULL,
  median_lead_time_days DOUBLE PRECISION,
  cost_per_verified_lead DOUBLE PRECISION,
  signal_ablation_results JSONB NOT NULL DEFAULT '[]'::jsonb,
  geographic_coverage JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-lead evaluation mapping to ground truth
CREATE TABLE IF NOT EXISTS lead_evaluations (
  id BIGSERIAL PRIMARY KEY,
  lead_id TEXT NOT NULL,
  ground_truth_id TEXT,
  is_true_positive BOOLEAN NOT NULL DEFAULT FALSE,
  is_false_positive BOOLEAN NOT NULL DEFAULT FALSE,
  lead_time_days DOUBLE PRECISION,
  prediction_date TEXT NOT NULL,
  actual_open_date TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes to optimize evaluation queries
CREATE INDEX IF NOT EXISTS idx_ground_truth_city_open_date ON ground_truth(city, actual_open_date);
CREATE INDEX IF NOT EXISTS idx_ground_truth_city_license_issue_date ON ground_truth(city, license_issue_date);
CREATE INDEX IF NOT EXISTS idx_ground_truth_business_name ON ground_truth(business_name);
CREATE INDEX IF NOT EXISTS idx_ground_truth_address ON ground_truth(address);

CREATE INDEX IF NOT EXISTS idx_evaluation_results_city_date ON evaluation_results(city, evaluation_date);

CREATE INDEX IF NOT EXISTS idx_lead_evaluations_lead_id ON lead_evaluations(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_evaluations_ground_truth_id ON lead_evaluations(ground_truth_id);
