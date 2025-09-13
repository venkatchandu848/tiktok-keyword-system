CREATE TABLE keywords (
    id SERIAL,
    video_id TEXT NOT NULL,
    keyword TEXT NOT NULL,
    modality TEXT NOT NULL,
    confidence FLOAT,
    detected_at TIMESTAMP with TIME ZONE DEFAULT now() NOT NULL,
    region TEXT,
    category TEXT,
    stats JSONB,    
    posted_at TIMESTAMP WITH TIME ZONE
);

ALTER TABLE keywords
CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('keywords', 'detected_at', if_not_exists => TRUE, migrate_data => TRUE);

-- Indexes
CREATE INDEX idx_keywords_video_id ON keywords(video_id);
CREATE INDEX idx_keywords_keyword ON keywords(keyword);
CREATE INDEX idx_keywords_detected_at ON keywords(detected_at DESC);

-- Rollups
CREATE MATERIALIZED VIEW IF NOT EXISTS keyword_hourly_counts
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', posted_at) AS bucket,
    keyword,
    region,
    category,
    COUNT(*) AS mentions
FROM keywords
GROUP BY bucket, region, category, keyword;

-- To keep the materialized view up-to-date with new data, you need to refresh it.
-- The underlying 'keywords' table can be updated (rows added or deleted).
-- However, the materialized view 'keyword_hourly_counts' will not automatically reflect changes unless refreshed.

-- To refresh the materialized view manually:
REFRESH MATERIALIZED VIEW keyword_hourly_counts;


-- Table to store trending keywords based on growth detection

CREATE TABLE IF NOT EXISTS trending_keywords (
    id SERIAL PRIMARY KEY,
    keyword TEXT NOT NULL,
    region TEXT,
    category TEXT,
    time_bucket TIMESTAMP WITH TIME ZONE NOT NULL,
    mentions INTEGER NOT NULL,
    growth_rate FLOAT,
    time_window TEXT NOT NULL,  -- 'hourly', 'daily', 'weekly'
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Optional indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_trending_keywords_keyword ON trending_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_trending_keywords_region_category ON trending_keywords(region, category);
CREATE INDEX IF NOT EXISTS idx_trending_keywords_time_window ON trending_keywords(time_window);


-- Table to save performance metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value FLOAT NOT NULL,
    metric_time TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Optional: quick lookups
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name_time ON pipeline_metrics(metric_name, metric_time);