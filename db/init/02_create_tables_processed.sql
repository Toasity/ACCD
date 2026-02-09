-- Create table processed.metrics_long for normalized long-format metrics
CREATE TABLE IF NOT EXISTS processed.metrics_long (
    asset TEXT NOT NULL,
    metric TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    freq TEXT NOT NULL,
    value DOUBLE PRECISION,
    is_missing BOOLEAN NOT NULL DEFAULT FALSE,
    source_endpoint TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset, metric, ts, freq)
);

-- Indexes to support queries
CREATE INDEX IF NOT EXISTS processed_metric_ts_idx ON processed.metrics_long (metric, ts);
CREATE INDEX IF NOT EXISTS processed_asset_ts_idx ON processed.metrics_long (asset, ts);
