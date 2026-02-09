-- Create analysis views: metric_coverage and metric_missing_rate
-- metric_coverage: per asset x metric x freq coverage stats
CREATE OR REPLACE VIEW analysis.metric_coverage AS
SELECT
  asset,
  metric,
  freq,
  MIN(ts) AS start_ts,
  MAX(ts) AS end_ts,
  COUNT(*) AS n_points,
  SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS n_missing
FROM processed.metrics_long
GROUP BY asset, metric, freq;

-- metric_missing_rate: compute missing rate safely (avoid div by zero)
CREATE OR REPLACE VIEW analysis.metric_missing_rate AS
SELECT
  mc.*,
  CASE WHEN mc.n_points = 0 THEN 0.0 ELSE (mc.n_missing::double precision / mc.n_points) END AS missing_rate
FROM analysis.metric_coverage mc;
