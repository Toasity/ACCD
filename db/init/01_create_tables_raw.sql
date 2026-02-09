-- Create table raw.api_responses to store original API responses
CREATE TABLE IF NOT EXISTS raw.api_responses (
    id BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status_code INT,
    payload JSONB NOT NULL
);

-- Index on endpoint
CREATE INDEX IF NOT EXISTS raw_api_endpoint_idx ON raw.api_responses (endpoint);

-- GIN index for payload JSONB
CREATE INDEX IF NOT EXISTS raw_api_payload_gin ON raw.api_responses USING gin (payload);
