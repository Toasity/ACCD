# Acceptance Checklist

## Tasks

- **Task 1 — Get familiar with CoinMetrics API**
  - **Code:** `src/coinmetrics/client.py`, `src/coinmetrics/endpoints.py`
  - **Evidence:** `raw.api_responses` contains successful rows for:
    - `endpoint='catalog/assets'` with `status_code=200`
    - `endpoint='timeseries/asset-metrics'` with `status_code=200`
  - **Reproduce:**
    ```bash
    docker compose run --rm app python scripts/10_etl_run.py --stage extract
    docker exec -it coinmetrics-db psql -U coinmetrics -d coinmetrics -c \
      "SELECT id, endpoint, status_code FROM raw.api_responses ORDER BY id DESC LIMIT 10;"
    ```
  - **Notes:** API parameter pitfall: an unsupported `limit` caused a 400 response; that error is retained in `raw.api_responses` for diagnostics.

- **Task 2 — Analyze the statistics of the data**
  - **Code:** `src/analysis/profiling.py`, `scripts/20_profile_run.py`
  - **Outputs:**
    - `reports/profiling/tables/coverage.csv`
    - `reports/profiling/tables/missing_rate.csv`
    - `reports/profiling/figures/value_hist.png`
  - **Reproduce:**
    ```bash
    docker compose run --rm app python scripts/20_profile_run.py
    ```

- **Task 3 — Build an ETL environment**
  - **Code:** `src/etl/extract.py`, `src/etl/transform.py`, `src/etl/load.py`
  - **Reproduce:**
    ```bash
    docker compose run --rm app python scripts/10_etl_run.py --stage load
    ```

- **Task 4 — Evaluate for what we can use metrics**
  - **Evidence:** `reports/final_report.md` (auto-generated)
  - **Contents:** data coverage, missingness analysis and a brief evaluation of metric usability
  - **Reproduce:**
    ```bash
    docker compose run --rm app python scripts/20_profile_run.py
    sed -n '1,120p' reports/final_report.md
    ```

## Requirements

- **REST API:** use `requests.Session` to call CoinMetrics v4 API — implemented in `src/coinmetrics/client.py`
- **Relational DBMS:** PostgreSQL via `docker-compose.yml`; raw/processed/analysis layers created by `db/init/*.sql`
- **Docker image:** `Dockerfile` + `docker compose up`
- **One-command run:**
  ```bash
  docker compose up -d db
  docker compose run --rm app python scripts/10_etl_run.py --stage load
  docker compose run --rm app python scripts/20_profile_run.py
  ```
- **Scrum:** plan and sprint records: `docs/05_sprints.md`
