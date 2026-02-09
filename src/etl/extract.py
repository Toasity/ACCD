"""ETL extract step: minimal implementation that writes a simulated
CoinMetrics API response into `raw.api_responses`.

This module intentionally implements only the `extract` (raw write)
functionality for course delivery. It does not call the real API.
"""
import json
from datetime import datetime

from src.config import get_cm_config
from src.db.engine import get_conn
from src.utils.logging import logger
import psycopg2.extras


def run_extract() -> int:
    """Construct a simulated payload and insert into raw.api_responses.

    Returns the inserted id.
    """
    cm = get_cm_config()

    from src.config import COINMETRICS_API_KEY
    sql = (
        "INSERT INTO raw.api_responses (endpoint, params, status_code, payload)"
        " VALUES (%s, %s, %s, %s) RETURNING id"
    )

    # Helper to normalize comma-separated params
    def _normalize_csv(value):
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            return ",".join([str(v).strip() for v in value if v is not None])
        s = str(value)
        # replace semicolons with commas, remove extra spaces
        s = s.replace(";", ",")
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return ",".join(parts)

    conn = get_conn()
    try:
        if COINMETRICS_API_KEY:
            # Use real API: first write catalog/assets response
            from src.coinmetrics.client import CoinMetricsClient, CoinMetricsError, normalize_time
            from src.coinmetrics.endpoints import fetch_assets, fetch_asset_metrics

            client = CoinMetricsClient(api_key=COINMETRICS_API_KEY)

            # Fetch and store catalog/assets (no 'limit' param sent)
            try:
                catalog_json = fetch_assets(client)
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, ("catalog/assets", psycopg2.extras.Json({}), 200, psycopg2.extras.Json(catalog_json)))
                        cid = cur.fetchone()[0]
                        logger.info("Inserted catalog raw response id=%s status=%s", cid, 200)
            except Exception as exc:
                # Capture structured error if available
                status = 500
                err_payload = str(exc)
                try:
                    from src.coinmetrics.client import CoinMetricsError

                    if isinstance(exc, CoinMetricsError):
                        status = getattr(exc, "status_code", 500)
                        err_payload = getattr(exc, "error_payload", str(exc))
                except Exception:
                    pass

                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, ("catalog/assets", psycopg2.extras.Json({}), status, psycopg2.extras.Json({"error": err_payload})))
                        cid = cur.fetchone()[0]
                        logger.info("Inserted catalog error raw response id=%s status=%s", cid, status)

            # Prepare timeseries params
            assets_raw = cm.get("assets")
            metrics_raw = cm.get("metrics")
            assets_str = _normalize_csv(assets_raw) or ""
            metrics_str = _normalize_csv(metrics_raw) or ""
            # Use first asset for now
            first_asset = assets_str.split(",")[0] if assets_str else ""
            start = cm.get("start_date")
            end = cm.get("end_date")
            # Normalize times for request params (convert YYYY-MM-DD -> ISO8601 UTC)
            try:
                start_t = normalize_time(start)
            except Exception:
                start_t = start
            try:
                end_t = normalize_time(end)
            except Exception:
                end_t = end
            freq = cm.get("frequency")

            ts_params = {
                "assets": first_asset,
                "metrics": metrics_str,
                "frequency": freq,
                "start_time": start_t,
                "end_time": end_t,
            }

            # Call timeseries endpoint and support pagination (merge pages)
            try:
                # First page using helper (params-based)
                first_page = fetch_asset_metrics(client, first_asset, [m.strip() for m in metrics_str.split(",") if m.strip()], start_t, end_t, freq)

                all_data = []
                total_pages = 0
                total_rows = 0

                page_json = first_page
                page_index = 1

                # Helper to extract page data list
                def _page_data(j):
                    if not isinstance(j, dict):
                        return []
                    d = j.get("data")
                    return d if isinstance(d, list) else []

                next_url = page_json.get("next_page_url") if isinstance(page_json, dict) else None

                # Iterate pages: include first page and follow next_page_url
                while True:
                    page_data = _page_data(page_json)
                    n_rows = len(page_data)
                    first_time = None
                    last_time = None
                    if n_rows:
                        first_time = page_data[0].get("time") or page_data[0].get("timestamp")
                        last_time = page_data[-1].get("time") or page_data[-1].get("timestamp")

                    logger.info("asset-metrics page=%s, n_rows=%s, first_time=%s, last_time=%s", page_index, n_rows, first_time, last_time)

                    # Optional audit insert per page
                    try:
                        audit_params = dict(ts_params)
                        audit_params["page"] = page_index
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(sql, (f"timeseries/asset-metrics(page)", psycopg2.extras.Json(audit_params), 200, psycopg2.extras.Json(page_json)))
                                _ = cur.fetchone()[0]
                    except Exception:
                        logger.debug("Failed to write per-page audit row for page %s", page_index)

                    # Accumulate
                    all_data.extend(page_data)
                    total_pages += 1
                    total_rows += n_rows

                    # Determine next page URL and break if none
                    if not next_url:
                        break

                    # Fetch next page (full URL). Use client's session to keep auth headers.
                    try:
                        resp = client.session.get(next_url, headers=client._build_headers(), timeout=client.timeout)
                        if resp.status_code != 200:
                            # Try to parse error payload
                            try:
                                err = resp.json()
                            except Exception:
                                err = resp.text[:2000]
                            raise CoinMetricsError(resp.status_code, next_url, err)

                        page_json = resp.json()
                        next_url = page_json.get("next_page_url") if isinstance(page_json, dict) else None
                        page_index += 1
                        # loop continues
                    except CoinMetricsError:
                        raise
                    except Exception as exc:
                        # On unexpected fetch error, abort and record what we have
                        logger.error("Error fetching next page %s: %s", next_url, exc)
                        raise

                # After collecting all pages, build merged payload
                merged = dict(first_page) if isinstance(first_page, dict) else {"data": []}
                merged["data"] = all_data
                # clear pagination marker to indicate merged completeness
                merged["next_page_url"] = ""
                merged["next_page_token"] = None

                logger.info("asset-metrics completed: total_pages=%s, total_rows=%s", total_pages, total_rows)

                # Insert merged record as the official timeseries/asset-metrics row
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, ("timeseries/asset-metrics", psycopg2.extras.Json(ts_params), 200, psycopg2.extras.Json(merged)))
                        inserted = cur.fetchone()[0]
                        logger.info("Inserted merged timeseries raw response id=%s (pages=%s rows=%s)", inserted, total_pages, total_rows)
                        return inserted
            except CoinMetricsError as cmerr:
                # Write the error payload into raw.api_responses for diagnostics
                err_payload = cmerr.error_payload
                status = getattr(cmerr, "status_code", None) or 500
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, ("timeseries/asset-metrics", psycopg2.extras.Json(ts_params), status, psycopg2.extras.Json({"error": err_payload})))
                        inserted = cur.fetchone()[0]
                        logger.info("Inserted timeseries error raw response id=%s status=%s", inserted, status)
                        return inserted
            except Exception as exc:
                logger.error("Unexpected error calling timeseries pagination: %s", exc)
                # Record a generic failure payload
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, ("timeseries/asset-metrics", psycopg2.extras.Json(ts_params), 500, psycopg2.extras.Json({"error": str(exc)})))
                        inserted = cur.fetchone()[0]
                        logger.info("Inserted timeseries error raw response id=%s status=500", inserted)
                        return inserted
        else:
            # Stub behavior (no API key)
            params = {
                "assets": cm.get("assets"),
                "metrics": cm.get("metrics"),
                "start_date": cm.get("start_date"),
                "end_date": cm.get("end_date"),
                "frequency": cm.get("frequency"),
            }
            payload = {
                "data": [
                    {
                        "asset": "btc",
                        "metric": "PriceUSD",
                        "time": "2013-01-01T00:00:00Z",
                        "value": 13.5,
                    }
                ],
                "meta": {"note": "stub for step2.2"},
            }
            status = 200
            endpoint = "timeseries.stub"
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (endpoint, psycopg2.extras.Json(params), status, psycopg2.extras.Json(payload)))
                    inserted = cur.fetchone()[0]
                    logger.info("Inserted stub raw.api_responses id=%s", inserted)
                    return inserted
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    _id = run_extract()
    print(_id)
