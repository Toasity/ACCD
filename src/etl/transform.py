"""ETL transform step: parse raw.api_responses payloads into normalized rows.

This module reads the most recent raw responses and converts them into a
list of metric rows ready for loading into `processed.metrics_long`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Any

from src.db.engine import get_conn
from src.utils.logging import logger
from src.config import get_cm_config


def _parse_time(t: str) -> datetime:
    """Parse ISO time strings, accepting a trailing Z for UTC."""
    if not t:
        raise ValueError("empty time string")
    # Accept Z as UTC
    if t.endswith("Z"):
        t = t.replace("Z", "+00:00")
    return datetime.fromisoformat(t)


def transform_latest_raw(limit: int = 50) -> List[Dict[str, Any]]:
    """Read latest N raw.api_responses and convert payload->data into rows.

    Returns a list of dicts with keys:
      asset, metric, ts (datetime tz-aware), freq, value, is_missing, source_endpoint
    """
    rows: List[Dict[str, Any]] = []
    cm_defaults = get_cm_config()

    conn = get_conn()
    try:
        # Look for the newest successful CoinMetrics timeseries record first
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, endpoint, params, payload FROM raw.api_responses WHERE endpoint=%s AND status_code=200 ORDER BY id DESC LIMIT 1",
                ("timeseries/asset-metrics",),
            )
            found = cur.fetchone()

            if not found:
                # Fallback to stub timeseries
                cur.execute(
                    "SELECT id, endpoint, params, payload FROM raw.api_responses WHERE endpoint=%s AND status_code=200 ORDER BY id DESC LIMIT 1",
                    ("timeseries.stub",),
                )
                found = cur.fetchone()

        if not found:
            logger.info("No successful timeseries raw record found (searched asset-metrics then stub)")
            return rows

        rid, endpoint, params, payload = found[0], found[1], found[2] or {}, found[3] or {}

        data_list = payload.get("data") if isinstance(payload, dict) else None
        if not data_list:
            logger.warning("raw id=%s endpoint=%s has empty or missing payload.data, skipping", rid, endpoint)
            return rows

        # Determine frequency: from params or env default
        freq = None
        try:
            if isinstance(params, dict):
                freq = params.get("frequency") or params.get("freq") or params.get("frequency")
        except Exception:
            freq = None
        if not freq:
            freq = cm_defaults.get("frequency", "1d")

        # Heuristic: detect stub format vs CoinMetrics v4 format
        first_item = data_list[0] if isinstance(data_list, (list, tuple)) and data_list else None
        is_stub = False
        if isinstance(first_item, dict):
            if "metric" in first_item and "value" in first_item:
                is_stub = True

        if is_stub:
            # Reuse previous stub-parsing logic
            for item in data_list:
                try:
                    asset = item.get("asset")
                    metric = item.get("metric")
                    time_s = item.get("time")
                    value = item.get("value") if "value" in item else None

                    if asset is None or metric is None or time_s is None:
                        raise KeyError("missing asset/metric/time in data item")

                    ts = _parse_time(time_s)

                    is_missing = value is None

                    row = {
                        "asset": asset,
                        "metric": metric,
                        "ts": ts,
                        "freq": freq,
                        "value": float(value) if value is not None else None,
                        "is_missing": bool(is_missing),
                        "source_endpoint": endpoint,
                    }
                    rows.append(row)
                except Exception as exc:
                    logger.error("Skipping stub data item in raw id=%s due to error: %s", rid, exc)
                    continue
        else:
            # CoinMetrics v4 parser: each element may contain time, asset and many metric columns
            for item in data_list:
                if not isinstance(item, dict):
                    logger.debug("Skipping non-dict data item in raw id=%s", rid)
                    continue

                time_s = item.get("time") or item.get("timestamp")
                if time_s is None:
                    logger.error("Skipping item without time in raw id=%s: %s", rid, item)
                    continue

                try:
                    ts = _parse_time(time_s)
                except Exception as exc:
                    logger.error("Invalid time in raw id=%s item=%s error=%s", rid, item, exc)
                    continue

                asset = item.get("asset")
                # Fall back to request params: 'assets' may be comma-separated
                if asset is None and isinstance(params, dict):
                    assets_param = params.get("assets") or params.get("asset")
                    if isinstance(assets_param, str):
                        asset = assets_param.split(",")[0].strip() if assets_param else None

                # Iterate over metric-like keys (everything except time/asset)
                for k, v in item.items():
                    if k in ("time", "timestamp", "asset"):
                        continue

                    metric = k
                    value = v
                    is_missing = value is None

                    # Try numeric coercion
                    coerced_value = None
                    if value is not None:
                        try:
                            coerced_value = float(value)
                        except Exception:
                            # leave as None (treated missing) but still record presence
                            coerced_value = None

                    row = {
                        "asset": asset,
                        "metric": metric,
                        "ts": ts,
                        "freq": freq,
                        "value": coerced_value,
                        "is_missing": bool(is_missing),
                        "source_endpoint": endpoint,
                    }
                    rows.append(row)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return rows


if __name__ == "__main__":
    out = transform_latest_raw(5)
    print(f"Transformed {len(out)} rows")
