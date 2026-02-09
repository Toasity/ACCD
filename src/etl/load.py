"""ETL load step: upsert normalized metric rows into processed.metrics_long."""
from __future__ import annotations

from typing import List, Dict, Any
from datetime import datetime

from src.db.engine import get_conn
from src.utils.logging import logger


def upsert_metrics(rows: List[Dict[str, Any]]) -> int:
    """Upsert a list of metric rows into processed.metrics_long.

    Each row dict must contain keys: asset, metric, ts (datetime), freq, value, is_missing, source_endpoint
    Returns the number of rows affected (inserted or updated).
    """
    if not rows:
        return 0

    sql = (
        "INSERT INTO processed.metrics_long (asset, metric, ts, freq, value, is_missing, source_endpoint, ingested_at)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, now())"
        " ON CONFLICT (asset, metric, ts, freq) DO UPDATE SET"
        " value = EXCLUDED.value,"
        " is_missing = EXCLUDED.is_missing,"
        " source_endpoint = EXCLUDED.source_endpoint,"
        " ingested_at = EXCLUDED.ingested_at"
    )

    conn = get_conn()
    affected = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    params = (
                        r.get("asset"),
                        r.get("metric"),
                        r.get("ts"),
                        r.get("freq"),
                        r.get("value"),
                        r.get("is_missing", False),
                        r.get("source_endpoint"),
                    )
                    cur.execute(sql, params)
                    # rowcount is 1 for each insert/update
                    affected += cur.rowcount if cur.rowcount is not None else 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Attempted %s (insert+update)", affected)
    return affected


if __name__ == "__main__":
    print("load module")
