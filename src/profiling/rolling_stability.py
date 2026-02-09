"""Rolling Stability analysis for metrics.

Produces rolling-window stability statistics per (asset, metric, freq).

Functions:
- compute_rolling_stability(conn) -> pd.DataFrame
- run_rolling_stability(output_dir="reports/profiling") -> dict

This module follows the project's lightweight style (psycopg2 + pandas).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from src.db.engine import get_conn
from src.utils.logging import logger


def compute_rolling_stability(conn) -> pd.DataFrame:
    """Compute rolling stability stats per (asset, metric, freq).

    Returns a DataFrame with columns:
      asset, metric, freq, mean_rolling_std, max_rolling_std, mean_rolling_cv

    Notes:
    - Uses a 30-day time window (`rolling(window='30D')`) on the DatetimeIndex.
    - Expects `processed.metrics_long` to contain `asset, metric, freq, ts, value`.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT asset, metric, freq, ts, value FROM processed.metrics_long WHERE value IS NOT NULL ORDER BY asset, metric, freq, ts"
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else None

    df = pd.DataFrame(rows, columns=cols if cols is not None else ["asset", "metric", "freq", "ts", "value"]) 

    if df.empty:
        return pd.DataFrame(
            columns=["asset", "metric", "freq", "mean_rolling_std", "max_rolling_std", "mean_rolling_cv"]
        )

    # parse and ensure ordering
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    results = []

    grouped = df.groupby(["asset", "metric", "freq"], sort=True)
    for (asset, metric, freq), g in grouped:
        g = g.sort_values("ts").set_index("ts")

        try:
            # rolling window 30 days (time-based)
            roll = g["value"].rolling("30D")
            rmean = roll.mean()
            rstd = roll.std()
            # rolling coefficient of variation
            with pd.option_context("mode.use_inf_as_na", True):
                rcv = (rstd / rmean).replace([float("inf"), float("-inf")], pd.NA)

            mean_rolling_std = float(rstd.mean()) if not rstd.dropna().empty else None
            max_rolling_std = float(rstd.max()) if not rstd.dropna().empty else None
            mean_rolling_cv = float(rcv.mean()) if not rcv.dropna().empty else None
        except Exception as exc:
            logger.warning("Rolling computation failed for %s/%s/%s: %s", asset, metric, freq, exc)
            mean_rolling_std = None
            max_rolling_std = None
            mean_rolling_cv = None

        results.append(
            {
                "asset": asset,
                "metric": metric,
                "freq": freq,
                "mean_rolling_std": mean_rolling_std,
                "max_rolling_std": max_rolling_std,
                "mean_rolling_cv": mean_rolling_cv,
            }
        )

    out_df = pd.DataFrame(results)
    return out_df[["asset", "metric", "freq", "mean_rolling_std", "max_rolling_std", "mean_rolling_cv"]]


def run_rolling_stability(output_dir: str = "reports/profiling") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out_dir = Path(output_dir)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    conn = get_conn()
    try:
        df = compute_rolling_stability(conn)
        path = tables_dir / "rolling_stability.csv"
        df.to_csv(path, index=False, header=True)
        out["rolling_stability_csv"] = str(path)
        out["rows_rolling_stability"] = len(df)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Rolling stability outputs: %s", out)
    return out


if __name__ == "__main__":
    print(run_rolling_stability())
