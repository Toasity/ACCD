"""Minimal profiling: export coverage/missing_rate tables and a histogram.

This module reads the `analysis.metric_coverage` and
`analysis.metric_missing_rate` views and writes CSVs plus a histogram
of non-null `value` observations from `processed.metrics_long`.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import matplotlib.pyplot as plt

from src.db.engine import get_conn
from src.utils.logging import logger


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def run_profiling(output_dir: str = "reports/profiling") -> Dict[str, Any]:
    out = {}
    out_dir = Path(output_dir)
    tables_dir = out_dir / "tables"
    figures_dir = out_dir / "figures"

    _ensure_dir(tables_dir)
    _ensure_dir(figures_dir)

    conn = get_conn()
    try:
        # Read coverage view with proper column names
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analysis.metric_coverage ORDER BY asset, metric, freq")
            cov_rows = cur.fetchall()
            cov_cols = [d[0] for d in cur.description] if cur.description else None

        df_cov = pd.DataFrame(cov_rows, columns=cov_cols)
        cov_path = tables_dir / "coverage.csv"
        df_cov.to_csv(cov_path, index=False, header=True)
        out["coverage_csv"] = str(cov_path)
        out["rows_coverage"] = len(df_cov)

        # Read missing rate view
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analysis.metric_missing_rate ORDER BY asset, metric, freq")
            miss_rows = cur.fetchall()
            miss_cols = [d[0] for d in cur.description] if cur.description else None

        df_miss = pd.DataFrame(miss_rows, columns=miss_cols)
        miss_path = tables_dir / "missing_rate.csv"
        df_miss.to_csv(miss_path, index=False, header=True)
        out["missing_rate_csv"] = str(miss_path)
        out["rows_missing_rate"] = len(df_miss)

        # Compute coverage structure (Sprint 2.1 - Coverage Structure Analysis)
        try:
            # compute_coverage_structure is defined below
            df_cov_struct = compute_coverage_structure(conn)
            struct_path = tables_dir / "coverage_structure.csv"
            df_cov_struct.to_csv(struct_path, index=False, header=True)
            out["coverage_structure_csv"] = str(struct_path)
            out["rows_coverage_structure"] = len(df_cov_struct)
        except Exception as exc:
            logger.warning("Failed to compute coverage structure: %s", exc)

        # Remove any testing temp file if present
        tmp_file = tables_dir / "_tmp.csv"
        if tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                logger.warning("Could not remove temporary file %s", tmp_file)

        # Read values and plot histogram
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM processed.metrics_long WHERE value IS NOT NULL")
            value_rows = cur.fetchall()

        # value_rows is list of tuples like [(val,), (val,), ...]
        values = [r[0] for r in value_rows if r and r[0] is not None]
        n_values = len(values)
        fig_path = figures_dir / "value_hist.png"

        if n_values > 0:
            plt.figure()
            plt.hist(values)
            plt.xlabel("value")
            plt.ylabel("count")
            plt.title("Histogram of metric values")
            plt.tight_layout()
            plt.savefig(fig_path)
            plt.close()
        else:
            # create an empty placeholder image
            plt.figure()
            plt.text(0.5, 0.5, "no values", ha="center", va="center")
            plt.axis("off")
            plt.savefig(fig_path)
            plt.close()

        out["value_hist"] = str(fig_path)
        out["n_values"] = n_values

    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Profiling outputs: %s", out)
    return out


if __name__ == "__main__":
    summary = run_profiling()
    print(summary)


def compute_coverage_structure(conn) -> pd.DataFrame:
    """Compute coverage structure metrics per (asset, metric, freq).

    Expects the view `analysis.metric_coverage` to contain at least:
      asset, metric, freq, start_ts, end_ts, n_points

    Returns a DataFrame with columns:
      asset, metric, freq, start_ts, end_ts, span_days, n_points,
      expected_points, coverage_ratio
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT asset, metric, freq, start_ts, end_ts, n_points FROM analysis.metric_coverage ORDER BY asset, metric, freq"
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else None

    df = pd.DataFrame(rows, columns=cols if cols is not None else ["asset", "metric", "freq", "start_ts", "end_ts", "n_points"])

    # Ensure timestamp columns are parsed
    if "start_ts" in df.columns:
        df["start_ts"] = pd.to_datetime(df["start_ts"], utc=True)
    if "end_ts" in df.columns:
        df["end_ts"] = pd.to_datetime(df["end_ts"], utc=True)

    # Compute span_days: inclusive days between start and end
    def _span_days(row):
        try:
            if pd.isna(row["start_ts"]) or pd.isna(row["end_ts"]):
                return None
            delta = (row["end_ts"].normalize() - row["start_ts"].normalize()).days
            return int(delta) + 1
        except Exception:
            return None

    df["span_days"] = df.apply(_span_days, axis=1)

    # expected_points: only compute for daily frequency '1d'
    def _expected(row):
        if row.get("freq") == "1d" and row.get("span_days") is not None:
            return int(row["span_days"])
        return None

    df["expected_points"] = df.apply(_expected, axis=1)

    # coverage_ratio: n_points / expected_points when expected_points > 0
    def _ratio(row):
        try:
            exp = row.get("expected_points")
            n = row.get("n_points")
            if exp is None or exp == 0 or pd.isna(n):
                return None
            return float(n) / float(exp)
        except Exception:
            return None

    df["coverage_ratio"] = df.apply(_ratio, axis=1)

    # Reorder columns for clarity
    out_cols = ["asset", "metric", "freq", "start_ts", "end_ts", "span_days", "n_points", "expected_points", "coverage_ratio"]
    for c in out_cols:
        if c not in df.columns:
            df[c] = None

    return df[out_cols]
