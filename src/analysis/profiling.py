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
