"""Generate a minimal final Markdown report from profiling CSVs."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.logging import logger
from src.db.engine import get_conn


def generate_final_report(
    coverage_csv: str = "reports/profiling/tables/coverage.csv",
    missing_csv: str = "reports/profiling/tables/missing_rate.csv",
    output_md: str = "reports/final_report.md",
):
    cov_path = Path(coverage_csv)
    miss_path = Path(missing_csv)
    out_path = Path(output_md)

    if not cov_path.exists() or not miss_path.exists():
        msg = (
            "Coverage or missing_rate CSV not found. "
            "请先运行 `python scripts/20_profile_run.py` 生成 profiling 产物。"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    df_cov = pd.read_csv(cov_path)
    df_miss = pd.read_csv(miss_path)

    # Basic coverage stats
    n_assets = int(df_cov["asset"].nunique()) if "asset" in df_cov.columns else 0
    n_metrics = int(df_cov["metric"].nunique()) if "metric" in df_cov.columns else 0
    freqs = sorted(df_cov["freq"].dropna().unique()) if "freq" in df_cov.columns else []
    total_points = int(df_cov["n_points"].sum()) if "n_points" in df_cov.columns else 0

    # time range
    start_ts = None
    end_ts = None
    if "start_ts" in df_cov.columns:
        start_ts = pd.to_datetime(df_cov["start_ts"]).min()
    if "end_ts" in df_cov.columns:
        end_ts = pd.to_datetime(df_cov["end_ts"]).max()

    # missing rate stats
    if "missing_rate" in df_miss.columns:
        miss_mean = float(df_miss["missing_rate"].mean())
        miss_max = float(df_miss["missing_rate"].max())
    else:
        # try to compute from n_missing / n_points
        if "n_missing" in df_miss.columns and "n_points" in df_miss.columns:
            miss_series = df_miss.apply(lambda r: (r["n_missing"] / r["n_points"]) if r["n_points"] else 0.0, axis=1)
            miss_mean = float(miss_series.mean())
            miss_max = float(miss_series.max())
            df_miss = df_miss.copy()
            df_miss["missing_rate"] = miss_series
        else:
            miss_mean = 0.0
            miss_max = 0.0

    # top 5 highest missing_rate
    top_missing = df_miss.sort_values("missing_rate", ascending=False).head(5)

    # Prepare output directory
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# 最终报告\n")
    lines.append("## 项目概述\n")
    lines.append(
        "本项目基于 CoinMetrics 数据构建 ETL 流程并在本地 Postgres 中存储原始与处理后数据，通过统计分析评估指标的覆盖与缺失特性。"
    )
    # Explicit statement about current data being stub/smoke test data
    # Determine whether real CoinMetrics timeseries and processed rows exist
    real_data = False
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM raw.api_responses WHERE endpoint=%s AND status_code=200",
                ("timeseries/asset-metrics",),
            )
            timeseries_ok = int(cur.fetchone()[0])

            cur.execute("SELECT count(*) FROM processed.metrics_long")
            processed_count = int(cur.fetchone()[0])

        if timeseries_ok > 0 and processed_count > 0:
            real_data = True
    except Exception as exc:
        logger.warning("Could not determine data source counts: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if real_data:
        lines.append(
            "已接入真实 CoinMetrics API（catalog/assets 与 timeseries/asset-metrics），并完成 raw→processed→profiling。"
        )
    else:
        lines.append(
            "注意：当前仓库中使用的是 stub / smoke 测试数据，仅用于验证 ETL 与分析链路；后续 Sprint 将接入真实 CoinMetrics API 数据。"
        )
    lines.append("\n")

    lines.append("## 数据覆盖\n")
    lines.append(f"- 资产数（unique assets）：**{n_assets}**")
    lines.append(f"- 指标数（unique metrics）：**{n_metrics}**")
    lines.append(f"- 频率（freq）列表：**{', '.join(map(str, freqs))}**")
    lines.append(f"- 总点数（n_points 总和）：**{total_points}**")
    if start_ts is not None and end_ts is not None:
        lines.append(f"- 时间范围：**{start_ts}** ~ **{end_ts}**")
    lines.append("\n")

    lines.append("## 缺失情况\n")
    lines.append(f"- 缺失率平均值：**{miss_mean:.4f}**")
    lines.append(f"- 缺失率最大值：**{miss_max:.4f}**")
    lines.append("\n")

    lines.append("### 缺失率最高的前 5 条（若不足则全部）：\n")
    # Convert top_missing to markdown table without external dependencies
    if not top_missing.empty:
        # Ensure required columns exist
        cols = [c for c in ["asset", "metric", "freq", "missing_rate"] if c in top_missing.columns]
        if not cols:
            lines.append("无可用缺失率数据。")
        else:
            # Build markdown table header
            header = "| " + " | ".join(cols) + " |"
            sep = "| " + " | ".join(["---"] * len(cols)) + " |"
            lines.append(header)
            lines.append(sep)
            for _, r in top_missing.iterrows():
                vals = [str(r[c]) for c in cols]
                lines.append("| " + " | ".join(vals) + " |")
    else:
        lines.append("无可用缺失率数据。")
    lines.append("\n")

    # 时间覆盖与结构分析（Sprint 2）
    struct_path = Path("reports/profiling/tables/coverage_structure.csv")
    if struct_path.exists():
        try:
            df_struct = pd.read_csv(struct_path)
            struct_rows = len(df_struct)
            # coverage_ratio stats (ignore nulls)
            if "coverage_ratio" in df_struct.columns:
                cr_series = pd.to_numeric(df_struct["coverage_ratio"], errors="coerce").dropna()
                cr_mean = float(cr_series.mean()) if not cr_series.empty else None
                cr_min = float(cr_series.min()) if not cr_series.empty else None
            else:
                cr_mean = None
                cr_min = None

            lines.append("## 时间覆盖与结构分析（Sprint 2）\n")
            lines.append(f"- coverage_structure 总行数：**{struct_rows}**")
            if cr_mean is not None:
                lines.append(f"- coverage_ratio 平均值：**{cr_mean:.4f}**")
            if cr_min is not None:
                lines.append(f"- coverage_ratio 最小值：**{cr_min:.4f}**")

            # lowest coverage_ratio top 5
            low_cr = df_struct.copy()
            if "coverage_ratio" in low_cr.columns:
                low_cr["coverage_ratio"] = pd.to_numeric(low_cr["coverage_ratio"], errors="coerce")
                low_cr = low_cr.dropna(subset=["coverage_ratio"]).sort_values("coverage_ratio", ascending=True).head(5)
            else:
                low_cr = low_cr.head(5)

            lines.append("\n")
            lines.append("### coverage_ratio 最低的前 5 条（若不足则全部）：\n")
            if not low_cr.empty:
                cols = [c for c in ["asset", "metric", "freq", "span_days", "n_points", "expected_points", "coverage_ratio"] if c in low_cr.columns]
                if cols:
                    header = "| " + " | ".join(cols) + " |"
                    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
                    lines.append(header)
                    lines.append(sep)
                    for _, r in low_cr.iterrows():
                        vals = [str(r[c]) for c in cols]
                        lines.append("| " + " | ".join(vals) + " |")
                else:
                    lines.append("无可用 coverage_structure 数据。")
            else:
                lines.append("无可用 coverage_ratio 数据。")
            lines.append("\n")
        except Exception as exc:
            logger.warning("Failed to read coverage_structure.csv: %s", exc)
    else:
        # no coverage_structure available
        lines.append("## 时间覆盖与结构分析（Sprint 2）\n")
        lines.append("- 未找到 coverage_structure 表（reports/profiling/tables/coverage_structure.csv）。请先运行 profiling 步骤。\n")

    lines.append("## 指标用途评估（最小结论）\n")
    lines.append("- 覆盖点数较多的指标/资产通常更适合用于跨资产比较。")
    lines.append("- 低缺失率意味着该指标更稳定，适合横向比较与解释性分析。")
    if not real_data:
        lines.append("- 目前样本量较小或为 stub 数据，仅用于功能演示；真实运行 CoinMetrics 后请扩规模以获取更可靠结论。")
    lines.append("- 报告为描述性与解释性分析，不构成预测或投资建议。")

    # 指标尺度与可比性讨论（Sprint 2）
    scale_path = Path("reports/profiling/tables/metric_scale.csv")
    lines.append("\n")
    if scale_path.exists():
        try:
            df_scale = pd.read_csv(scale_path)
            rows_scale = len(df_scale)

            # list unique magnitude_order values
            mag_vals = []
            if "magnitude_order" in df_scale.columns:
                mag_vals = sorted(df_scale["magnitude_order"].dropna().unique().tolist())

            lines.append("## 指标尺度与可比性讨论（Sprint 2）\n")
            lines.append(f"- metric_scale 总行数：**{rows_scale}**")
            lines.append(f"- magnitude_order 唯一值：**{', '.join(map(str, mag_vals)) if mag_vals else '无'}**")
            lines.append("\n")

            # Include full table (small rows expected)
            if not df_scale.empty:
                cols = [c for c in ["asset", "metric", "freq", "n_values", "min_value", "max_value", "mean_value", "std_value", "magnitude_order", "coefficient_of_variation"] if c in df_scale.columns]
                header = "| " + " | ".join(cols) + " |"
                sep = "| " + " | ".join(["---"] * len(cols)) + " |"
                lines.append(header)
                lines.append(sep)
                for _, r in df_scale.iterrows():
                    vals = [str(r[c]) if pd.notna(r[c]) else "" for c in cols]
                    lines.append("| " + " | ".join(vals) + " |")
                lines.append("\n")

            # Dynamic interpretation bullets
            bullets = []
            try:
                # compute per-asset magnitude spread
                if "magnitude_order" in df_scale.columns:
                    tmp = df_scale.dropna(subset=["magnitude_order"]).copy()
                    tmp["magnitude_order"] = pd.to_numeric(tmp["magnitude_order"], errors="coerce")
                    spread = tmp.groupby("asset")["magnitude_order"].agg(lambda s: (s.max() - s.min()) if not s.empty else 0)
                    large_diff_assets = spread[spread >= 2]
                    if not large_diff_assets.empty:
                        example = large_diff_assets.index[0]
                        bullets.append(f"不同指标数值尺度差异显著（例如资产 {example} 中存在差异 >= 10^2），直接对原值同图比较会掩盖结构；建议对数变换/归一化或分别绘图。")
                    else:
                        bullets.append("指标尺度差异较小，原值同图比较的可解释性较好。")
                else:
                    bullets.append("无法计算 magnitude_order（缺失列），请检查 metric_scale 输出。")
            except Exception:
                bullets.append("无法自动判定尺度差异，建议手动检查 metric_scale 表。")

            # Add an extra general suggestion
            bullets.append("建议对数变换或标准化作为常见处理，尤其在不同数量级指标同时展示时。")

            for b in bullets:
                lines.append(f"- {b}")
            lines.append("\n")
        except Exception as exc:
            logger.warning("Failed to read metric_scale.csv: %s", exc)
    else:
        lines.append("## 指标尺度与可比性讨论（Sprint 2）\n")
        lines.append("- 未找到 metric_scale 表（reports/profiling/tables/metric_scale.csv）。请先运行 profiling 步骤。\n")

    # Assemble text once and write with explicit overwrite mode
    out_text = "\n".join(lines)
    # Trim accidental trailing percent signs or stray characters, ensure ends with newline
    out_text = out_text.rstrip()
    while out_text.endswith("%"):
        out_text = out_text[:-1].rstrip()
    if not out_text.endswith("\n"):
        out_text += "\n"

    # Write explicitly in overwrite mode to avoid accidental appends
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(out_text)

    logger.info("Wrote final report to %s", out_path)


if __name__ == "__main__":
    generate_final_report()
