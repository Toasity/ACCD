"""Run profiling and export CSVs + figures."""
import sys
from src.analysis.profiling import run_profiling


def main(argv=None):
    summary = run_profiling()
    print("Profiling summary:")
    print(f" coverage CSV: {summary.get('coverage_csv')} (rows={summary.get('rows_coverage')})")
    print(f" missing rate CSV: {summary.get('missing_rate_csv')} (rows={summary.get('rows_missing_rate')})")
    print(f" value histogram: {summary.get('value_hist')} (n_values={summary.get('n_values')})")
    # Generate final markdown report (Task 2 + Task 4 minimal evidence)
    try:
        from src.analysis.reporting import generate_final_report

        generate_final_report(
            coverage_csv=summary.get("coverage_csv"),
            missing_csv=summary.get("missing_rate_csv"),
            output_md="reports/final_report.md",
        )
        print(" final report:", "reports/final_report.md")
    except Exception as e:
        # Log error but do not fail the profiling step
        from src.utils.logging import logger

        logger.error("Failed to generate final_report.md: %s", e)
    return 0


if __name__ == "__main__":
    code = main()
    sys.exit(code)
