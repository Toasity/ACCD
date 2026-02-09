"""CLI entry for running ETL stages.

Supported stages: extract, transform, load, all. Currently only `extract`
is implemented (writes a stub row into `raw.api_responses`).
"""
import sys
import argparse
import pathlib

# Small import fallback: if `src` is not importable (e.g., when running
# the script directly in some environments), add project root to `sys.path`.
try:
    import src  # type: ignore
except ModuleNotFoundError:
    root = pathlib.Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from src.utils.logging import logger
from src.etl.extract import run_extract


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run ETL stages")
    parser.add_argument("--stage", default="extract", choices=["extract", "transform", "load", "all"], help="Which stage to run")
    args = parser.parse_args(argv)

    logger.info("Starting ETL stage=%s", args.stage)

    if args.stage in ("extract", "all"):
        inserted = run_extract()
        print(inserted)
        logger.info("ETL extract completed, inserted id=%s", inserted)
        if args.stage == "extract":
            return 0

    # Transform stage: parse raw -> rows
    if args.stage in ("transform", "load", "all"):
        from src.etl.transform import transform_latest_raw

        rows = transform_latest_raw(limit=50)
        logger.info("Transformed rows count=%s", len(rows))
        if args.stage == "transform":
            # Print sample
            if rows:
                print("sample:", rows[0])
            print(len(rows))
            return 0

    # Load stage: upsert transformed rows
    if args.stage in ("load", "all"):
        from src.etl.load import upsert_metrics

        # If we came from extract in 'all', rows variable may not be defined yet
        try:
            rows
        except NameError:
            from src.etl.transform import transform_latest_raw

            rows = transform_latest_raw(limit=50)

        affected = upsert_metrics(rows)
        print(affected)
        return 0

    logger.warning("Stage %s not implemented in this minimal runner", args.stage)
    return 0


if __name__ == "__main__":
    code = main()
    sys.exit(code)
