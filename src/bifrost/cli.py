import argparse

from bifrost.pipeline import run_medallion_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Bifrost Lakehouse Compute Router")
    parser.add_argument(
        "--engine",
        choices=["duckdb", "pyspark"],
        default=None,
        help="Force a specific compute engine instead of automatic metadata routing",
    )
    args = parser.parse_args()
    run_medallion_pipeline(force_engine=args.engine)


if __name__ == "__main__":
    main()
