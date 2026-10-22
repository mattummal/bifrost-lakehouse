from pathlib import Path

from bifrost.pipeline import run_bifrost_soda_validation, run_medallion_pipeline


def test_medallion_pipeline_and_contracts(tmp_path: Path) -> None:
    # Run the medallion pipeline (Bronze -> Silver -> Gold)
    run_medallion_pipeline()

    # Run Soda Core multi-tier data contracts against the generated Delta tables
    contracts_passed = run_bifrost_soda_validation()
    assert contracts_passed, "Soda Core data contract checks failed across tiers!"
