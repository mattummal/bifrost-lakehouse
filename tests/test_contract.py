from pathlib import Path

import pytest
from deltalake import write_deltalake

from bifrost.pipeline import run_bifrost_soda_validation, run_medallion_pipeline
from tests.conftest import make_bronze_table, tick


def test_medallion_pipeline_and_contracts(sample_bronze_delta_table: Path) -> None:
    # Pipeline enforces the contract itself (raises on failure); re-validate via env-based resolution.
    run_medallion_pipeline(force_engine="duckdb")

    assert run_bifrost_soda_validation(), "Soda Core data contract checks failed across tiers!"


def test_contract_validates_the_resolved_tables_not_the_default_dir(sample_bronze_delta_table: Path) -> None:
    """
    Regression: validation used to read hardcoded ./.lakehouse_data paths. Corrupting the isolated
    Silver table must now make the contract fail.
    """
    lakehouse_root = sample_bronze_delta_table.parent.parent
    run_medallion_pipeline(force_engine="duckdb")

    dup = tick("BTCUSDT", 101, 60000.0, 2.0, 60010.0, 2.0, 1729190000000000)
    corrupted = make_bronze_table([dup, dup]).append_column("mid_price", [[60005.0, 60005.0]])
    write_deltalake(str(lakehouse_root / "silver" / "orderbook_validated"), corrupted, mode="overwrite")

    assert not run_bifrost_soda_validation()


def test_contract_rejects_missing_contract_file(sample_bronze_delta_table: Path, tmp_path: Path) -> None:
    run_medallion_pipeline(force_engine="duckdb")

    with pytest.raises(FileNotFoundError):
        run_bifrost_soda_validation(contract_path=tmp_path / "missing.yml")
