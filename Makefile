.PHONY: up up-azurite down ingest transform transform-spark test test-rust test-python lint clean

# Default: Zero-dependency native local Delta Lake storage
up:
	mkdir -p .lakehouse_data/bronze .lakehouse_data/silver .lakehouse_data/gold
	@echo "✓ Local Medallion Lakehouse initialized at ./.lakehouse_data/ (Zero-Docker mode)"

# Optional: Run against Azurite container if Docker is installed
up-azurite:
	docker compose up -d

down:
	-docker compose down

ingest:
	@set -a; [ -f .env ] && . .env; set +a; \
	cd crates/edge-ingester && cargo run --release

transform:
	@set -a; [ -f .env ] && . .env; set +a; \
	uv run bifrost

transform-spark:
	@set -a; [ -f .env ] && . .env; set +a; \
	uv run --extra spark bifrost --engine=pyspark

test-rust:
	cd crates/edge-ingester && cargo test

test-python:
	@set -a; [ -f .env ] && . .env; set +a; \
	uv run pytest -v

lint:
	cd crates/edge-ingester && cargo clippy -- -D warnings
	uv run ruff check .
	uv run mypy src tests

test: test-rust test-python lint

clean:
	rm -rf .lakehouse_data .azurite_data .pytest_cache .mypy_cache .ruff_cache