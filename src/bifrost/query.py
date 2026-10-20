import sqlglot

# Canonical Gold Microstructure Query authored in DuckDB dialect
CANONICAL_GOLD_MICROSTRUCTURE_SQL = """
SELECT
    symbol,
    DATE_TRUNC('minute', to_timestamp(exchange_ts_us / 1000000.0)) AS window_start,
    COUNT(*) AS tick_count,
    ROUND(AVG((best_bid_price + best_ask_price) / 2.0), 4) AS avg_mid_price,
    ROUND(AVG(
        (best_bid_qty * best_ask_price + best_ask_qty * best_bid_price)
        / NULLIF(best_bid_qty + best_ask_qty, 0.0)
    ), 4) AS avg_vw_micro_price,
    ROUND(AVG(
        ((best_ask_price - best_bid_price) / ((best_bid_price + best_ask_price) / 2.0)) * 10000.0
    ), 4) AS avg_spread_bps,
    ROUND(AVG(
        (best_bid_qty - best_ask_qty) / NULLIF(best_bid_qty + best_ask_qty, 0.0)
    ), 4) AS avg_orderbook_imbalance
FROM silver_ticks
GROUP BY 1, 2
ORDER BY window_start DESC, symbol
"""


def get_gold_query(target_dialect: str = "duckdb") -> str:
    """
    Transpiles the canonical DuckDB microstructure SQL into the target execution engine's dialect
    (e.g., 'duckdb', 'spark', 'databricks').
    """
    return sqlglot.transpile(
        CANONICAL_GOLD_MICROSTRUCTURE_SQL,
        read="duckdb",
        write=target_dialect,
        pretty=True,
    )[0]
