"""
generate_futures_prices.py  (STUDENT PROJECT - non-commercial, educational use only)

What this file does, in plain English:
  Real ICE coffee futures prices come from licensed (paid) data feeds, so this
  project SIMULATES an ICE-style price instead. Every row is labelled
  price_source = 'simulated' so it can never be mistaken for real market data.

  The price is a "random walk": each run looks up the previous price stored in the
  database and moves it by a small random percentage. Over many runs the price
  drifts up and down like a real market, which makes the MtM P&L move over time.

It is imported by generate_trades.py (run that file, not this one).
"""

from pathlib import Path

import duckdb
import pandas as pd

from generate_exchange_rates import info, ok, section, warn

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
TABLE_NAME = "raw_futures_prices"

# Column name -> DuckDB type. One row is written per run.
TABLE_COLUMNS = {
    "run_id": "VARCHAR PRIMARY KEY",
    "price_timestamp": "TIMESTAMP",  # UTC
    "contract": "VARCHAR",
    "price_usd_per_bag": "DECIMAL(12,2)",  # USD per 60 kg bag
    "price_source": "VARCHAR",  # always 'simulated' in this project
}

CONTRACT_NAME = "ICE-style coffee futures (SIMULATED)"
PRICE_SOURCE = "simulated"
START_PRICE_USD = 210.00  # used only on the very first run
STEP_VOLATILITY = 0.01  # typical move per run: about 1% (one standard deviation)
PRICE_FLOOR_USD = 120.00  # keeps the simulation in a sensible range
PRICE_CEILING_USD = 320.00


def read_last_price(db_path):
    """Return the most recent stored futures price (float), or None if there is none yet."""
    db_path = Path(db_path)
    if not db_path.exists():
        info("No database file yet - this is the very first run")
        return None

    con = None
    try:
        try:
            con = duckdb.connect(str(db_path), read_only=True)
        except duckdb.IOException as exc:
            raise RuntimeError(
                f"Could not open the database to read the last price (is another program, e.g. "
                f"DBeaver or another Python session, holding it open? Close it and retry): {exc}"
            ) from exc

        table_count = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [TABLE_NAME]
        ).fetchone()[0]
        if table_count == 0:
            info(f"Table '{TABLE_NAME}' does not exist yet - first run for prices")
            return None

        row = con.execute(
            f"SELECT price_usd_per_bag FROM {TABLE_NAME} ORDER BY price_timestamp DESC, run_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            info(f"Table '{TABLE_NAME}' is empty - first run for prices")
            return None
        return float(row[0])
    finally:
        if con is not None:
            con.close()


def simulate_futures_price(rng, db_path, run_id, run_ts):
    """
    Returns (one_row_dataframe, new_price_float).
    Steps: read previous price -> apply a small random move -> keep it in a sane range.
    """
    section("SECTION 2 - Simulating the ICE-style futures price")
    info("Real ICE futures prices are licensed data, so this project SIMULATES one.")
    info("Method: random walk. Start from the previous stored price and move it by a small random %.")
    info(f"Every row is labelled price_source='{PRICE_SOURCE}'.")

    previous = read_last_price(db_path)
    if previous is None:
        previous = START_PRICE_USD
        info(f"Starting from the default price of {START_PRICE_USD} USD per bag")
    else:
        info(f"Previous stored price: {previous} USD per bag")

    step = float(rng.normal(0, STEP_VOLATILITY))
    unclipped = previous * (1 + step)
    new_price = round(min(max(unclipped, PRICE_FLOOR_USD), PRICE_CEILING_USD), 2)
    info(f"Random move this run: {step:+.2%}")
    if new_price != round(unclipped, 2):
        warn(
            f"Price {round(unclipped, 2)} was outside {PRICE_FLOOR_USD}-{PRICE_CEILING_USD}; "
            f"clipped to {new_price}"
        )

    df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "price_timestamp": run_ts,
                "contract": CONTRACT_NAME,
                "price_usd_per_bag": new_price,
                "price_source": PRICE_SOURCE,
            }
        ]
    )
    ok(f"New simulated futures price: {new_price} USD per bag (was {round(previous, 2)})")
    return df, new_price
