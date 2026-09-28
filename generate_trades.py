"""
generate_trades.py  (STUDENT PROJECT - non-commercial, educational use only)

This is the file you run:   python generate_trades.py
(generate_exchange_rates.py must sit in the same folder.)

What it does, in plain English:
  SECTION 1 - Gets FX rates from generate_exchange_rates.py (live, or labelled fallback).
  SECTION 2 - Generates 50 synthetic (fake) coffee trades and attaches the FX rates.
  SECTION 3 - Checks the generated data for problems BEFORE it touches the database.
  SECTION 4 - Writes the trades into a local DuckDB file, all-or-nothing (transaction).

Every step prints what it is doing, so you can read the output like a story.
[OK] = step passed, [WARN] = something odd but handled, [FAIL] = step broke.
"""

import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# Re-use the FX fetcher and the shared print helpers from the other file.
from generate_exchange_rates import fail, fetch_live_fx_rates, info, ok, section, warn

# ----------------------------------------------------------------------------
# CONFIGURATION (everything you might want to change lives here)
# ----------------------------------------------------------------------------
# Override with an environment variable if you want the DB somewhere else.
DB_PATH = Path(os.environ.get("NKG_DB_PATH", "nkg_trading.duckdb")).resolve()
TABLE_NAME = "raw_coffee_trades"

N_TRADES = 50
RANDOM_SEED = None  # set to an int (e.g. 42) for reproducible fake data

ALLOWED_EUDR_STATUS = ["Verified", "Pending Polygon Mapping"]

# Column name -> DuckDB type. Single source of truth for table + DataFrame checks.
TABLE_COLUMNS = {
    "trade_id": "VARCHAR PRIMARY KEY",
    "trade_timestamp": "TIMESTAMP",  # stored in UTC
    "trader": "VARCHAR",
    "counterparty": "VARCHAR",
    "origin": "VARCHAR",
    "warehouse_location": "VARCHAR",
    "trade_type": "VARCHAR",
    "quantity_bags": "INTEGER",
    "purchase_price_usd": "DECIMAL(12,2)",
    "live_market_price_usd": "DECIMAL(12,2)",
    "fx_usd_eur": "DECIMAL(18,6)",
    "fx_usd_brl": "DECIMAL(18,6)",
    "eudr_status": "VARCHAR",
    "fx_source": "VARCHAR",  # 'live' or 'fallback' - tells you how trustworthy the FX is
    "run_id": "VARCHAR",  # which script run created the row
    "ingested_at": "TIMESTAMP",  # UTC time the row was written
}


# ----------------------------------------------------------------------------
# CUSTOM ERRORS (so the final error message says WHICH kind of problem it was)
# ----------------------------------------------------------------------------
class DataValidationError(Exception):
    """Generated data broke a rule (nulls, negative prices, duplicate ids...)."""


class SchemaMismatchError(Exception):
    """The table already in the DB has different columns than this script expects."""


# ----------------------------------------------------------------------------
# SECTION 2 - GENERATE TRADES
# ----------------------------------------------------------------------------
def generate_raw_coffee_trades(fx):
    section("SECTION 2 - Generating synthetic coffee trades")

    rng = np.random.default_rng(RANDOM_SEED)  # local generator, no global state
    info(f"Random seed: {RANDOM_SEED if RANDOM_SEED is not None else 'not fixed (different data every run)'}")

    run_id = uuid.uuid4().hex[:8]
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)  # naive UTC
    info(f"run_id = {run_id}, run timestamp (UTC) = {run_ts}")

    origins = ["Brazil (Santos)", "Colombia (Excelso)", "Vietnam (Robusta)", "Ethiopia (Yirgacheffe)"]
    warehouses = ["NKG Kala Hamburg", "Antwerp Port Hub", "Santos Warehouse", "Bremen Logistics Center"]
    roasters = ["Nestlé", "JDE Peet's", "Starbucks", "Tchibo", "Dallmayr"]
    traders = ["A. Mueller", "L. Schmidt", "C. Santos", "H. Weber"]

    n = N_TRADES
    base_purchase = np.round(rng.uniform(180, 240, size=n), 2)
    live_price = np.round(base_purchase + rng.uniform(-15, 20, size=n), 2)
    live_price = np.clip(live_price, 0.01, None)  # a price can never be zero/negative
    info(f"Created {n} purchase and market prices")

    df = pd.DataFrame(
        {
            # run_id in the id => running the script twice never collides on trade_id
            "trade_id": [f"NKG-{run_ts:%Y%m%d}-{run_id}-{i:04d}" for i in range(n)],
            "trade_timestamp": run_ts,  # one timestamp for the whole batch
            "trader": rng.choice(traders, size=n),
            "counterparty": rng.choice(roasters, size=n),
            "origin": rng.choice(origins, size=n),
            "warehouse_location": rng.choice(warehouses, size=n),
            "trade_type": rng.choice(
                ["Physical Buy", "Physical Sell", "Futures Hedge"], size=n, p=[0.45, 0.45, 0.10]
            ),
            "quantity_bags": rng.integers(200, 4000, size=n),
            "purchase_price_usd": base_purchase,
            "live_market_price_usd": live_price,
            "fx_usd_eur": fx["USD_EUR"],
            "fx_usd_brl": fx["USD_BRL"],
            "eudr_status": rng.choice(ALLOWED_EUDR_STATUS, size=n, p=[0.85, 0.15]),
            "fx_source": fx["source"],
            "run_id": run_id,
            "ingested_at": run_ts,
        }
    )
    ok(f"Built DataFrame with {len(df)} rows and {len(df.columns)} columns")
    info("Preview of first 3 rows:\n" + df.head(3).to_string(index=False))
    info("Trades per type:\n" + df["trade_type"].value_counts().to_string())
    return df, run_id


# ----------------------------------------------------------------------------
# SECTION 3 - VALIDATE
# ----------------------------------------------------------------------------
def validate_trades(df):
    section("SECTION 3 - Validating data before writing")

    if df.empty:
        raise DataValidationError("DataFrame is empty - nothing to ingest")
    ok(f"DataFrame has {len(df)} rows")

    missing = [c for c in TABLE_COLUMNS if c not in df.columns]
    if missing:
        raise DataValidationError(f"DataFrame is missing expected columns: {missing}")
    ok("All expected columns are present")

    null_counts = df[list(TABLE_COLUMNS)].isnull().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        raise DataValidationError(f"Null values found: {bad_nulls.to_dict()}")
    ok("No null values in any column")

    if df["trade_id"].duplicated().any():
        dupes = df.loc[df["trade_id"].duplicated(), "trade_id"].tolist()[:5]
        raise DataValidationError(f"Duplicate trade_ids inside this batch, e.g. {dupes}")
    ok("All trade_ids are unique within the batch")

    for col in ("purchase_price_usd", "live_market_price_usd", "fx_usd_eur", "fx_usd_brl"):
        if (df[col] <= 0).any():
            raise DataValidationError(f"Column '{col}' contains zero or negative values")
    ok("All prices and FX rates are positive")

    if (df["quantity_bags"] <= 0).any():
        raise DataValidationError("quantity_bags contains zero or negative values")
    ok("All quantities are positive")

    bad_status = set(df["eudr_status"]) - set(ALLOWED_EUDR_STATUS)
    if bad_status:
        raise DataValidationError(f"Unexpected eudr_status values: {sorted(bad_status)}")
    ok("All eudr_status values are in the allowed list")

    return df[list(TABLE_COLUMNS)]  # keep only (and in order) the columns the table has


# ----------------------------------------------------------------------------
# SECTION 4 - WRITE TO DUCKDB
# ----------------------------------------------------------------------------
def ensure_table(con):
    """Create the table if missing; if it exists, make sure its columns match ours."""
    column_sql = ",\n    ".join(f"{name} {dtype}" for name, dtype in TABLE_COLUMNS.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {column_sql}\n)")

    actual = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?", [TABLE_NAME]
        ).fetchall()
    }
    expected = set(TABLE_COLUMNS)
    if actual != expected:
        raise SchemaMismatchError(
            f"Table '{TABLE_NAME}' already exists with different columns.\n"
            f"           Missing in DB: {sorted(expected - actual)}\n"
            f"           Unexpected in DB: {sorted(actual - expected)}\n"
            f"           Fix: delete {DB_PATH} (fine for a learning project) or rename the table, then re-run."
        )
    ok(f"Table '{TABLE_NAME}' exists and its columns match the script")


def write_to_duckdb(df):
    section("SECTION 4 - Writing to DuckDB")
    info(f"Database file: {DB_PATH}")

    con = None
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            con = duckdb.connect(str(DB_PATH))
        except duckdb.IOException as exc:
            raise RuntimeError(
                f"Could not open the database (is another program, e.g. DBeaver/VS Code/another "
                f"Python session, holding it open? Close it and retry): {exc}"
            ) from exc
        ok("Connected to database")

        ensure_table(con)

        rows_before = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        info(f"Rows in table before insert: {rows_before}")

        cols = ", ".join(TABLE_COLUMNS)
        con.register("trades_df", df)
        info("Starting transaction (all rows are inserted, or none are)")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"INSERT INTO {TABLE_NAME} ({cols}) SELECT {cols} FROM trades_df")
            rows_after = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
            if rows_after - rows_before != len(df):
                raise RuntimeError(
                    f"Row count check failed: expected +{len(df)}, got +{rows_after - rows_before}"
                )
            con.execute("COMMIT")
            ok(f"Transaction committed. Rows in table after insert: {rows_after} (+{len(df)})")
        except Exception:
            con.execute("ROLLBACK")
            warn("Transaction rolled back - the database was left unchanged")
            raise
        finally:
            con.unregister("trades_df")
    finally:
        if con is not None:
            con.close()
            info("Database connection closed")


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    # Windows consoles can choke on characters like 'é' in "Nestlé"; force UTF-8 output.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print(f"NKG coffee trades ingestion - started {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    stage = "startup"
    try:
        stage = "SECTION 1 (fetch FX rates)"
        fx = fetch_live_fx_rates()

        stage = "SECTION 2 (generate trades)"
        df, run_id = generate_raw_coffee_trades(fx)

        stage = "SECTION 3 (validate data)"
        df = validate_trades(df)

        stage = "SECTION 4 (write to DuckDB)"
        write_to_duckdb(df)

    except DataValidationError as exc:
        fail(f"Stopped in {stage}. Bad data, nothing was written: {exc}")
        return 1
    except SchemaMismatchError as exc:
        fail(f"Stopped in {stage}. {exc}")
        return 1
    except duckdb.ConstraintException as exc:
        fail(f"Stopped in {stage}. Duplicate trade_id already in the table: {exc}")
        return 1
    except duckdb.Error as exc:
        fail(f"Stopped in {stage}. DuckDB error: {exc}")
        return 1
    except Exception as exc:  # last safety net - always say WHERE it broke
        fail(f"Stopped in {stage}. Unexpected {type(exc).__name__}: {exc}")
        return 1

    section("DONE")
    ok(f"Ingested {len(df)} raw trades (run_id={run_id}, fx_source={fx['source']}) into {DB_PATH.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
