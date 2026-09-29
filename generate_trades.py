"""
generate_trades.py  (STUDENT PROJECT - non-commercial, educational use only)

This is the file you run:   python generate_trades.py
(generate_exchange_rates.py and generate_futures_prices.py must sit in the same folder.)

What it does, in plain English:
  SECTION 1 - Gets USD->EUR / USD->BRL rates (live, or clearly labelled fallback).
  SECTION 2 - On the FIRST run of each Berlin calendar day only: seeds a plausible
              random-walk price history from 08:00 to now, so the price chart never
              opens as a single flat point (see generate_futures_prices.py).
  SECTION 3 - Simulates today's live ICE-style futures price tick (labelled 'simulated').
  SECTION 4 - Generates a small batch of synthetic coffee trades priced around that
              futures price, plus the shipping containers that carry the physical
              purchases to port hubs.
  SECTION 5 - Checks all the generated data for problems BEFORE it touches the database.
  SECTION 6 - Writes three tables to DuckDB in ONE transaction (all rows land, or none do):
                raw_futures_prices, raw_coffee_trades, raw_shipments
  SECTION 7 - Prunes rows from BEFORE today (Berlin calendar day) out of every raw table,
              so the database resets itself each morning instead of growing forever.

Every step prints what it is doing, so you can read the output like a story.
[OK] = step passed, [WARN] = something odd but handled, [FAIL] = step broke.
"""

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd

from generate_exchange_rates import fail, fetch_live_fx_rates, info, ok, section, warn
from generate_futures_prices import TABLE_COLUMNS as FUTURES_COLUMNS
from generate_futures_prices import TABLE_NAME as FUTURES_TABLE
from generate_futures_prices import backfill_todays_price_history, simulate_futures_price

# ----------------------------------------------------------------------------
# CONFIGURATION (everything you might want to change lives here)
# ----------------------------------------------------------------------------
# Override with an environment variable if you want the DB somewhere else.
DB_PATH = Path(os.environ.get("NKG_DB_PATH", "nkg_trading.duckdb")).resolve()

N_TRADES = int(os.environ.get("NKG_TRADE_COUNT", "50"))  # override for a lighter, faster loop cadence
RANDOM_SEED = None  # set to an int (e.g. 42) for reproducible fake data
PRICE_SPREAD_USD = 15  # trade prices are set within +/- this many USD of the futures price

# Pruning: this project is meant to run only during a German working day (see
# refresh_loop.py) and be stopped overnight, so "keep everything from today"
# is simpler and safer than a row-count cutoff - a row-count budget has to be
# recalculated by hand every time the loop's cadence changes (it already bit
# us once: at 1 run/minute, "keep the last 240 runs" only covers 4 hours,
# which would have quietly deleted the morning's backfilled price history by
# early afternoon). "Today, Berlin calendar day" needs no tuning and can
# never lose data still relevant to today's chart.
BUSINESS_TZ = ZoneInfo("Europe/Berlin")

ORIGINS = ["Brazil (Santos)", "Colombia (Excelso)", "Vietnam (Robusta)", "Ethiopia (Yirgacheffe)"]
WAREHOUSES = ["NKG Kala Hamburg", "Antwerp Port Hub", "Santos Warehouse", "Bremen Logistics Center"]
ROASTERS = ["Nestlé", "JDE Peet's", "Starbucks", "Tchibo", "Dallmayr"]
TRADERS = ["A. Mueller", "L. Schmidt", "C. Santos", "H. Weber"]
TRADE_TYPES = ["Physical Buy", "Physical Sell", "Futures Hedge"]
TRADE_TYPE_PROBABILITIES = [0.45, 0.45, 0.10]
ALLOWED_EUDR_STATUS = ["Verified", "Pending Polygon Mapping"]

# Column name -> DuckDB type. Trade prices are USD per 60 kg bag.
TRADES_TABLE = "raw_coffee_trades"
TRADES_COLUMNS = {
    "trade_id": "VARCHAR PRIMARY KEY",
    "trade_timestamp": "TIMESTAMP",  # UTC
    "trader": "VARCHAR",
    "counterparty": "VARCHAR",
    "origin": "VARCHAR",
    "warehouse_location": "VARCHAR",
    "trade_type": "VARCHAR",
    "quantity_bags": "INTEGER",
    "trade_price_usd": "DECIMAL(12,2)",  # price agreed on the trade (buy price or sell price)
    "fx_usd_eur": "DECIMAL(18,6)",  # EUR per 1 USD when the row was loaded
    "fx_usd_brl": "DECIMAL(18,6)",
    "fx_source": "VARCHAR",  # 'live' or 'fallback'
    "eudr_status": "VARCHAR",
    "run_id": "VARCHAR",  # which script run created the row
    "ingested_at": "TIMESTAMP",  # UTC
}

# Shipping containers: only Physical Buy trades create containers (goods arriving).
SHIPMENTS_TABLE = "raw_shipments"
SHIPMENTS_COLUMNS = {
    "container_id": "VARCHAR PRIMARY KEY",
    "trade_id": "VARCHAR",
    "origin": "VARCHAR",
    "origin_port": "VARCHAR",
    "destination_hub": "VARCHAR",
    "bags_in_container": "INTEGER",
    "status": "VARCHAR",
    "eta_date": "DATE",
    "run_id": "VARCHAR",
    "ingested_at": "TIMESTAMP",  # UTC
}

BAGS_PER_CONTAINER = 275  # roughly one 20-foot container of 60 kg bags
ORIGIN_PORTS = {
    "Brazil (Santos)": "Port of Santos",
    "Colombia (Excelso)": "Cartagena",
    "Vietnam (Robusta)": "Ho Chi Minh City (Cat Lai)",
    "Ethiopia (Yirgacheffe)": "Djibouti",
}
# status -> (earliest, latest) ETA in days from today (negative = already happened)
SHIPMENT_STATUS_ETA_DAYS = {
    "Booked at Origin": (30, 50),
    "In Transit": (7, 35),
    "Arrived at Hub": (-2, 0),
    "Customs Clearance": (0, 5),
    "Delivered to Warehouse": (-20, -1),
}
SHIPMENT_STATUS_PROBABILITIES = [0.15, 0.35, 0.15, 0.10, 0.25]


# ----------------------------------------------------------------------------
# CUSTOM ERRORS (so the final error message says WHICH kind of problem it was)
# ----------------------------------------------------------------------------
class DataValidationError(Exception):
    """Generated data broke a rule (nulls, negative prices, duplicate ids...)."""


class SchemaMismatchError(Exception):
    """A table already in the DB has different columns than this script expects."""


# ----------------------------------------------------------------------------
# SECTION 3 - GENERATE TRADES AND SHIPMENTS
# ----------------------------------------------------------------------------
def generate_raw_coffee_trades(fx, futures_price, rng, run_id, run_ts):
    section("SECTION 4 - Generating synthetic trades and shipments")
    info(
        f"Trade prices are set within +/- {PRICE_SPREAD_USD} USD of the simulated futures price "
        f"({futures_price} USD per bag)"
    )

    n = N_TRADES
    trade_price = np.round(futures_price + rng.uniform(-PRICE_SPREAD_USD, PRICE_SPREAD_USD, size=n), 2)
    trade_price = np.clip(trade_price, 0.01, None)  # a price can never be zero/negative
    info(f"Created {n} trade prices")

    df = pd.DataFrame(
        {
            # run_id in the id => running the script twice never collides on trade_id
            "trade_id": [f"NKG-{run_ts:%Y%m%d}-{run_id}-{i:04d}" for i in range(n)],
            "trade_timestamp": run_ts,  # one timestamp for the whole batch
            "trader": rng.choice(TRADERS, size=n),
            "counterparty": rng.choice(ROASTERS, size=n),
            "origin": rng.choice(ORIGINS, size=n),
            "warehouse_location": rng.choice(WAREHOUSES, size=n),
            "trade_type": rng.choice(TRADE_TYPES, size=n, p=TRADE_TYPE_PROBABILITIES),
            "quantity_bags": rng.integers(200, 4000, size=n),
            "trade_price_usd": trade_price,
            "fx_usd_eur": fx["USD_EUR"],
            "fx_usd_brl": fx["USD_BRL"],
            "fx_source": fx["source"],
            "eudr_status": rng.choice(ALLOWED_EUDR_STATUS, size=n, p=[0.85, 0.15]),
            "run_id": run_id,
            "ingested_at": run_ts,
        }
    )
    ok(f"Built trades DataFrame with {len(df)} rows and {len(df.columns)} columns")
    preview_cols = ["trade_id", "trade_type", "origin", "quantity_bags", "trade_price_usd"]
    info("Preview of first 3 trades:\n" + df[preview_cols].head(3).to_string(index=False))
    info("Trades per type:\n" + df["trade_type"].value_counts().to_string())
    return df


def generate_shipments(trades_df, rng, run_id, run_ts):
    """
    Every Physical Buy is split into containers of up to 275 bags. All containers of one
    trade share a status (they travel together); their ETAs differ by a couple of days.
    """
    info("--- Shipments: turning each Physical Buy into shipping containers ---")
    info(f"Rule: one container holds up to {BAGS_PER_CONTAINER} bags; only Physical Buys create containers")

    buys = trades_df[trades_df["trade_type"] == "Physical Buy"]
    status_names = list(SHIPMENT_STATUS_ETA_DAYS)
    today = run_ts.date()
    rows = []
    counter = 0

    for trade in buys.itertuples(index=False):
        full_containers, remainder = divmod(int(trade.quantity_bags), BAGS_PER_CONTAINER)
        loads = [BAGS_PER_CONTAINER] * full_containers + ([remainder] if remainder else [])

        status = str(rng.choice(status_names, p=SHIPMENT_STATUS_PROBABILITIES))
        earliest, latest = SHIPMENT_STATUS_ETA_DAYS[status]
        base_offset = int(rng.integers(earliest, latest + 1))

        for bags in loads:
            eta = today + timedelta(days=base_offset + int(rng.integers(0, 3)))
            rows.append(
                {
                    "container_id": f"CNT-{run_id}-{counter:05d}",
                    "trade_id": trade.trade_id,
                    "origin": trade.origin,
                    "origin_port": ORIGIN_PORTS[trade.origin],
                    "destination_hub": trade.warehouse_location,
                    "bags_in_container": bags,
                    "status": status,
                    "eta_date": eta,
                    "run_id": run_id,
                    "ingested_at": run_ts,
                }
            )
            counter += 1

    df = pd.DataFrame(rows, columns=list(SHIPMENTS_COLUMNS))
    ok(f"{len(buys)} Physical Buy trades became {len(df)} containers")
    if not df.empty:
        info("Containers per status:\n" + df["status"].value_counts().to_string())
    else:
        warn("No Physical Buy trades in this batch, so there are no containers this run")
    return df


# ----------------------------------------------------------------------------
# SECTION 4 - VALIDATE
# ----------------------------------------------------------------------------
def _require_columns_and_no_nulls(label, df, columns):
    """Shared checks: expected columns exist and none of them contain nulls."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise DataValidationError(f"{label}: missing expected columns: {missing}")
    null_counts = df[list(columns)].isnull().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        raise DataValidationError(f"{label}: null values found: {bad_nulls.to_dict()}")
    ok(f"{label}: all expected columns present, no nulls")


def validate_futures(df):
    info("--- Checking futures price ---")
    if len(df) != 1:
        raise DataValidationError(f"Futures prices: expected exactly 1 row per run, got {len(df)}")
    _require_columns_and_no_nulls("Futures prices", df, FUTURES_COLUMNS)
    if (df["price_usd_per_bag"] <= 0).any():
        raise DataValidationError("Futures prices: price must be positive")
    ok("Futures prices: exactly 1 row and the price is positive")
    return df[list(FUTURES_COLUMNS)]


def validate_trades(df):
    info("--- Checking trades ---")
    if df.empty:
        raise DataValidationError("Trades: DataFrame is empty - nothing to ingest")
    ok(f"Trades: DataFrame has {len(df)} rows")

    _require_columns_and_no_nulls("Trades", df, TRADES_COLUMNS)

    if df["trade_id"].duplicated().any():
        dupes = df.loc[df["trade_id"].duplicated(), "trade_id"].tolist()[:5]
        raise DataValidationError(f"Trades: duplicate trade_ids inside this batch, e.g. {dupes}")
    ok("Trades: all trade_ids are unique within the batch")

    for col in ("trade_price_usd", "fx_usd_eur", "fx_usd_brl"):
        if (df[col] <= 0).any():
            raise DataValidationError(f"Trades: column '{col}' contains zero or negative values")
    ok("Trades: all prices and FX rates are positive")

    if (df["quantity_bags"] <= 0).any():
        raise DataValidationError("Trades: quantity_bags contains zero or negative values")
    ok("Trades: all quantities are positive")

    bad_types = set(df["trade_type"]) - set(TRADE_TYPES)
    if bad_types:
        raise DataValidationError(f"Trades: unexpected trade_type values: {sorted(bad_types)}")
    bad_status = set(df["eudr_status"]) - set(ALLOWED_EUDR_STATUS)
    if bad_status:
        raise DataValidationError(f"Trades: unexpected eudr_status values: {sorted(bad_status)}")
    ok("Trades: trade_type and eudr_status values are all in the allowed lists")

    return df[list(TRADES_COLUMNS)]


def validate_shipments(shipments_df, trades_df):
    info("--- Checking shipments ---")
    if shipments_df.empty:
        warn("Shipments: empty batch (no Physical Buys) - nothing to check")
        return shipments_df[list(SHIPMENTS_COLUMNS)]

    _require_columns_and_no_nulls("Shipments", shipments_df, SHIPMENTS_COLUMNS)

    if shipments_df["container_id"].duplicated().any():
        raise DataValidationError("Shipments: duplicate container_ids inside this batch")
    ok("Shipments: all container_ids are unique")

    if (shipments_df["bags_in_container"] <= 0).any():
        raise DataValidationError("Shipments: bags_in_container must be positive")
    if (shipments_df["bags_in_container"] > BAGS_PER_CONTAINER).any():
        raise DataValidationError(f"Shipments: a container holds more than {BAGS_PER_CONTAINER} bags")
    ok(f"Shipments: every container holds between 1 and {BAGS_PER_CONTAINER} bags")

    bad_status = set(shipments_df["status"]) - set(SHIPMENT_STATUS_ETA_DAYS)
    if bad_status:
        raise DataValidationError(f"Shipments: unexpected status values: {sorted(bad_status)}")
    ok("Shipments: all status values are in the allowed list")

    unknown_trades = set(shipments_df["trade_id"]) - set(trades_df["trade_id"])
    if unknown_trades:
        raise DataValidationError(f"Shipments: containers point at unknown trade_ids, e.g. {sorted(unknown_trades)[:3]}")
    ok("Shipments: every container belongs to a trade in this batch")

    # The bags across a trade's containers must add up to the trade's quantity.
    shipped = shipments_df.groupby("trade_id")["bags_in_container"].sum()
    expected = trades_df.loc[trades_df["trade_type"] == "Physical Buy"].set_index("trade_id")["quantity_bags"]
    mismatched = shipped.reindex(expected.index).fillna(-1) != expected
    if mismatched.any():
        raise DataValidationError(
            f"Shipments: container bags do not add up to the trade quantity for {int(mismatched.sum())} trade(s)"
        )
    ok("Shipments: container bags add up exactly to each Physical Buy quantity")

    return shipments_df[list(SHIPMENTS_COLUMNS)]


# ----------------------------------------------------------------------------
# SECTION 5 - WRITE TO DUCKDB
# ----------------------------------------------------------------------------
def ensure_table(con, table, columns):
    """Create the table if missing; if it exists, make sure its columns match ours."""
    column_sql = ",\n    ".join(f"{name} {dtype}" for name, dtype in columns.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} (\n    {column_sql}\n)")

    actual = {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table]
        ).fetchall()
    }
    expected = set(columns)
    if actual != expected:
        raise SchemaMismatchError(
            f"Table '{table}' already exists with different columns.\n"
            f"           Missing in DB: {sorted(expected - actual)}\n"
            f"           Unexpected in DB: {sorted(actual - expected)}\n"
            f"           Fix: delete {DB_PATH} (fine for a learning project) and re-run."
        )
    ok(f"Table '{table}' exists and its columns match the script")


def count_rows(con, table):
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def write_to_duckdb(batches):
    """batches: list of (table_name, columns_dict, dataframe). Written in ONE transaction."""
    section("SECTION 6 - Writing to DuckDB")
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

        for table, columns, _ in batches:
            ensure_table(con, table, columns)

        rows_before = {table: count_rows(con, table) for table, _, _ in batches}
        info(f"Rows before insert: {rows_before}")

        info("Starting transaction (every table gets its rows, or none do)")
        con.execute("BEGIN TRANSACTION")
        try:
            for table, columns, df in batches:
                if df.empty:
                    warn(f"{table}: nothing to insert this run")
                    continue
                cols = ", ".join(columns)
                con.register("batch_df", df)
                try:
                    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM batch_df")
                finally:
                    con.unregister("batch_df")
                added = count_rows(con, table) - rows_before[table]
                if added != len(df):
                    raise RuntimeError(f"{table}: row count check failed, expected +{len(df)}, got +{added}")
                ok(f"{table}: inserted {added} rows")
            con.execute("COMMIT")
            ok("Transaction committed")
        except Exception:
            con.execute("ROLLBACK")
            warn("Transaction rolled back - the database was left unchanged")
            raise
    finally:
        if con is not None:
            con.close()
            info("Database connection closed")


def berlin_midnight_utc(now_utc):
    """
    Midnight at the start of 'now's Berlin calendar date, expressed as a naive
    UTC timestamp (matching how every timestamp in this project is stored).
    Used only for pruning - "delete anything from before today" - which is a
    different boundary from workday_start_utc's 08:00 (used for backfill).
    """
    now_utc_aware = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    now_berlin = now_utc_aware.astimezone(BUSINESS_TZ)
    midnight_berlin = now_berlin.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_berlin.astimezone(timezone.utc).replace(tzinfo=None)


def prune_old_runs(now_utc):
    """
    Keeps the database bounded without ever needing to be re-tuned when the
    loop's cadence changes: deletes any row timestamped before the start of
    today (Berlin calendar day) from every raw table. Since this project is
    only meant to run during a working day and be stopped overnight, "keep
    today, drop everything older" naturally resets the database each morning
    at whatever size a working day's worth of data comes to - it also means
    this morning's backfilled price history is never at risk of being pruned
    away later the same afternoon, which a fixed row-count budget could do.

    This is its own short connection, separate from the write transaction in
    write_to_duckdb, so a pruning problem can never undo data that was
    already safely written.
    """
    section("SECTION 7 - Pruning rows from before today (Berlin calendar day)")
    cutoff = berlin_midnight_utc(now_utc)
    info(f"Keeping rows from {cutoff} UTC (today, Europe/Berlin) onward; anything older is removed")

    con = None
    try:
        con = duckdb.connect(str(DB_PATH))
        # Futures prices are keyed by price_timestamp; trades and shipments
        # both carry ingested_at, which is when this project actually wrote
        # them (simpler and always present, unlike trade_timestamp/eta_date
        # which mean different things on the two tables).
        for table, time_column in (
            (FUTURES_TABLE, "price_timestamp"),
            (TRADES_TABLE, "ingested_at"),
            (SHIPMENTS_TABLE, "ingested_at"),
        ):
            table_exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
            ).fetchone()[0]
            if not table_exists:
                info(f"{table}: does not exist yet - nothing to prune")
                continue
            before = count_rows(con, table)
            con.execute(f"DELETE FROM {table} WHERE {time_column} < ?", [cutoff])
            after = count_rows(con, table)
            removed = before - after
            if removed:
                info(f"{table}: removed {removed} row(s) from before today ({after} row(s) remain)")
            else:
                ok(f"{table}: nothing to prune yet ({after} row(s), all from today)")
    finally:
        if con is not None:
            con.close()


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
        # One id and one timestamp shared by everything created in this run.
        run_id = uuid.uuid4().hex[:8]
        run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        rng = np.random.default_rng(RANDOM_SEED)  # local generator, no global state
        info(f"run_id = {run_id}, run timestamp (UTC) = {run_ts}")
        info(f"Random seed: {RANDOM_SEED if RANDOM_SEED is not None else 'not fixed (different data every run)'}")

        stage = "SECTION 1 (fetch FX rates)"
        fx = fetch_live_fx_rates()

        # Backfill runs BEFORE the live tick and is non-fatal on its own: if it
        # fails, today's chart just starts a little thinner than planned, but
        # that must never stop this run's own live price/trades/shipments from
        # being generated and written.
        stage = "SECTION 2 (seed today's price history, first run of the day only)"
        try:
            backfill_todays_price_history(rng, DB_PATH, now_utc=run_ts)
        except duckdb.Error as exc:
            warn(f"Backfilling today's price history failed (today's chart may start thin): {exc}")

        stage = "SECTION 3 (simulate futures price)"
        futures_df, futures_price = simulate_futures_price(rng, DB_PATH, run_id, run_ts)

        stage = "SECTION 4 (generate trades and shipments)"
        trades_df = generate_raw_coffee_trades(fx, futures_price, rng, run_id, run_ts)
        shipments_df = generate_shipments(trades_df, rng, run_id, run_ts)

        stage = "SECTION 5 (validate data)"
        section("SECTION 5 - Validating data before writing")
        futures_df = validate_futures(futures_df)
        trades_df = validate_trades(trades_df)
        shipments_df = validate_shipments(shipments_df, trades_df)

        stage = "SECTION 6 (write to DuckDB)"
        write_to_duckdb(
            [
                (FUTURES_TABLE, FUTURES_COLUMNS, futures_df),
                (TRADES_TABLE, TRADES_COLUMNS, trades_df),
                (SHIPMENTS_TABLE, SHIPMENTS_COLUMNS, shipments_df),
            ]
        )

        # Pruning runs AFTER the write above has already succeeded and committed,
        # so a pruning problem is only ever a warning, never a reason to report
        # this run as failed - this run's data is safely on disk either way.
        try:
            prune_old_runs(run_ts)
        except duckdb.Error as exc:
            warn(f"Pruning old runs failed, but this run's data was already written safely: {exc}")

    except DataValidationError as exc:
        fail(f"Stopped in {stage}. Bad data, nothing was written: {exc}")
        return 1
    except SchemaMismatchError as exc:
        fail(f"Stopped in {stage}. {exc}")
        return 1
    except RuntimeError as exc:
        fail(f"Stopped in {stage}. {exc}")
        return 1
    except duckdb.ConstraintException as exc:
        fail(f"Stopped in {stage}. A primary-key rule was broken (duplicate id already in a table): {exc}")
        return 1
    except duckdb.Error as exc:
        fail(f"Stopped in {stage}. DuckDB error: {exc}")
        return 1
    except Exception as exc:  # last safety net - always say WHERE it broke
        fail(f"Stopped in {stage}. Unexpected {type(exc).__name__}: {exc}")
        return 1

    section("DONE")
    ok(
        f"Run {run_id}: 1 futures price ({futures_price} USD/bag, simulated), {len(trades_df)} trades and "
        f"{len(shipments_df)} containers written to {DB_PATH.name} (fx_source={fx['source']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
