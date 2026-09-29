"""
generate_futures_prices.py  (STUDENT PROJECT - non-commercial, educational use only)

What this file does, in plain English:
  Real ICE coffee futures prices come from licensed (paid) data feeds, so this
  project SIMULATES an ICE-style price instead. Every row is labelled
  price_source = 'simulated' (or 'simulated-backfill', see below) so it can
  never be mistaken for real market data.

  The price is a "random walk": each run looks up the previous price stored in
  the database and moves it by a small random percentage. Over many runs the
  price drifts up and down like a real market, which makes the MtM P&L and the
  dashboard's price chart move over time.

  BACKFILL: the plan is to run this only during a German working day (roughly
  08:00-17:00 Europe/Berlin) and stop it overnight. That means the very first
  run each morning would otherwise start the price chart as a single flat
  point. To avoid that, the FIRST run of each Berlin calendar day seeds a
  plausible random-walk history from 08:00 to "now", a few minutes apart, so
  the chart already shows a realistic morning by the time anyone looks at it.
  This only ever touches raw_futures_prices - it never invents trades or
  shipments for those backfilled timestamps, so P&L and exposure numbers are
  never affected by it, only the price chart is.

It is imported by generate_trades.py (run that file, not this one).
"""

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from generate_exchange_rates import info, ok, section, warn

# ----------------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------------
TABLE_NAME = "raw_futures_prices"

# Column name -> DuckDB type. One row per live run, plus a batch on the first
# run of each day (see backfill_todays_price_history below).
TABLE_COLUMNS = {
    "run_id": "VARCHAR PRIMARY KEY",
    "price_timestamp": "TIMESTAMP",  # UTC
    "contract": "VARCHAR",
    "price_usd_per_bag": "DECIMAL(12,2)",  # USD per 60 kg bag
    "price_source": "VARCHAR",  # 'simulated' (this run's own tick) or 'simulated-backfill'
}

CONTRACT_NAME = "ICE-style coffee futures (SIMULATED)"
PRICE_SOURCE = "simulated"
BACKFILL_PRICE_SOURCE = "simulated-backfill"
START_PRICE_USD = 210.00  # used only if there is no earlier price at all (very first run ever)
STEP_VOLATILITY = 0.01  # typical move per step: about 1% (one standard deviation)
PRICE_FLOOR_USD = 120.00  # keeps the simulation in a sensible range
PRICE_CEILING_USD = 320.00

# Backfill: how far apart the seeded historical points are, and a safety cap
# on how many it will ever generate in one go (guards against a wrong clock
# or a very late first run turning into thousands of rows).
BUSINESS_TZ = ZoneInfo("Europe/Berlin")
WORKDAY_START_HOUR = 8  # 08:00 Europe/Berlin - matches the planned Azure schedule
BACKFILL_INTERVAL_MINUTES = 5
MAX_BACKFILL_POINTS = 200


def workday_start_utc(now_utc):
    """
    Today's 08:00 Europe/Berlin, expressed as a naive UTC timestamp (matching
    how every other timestamp in this project is stored). If 'now' is already
    before 08:00 Berlin time, returns 'now' itself instead - there is nothing
    to backfill yet before the working day has started.

    Accepts `now_utc` either naive (the project's usual convention, e.g.
    datetime.now(timezone.utc).replace(tzinfo=None)) or timezone-aware - a
    naive value is always treated as UTC, never as the caller's local time.
    """
    now_utc_aware = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    now_berlin = now_utc_aware.astimezone(BUSINESS_TZ)
    start_berlin = now_berlin.replace(hour=WORKDAY_START_HOUR, minute=0, second=0, microsecond=0)
    if now_berlin < start_berlin:
        start_berlin = now_berlin
    return start_berlin.astimezone(timezone.utc).replace(tzinfo=None)


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


def _walk_one_step(rng, previous_price):
    """One random-walk step, shared by the live tick and the backfill loop."""
    step = float(rng.normal(0, STEP_VOLATILITY))
    unclipped = previous_price * (1 + step)
    clipped = round(min(max(unclipped, PRICE_FLOOR_USD), PRICE_CEILING_USD), 2)
    if clipped != round(unclipped, 2):
        warn(f"Price {round(unclipped, 2)} was outside {PRICE_FLOOR_USD}-{PRICE_CEILING_USD}; clipped to {clipped}")
    return clipped


def backfill_todays_price_history(rng, db_path, now_utc):
    """
    Runs on EVERY call, but only actually does anything on the first call of
    each Berlin calendar day: if raw_futures_prices already has a row at or
    after today's 08:00 Berlin, it does nothing (today's history has already
    started). Otherwise it seeds one point every BACKFILL_INTERVAL_MINUTES
    from 08:00 to now, continuing the random walk from wherever the price
    last was (yesterday's close, if any), so the chart opens with a plausible
    morning instead of a single flat point.

    Accepts `now_utc` either naive-UTC or timezone-aware (see workday_start_utc).
    """
    section("SECTION 2 - Seeding today's price history (first run of the day only)")
    db_path = Path(db_path)
    if now_utc.tzinfo is not None:  # normalise to naive UTC, matching every stored timestamp
        now_utc = now_utc.astimezone(timezone.utc).replace(tzinfo=None)
    start_ts = workday_start_utc(now_utc)

    con = None
    try:
        if db_path.exists():
            con = duckdb.connect(str(db_path))
            table_exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [TABLE_NAME]
            ).fetchone()[0]
            already_today = (
                con.execute(
                    f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE price_timestamp >= ?", [start_ts]
                ).fetchone()[0]
                if table_exists
                else 0
            )
        else:
            table_exists = False
            already_today = 0

        if already_today > 0:
            info(f"Already have {already_today} price point(s) since {start_ts} UTC (today) - no backfill needed")
            return

        n_points = int((now_utc - start_ts).total_seconds() // (BACKFILL_INTERVAL_MINUTES * 60))
        n_points = max(0, min(n_points, MAX_BACKFILL_POINTS))
        if n_points == 0:
            info("Nothing to backfill yet (the working day has just started, or hasn't started)")
            return

        info(
            f"First run today - seeding {n_points} historical price points, "
            f"{BACKFILL_INTERVAL_MINUTES} min apart, from {start_ts} UTC (08:00 Berlin) to now"
        )

        previous = read_last_price(db_path)
        if previous is None:
            previous = START_PRICE_USD
            info(f"No earlier price on record - backfill starts from the default {START_PRICE_USD} USD per bag")
        else:
            info(f"Continuing the random walk from the last known price: {previous} USD per bag")

        rows = []
        price = previous
        for i in range(1, n_points + 1):
            price = _walk_one_step(rng, price)
            rows.append(
                {
                    "run_id": f"backfill-{uuid.uuid4().hex[:8]}",
                    "price_timestamp": start_ts + timedelta(minutes=BACKFILL_INTERVAL_MINUTES * i),
                    "contract": CONTRACT_NAME,
                    "price_usd_per_bag": price,
                    "price_source": BACKFILL_PRICE_SOURCE,
                }
            )
        df = pd.DataFrame(rows, columns=list(TABLE_COLUMNS))
        if (df["price_usd_per_bag"] <= 0).any():
            raise RuntimeError("Backfill produced a non-positive price - refusing to write it")

        if con is None:  # db file didn't exist before this call
            db_path.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(str(db_path))
        if not table_exists:
            column_sql = ",\n    ".join(f"{name} {dtype}" for name, dtype in TABLE_COLUMNS.items())
            con.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {column_sql}\n)")

        cols = ", ".join(TABLE_COLUMNS)
        con.register("backfill_df", df)
        try:
            con.execute(f"INSERT INTO {TABLE_NAME} ({cols}) SELECT {cols} FROM backfill_df")
        finally:
            con.unregister("backfill_df")
        ok(f"Backfilled {len(df)} price points, ending at {price} USD/bag - today's live ticks continue from there")
    finally:
        if con is not None:
            con.close()


def simulate_futures_price(rng, db_path, run_id, run_ts):
    """
    Returns (one_row_dataframe, new_price_float) - today's single live tick.
    Steps: read previous price (which may be a point backfill_todays_price_history
    just added) -> apply a small random move -> keep it in a sane range.
    """
    section("SECTION 3 - Simulating today's live futures price tick")
    info("Real ICE futures prices are licensed data, so this project SIMULATES one.")
    info("Method: random walk. Start from the previous stored price and move it by a small random %.")
    info(f"This row is labelled price_source='{PRICE_SOURCE}'.")

    previous = read_last_price(db_path)
    if previous is None:
        previous = START_PRICE_USD
        info(f"Starting from the default price of {START_PRICE_USD} USD per bag")
    else:
        info(f"Previous stored price: {previous} USD per bag")

    new_price = _walk_one_step(rng, previous)
    info(f"Random move this run: {(new_price / previous - 1):+.2%}")

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
