"""
refresh_loop.py - keeps the data live for the dashboard.
STUDENT PROJECT - non-commercial.

What it does, in plain English:
  Repeats forever: run the trade loader (a SMALL batch of new trades, plus one
  new futures price), run dbt to rebuild the metric tables, then wait. Start
  this in its own terminal and leave it running - the Streamlit dashboard just
  reads whatever this loop last wrote.

  The batch is kept small ON PURPOSE, and the loader prunes old runs after
  every write (see generate_trades.py, "Pruning old runs"), so this can run
  for hours without the database file growing without bound. The effect on
  the dashboard is a small, visible change roughly once a minute, rather than
  a big jump every few minutes.

Run it from the project root:
    uv run python refresh_loop.py
Stop it with Ctrl+C.
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

INTERVAL_SECONDS = 60  # how often a new batch lands - matches the dashboard's own refresh
TRADES_PER_CYCLE = 5  # small on purpose; keeps each cycle's data (and the DB) small
DBT_PROJECT_DIR = Path(__file__).resolve().parent / "dbt_nkg"


def stamp():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def run_step(label, cmd, cwd=None, env=None):
    """Run one command, stream its output live, and return True/False for pass/fail."""
    print(f"[{stamp()}] [START] {label}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode == 0:
        print(f"[{stamp()}] [OK   ] {label}")
        return True
    print(f"[{stamp()}] [FAIL ] {label} (exit code {result.returncode}) - see output above for the reason")
    return False


def main():
    print(
        f"Refresh loop started - {TRADES_PER_CYCLE} trades every {INTERVAL_SECONDS} s, "
        "old runs pruned automatically. Press Ctrl+C to stop."
    )
    loader_env = {**os.environ, "NKG_TRADE_COUNT": str(TRADES_PER_CYCLE)}
    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n{'=' * 60}\nCycle {cycle} - {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n{'=' * 60}")

            loaded = run_step("Load new trades", [sys.executable, "generate_trades.py"], env=loader_env)
            if loaded:
                run_step("Rebuild dbt models", ["uv", "run", "dbt", "run"], cwd=DBT_PROJECT_DIR)
            else:
                print(f"[{stamp()}] [WARN ] Skipping dbt run - the loader failed, so the raw data did not change")

            print(f"[{stamp()}] Sleeping {INTERVAL_SECONDS} s until the next cycle")
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"\n[{stamp()}] Stopped by user (Ctrl+C)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
