#!/usr/bin/env python3
"""Build WEALTH_POC in required phase order."""

# EXISTING DOMAIN AUDIT:
# Existing domain core tables/columns verified in create_schema.py.
# Iran/oil event window found in data_generator.py (2026-02-01..2026-03-10).
# Daily update behavior verified in daily_financial_update.py; append/update semantics preserved.
# Existing domain had no explicit random seed.

from __future__ import annotations

from pathlib import Path

from snowflake_connection import get_snowflake_connection
from generators import wealth_activity, wealth_anomaly_override, wealth_benchmarks, wealth_cash_flows, wealth_dimensions, wealth_holdings, wealth_performance, wealth_trades


def run_sql_file(sql_path: Path):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE SE_DEMOS_NEW")
        cur.execute("CREATE SCHEMA IF NOT EXISTS SE_DEMOS_NEW.WEALTH_POC")
        cur.execute("USE SCHEMA SE_DEMOS_NEW.WEALTH_POC")
        with open(sql_path, "r", encoding="utf-8") as f:
            script = f.read()
        for _ in conn.execute_string(script):
            pass
        conn.commit()
    finally:
        cur.close()
        conn.close()


def phase0_audit():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE SE_DEMOS_NEW")
        cur.execute(
            """SELECT table_name, row_count
               FROM information_schema.tables
               WHERE table_schema = 'WEALTH_POC'
               ORDER BY table_name"""
        )
        rows = cur.fetchall()
        print("Phase 0 audit table counts:")
        for r in rows:
            print(r)
    finally:
        cur.close()
        conn.close()


def main():
    root = Path(__file__).parent
    phase0_audit()
    run_sql_file(root / "schema" / "wealth_poc_ddl.sql")
    wealth_benchmarks.main()
    wealth_dimensions.main()
    wealth_performance.main()
    wealth_holdings.main()
    wealth_trades.main()
    wealth_activity.main()
    wealth_cash_flows.main()
    wealth_anomaly_override.main()
    run_sql_file(root / "schema" / "wealth_poc_views.sql")
    print("WEALTH_POC build complete.")


if __name__ == "__main__":
    main()
