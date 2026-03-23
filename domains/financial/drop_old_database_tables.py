"""
Drop the five finance tables in a given database/schema. Used to remove SE_DEMO_DB tables.

Usage (drop SE_DEMO_DB tables; .env stays SE_DEMOS_NEW):
  SNOWFLAKE_DATABASE=SE_DEMO_DB SNOWFLAKE_SCHEMA=PUBLIC python drop_old_database_tables.py
  (Or FINANCE_MAIN if that schema exists in the target database.)

Drops: transactions, portfolio_holdings, loans, accounts, customers.
"""
import logging
import sys

from snowflake_connection import get_snowflake_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TABLES = ["transactions", "portfolio_holdings", "loans", "accounts", "customers"]


def drop_old_tables():
    import os
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    db = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA") or "FINANCE_MAIN"
    try:
        logger.info("Dropping tables in %s.%s (order: %s)", db, schema, ", ".join(TABLES))
        for table in TABLES:
            try:
                qualified = f"{db}.{schema}.{table}" if db and schema else table
                cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
                logger.info("  Dropped %s", qualified)
            except Exception as e:
                logger.warning("  %s: %s", table, e)
        logger.info("Done.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    drop_old_tables()
    sys.exit(0)
