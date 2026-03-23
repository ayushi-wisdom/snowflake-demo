"""
One-time migration: add rate_type and original_interest_rate to loans for variable-rate reset demo.
Run once on existing Snowflake DB. Safe to re-run if columns already exist (skips ADD).
"""
import logging
import sys
from snowflake_connection import get_snowflake_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def migrate():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        # Use project target
        cursor.execute("USE DATABASE SE_DEMOS_NEW")
        cursor.execute("USE SCHEMA FINANCE_MAIN")

        # Add columns if not present
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = CURRENT_DATABASE() AND table_schema = CURRENT_SCHEMA()
              AND table_name = 'LOANS'
        """)
        existing = {row[0].upper() for row in cursor.fetchall()}
        if "RATE_TYPE" not in existing:
            cursor.execute("ALTER TABLE loans ADD COLUMN rate_type VARCHAR(20) DEFAULT 'fixed'")
            logger.info("Added column: rate_type")
        else:
            logger.info("Column rate_type already exists")
        if "ORIGINAL_INTEREST_RATE" not in existing:
            cursor.execute("ALTER TABLE loans ADD COLUMN original_interest_rate FLOAT")
            logger.info("Added column: original_interest_rate")
        else:
            logger.info("Column original_interest_rate already exists")

        # Backfill: set original_interest_rate = interest_rate where null
        cursor.execute("""
            UPDATE loans SET original_interest_rate = interest_rate WHERE original_interest_rate IS NULL
        """)
        logger.info("Backfilled original_interest_rate for %s rows", cursor.rowcount)

        # Set ~30% of loans to variable (deterministic by loan_id hash)
        cursor.execute("""
            UPDATE loans
            SET rate_type = 'variable'
            WHERE MOD(ABS(HASH(loan_id)), 100) < 30
        """)
        logger.info("Set rate_type = 'variable' for %s loans", cursor.rowcount)

        conn.commit()
        logger.info("Migration complete.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        logger.exception("Migration failed")
        sys.exit(1)
