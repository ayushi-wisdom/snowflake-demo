"""
One-time: remove transactions in the last 6 months for "dormant" accounts (~8% of accounts),
so existing data shows some accounts with no use in 6 months. Uses same deterministic rule as generate_transactions.

Run once after adding dormant logic. Then recalculates account balances.

Usage: python apply_dormant_to_existing_data.py
"""
import logging
import sys
from datetime import date, timedelta

from snowflake_connection import get_snowflake_connection
from data_generator import DORMANT_DAYS, _is_dormant_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def apply_dormant():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT account_id FROM accounts")
        all_ids = [row[0] for row in cursor.fetchall()]
        dormant_ids = [aid for aid in all_ids if _is_dormant_account(aid)]
        logger.info("Found %s dormant accounts (out of %s)", len(dormant_ids), len(all_ids))
        if not dormant_ids:
            return

        cutoff = date.today() - timedelta(days=DORMANT_DAYS)
        placeholders = ",".join(["%s"] * len(dormant_ids))
        cursor.execute(
            f"DELETE FROM transactions WHERE account_id IN ({placeholders}) AND transaction_date >= %s",
            dormant_ids + [cutoff],
        )
        deleted = cursor.rowcount
        logger.info("Deleted %s recent transactions for dormant accounts (since %s)", deleted, cutoff)

        logger.info("Recalculating account balances...")
        cursor.execute(
            """
            UPDATE accounts a
            SET
                current_balance = COALESCE((
                    SELECT SUM(t.amount) FROM transactions t
                    WHERE t.account_id = a.account_id AND t.status = 'Completed'
                ), 0),
                available_balance = COALESCE((
                    SELECT SUM(t.amount) FROM transactions t
                    WHERE t.account_id = a.account_id AND t.status = 'Completed'
                ), 0) * 0.95,
                balance_last_updated_date = %s
            """,
            (date.today(),),
        )
        logger.info("Done. %s accounts now have no transactions in the last 6 months.", len(dormant_ids))
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    apply_dormant()
    sys.exit(0)
