"""
Fill missing transaction dates between two dates.
Usage: python fill_missing_transactions.py
       (fills 2026-02-19 through 2026-03-05 by default)
"""
from snowflake_connection import get_snowflake_connection
from daily_financial_update import get_existing_accounts, insert_transactions
from data_generator import generate_transactions
from datetime import date, timedelta
import logging
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

START_DATE = date(2026, 2, 19)
END_DATE = date(2026, 3, 5)


def fill_missing_dates():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # All dates we want in range [START_DATE, END_DATE]
        all_dates = []
        d = START_DATE
        while d <= END_DATE:
            all_dates.append(d)
            d += timedelta(days=1)

        # Existing transaction dates in that range
        cursor.execute("""
            SELECT DISTINCT transaction_date 
            FROM transactions 
            WHERE transaction_date BETWEEN %s AND %s
            ORDER BY transaction_date
        """, (START_DATE, END_DATE))
        existing_dates = {row[0] for row in cursor.fetchall()}

        missing_dates = [d for d in all_dates if d not in existing_dates]
        if not missing_dates:
            logger.info(f"No missing dates between {START_DATE} and {END_DATE}. All {len(all_dates)} days have data.")
            return

        logger.info(f"Found {len(missing_dates)} missing days between {START_DATE} and {END_DATE}.")
        logger.info(f"Missing dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")

        accounts = get_existing_accounts(cursor)
        if not accounts:
            logger.error("No active accounts found. Cannot generate transactions.")
            return

        logger.info(f"Using {len(accounts)} active accounts for transaction generation.")

        total_inserted = 0
        for d in missing_dates:
            count = random.randint(500, 1500)
            txns = generate_transactions(accounts, transactions_per_day=count, target_date=d)
            logger.info(f"  {d}: generating {len(txns)} transactions")
            insert_transactions(cursor, txns)
            total_inserted += len(txns)

        logger.info(f"Done. Inserted {total_inserted:,} transactions for {len(missing_dates)} days.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    fill_missing_dates()
