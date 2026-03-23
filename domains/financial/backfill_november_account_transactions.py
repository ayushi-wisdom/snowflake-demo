"""
Backfill transactions for November promotion accounts (customers/accounts created or opened in Nov 2025).
Run after add_november_promotion_customers.py so those accounts have transaction history.

Usage: python backfill_november_account_transactions.py
"""
import logging
import random
import sys
from datetime import date, timedelta

from snowflake_connection import get_snowflake_connection
from daily_financial_update import get_existing_accounts, insert_transactions
from data_generator import generate_transactions

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

NOV_START = date(2025, 11, 1)
NOV_END = date(2025, 11, 30)


def get_november_promotion_accounts(cursor):
    """Accounts whose customer has created_date in Nov 2025 (promotion signups)."""
    cursor.execute("""
        SELECT a.account_id, a.account_type, a.status, a.opened_date
        FROM accounts a
        JOIN customers c ON c.customer_id = a.customer_id
        WHERE c.created_date BETWEEN %s AND %s
        ORDER BY a.opened_date
    """, (NOV_START, NOV_END))
    return [
        {"account_id": row[0], "account_type": row[1], "status": row[2], "opened_date": row[3]}
        for row in cursor.fetchall()
    ]


def backfill_november_transactions():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        nov_accounts = get_november_promotion_accounts(cursor)
        if not nov_accounts:
            logger.info("No November promotion accounts found. Run add_november_promotion_customers.py first.")
            return
        logger.info("Found %s November promotion accounts to backfill", len(nov_accounts))

        end_date = date.today()
        total_inserted = 0
        d = NOV_START
        while d <= end_date:
            # Only include accounts already opened by this date
            accounts_for_date = [a for a in nov_accounts if a["opened_date"] <= d and a["status"] == "Active"]
            if not accounts_for_date:
                d += timedelta(days=1)
                continue
            # ~1.5–3 transactions per account per day on average
            per_day = max(1, int(len(accounts_for_date) * random.uniform(1.5, 3)))
            txns = generate_transactions(accounts_for_date, transactions_per_day=per_day, target_date=d)
            if txns:
                insert_transactions(cursor, txns)
                total_inserted += len(txns)
            d += timedelta(days=1)

        logger.info("Backfilled %s transactions for November promotion accounts (%s to %s)", total_inserted, NOV_START, end_date)
    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    backfill_november_transactions()
    sys.exit(0)
