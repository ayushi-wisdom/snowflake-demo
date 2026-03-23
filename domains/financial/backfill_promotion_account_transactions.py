"""
Backfill transactions for promotion accounts (customers created in a given date range).
Run after add_manual_anomaly.py --type promotion --start ... --end ... so those accounts have history.

Usage:
  python backfill_promotion_account_transactions.py --start 2026-04-01 --end 2026-04-30
"""
import argparse
import logging
import random
import sys
from datetime import date, timedelta

from snowflake_connection import get_snowflake_connection
from daily_financial_update import insert_transactions
from data_generator import generate_transactions

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_promotion_accounts(cursor, start: date, end: date):
    """Accounts whose customer has created_date in [start, end] (promotion signups)."""
    cursor.execute("""
        SELECT a.account_id, a.account_type, a.status, a.opened_date
        FROM accounts a
        JOIN customers c ON c.customer_id = a.customer_id
        WHERE c.created_date BETWEEN %s AND %s
        ORDER BY a.opened_date
    """, (start, end))
    return [
        {"account_id": row[0], "account_type": row[1], "status": row[2], "opened_date": row[3]}
        for row in cursor.fetchall()
    ]


def backfill_promotion_transactions(start: date, end: date):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        accounts = get_promotion_accounts(cursor, start, end)
        if not accounts:
            logger.warning("No promotion accounts found for %s–%s. Run add_manual_anomaly.py --type promotion first.", start, end)
            return
        logger.info("Found %s promotion accounts to backfill", len(accounts))

        end_date = date.today()
        total_inserted = 0
        d = start
        while d <= end_date:
            accounts_for_date = [a for a in accounts if a["opened_date"] <= d and a["status"] == "Active"]
            if not accounts_for_date:
                d += timedelta(days=1)
                continue
            per_day = max(1, int(len(accounts_for_date) * random.uniform(1.5, 3)))
            txns = generate_transactions(accounts_for_date, transactions_per_day=per_day, target_date=d)
            if txns:
                insert_transactions(cursor, txns)
                total_inserted += len(txns)
            d += timedelta(days=1)

        logger.info("Backfilled %s transactions for promotion accounts (%s to %s)", total_inserted, start, end_date)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill transactions for promotion cohort (created in date range).")
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD (promotion window)")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD (promotion window)")
    args = parser.parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    backfill_promotion_transactions(start, end)
    sys.exit(0)
