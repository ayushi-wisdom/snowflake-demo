"""
Backfill transactions for a date range with all date-based anomalies applied, then recalculate balances.

Anomalies: pay-day, tax-deadline, Iran/oil (Feb 1 - Mar 10), holiday spending, quarter-end, FOMC.

Usage:
  python backfill_with_anomalies.py
  python backfill_with_anomalies.py --start 2025-03-10 --end 2026-03-10

Defaults: start = (end - 364 days), end = DATA_AS_OF_DATE from .env if set, else today. Data never extends past end.
"""
import argparse
import logging
import random
import sys
from datetime import date, timedelta

from config import get_data_as_of_date
from snowflake_connection import get_snowflake_connection
from daily_financial_update import get_existing_accounts, insert_transactions
from data_generator import (
    generate_transactions,
    _is_payday,
    _is_tax_deadline_window,
    _is_iran_oil_event_window,
    _is_holiday_spending_window,
    _is_quarter_end,
    _is_fomc_day,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def backfill_with_anomalies(start_date: date, end_date: date):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("USE DATABASE SE_DEMOS_NEW")
    cursor.execute("USE SCHEMA FINANCE_MAIN")
    try:
        cursor.execute(
            "DELETE FROM transactions WHERE transaction_date >= %s AND transaction_date <= %s",
            (start_date, end_date),
        )
        deleted = cursor.rowcount
        logger.info(f"Deleted {deleted:,} transactions from {start_date} to {end_date}")

        accounts = get_existing_accounts(cursor)
        if not accounts:
            logger.error("No active accounts found.")
            return

        total_inserted = 0
        d = start_date
        while d <= end_date:
            base_count = random.randint(650, 1000)  # Tighter range to avoid huge weekly spikes
            txns = generate_transactions(
                accounts,
                transactions_per_day=base_count,
                target_date=d,
                reference_date=end_date,
            )
            if txns:
                insert_transactions(cursor, txns)
                total_inserted += len(txns)
                labels = []
                if _is_payday(d):
                    labels.append("pay-day")
                if _is_tax_deadline_window(d):
                    labels.append("tax-deadline")
                if _is_iran_oil_event_window(d):
                    labels.append("iran-oil")
                h = _is_holiday_spending_window(d)
                if h:
                    labels.append(h)
                if _is_quarter_end(d):
                    labels.append("quarter-end")
                if _is_fomc_day(d):
                    labels.append("fomc")
                label = " (" + ", ".join(labels) + ")" if labels else ""
                logger.info(f"  {d}: {len(txns)} transactions{label}")
            d += timedelta(days=1)

        logger.info(f"Inserted {total_inserted:,} transactions total")

        logger.info("Recalculating account balances from transaction totals...")
        cursor.execute(
            """
            UPDATE accounts a
            SET
                current_balance = COALESCE((
                    SELECT SUM(t.amount)
                    FROM transactions t
                    WHERE t.account_id = a.account_id AND t.status = 'Completed'
                ), 0),
                available_balance = COALESCE((
                    SELECT SUM(t.amount)
                    FROM transactions t
                    WHERE t.account_id = a.account_id AND t.status = 'Completed'
                ), 0) * 0.95,
                balance_last_updated_date = %s
            """,
            (end_date,),
        )
        logger.info(f"✓ Updated balances for {cursor.rowcount} accounts")
        logger.info("Done. Pay-day, tax-deadline, and Iran/oil anomalies applied where applicable.")
    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Backfill transactions with pay-day and tax-deadline anomalies"
    )
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (default: today - 364)")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: DATA_AS_OF_DATE or today)")
    args = parser.parse_args()
    end_date = date.fromisoformat(args.end) if args.end else get_data_as_of_date()
    start_date = (
        date.fromisoformat(args.start)
        if args.start
        else end_date - timedelta(days=364)
    )
    if start_date > end_date:
        logger.error("Start date must be <= end date.")
        sys.exit(1)
    backfill_with_anomalies(start_date, end_date)
    sys.exit(0)


if __name__ == "__main__":
    main()
