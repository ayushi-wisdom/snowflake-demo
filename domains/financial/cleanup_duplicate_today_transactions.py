"""
One-time cleanup: remove duplicate transactions for today (from second run on same day)
and recalculate account balances from transaction totals.

Usage: python cleanup_duplicate_today_transactions.py [--date YYYY-MM-DD] [--count N]
  --date  Target date to clean (default: today)
  --count Number of most-recent transactions to remove for that date (default: 938 for 2026-03-09)
"""
import argparse
import logging
import sys
from datetime import date

from snowflake_connection import get_snowflake_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def cleanup_duplicates(target_date: date, remove_count: int):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        # 1) Count current transactions for the date
        cursor.execute(
            "SELECT COUNT(*) FROM transactions WHERE transaction_date = %s",
            (target_date,),
        )
        total = cursor.fetchone()[0]
        logger.info(f"Transactions for {target_date}: {total}")

        if total == 0:
            logger.info("Nothing to clean for this date.")
            return
        if remove_count <= 0:
            logger.info("remove_count must be positive.")
            return
        if remove_count >= total:
            logger.warning(
                f"remove_count ({remove_count}) >= total ({total}); would remove all for date. Aborting."
            )
            return

        # 2) Get transaction_ids of the N most recently created for this date (the duplicate batch)
        # LIMIT must be literal (Snowflake bind limitation); remove_count is from our script
        cursor.execute(
            """
            SELECT transaction_id FROM (
                SELECT transaction_id
                FROM transactions
                WHERE transaction_date = %s
                ORDER BY created_at DESC
                LIMIT """ + str(int(remove_count)) + """
            )
            """,
            (target_date,),
        )
        ids_to_delete = [row[0] for row in cursor.fetchall()]
        logger.info(f"Removing {len(ids_to_delete)} most recent transactions for {target_date}")

        if not ids_to_delete:
            return

        # 3) Delete in batches (Snowflake IN list limit)
        batch_size = 1000
        deleted = 0
        for i in range(0, len(ids_to_delete), batch_size):
            batch = ids_to_delete[i : i + batch_size]
            placeholders = ",".join(["%s"] * len(batch))
            cursor.execute(
                f"DELETE FROM transactions WHERE transaction_id IN ({placeholders})",
                batch,
            )
            deleted += cursor.rowcount
        logger.info(f"✓ Deleted {deleted} duplicate transactions")

        # 4) Recalculate account balances from full transaction sum (so balances match remaining data)
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
            (target_date,),
        )
        rows_updated = cursor.rowcount
        logger.info(f"✓ Updated balances for {rows_updated} accounts")
        logger.info("Cleanup done.")
    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Remove duplicate transactions for a date")
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date to clean (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=938,
        help="Number of most recent transactions to remove for that date",
    )
    args = parser.parse_args()
    target_date = date.fromisoformat(args.date)
    cleanup_duplicates(target_date, args.count)
    sys.exit(0)


if __name__ == "__main__":
    main()
