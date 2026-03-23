"""
Run weekly: add a few new customers (and accounts), and mark a few existing customers Inactive.
Keeps the base customer list from feeling static after launch — some churn, some growth.
Never delete customers; Inactive keeps full history.

Usage: python weekly_customer_churn.py
Schedule: e.g. cron 0 3 * * 0 (Sunday 3 AM) or launchd weekly.
"""
import logging
import random
import sys
from datetime import date, timedelta

from snowflake_connection import get_snowflake_connection
from daily_financial_update import (
    insert_customers,
    insert_accounts,
    insert_loans,
    get_existing_accounts,
)
from data_generator import generate_customers, generate_accounts, generate_loans

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# A few new customers per week — not too many
NEW_CUSTOMERS_MIN = 3
NEW_CUSTOMERS_MAX = 5
# Mark a few existing Active customers as Inactive (churn)
CHURN_MARK_INACTIVE_MIN = 1
CHURN_MARK_INACTIVE_MAX = 3


def weekly_churn():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        today = date.today()
        week_start = today - timedelta(days=7)

        # 1) Mark 1–3 random Active customers as Inactive (churn; never delete)
        cursor.execute("SELECT customer_id FROM customers WHERE status = 'Active' ORDER BY RANDOM() LIMIT 50")
        active_ids = [row[0] for row in cursor.fetchall()]
        n_churn = min(
            random.randint(CHURN_MARK_INACTIVE_MIN, CHURN_MARK_INACTIVE_MAX),
            len(active_ids)
        )
        if n_churn and active_ids:
            churn_ids = list(random.sample(active_ids, n_churn))
            placeholders = ",".join(["%s"] * len(churn_ids))
            cursor.execute(f"UPDATE customers SET status = 'Inactive', updated_at = CURRENT_TIMESTAMP() WHERE customer_id IN ({placeholders})", churn_ids)
            logger.info("Marked %s customer(s) Inactive (historical data kept): %s", n_churn, churn_ids[:3])
        else:
            logger.info("No customers marked Inactive this week")

        # 2) Add 3–5 new customers with created_date in the last 7 days
        n_new = random.randint(NEW_CUSTOMERS_MIN, NEW_CUSTOMERS_MAX)
        new_customers = generate_customers(
            count=n_new,
            created_date_min=week_start,
            created_date_max=today,
            all_active=True,
        )
        if not new_customers:
            logger.info("No new customers added")
            return
        new_accounts = generate_accounts(new_customers, accounts_per_customer=(1, 2))
        insert_customers(cursor, new_customers)
        insert_accounts(cursor, new_accounts)
        logger.info("Added %s new customers and %s accounts", len(new_customers), len(new_accounts))

        # 3) Optional: add loans for ~30% of new customers
        if new_accounts:
            new_loans = generate_loans(new_customers, new_accounts)
            if new_loans:
                insert_loans(cursor, new_loans)
                logger.info("Added %s new loans", len(new_loans))

    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    weekly_churn()
    sys.exit(0)
