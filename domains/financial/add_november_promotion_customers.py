"""
One-time: add November promotion influx — customers who signed up for $100 when they open an account.
Creates customers with created_date in Nov 2025 and accounts with opened_date in Nov 2025;
Checking/Savings accounts get +$100 in current_balance to reflect the signup bonus.

Usage: python add_november_promotion_customers.py
"""
import logging
import sys
from datetime import date

from snowflake_connection import get_snowflake_connection
from daily_financial_update import insert_customers, insert_accounts
from data_generator import generate_customers, generate_accounts

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

NOV_START = date(2025, 11, 1)
NOV_END = date(2025, 11, 30)
PROMO_SIGNUP_BONUS = 100.0
NOV_PROMO_CUSTOMERS = 120  # Influx size


def add_november_promotion():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        customers = generate_customers(
            count=NOV_PROMO_CUSTOMERS,
            created_date_min=NOV_START,
            created_date_max=NOV_END,
            all_active=True,
        )
        accounts = generate_accounts(
            customers,
            accounts_per_customer=(1, 2),
            signup_bonus_for_dates=(NOV_START, NOV_END),
            signup_bonus_amount=PROMO_SIGNUP_BONUS,
        )
        insert_customers(cursor, customers)
        insert_accounts(cursor, accounts)
        logger.info("Added %s November promotion customers and %s accounts ($100 signup bonus for Checking/Savings opened in Nov 2025)", len(customers), len(accounts))
    except Exception as e:
        logger.error(str(e))
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    add_november_promotion()
    sys.exit(0)
