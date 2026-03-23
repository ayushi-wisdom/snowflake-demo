"""
Run monthly (or whenever): add a manual anomaly — big customer churn, big customer join, or a new promotion.
Keeps the demo interesting without editing code each time.

Usage:
  python add_manual_anomaly.py --type big_churn --count 1
  python add_manual_anomaly.py --type big_join [--count 1]
  python add_manual_anomaly.py --type promotion --start 2026-04-01 --end 2026-04-30 [--bonus 50] [--count 60]
"""
import argparse
import logging
import random
import sys
from datetime import date, timedelta

from snowflake_connection import get_snowflake_connection
from daily_financial_update import insert_customers, insert_accounts, insert_loans
from data_generator import generate_customers, generate_accounts, generate_loans

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_big_churn(cursor, count: int = 1, close_accounts: bool = True):
    """Mark 1–2 high-value (by total balance) Active customers as Inactive; optionally close their accounts."""
    cursor.execute("""
        SELECT c.customer_id, COALESCE(SUM(a.current_balance), 0) AS total_balance
        FROM customers c
        LEFT JOIN accounts a ON a.customer_id = c.customer_id
        WHERE c.status = 'Active'
        GROUP BY c.customer_id
        ORDER BY total_balance DESC
        LIMIT %s
    """, (count,))
    rows = cursor.fetchall()
    if not rows:
        logger.warning("No active customers found for big_churn")
        return
    customer_ids = [r[0] for r in rows]
    logger.info("Marking %s high-value customer(s) Inactive: %s (total balances: %s)",
                len(customer_ids), customer_ids, [r[1] for r in rows])
    placeholders = ",".join(["%s"] * len(customer_ids))
    cursor.execute(
        f"UPDATE customers SET status = 'Inactive', updated_at = CURRENT_TIMESTAMP() WHERE customer_id IN ({placeholders})",
        customer_ids,
    )
    if close_accounts:
        cursor.execute(
            f"UPDATE accounts SET status = 'Closed', updated_at = CURRENT_TIMESTAMP() WHERE customer_id IN ({placeholders})",
            customer_ids,
        )
        logger.info("Closed all accounts for those customers")
    logger.info("Done. Big churn applied.")


def run_big_join(cursor, count: int = 1):
    """Add 1–2 'whale' customers with high balances and several accounts."""
    today = date.today()
    week_start = today - timedelta(days=7)
    customers = generate_customers(
        count=count,
        created_date_min=week_start,
        created_date_max=today,
        all_active=True,
    )
    # 4–6 accounts per whale, high balances
    accounts = generate_accounts(customers, accounts_per_customer=(4, 6))
    # Override to whale-sized balances
    for a in accounts:
        at = a["account_type"]
        if at == "Credit Card":
            a["current_balance"] = round(random.uniform(-5000, 0), 2)
        elif at == "Investment":
            a["current_balance"] = round(random.uniform(200000, 500000), 2)
        elif at == "Savings":
            a["current_balance"] = round(random.uniform(50000, 200000), 2)
        else:
            a["current_balance"] = round(random.uniform(20000, 80000), 2)
        a["available_balance"] = round(a["current_balance"] * random.uniform(0.95, 1.0), 2)
    insert_customers(cursor, customers)
    insert_accounts(cursor, accounts)
    loans = generate_loans(customers, accounts)
    if loans:
        insert_loans(cursor, loans)
    logger.info("Added %s whale customer(s) with %s accounts and %s loans", len(customers), len(accounts), len(loans))


def run_promotion(cursor, start: date, end: date, bonus: float, count: int):
    """Add a cohort of new customers in [start,end] with signup bonus on Checking/Savings."""
    customers = generate_customers(
        count=count,
        created_date_min=start,
        created_date_max=end,
        all_active=True,
    )
    accounts = generate_accounts(
        customers,
        accounts_per_customer=(1, 3),
        signup_bonus_for_dates=(start, end),
        signup_bonus_amount=bonus,
    )
    insert_customers(cursor, customers)
    insert_accounts(cursor, accounts)
    logger.info("Added %s promotion customers and %s accounts (bonus $%s for Checking/Savings opened %s–%s)",
                len(customers), len(accounts), bonus, start, end)
    # Optional: add loans for some
    loans = generate_loans(customers, accounts)
    if loans:
        insert_loans(cursor, loans)
        logger.info("Added %s loans for promotion customers", len(loans))


def main():
    parser = argparse.ArgumentParser(
        description="Add a manual anomaly: big customer churn, big join, or a new promotion."
    )
    parser.add_argument("--type", choices=["big_churn", "big_join", "promotion"], required=True)
    parser.add_argument("--count", type=int, default=1, help="For churn/join: number of customers (default 1). For promotion: cohort size (default 60).")
    parser.add_argument("--no-close-accounts", action="store_true", help="For big_churn: do not close the churned customers' accounts")
    parser.add_argument("--start", type=str, help="For promotion: start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="For promotion: end date YYYY-MM-DD")
    parser.add_argument("--bonus", type=float, default=100.0, help="For promotion: signup bonus amount (default 100)")
    args = parser.parse_args()

    if args.type == "promotion":
        if not args.start or not args.end:
            parser.error("promotion requires --start and --end (YYYY-MM-DD)")
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        count = args.count if args.count != 1 else 60
    else:
        count = max(1, min(args.count, 5))  # churn/join: 1–5

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        if args.type == "big_churn":
            run_big_churn(cursor, count=count, close_accounts=not args.no_close_accounts)
        elif args.type == "big_join":
            run_big_join(cursor, count=count)
        else:
            run_promotion(cursor, start, end, args.bonus, count)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
    sys.exit(0)
