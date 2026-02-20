"""
Daily update script for financial services dataset
Generates and loads synthetic data into Snowflake
"""
from snowflake_connection import get_snowflake_connection
from data_generator import (
    generate_customers, generate_accounts, generate_transactions,
    generate_portfolio_holdings, generate_loans
)
from datetime import date, timedelta, datetime
import logging
import random
import time
from data_generator import fetch_real_market_prices

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def insert_customers(cursor, customers):
    """Insert customers into database"""
    if not customers:
        return
    
    logger.info(f"Inserting {len(customers)} customers...")
    insert_sql = """
        INSERT INTO customers (
            customer_id, first_name, last_name, email, phone, date_of_birth,
            address_city, address_state, address_zip, customer_segment,
            created_date, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    values = [
        (
            c["customer_id"], c["first_name"], c["last_name"], c["email"], c["phone"],
            c["date_of_birth"], c["address_city"], c["address_state"], c["address_zip"],
            c["customer_segment"], c["created_date"], c["status"]
        )
        for c in customers
    ]
    
    cursor.executemany(insert_sql, values)
    logger.info(f"✓ Inserted {len(customers)} customers")

def insert_accounts(cursor, accounts):
    """Insert accounts into database"""
    if not accounts:
        return
    
    logger.info(f"Inserting {len(accounts)} accounts...")
    insert_sql = """
        INSERT INTO accounts (
            account_id, customer_id, account_number, account_type, opened_date,
            status, currency, current_balance, available_balance, balance_last_updated_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    values = [
        (
            a["account_id"], a["customer_id"], a["account_number"], a["account_type"],
            a["opened_date"], a["status"], a["currency"], a["current_balance"],
            a["available_balance"], a["balance_last_updated_date"]
        )
        for a in accounts
    ]
    
    cursor.executemany(insert_sql, values)
    logger.info(f"✓ Inserted {len(accounts)} accounts")

def insert_transactions(cursor, transactions):
    """Insert transactions into database"""
    if not transactions:
        return
    
    logger.info(f"Inserting {len(transactions)} transactions...")
    insert_sql = """
        INSERT INTO transactions (
            transaction_id, account_id, transaction_date, transaction_time, amount,
            transaction_type, merchant_name, category, description, status, reference_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    logger.info("Preparing transaction values...")
    values = [
        (
            t["transaction_id"], t["account_id"], t["transaction_date"], t["transaction_time"],
            t["amount"], t["transaction_type"], t["merchant_name"], t["category"],
            t["description"], t["status"], t["reference_number"]
        )
        for t in transactions
    ]
    logger.info(f"Prepared {len(values)} transaction records")
    
    # Insert in batches to avoid memory issues
    batch_size = 1000
    total_batches = (len(values) + batch_size - 1) // batch_size
    logger.info(f"Inserting in {total_batches} batches of {batch_size}...")
    
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        logger.info(f"Inserting batch {batch_num}/{total_batches} ({len(batch)} records)...")
        cursor.executemany(insert_sql, batch)
        logger.info(f"✓ Batch {batch_num} completed")
    
    logger.info(f"✓ Inserted {len(transactions)} transactions")


def insert_portfolio_holdings(cursor, holdings):
    """Insert or update portfolio holdings using batch INSERT/UPDATE"""
    if not holdings:
        return
    
    logger.info(f"Inserting/updating {len(holdings)} portfolio holdings...")
    
    # Use batch processing - first get existing holding_ids
    logger.info("Checking existing holdings...")
    existing_holdings = set()
    if holdings:
        holding_ids = [h["holding_id"] for h in holdings]
        # Check in batches
        batch_size = 1000
        for i in range(0, len(holding_ids), batch_size):
            batch_ids = holding_ids[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch_ids))
            cursor.execute(f"SELECT holding_id FROM portfolio_holdings WHERE holding_id IN ({placeholders})", batch_ids)
            existing_holdings.update(row[0] for row in cursor.fetchall())
    
    logger.info(f"Found {len(existing_holdings)} existing holdings, {len(holdings) - len(existing_holdings)} new")
    
    # Separate into updates and inserts
    updates = [h for h in holdings if h["holding_id"] in existing_holdings]
    inserts = [h for h in holdings if h["holding_id"] not in existing_holdings]
    
    # Batch UPDATE
    if updates:
        logger.info(f"Updating {len(updates)} holdings...")
        update_sql = """
            UPDATE portfolio_holdings
            SET current_price = %s,
                market_value = %s,
                unrealized_pnl = %s,
                updated_date = %s,
                updated_at = CURRENT_TIMESTAMP()
            WHERE holding_id = %s
        """
        update_values = [
            (h["current_price"], h["market_value"], h["unrealized_pnl"], h["updated_date"], h["holding_id"])
            for h in updates
        ]
        # Execute in batches
        batch_size = 500
        for i in range(0, len(update_values), batch_size):
            batch = update_values[i:i + batch_size]
            cursor.executemany(update_sql, batch)
        logger.info(f"✓ Updated {len(updates)} holdings")
    
    # Batch INSERT
    if inserts:
        logger.info(f"Inserting {len(inserts)} new holdings...")
        insert_sql = """
            INSERT INTO portfolio_holdings (
                holding_id, account_id, security_symbol, security_name, quantity,
                purchase_date, purchase_price, current_price, market_value,
                unrealized_pnl, updated_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_values = [
            (
                h["holding_id"], h["account_id"], h["security_symbol"], h["security_name"],
                h["quantity"], h["purchase_date"], h["purchase_price"], h["current_price"],
                h["market_value"], h["unrealized_pnl"], h["updated_date"]
            )
            for h in inserts
        ]
        # Execute in batches
        batch_size = 500
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
        logger.info(f"✓ Inserted {len(inserts)} holdings")
    
    logger.info(f"✓ Inserted/updated {len(holdings)} portfolio holdings")

def insert_loans(cursor, loans):
    """Insert loans into database"""
    if not loans:
        return
    
    logger.info(f"Inserting {len(loans)} loans...")
    insert_sql = """
        INSERT INTO loans (
            loan_id, customer_id, account_id, loan_type, original_principal,
            current_principal_balance, interest_rate, term_months, origination_date,
            maturity_date, days_past_due, next_payment_date, next_payment_amount,
            status, balance_last_updated_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    values = [
        (
            l["loan_id"], l["customer_id"], l["account_id"], l["loan_type"],
            l["original_principal"], l["current_principal_balance"], l["interest_rate"],
            l["term_months"], l["origination_date"], l["maturity_date"],
            l["days_past_due"], l["next_payment_date"], l["next_payment_amount"],
            l["status"], l["balance_last_updated_date"]
        )
        for l in loans
    ]
    
    cursor.executemany(insert_sql, values)
    logger.info(f"✓ Inserted {len(loans)} loans")

def update_account_balances(cursor, transactions):
    """Update account balances based on transactions"""
    logger.info("Updating account balances from transactions...")
    
    update_sql = """
        UPDATE accounts a
        SET 
            current_balance = a.current_balance + COALESCE((
                SELECT SUM(t.amount)
                FROM transactions t
                WHERE t.account_id = a.account_id
                  AND t.transaction_date = CURRENT_DATE()
                  AND t.status = 'Completed'
            ), 0),
            available_balance = a.current_balance + COALESCE((
                SELECT SUM(t.amount)
                FROM transactions t
                WHERE t.account_id = a.account_id
                  AND t.transaction_date = CURRENT_DATE()
                  AND t.status = 'Completed'
            ), 0) * 0.95,
            balance_last_updated_date = CURRENT_DATE()
        WHERE EXISTS (
            SELECT 1 FROM transactions t
            WHERE t.account_id = a.account_id
              AND t.transaction_date = CURRENT_DATE()
        )
    """
    
    cursor.execute(update_sql)
    rows_updated = cursor.rowcount
    logger.info(f"✓ Updated balances for {rows_updated} accounts")

def check_master_data_exists(cursor) -> tuple:
    """Check if master data already exists"""
    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM accounts")
    account_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM loans")
    loan_count = cursor.fetchone()[0]
    
    return customer_count > 0, account_count > 0, loan_count > 0

def get_existing_accounts(cursor):
    """Get existing accounts for transaction generation"""
    cursor.execute("SELECT account_id, account_type, status, opened_date FROM accounts WHERE status = 'Active'")
    return [
        {"account_id": row[0], "account_type": row[1], "status": row[2], "opened_date": row[3]}
        for row in cursor.fetchall()
    ]

def daily_update():
    """Main daily update function"""
    start_time = time.time()
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        target_date = date.today()
        logger.info("=" * 60)
        logger.info(f"Starting daily update for {target_date}")
        logger.info("=" * 60)
        
        # Check if master data exists
        customers_exist, accounts_exist, loans_exist = check_master_data_exists(cursor)
        
        # Generate master data if it doesn't exist
        if not customers_exist:
            logger.info("Generating master data (customers, accounts, loans)...")
            customers = generate_customers(count=1500)
            accounts = generate_accounts(customers)
            loans = generate_loans(customers, accounts)
            
            insert_customers(cursor, customers)
            insert_accounts(cursor, accounts)
            insert_loans(cursor, loans)
        else:
            logger.info("Master data already exists, skipping generation")
            # Get existing accounts for transaction generation
            accounts = get_existing_accounts(cursor)
        
        # Generate daily data
        logger.info("=" * 60)
        logger.info("Generating daily data...")
        logger.info("=" * 60)
        
        # Get accounts for transaction generation
        if accounts_exist:
            logger.info("Fetching existing accounts for transaction generation...")
            accounts = get_existing_accounts(cursor)
            logger.info(f"Found {len(accounts)} active accounts")
        
        # Transactions
        logger.info("-" * 60)
        logger.info("Step 1: Generating transactions...")
        # More realistic: 500-1,500 transactions per day (~0.2-0.5 per account on average)
        transaction_count = random.randint(500, 1500)
        logger.info(f"Generating {transaction_count} transactions for today...")
        transactions = generate_transactions(
            accounts if accounts_exist else [],
            transactions_per_day=transaction_count,
            target_date=target_date
        )
        logger.info(f"Generated {len(transactions)} transactions for today")
        insert_transactions(cursor, transactions)
        
        # Generate historical transactions (last 365 days / 1 year) if needed
        cursor.execute("SELECT COUNT(*) FROM transactions")
        existing_transaction_count = cursor.fetchone()[0]
        
        # Check if we have a full year of data
        cursor.execute("SELECT COUNT(DISTINCT transaction_date) FROM transactions")
        distinct_dates = cursor.fetchone()[0]
        
        # Check which dates we already have
        cursor.execute("SELECT DISTINCT transaction_date FROM transactions ORDER BY transaction_date")
        existing_dates = {row[0] for row in cursor.fetchall()}
        
        # Generate historical if we have less than 365 days of data
        if distinct_dates < 365:
            logger.info(f"Found {existing_transaction_count} transactions across {distinct_dates} days. Generating historical transactions for full year (365 days)...")
            
            historical_transactions = []
            total_days = 365
            
            # Process in batches to avoid memory issues
            batch_size = 30  # Process 30 days at a time
            for batch_start in range(1, total_days + 1, batch_size):
                batch_end = min(batch_start + batch_size, total_days + 1)
                batch_days = batch_end - batch_start
                
                batch_transactions = []
                batch_dates_to_generate = []
                
                for days_ago in range(batch_start, batch_end):
                    hist_date = target_date - timedelta(days=days_ago)
                    # Only generate if we don't already have data for this date
                    if hist_date not in existing_dates:
                        batch_dates_to_generate.append(hist_date)
                        hist_count = random.randint(300, 1000)  # Fewer transactions in the past
                        hist_txns = generate_transactions(
                            accounts if accounts_exist else [],
                            transactions_per_day=hist_count,
                            target_date=hist_date
                        )
                        batch_transactions.extend(hist_txns)
                
                if batch_transactions:
                    logger.info(f"Generating historical transactions for {len(batch_dates_to_generate)} missing days (days {batch_start}-{batch_end-1} ago)...")
                    logger.info(f"Inserting batch of {len(batch_transactions)} historical transactions...")
                    insert_transactions(cursor, batch_transactions)
                    historical_transactions.extend(batch_transactions)
                    logger.info(f"✓ Batch completed. Total historical transactions so far: {len(historical_transactions)}")
            
            if historical_transactions:
                logger.info(f"✓ Generated {len(historical_transactions)} total historical transactions (filling gaps to 1 year)")
            else:
                logger.info("✓ All 365 days already have transaction data")
        else:
            logger.info(f"Skipping historical transactions - already have {distinct_dates} days of transaction data (>= 365 days)")
        
        # Update account balances
        logger.info("-" * 60)
        logger.info("Step 2: Updating account balances from transactions...")
        if transactions:
            update_account_balances(cursor, transactions)
        logger.info("Account balances updated")
        
        # Portfolio holdings (only if we have accounts)
        # Note: Real market prices should be provided via web search
        logger.info("-" * 60)
        logger.info("Step 3: Generating portfolio holdings with real market prices...")
        if accounts_exist or not customers_exist:
            if not accounts_exist:
                logger.info("Fetching investment accounts from database...")
                cursor.execute("SELECT account_id, account_type, status, opened_date FROM accounts WHERE account_type = 'Investment' AND status = 'Active'")
                investment_accounts = [
                    {"account_id": row[0], "account_type": row[1], "status": row[2], "opened_date": row[3]}
                    for row in cursor.fetchall()
                ]
                logger.info(f"Found {len(investment_accounts)} investment accounts")
            else:
                investment_accounts = [a for a in accounts if a["account_type"] == "Investment" and a.get("opened_date")]
                logger.info(f"Using {len(investment_accounts)} investment accounts from generated data")
            
            if investment_accounts:
                logger.info(f"Fetching market prices for portfolio holdings...")
                # Use synthetic prices for now
                # When integrated with your tool with web search, you can:
                #   1. Fetch real prices using your web_search tool
                #   2. Pass them as: fetch_real_market_prices(real_prices_dict=your_price_dict)
                # For now, using synthetic prices
                real_prices = fetch_real_market_prices()
                logger.info(f"Fetched {len(real_prices)} real market prices")
                
                logger.info(f"Generating holdings for {len(investment_accounts)} investment accounts...")
                holdings = generate_portfolio_holdings(investment_accounts, real_prices)
                logger.info(f"Generated {len(holdings)} portfolio holdings")
                insert_portfolio_holdings(cursor, holdings)
            else:
                logger.info("No investment accounts found, skipping portfolio holdings")
        else:
            logger.info("Skipping portfolio holdings (no accounts available)")
        
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"✓ Daily update completed successfully!")
        logger.info(f"  Date: {target_date}")
        logger.info(f"  Total time: {elapsed_time:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during daily update: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    daily_update()
