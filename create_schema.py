"""
Create all financial services tables in Snowflake
"""
from snowflake_connection import get_snowflake_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_all_tables():
    """Create all financial services tables"""
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Customers table
        logger.info("Creating customers table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                phone VARCHAR(20),
                date_of_birth DATE,
                address_city VARCHAR(100),
                address_state VARCHAR(2),
                address_zip VARCHAR(10),
                customer_segment VARCHAR(50),
                created_date DATE,
                status VARCHAR(20),
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("✓ customers table created")
        
        # 2. Accounts table
        logger.info("Creating accounts table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                account_number VARCHAR(50),
                account_type VARCHAR(50),
                opened_date DATE,
                status VARCHAR(20),
                currency VARCHAR(3),
                current_balance FLOAT,
                available_balance FLOAT,
                balance_last_updated_date DATE,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("✓ accounts table created")
        
        # 3. Transactions table
        logger.info("Creating transactions table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                account_id VARCHAR(50),
                transaction_date DATE,
                transaction_time TIMESTAMP_NTZ,
                amount FLOAT,
                transaction_type VARCHAR(50),
                merchant_name VARCHAR(255),
                category VARCHAR(100),
                description VARCHAR(500),
                status VARCHAR(20),
                reference_number VARCHAR(100),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("✓ transactions table created")
        
        # 4. Portfolio holdings table
        logger.info("Creating portfolio_holdings table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_holdings (
                holding_id VARCHAR(50) PRIMARY KEY,
                account_id VARCHAR(50),
                security_symbol VARCHAR(10),
                security_name VARCHAR(255),
                quantity FLOAT,
                purchase_date DATE,
                purchase_price FLOAT,
                current_price FLOAT,
                market_value FLOAT,
                unrealized_pnl FLOAT,
                updated_date DATE,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("✓ portfolio_holdings table created")
        
        # 5. Loans table
        logger.info("Creating loans table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS loans (
                loan_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                account_id VARCHAR(50),
                loan_type VARCHAR(50),
                original_principal FLOAT,
                current_principal_balance FLOAT,
                interest_rate FLOAT,
                term_months INTEGER,
                origination_date DATE,
                maturity_date DATE,
                days_past_due INTEGER,
                next_payment_date DATE,
                next_payment_amount FLOAT,
                status VARCHAR(20),
                balance_last_updated_date DATE,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("✓ loans table created")
        
        logger.info("\n✓ All tables created successfully!")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_all_tables()
