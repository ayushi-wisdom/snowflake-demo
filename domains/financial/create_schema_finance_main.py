"""
Create FINANCE_MAIN schema in SE_DEMOS_NEW (uses .env SNOWFLAKE_DATABASE).
"""
from snowflake_connection import get_snowflake_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_finance_main_schema():
    """Create FINANCE_MAIN schema in the database from .env (SE_DEMOS_NEW)."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT CURRENT_DATABASE()")
        db = cursor.fetchone()[0]
        logger.info("Creating FINANCE_MAIN schema in %s...", db)
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.FINANCE_MAIN")
        logger.info("✓ FINANCE_MAIN schema created successfully!")
        
        cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
        schemas = cursor.fetchall()
        logger.info("\nSchemas in %s:", db)
        for schema in schemas:
            logger.info(f"  - {schema[1]}")
        
    except Exception as e:
        logger.error(f"Error creating schema: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_finance_main_schema()
