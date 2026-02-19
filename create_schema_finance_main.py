"""
Create FINANCE_MAIN schema in SE_DEMO_DB
"""
from snowflake_connection import get_snowflake_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_finance_main_schema():
    """Create FINANCE_MAIN schema"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        logger.info("Creating FINANCE_MAIN schema in SE_DEMO_DB...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS SE_DEMO_DB.FINANCE_MAIN")
        logger.info("✓ FINANCE_MAIN schema created successfully!")
        
        # Verify it was created
        cursor.execute("SHOW SCHEMAS IN DATABASE SE_DEMO_DB")
        schemas = cursor.fetchall()
        logger.info("\nSchemas in SE_DEMO_DB:")
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
