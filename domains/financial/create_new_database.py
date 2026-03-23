"""
Create new database SE_DEMOS_NEW and FINANCE_MAIN schema
"""
from snowflake_connection import get_snowflake_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_new_database():
    """Create SE_DEMOS_NEW database and FINANCE_MAIN schema"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Create database
        logger.info("Creating database SE_DEMOS_NEW...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS SE_DEMOS_NEW")
        logger.info("✓ Database SE_DEMOS_NEW created successfully!")
        
        # Create schema
        logger.info("Creating schema FINANCE_MAIN in SE_DEMOS_NEW...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS SE_DEMOS_NEW.FINANCE_MAIN")
        logger.info("✓ Schema FINANCE_MAIN created successfully!")
        
        # Verify
        cursor.execute("SHOW DATABASES LIKE 'SE_DEMOS_NEW'")
        dbs = cursor.fetchall()
        logger.info(f"\nDatabase created: {dbs[0][1] if dbs else 'Not found'}")
        
        cursor.execute("SHOW SCHEMAS IN DATABASE SE_DEMOS_NEW")
        schemas = cursor.fetchall()
        logger.info("\nSchemas in SE_DEMOS_NEW:")
        for schema in schemas:
            logger.info(f"  - {schema[1]}")
        
    except Exception as e:
        logger.error(f"Error creating database/schema: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_new_database()
