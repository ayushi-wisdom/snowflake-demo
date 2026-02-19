import snowflake.connector
from snowflake_connection import get_snowflake_connection
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connection():
    """Test Snowflake connection and list available databases"""
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        logger.info("✓ Connection successful!")
        
        # Get current user and role
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        logger.info(f"Current User: {result[0]}")
        logger.info(f"Current Role: {result[1]}")
        logger.info(f"Current Warehouse: {result[2]}")
        logger.info(f"Current Database: {result[3]}")
        logger.info(f"Current Schema: {result[4]}")
        
        # List available databases
        logger.info("\nAvailable databases:")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        for db in databases:
            logger.info(f"  - {db[1]}")
        
        cursor.close()
        conn.close()
        logger.info("\n✓ Connection test completed successfully!")
        
    except Exception as e:
        logger.error(f"✗ Connection test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_connection()
