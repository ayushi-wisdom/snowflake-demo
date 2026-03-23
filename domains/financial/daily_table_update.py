import snowflake.connector
from snowflake_connection import get_snowflake_connection
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_or_update_table():
    """Create table if not exists and update with daily data"""
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date DATE,
            metric_name VARCHAR(100),
            metric_value FLOAT,
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (date, metric_name)
        )
        """
        cursor.execute(create_table_sql)
        logger.info("Table created/verified successfully")
        
        # Insert or update today's data
        merge_sql = """
        MERGE INTO daily_metrics AS target
        USING (
            SELECT 
                CURRENT_DATE() AS date,
                'sample_metric' AS metric_name,
                12345.67 AS metric_value
        ) AS source
        ON target.date = source.date 
           AND target.metric_name = source.metric_name
        WHEN MATCHED THEN
            UPDATE SET 
                metric_value = source.metric_value,
                updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (date, metric_name, metric_value)
            VALUES (source.date, source.metric_name, source.metric_value)
        """
        cursor.execute(merge_sql)
        logger.info(f"Data updated successfully at {datetime.now()}")
        
    except Exception as e:
        logger.error(f"Error updating table: {str(e)}")
        raise
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_or_update_table()