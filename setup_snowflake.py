# setup_snowflake.py
from loaders.snowflake_loader import SnowflakeStageLoader
from utils.logging_config import setup_logging
from dotenv import load_dotenv
import os
import logging

load_dotenv()
setup_logging(log_level='INFO')
logger = logging.getLogger(__name__)

def setup_snowflake_schema():
    """Create necessary schemas and tables in Snowflake"""
    
    config = {
        'account': os.getenv('SF_ACCOUNT'),
        'user': os.getenv('SF_USER'),
        'password': os.getenv('SF_PASSWORD'),
        'warehouse': os.getenv('SF_WAREHOUSE'),
        'database': os.getenv('SF_DATABASE'),
        'schema': 'PUBLIC',  # Start with PUBLIC
        'role': os.getenv('SF_ROLE')
    }
    
    try:
        # Connect to Snowflake
        loader = SnowflakeStageLoader(**config)
        
        # Test connection
        logger.info("Testing Snowflake connection...")
        if not loader.test_connection():
            logger.error("Connection test failed!")
            return
        
        cursor = loader.conn.cursor()
        
        # Create ETL_MAPPER schema
        logger.info("Creating ETL_MAPPER schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS ETL_MAPPER")
        
        # Create SILVER schema
        logger.info("Creating SILVER schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
        
        # Switch to ETL_MAPPER schema
        cursor.execute("USE SCHEMA ETL_MAPPER")
        
        # Create stage table
        logger.info("Creating stage table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS STAGE_XML_RAW (
                id NUMBER AUTOINCREMENT,
                xml_data VARIANT,
                source_file VARCHAR(500),
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (id)
            )
        """)
        
        # Create sample Silver tables for testing
        logger.info("Creating sample Silver layer tables...")
        cursor.execute("USE SCHEMA SILVER")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CUSTOMER (
                customer_id NUMBER,
                customer_name VARCHAR(200),
                email VARCHAR(200),
                phone VARCHAR(50),
                created_date DATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ORDERS (
                order_id NUMBER,
                customer_id NUMBER,
                order_date DATE,
                order_amount DECIMAL(10,2),
                status VARCHAR(50)
            )
        """)
        
        logger.info("✅ Snowflake setup completed successfully!")
        
        cursor.close()
        loader.close()
        
    except Exception as e:
        logger.error(f"❌ Setup failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    setup_snowflake_schema()
