# test_snowflake_connection.py
from loaders.snowflake_loader import SnowflakeStageLoader
from utils.logging_config import setup_logging
from dotenv import load_dotenv
import os

load_dotenv()
setup_logging(log_level='INFO')

config = {
    'account': os.getenv('SF_ACCOUNT'),
    'user': os.getenv('SF_USER'),
    'password': os.getenv('SF_PASSWORD'),
    'warehouse': os.getenv('SF_WAREHOUSE'),
    'database': os.getenv('SF_DATABASE'),
    'schema': os.getenv('SF_SCHEMA'),
    'role': os.getenv('SF_ROLE')
}

print("Testing Snowflake connection...")
print(f"Account: {config['account']}")
print(f"Database: {config['database']}")
print(f"Schema: {config['schema']}")
print(f"User: {config['user']}")

try:
    loader = SnowflakeStageLoader(**config)
    loader.test_connection()
    loader.close()
    print("\n✅ Connection successful!")
except Exception as e:
    print(f"\n❌ Connection failed: {str(e)}")
