import os
from dotenv import load_dotenv
from utils.database_helper import DatabaseHelper

load_dotenv()

config = {
    'account': os.getenv('SF_ACCOUNT'),
    'user': os.getenv('SF_USER'),
    'password': os.getenv('SF_PASSWORD'),
    'warehouse': os.getenv('SF_WAREHOUSE'),
    'database': os.getenv('SF_DATABASE'),
    'schema': 'ETL_MAPPER',
    'role': os.getenv('SF_ROLE')
}

db_helper = DatabaseHelper(config)

print("🔍 Checking pending mappings...")
df = db_helper.load_pending_mappings()

print(f"\n�� Result shape: {df.shape}")
print(f"📋 Columns: {list(df.columns)}")
print(f"\n🔍 First few rows:")
print(df.head())

if df.empty:
    print("\n⚠️ DataFrame is empty - no pending mappings found")
else:
    print(f"\n✅ Found {len(df)} rows")
    if 'xml_id' in df.columns:
        print(f"✅ xml_id column exists with {df['xml_id'].nunique()} unique values")
    else:
        print("❌ xml_id column is MISSING!")
