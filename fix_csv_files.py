import pandas as pd

# Fix homeowners CSV
try:
    df = pd.read_csv('reference_data/old_mappings_homeowners.csv', on_bad_lines='skip')
    df.to_csv('reference_data/old_mappings_homeowners_fixed.csv', index=False)
    print("✅ Fixed homeowners CSV")
except Exception as e:
    print(f"❌ Error fixing homeowners CSV: {e}")

# Fix commercial CSV  
try:
    df = pd.read_csv('reference_data/old_mappings_commercial.csv', on_bad_lines='skip')
    df.to_csv('reference_data/old_mappings_commercial_fixed.csv', index=False)
    print("✅ Fixed commercial CSV")
except Exception as e:
    print(f"❌ Error fixing commercial CSV: {e}")

print("\n✅ CSV files fixed!")
