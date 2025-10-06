import re

with open('utils/database_helper.py', 'r') as f:
    content = f.read()

# Find the load_pending_mappings method and add .columns = ... lower()
old_pattern = r'(df = pd\.read_sql\(query, conn\))'
new_code = r'\1\n            df.columns = df.columns.str.lower()  # Convert Snowflake uppercase to lowercase'

content = re.sub(old_pattern, new_code, content)

# Also fix load_approved_mappings
content = content.replace(
    'def load_approved_mappings(self) -> pd.DataFrame:',
    'def load_approved_mappings(self) -> pd.DataFrame:'
)

with open('utils/database_helper.py', 'w') as f:
    f.write(content)

print("✅ Fixed column name case")
