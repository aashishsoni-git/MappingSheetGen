import os

xml_files = [
    'data/quote_personal_auto_001.xml',
    'data/policy_homeowners_001.xml', 
    'data/policy_commercial_property_001.xml',
    'data/policy_umbrella_001.xml',
    'data/policy_personal_auto_renewal_001.xml'
]

for filepath in xml_files:
    if not os.path.exists(filepath):
        print(f"⚠️ File not found: {filepath}")
        continue
    
    # Read file
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the <?xml declaration
    xml_start = content.find('<?xml')
    
    if xml_start > 0:
        # There's content before <?xml - remove it
        content = content[xml_start:]
        print(f"✅ Fixed: {filepath} (removed {xml_start} characters before XML declaration)")
    elif xml_start == 0:
        print(f"✅ OK: {filepath} (already starts with XML declaration)")
    else:
        print(f"❌ ERROR: {filepath} (no XML declaration found)")
        continue
    
    # Write back
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

print("\n✅ All XML files fixed!")
