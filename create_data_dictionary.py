import pandas as pd

# Comprehensive Data Dictionary with full business context
data_dictionary = [
    # POLICY Table
    ['SILVER.POLICY', 'policy_id', 'VARCHAR(50)', 'Unique identifier for insurance policy', 'Must be unique and not null. Primary key.', 'POL-AUTO-2025-00123', 'Must match source system format', 'Indexed', 'QUOTE.quote_id (for conversion)', '2025-10-05'],
    ['SILVER.POLICY', 'policy_number', 'VARCHAR(100)', 'Business-friendly policy number displayed to customers', 'Not null. Must be unique within product line.', 'AUTO-99887', 'Uppercase format preferred', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'policy_type', 'VARCHAR(50)', 'Type of insurance policy', 'Must be from approved list: Auto, Homeowners, Commercial, Umbrella, etc.', 'PersonalAuto', 'Standardized values only', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'policy_status', 'VARCHAR(50)', 'Current lifecycle status of policy', 'Values: Draft, Quoted, Bound, InForce, Cancelled, Expired, Renewed', 'InForce', 'Track status changes in audit table', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'product_code', 'VARCHAR(50)', 'Internal product code identifier', 'Must match PRODUCT_CATALOG table. Not null.', 'PA001', 'Standard format: 2-3 letters + 3 digits', 'Indexed', 'PRODUCT_CATALOG.product_code', '2025-10-05'],
    ['SILVER.POLICY', 'product_name', 'VARCHAR(200)', 'Full product name', 'Descriptive name for reporting', 'Personal Auto Insurance', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'effective_date', 'DATE', 'Date when policy coverage begins', 'Must be a valid date. Cannot be in past for new business.', '2025-11-01', 'Validate against business rules', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'expiration_date', 'DATE', 'Date when policy coverage ends', 'Must be after effective_date. Typically 6 or 12 months after effective.', '2026-11-01', 'Must be >= effective_date', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'premium_amount', 'NUMBER(15,2)', 'Total annual premium amount in USD', 'Must be positive. Includes all fees and taxes.', '1250.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'base_premium', 'NUMBER(15,2)', 'Base premium before discounts and surcharges', 'Must be positive', '1375.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'discount_amount', 'NUMBER(15,2)', 'Total discount amount applied', 'Can be zero or positive', '125.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'tax_amount', 'NUMBER(15,2)', 'Total tax amount', 'Calculated based on state tax rates', '84.38', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'account_id', 'VARCHAR(50)', 'Reference to customer account', 'Foreign key to CUSTOMER table. Not null.', 'ACC-2025-9876', 'None', 'Indexed', 'CUSTOMER.account_id', '2025-10-05'],
    ['SILVER.POLICY', 'agent_code', 'VARCHAR(50)', 'Code identifying the selling agent', 'Must exist in AGENT table', 'AGT-001', 'None', 'Indexed', 'AGENT.agent_code', '2025-10-05'],
    ['SILVER.POLICY', 'underwriter_id', 'VARCHAR(50)', 'ID of underwriter who approved policy', 'Reference to UNDERWRITER table', 'UW-045', 'None', 'None', 'UNDERWRITER.underwriter_id', '2025-10-05'],
    ['SILVER.POLICY', 'source_system', 'VARCHAR(50)', 'Source system that created the policy', 'Values: DuckCreek, Guidewire, Manual, API', 'DuckCreek', 'Track data lineage', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'transaction_id', 'VARCHAR(100)', 'Unique transaction identifier from source', 'Unique per transaction. Used for deduplication.', 'TXN-POL-20251005-001', 'None', 'Unique', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'created_by', 'VARCHAR(100)', 'User who created the record', 'Username or system identifier', 'sarah.johnson', 'Lowercase preferred', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'created_date', 'TIMESTAMP_NTZ', 'Timestamp when record was created', 'System-generated timestamp', '2025-10-05 10:30:00', 'UTC timezone', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'modified_by', 'VARCHAR(100)', 'User who last modified the record', 'Username or system identifier', 'sarah.johnson', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.POLICY', 'modified_date', 'TIMESTAMP_NTZ', 'Timestamp of last modification', 'System-generated timestamp', '2025-10-05 10:30:00', 'UTC timezone', 'None', 'None', '2025-10-05'],
    
    # QUOTE Table
    ['SILVER.QUOTE', 'quote_id', 'VARCHAR(50)', 'Unique identifier for quote', 'Primary key. Must be unique.', 'Q-AUTO-2025-00123', 'Format: Q-{PRODUCT}-{YEAR}-{SEQ}', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'quote_number', 'VARCHAR(100)', 'Business-friendly quote number', 'Not null. Displayed to customers.', 'QT-PA-00123', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'quote_status', 'VARCHAR(50)', 'Current status of quote', 'Values: Draft, Quoted, Expired, Converted, Declined', 'Quoted', 'Track lifecycle', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'quote_date', 'DATE', 'Date quote was generated', 'Not null. Must be <= current date.', '2025-10-05', 'Validate date range', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'expiry_date', 'DATE', 'Date when quote expires', 'Must be > quote_date. Typically 30 days.', '2025-11-05', 'Auto-calculate if not provided', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'effective_date', 'DATE', 'Proposed policy effective date', 'Future date when coverage would begin', '2025-10-15', 'Must be future date', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'product_code', 'VARCHAR(50)', 'Product being quoted', 'Foreign key to product catalog', 'PA001', 'Not null', 'Indexed', 'PRODUCT_CATALOG.product_code', '2025-10-05'],
    ['SILVER.QUOTE', 'product_name', 'VARCHAR(200)', 'Product name', 'Descriptive name', 'Personal Auto Insurance', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'quoted_premium', 'NUMBER(15,2)', 'Quoted premium amount', 'Must be positive. Annual premium.', '1250.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'discount_amount', 'NUMBER(15,2)', 'Total discounts applied', 'Sum of all discount line items', '125.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'tax_amount', 'NUMBER(15,2)', 'Estimated tax amount', 'Based on state tax rates', '84.38', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'total_premium', 'NUMBER(15,2)', 'Total amount including taxes and fees', 'quoted_premium - discount_amount + tax_amount + fee_amount', '1209.38', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'account_id', 'VARCHAR(50)', 'Customer account reference', 'Foreign key to CUSTOMER', 'ACC-2025-9876', 'Not null', 'Indexed', 'CUSTOMER.account_id', '2025-10-05'],
    ['SILVER.QUOTE', 'agent_code', 'VARCHAR(50)', 'Agent who created quote', 'Foreign key to AGENT', 'AGT-001', 'Not null', 'Indexed', 'AGENT.agent_code', '2025-10-05'],
    ['SILVER.QUOTE', 'agent_name', 'VARCHAR(200)', 'Agent full name', 'Denormalized for reporting', 'Sarah Johnson', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'underwriter_name', 'VARCHAR(200)', 'Assigned underwriter name', 'Optional. Only for referred quotes.', 'Michael Brown', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'business_type', 'VARCHAR(50)', 'Type of business transaction', 'Values: New, Renewal, Rewrite, Endorsement', 'New', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'distribution_channel', 'VARCHAR(50)', 'Sales distribution channel', 'Values: Agent, Broker, Direct, Online, Call Center', 'Agent', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'created_by', 'VARCHAR(100)', 'User who created quote', 'Username', 'sarah.johnson', 'Lowercase', 'None', 'None', '2025-10-05'],
    ['SILVER.QUOTE', 'created_date', 'TIMESTAMP_NTZ', 'Creation timestamp', 'System timestamp', '2025-10-05 10:30:00', 'UTC', 'None', 'None', '2025-10-05'],
    
    # RISK Table
    ['SILVER.RISK', 'risk_id', 'VARCHAR(50)', 'Unique identifier for risk item', 'Primary key. One policy can have multiple risks.', 'RSK-AUTO-001', 'Format: RSK-{TYPE}-{SEQ}', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'policy_id', 'VARCHAR(50)', 'Reference to parent policy', 'Foreign key. Can be null for quotes.', 'POL-AUTO-2025-00123', 'Not null for policies', 'Indexed', 'POLICY.policy_id', '2025-10-05'],
    ['SILVER.RISK', 'quote_id', 'VARCHAR(50)', 'Reference to quote', 'Foreign key. Null for issued policies.', 'Q-AUTO-2025-00123', 'Can be null', 'Indexed', 'QUOTE.quote_id', '2025-10-05'],
    ['SILVER.RISK', 'risk_number', 'INTEGER', 'Sequential risk number within policy', 'Starts at 1. Unique within policy.', '1', 'Positive integer', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'risk_type', 'VARCHAR(100)', 'Type of insured risk', 'Product-specific. Auto: Vehicle, HO: Dwelling, etc.', 'Vehicle', 'Varies by product', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'risk_description', 'VARCHAR(500)', 'Human-readable risk description', 'Free text description', 'Primary Vehicle - 2023 Toyota Camry', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'vehicle_make', 'VARCHAR(100)', 'Vehicle manufacturer', 'Auto products only. Standardized values.', 'Toyota', 'Uppercase', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'vehicle_model', 'VARCHAR(100)', 'Vehicle model name', 'Auto products only', 'Camry', 'Uppercase', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'vehicle_year', 'INTEGER', 'Vehicle model year', 'Auto products. Must be between 1900 and current year + 1.', '2023', 'Valid year range', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'vehicle_vin', 'VARCHAR(50)', 'Vehicle Identification Number', 'Auto products. Must be 17 characters.', '1HGBH41JXMN109186', '17 characters. Uppercase.', 'Unique', 'None', '2025-10-05'],
    ['SILVER.RISK', 'vehicle_usage', 'VARCHAR(50)', 'How vehicle is used', 'Values: Commute, Pleasure, Business, Farm', 'Commute', 'Affects rating', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'annual_mileage', 'INTEGER', 'Estimated annual miles driven', 'Auto products. Used for rating.', '15000', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'driver_age', 'INTEGER', 'Primary driver age', 'Auto products. Calculated from DOB.', '40', 'Must be 16-100', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'driver_gender', 'CHAR(1)', 'Primary driver gender', 'Values: M, F, X', 'M', 'Single character', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'construction_type', 'VARCHAR(100)', 'Building construction type', 'Property products. Values: Frame, Masonry, Fire Resistive, etc.', 'Frame', 'Property only', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'building_year', 'INTEGER', 'Year building was constructed', 'Property products. Must be between 1800 and current year.', '1995', 'Valid year range', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'square_footage', 'INTEGER', 'Total square footage of building', 'Property products', '2200', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'replacement_cost', 'NUMBER(15,2)', 'Estimated cost to replace building', 'Property products. Used for coverage limits.', '380000.00', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'location_address', 'VARCHAR(500)', 'Physical address of risk location', 'Full street address', '456 Oak Avenue, Naperville, IL 60540', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.RISK', 'location_city', 'VARCHAR(100)', 'City name', 'Not null', 'Chicago', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'location_state', 'VARCHAR(2)', 'State code', 'Two-letter state abbreviation', 'IL', 'Uppercase', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.RISK', 'location_zipcode', 'VARCHAR(10)', 'ZIP or postal code', 'Used for territory rating', '60601', 'Format: 12345 or 12345-6789', 'Indexed', 'None', '2025-10-05'],
    
    # COVERAGE Table
    ['SILVER.COVERAGE', 'coverage_id', 'VARCHAR(50)', 'Unique coverage identifier', 'Primary key', 'COV-AUTO-001', 'Format: COV-{TYPE}-{SEQ}', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'policy_id', 'VARCHAR(50)', 'Reference to policy', 'Foreign key. Can be null for quotes.', 'POL-AUTO-2025-00123', 'Can be null', 'Indexed', 'POLICY.policy_id', '2025-10-05'],
    ['SILVER.COVERAGE', 'quote_id', 'VARCHAR(50)', 'Reference to quote', 'Foreign key', 'Q-AUTO-2025-00123', 'Can be null', 'Indexed', 'QUOTE.quote_id', '2025-10-05'],
    ['SILVER.COVERAGE', 'risk_id', 'VARCHAR(50)', 'Risk this coverage applies to', 'Foreign key to RISK', 'RSK-AUTO-001', 'Not null', 'Indexed', 'RISK.risk_id', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_code', 'VARCHAR(50)', 'Internal coverage code', 'Standardized codes: BI, PD, COLL, COMP, etc.', 'BI', 'Uppercase', 'Indexed', 'COVERAGE_CATALOG.coverage_code', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_name', 'VARCHAR(200)', 'Full coverage name', 'Customer-facing name', 'Bodily Injury Liability', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_type', 'VARCHAR(50)', 'Coverage category', 'Values: Liability, Physical Damage, Medical, etc.', 'Liability', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_category', 'VARCHAR(50)', 'Coverage classification', 'Values: Required, Optional, Package', 'Required', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_limit', 'NUMBER(15,2)', 'Maximum coverage amount', 'Limit of liability or insured value', '100000', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_deductible', 'NUMBER(15,2)', 'Deductible amount', 'Amount insured pays before coverage applies', '500', 'Zero or positive', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'coverage_premium', 'NUMBER(15,2)', 'Premium for this coverage', 'Portion of total premium', '425.00', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'is_mandatory', 'BOOLEAN', 'Whether coverage is required', 'True for state-mandated or product-required coverages', 'TRUE', 'Boolean', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'effective_date', 'DATE', 'Coverage effective date', 'When coverage begins', '2025-10-15', 'Must be valid date', 'None', 'None', '2025-10-05'],
    ['SILVER.COVERAGE', 'expiration_date', 'DATE', 'Coverage expiration date', 'When coverage ends', '2026-10-15', 'Must be > effective_date', 'None', 'None', '2025-10-05'],
    
    # PAYMENT Table
    ['SILVER.PAYMENT', 'payment_id', 'VARCHAR(50)', 'Unique payment identifier', 'Primary key', 'PAY-HO-001', 'Format: PAY-{TYPE}-{SEQ}', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'policy_id', 'VARCHAR(50)', 'Reference to policy', 'Foreign key. Can be null for quote deposits.', 'POL-HO-2025-00234', 'Can be null', 'Indexed', 'POLICY.policy_id', '2025-10-05'],
    ['SILVER.PAYMENT', 'quote_id', 'VARCHAR(50)', 'Reference to quote', 'Foreign key for quote deposits', 'Q-HO-2025-00189', 'Can be null', 'Indexed', 'QUOTE.quote_id', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_number', 'VARCHAR(100)', 'Business payment number', 'Unique payment identifier', 'PAY-HO-001-001', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_type', 'VARCHAR(50)', 'Type of payment', 'Values: Deposit, Installment, Renewal, Endorsement, Refund', 'Renewal', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_method', 'VARCHAR(50)', 'How payment is made', 'Values: CreditCard, BankTransfer, Check, Cash, EFT', 'BankTransfer', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_status', 'VARCHAR(50)', 'Current payment status', 'Values: Pending, Scheduled, Processed, Failed, Refunded', 'Scheduled', 'Track status changes', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_amount', 'NUMBER(15,2)', 'Amount of this payment', 'Must be positive', '1850.00', 'Precision: 2 decimals', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'payment_term', 'VARCHAR(50)', 'Payment frequency', 'Values: Monthly, Quarterly, SemiAnnual, Annual, Single', 'Quarterly', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'total_installments', 'INTEGER', 'Total number of installments', 'Number of payments in payment plan', '4', 'Must be positive', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'installment_number', 'INTEGER', 'Current installment number', 'Which installment this payment represents', '1', 'Must be <= total_installments', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'due_date', 'DATE', 'Date payment is due', 'Not null', '2025-11-01', 'Must be valid date', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'processed_date', 'TIMESTAMP_NTZ', 'When payment was processed', 'Null until processed', '2025-11-01 08:30:00', 'UTC timezone', 'None', 'None', '2025-10-05'],
    ['SILVER.PAYMENT', 'transaction_reference', 'VARCHAR(100)', 'External transaction reference', 'From payment processor', 'TXN-STRIPE-987654', 'None', 'Unique', 'None', '2025-10-05'],
    
    # CUSTOMER Table
    ['SILVER.CUSTOMER', 'account_id', 'VARCHAR(50)', 'Unique account identifier', 'Primary key', 'ACC-2025-5432', 'Format: ACC-{YEAR}-{SEQ}', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'account_number', 'VARCHAR(100)', 'Business account number', 'Customer-facing account number', 'ACC-5432', 'None', 'Unique', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'account_name', 'VARCHAR(200)', 'Account holder name', 'Primary account name', 'Emily Davis', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'account_type', 'VARCHAR(50)', 'Type of account', 'Values: Personal, Commercial, Government', 'Personal', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'first_name', 'VARCHAR(100)', 'Customer first name', 'Not null for personal accounts', 'Emily', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'last_name', 'VARCHAR(100)', 'Customer last name', 'Not null for personal accounts', 'Davis', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'date_of_birth', 'DATE', 'Customer date of birth', 'Used for age calculation and rating', '1982-09-22', 'Must be past date', 'None', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'email', 'VARCHAR(200)', 'Primary email address', 'Used for communication', 'emily.davis@email.com', 'Valid email format', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'phone', 'VARCHAR(20)', 'Primary phone number', 'Contact phone', '815-555-0234', 'Format: XXX-XXX-XXXX', 'None', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'address_line1', 'VARCHAR(200)', 'Street address line 1', 'Not null', '456 Oak Avenue', 'None', 'None', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'city', 'VARCHAR(100)', 'City name', 'Not null', 'Naperville', 'None', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'state', 'VARCHAR(2)', 'State code', 'Two-letter abbreviation', 'IL', 'Uppercase', 'Indexed', 'None', '2025-10-05'],
    ['SILVER.CUSTOMER', 'zipcode', 'VARCHAR(10)', 'ZIP code', 'Not null', '60540', 'Format: 12345 or 12345-6789', 'Indexed', 'None', '2025-10-05'],
]

# Create DataFrame
df = pd.DataFrame(data_dictionary, columns=[
    'table_name', 'column_name', 'data_type', 'business_description', 
    'business_rules', 'example_value', 'data_quality_rules', 
    'indexing', 'related_tables', 'last_updated'
])

# Save to Excel with multiple sheets
with pd.ExcelWriter('reference_data/data_dictionary_complete.xlsx', engine='openpyxl') as writer:
    # Main dictionary
    df.to_excel(writer, sheet_name='DataDictionary', index=False)
    
    # Summary by table
    summary = df.groupby('table_name').agg({
        'column_name': 'count',
        'data_type': lambda x: x.value_counts().to_dict()
    }).reset_index()
    summary.columns = ['table_name', 'column_count', 'data_type_distribution']
    summary.to_excel(writer, sheet_name='TableSummary', index=False)
    
    # Metadata
    metadata = pd.DataFrame({
        'Property': ['Total Tables', 'Total Columns', 'Last Updated', 'Version', 'Owner'],
        'Value': [df['table_name'].nunique(), len(df), '2025-10-05', '1.0', 'Data Engineering Team']
    })
    metadata.to_excel(writer, sheet_name='Metadata', index=False)

print(f"✅ Created data_dictionary_complete.xlsx ({len(df)} column definitions)")
print(f"   Tables: {df['table_name'].nunique()}")
print(f"   Columns: {len(df)}")
