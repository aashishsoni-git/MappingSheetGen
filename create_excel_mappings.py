import pandas as pd
import os

os.makedirs('reference_data', exist_ok=True)

# Personal Auto Mappings - Comprehensive
pa_mappings = [
    ['DuckCreek', '/Session/TransactionInfo/TransactionID', '', 'SILVER.POLICY', 'transaction_id', '', 'Direct mapping - unique transaction identifier', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/TransactionInfo/SourceSystem', '', 'SILVER.POLICY', 'source_system', '', 'Direct mapping - source system name', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteID', '', 'SILVER.QUOTE', 'quote_id', '', 'Direct mapping - primary key', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteNumber', '', 'SILVER.QUOTE', 'quote_number', 'UPPER(value)', 'Uppercase standardization', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Status', '', 'SILVER.QUOTE', 'quote_status', '', 'Direct mapping - quote lifecycle status', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteDate', '', 'SILVER.QUOTE', 'quote_date', 'TO_DATE(value)', 'Date conversion from string', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/ExpiryDate', '', 'SILVER.QUOTE', 'expiry_date', 'TO_DATE(value)', 'Date conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/EffectiveDate', '', 'SILVER.QUOTE', 'effective_date', 'TO_DATE(value)', 'Date conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Product/ProductCode', '', 'SILVER.QUOTE', 'product_code', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Product/ProductName', '', 'SILVER.QUOTE', 'product_name', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Premium/QuotedPremium', '', 'SILVER.QUOTE', 'quoted_premium', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion with precision', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Premium/DiscountAmount', '', 'SILVER.QUOTE', 'discount_amount', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Premium/TaxAmount', '', 'SILVER.QUOTE', 'tax_amount', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/Premium/TotalPremium', '', 'SILVER.QUOTE', 'total_premium', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Account/AccountID', '', 'SILVER.QUOTE', 'account_id', '', 'Foreign key to account', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Agent/AgentCode', '', 'SILVER.QUOTE', 'agent_code', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Agent/AgentName', '', 'SILVER.QUOTE', 'agent_name', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Underwriter/UnderwriterName', '', 'SILVER.QUOTE', 'underwriter_name', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/BusinessInfo/BusinessType', '', 'SILVER.QUOTE', 'business_type', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/BusinessInfo/DistributionChannel', '', 'SILVER.QUOTE', 'distribution_channel', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/UserDetails/CreatedBy', '', 'SILVER.QUOTE', 'created_by', 'LOWER(value)', 'Lowercase standardization', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/UserDetails/CreatedDate', '', 'SILVER.QUOTE', 'created_date', 'TO_TIMESTAMP_NTZ(value)', 'Timestamp conversion without timezone', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/RiskID', '', 'SILVER.RISK', 'risk_id', '', 'Direct mapping - primary key', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteID', '', 'SILVER.RISK', 'quote_id', '', 'Foreign key to quote', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/RiskType', '', 'SILVER.RISK', 'risk_type', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/RiskNumber', '', 'SILVER.RISK', 'risk_number', 'CAST(value AS INTEGER)', 'Integer conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/Make', '', 'SILVER.RISK', 'vehicle_make', 'UPPER(TRIM(value))', 'Uppercase and trim whitespace', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/Model', '', 'SILVER.RISK', 'vehicle_model', 'UPPER(TRIM(value))', 'Uppercase and trim', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/ModelYear', '', 'SILVER.RISK', 'vehicle_year', 'CAST(value AS INTEGER)', 'Integer conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/VIN', '', 'SILVER.RISK', 'vehicle_vin', 'UPPER(value)', 'Uppercase VIN', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/Usage', '', 'SILVER.RISK', 'vehicle_usage', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Vehicle/AnnualMileage', '', 'SILVER.RISK', 'annual_mileage', 'CAST(value AS INTEGER)', 'Integer conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Driver/Age', '', 'SILVER.RISK', 'driver_age', 'CAST(value AS INTEGER)', 'Integer conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/Driver/Gender', '', 'SILVER.RISK', 'driver_gender', 'UPPER(LEFT(value,1))', 'First character uppercase (M/F)', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/CoverageID', '', 'SILVER.COVERAGE', 'coverage_id', '', 'Direct mapping - primary key', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteID', '', 'SILVER.COVERAGE', 'quote_id', '', 'Foreign key to quote', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/RiskID', '', 'SILVER.COVERAGE', 'risk_id', '', 'Foreign key to risk', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/CoverageCode', '', 'SILVER.COVERAGE', 'coverage_code', 'UPPER(value)', 'Uppercase code', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/CoverageName', '', 'SILVER.COVERAGE', 'coverage_name', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/CoverageType', '', 'SILVER.COVERAGE', 'coverage_type', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/Limit', '', 'SILVER.COVERAGE', 'coverage_limit', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/Deductible', '', 'SILVER.COVERAGE', 'coverage_deductible', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/Premium', '', 'SILVER.COVERAGE', 'coverage_premium', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Coverages/Coverage/IsMandatory', '', 'SILVER.COVERAGE', 'is_mandatory', "CASE WHEN LOWER(value)='true' THEN TRUE ELSE FALSE END", 'Boolean conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/PaymentID', '', 'SILVER.PAYMENT', 'payment_id', '', 'Direct mapping - primary key', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Quote/QuoteID', '', 'SILVER.PAYMENT', 'quote_id', '', 'Foreign key to quote', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/PaymentType', '', 'SILVER.PAYMENT', 'payment_type', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/PaymentMethod', '', 'SILVER.PAYMENT', 'payment_method', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/PaymentTerm', '', 'SILVER.PAYMENT', 'payment_term', '', 'Direct mapping', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/TotalInstallments', '', 'SILVER.PAYMENT', 'total_installments', 'CAST(value AS INTEGER)', 'Integer conversion', 'PA001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Payment/DownPaymentAmount', '', 'SILVER.PAYMENT', 'down_payment', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'PA001', '2025-10-05', 'system'],
]

pa_df = pd.DataFrame(pa_mappings, columns=[
    'source_system', 'source_node', 'source_attribute', 'target_table', 
    'target_column', 'transformation', 'notes', 'product_code', 'created_date', 'created_by'
])

# Save to Excel
with pd.ExcelWriter('reference_data/historical_mappings_personal_auto.xlsx', engine='openpyxl') as writer:
    pa_df.to_excel(writer, sheet_name='Mappings', index=False)
    
    # Add metadata sheet
    metadata = pd.DataFrame({
        'Property': ['Product', 'Total Mappings', 'Last Updated', 'Version', 'Source'],
        'Value': ['Personal Auto Insurance (PA001)', len(pa_df), '2025-10-05', '1.0', 'DuckCreek 7.5']
    })
    metadata.to_excel(writer, sheet_name='Metadata', index=False)

print(f"✅ Created historical_mappings_personal_auto.xlsx ({len(pa_df)} mappings)")

# Homeowners Mappings
ho_mappings = [
    ['DuckCreek', '/Session/Data/Policy/PolicyID', '', 'SILVER.POLICY', 'policy_id', '', 'Direct mapping - primary key', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/PolicyNumber', '', 'SILVER.POLICY', 'policy_number', 'UPPER(value)', 'Uppercase', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/PolicyType', '', 'SILVER.POLICY', 'policy_type', '', 'Direct mapping', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/PolicyStatus', '', 'SILVER.POLICY', 'policy_status', '', 'Direct mapping', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/Product/ProductCode', '', 'SILVER.POLICY', 'product_code', '', 'Direct mapping', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/Dates/EffectiveDate', '', 'SILVER.POLICY', 'effective_date', 'TO_DATE(value)', 'Date conversion', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/Dates/ExpirationDate', '', 'SILVER.POLICY', 'expiration_date', 'TO_DATE(value)', 'Date conversion', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/Premium/TotalPremium', '', 'SILVER.POLICY', 'premium_amount', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/RiskID', '', 'SILVER.RISK', 'risk_id', '', 'Direct mapping', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Policy/PolicyID', '', 'SILVER.RISK', 'policy_id', '', 'Foreign key', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/PropertyDetails/ConstructionType', '', 'SILVER.RISK', 'construction_type', '', 'Direct mapping', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/PropertyDetails/BuildingYear', '', 'SILVER.RISK', 'building_year', 'CAST(value AS INTEGER)', 'Integer conversion', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/PropertyDetails/SquareFootage', '', 'SILVER.RISK', 'square_footage', 'CAST(value AS INTEGER)', 'Integer conversion', 'HO003', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/Risks/Risk/PropertyValue/ReplacementCost', '', 'SILVER.RISK', 'replacement_cost', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'HO003', '2025-10-05', 'system'],
]

ho_df = pd.DataFrame(ho_mappings, columns=[
    'source_system', 'source_node', 'source_attribute', 'target_table', 
    'target_column', 'transformation', 'notes', 'product_code', 'created_date', 'created_by'
])

with pd.ExcelWriter('reference_data/historical_mappings_homeowners.xlsx', engine='openpyxl') as writer:
    ho_df.to_excel(writer, sheet_name='Mappings', index=False)

print(f"✅ Created historical_mappings_homeowners.xlsx ({len(ho_df)} mappings)")

# Commercial Property Mappings
cp_mappings = [
    ['DuckCreek', '/Session/Data/PolicyData/ID', '', 'SILVER.POLICY', 'policy_id', '', 'Direct mapping - alternate node structure', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/Number', '', 'SILVER.POLICY', 'policy_number', 'UPPER(value)', 'Uppercase', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/LineOfBusiness', '', 'SILVER.POLICY', 'policy_type', '', 'Direct mapping', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/StatusCode', '', 'SILVER.POLICY', 'policy_status', '', 'Direct mapping', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/ProductDetails/ProdCode', '', 'SILVER.POLICY', 'product_code', '', 'Direct mapping', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/PolicyDates/InceptionDate', '', 'SILVER.POLICY', 'effective_date', 'TO_DATE(value)', 'Date conversion', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/PremiumInfo/TotalPremium', '', 'SILVER.POLICY', 'premium_amount', 'CAST(value AS NUMBER(15,2))', 'Decimal conversion', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/RiskData/RiskIdentifier', '', 'SILVER.RISK', 'risk_id', '', 'Direct mapping - commercial structure', 'CP001', '2025-10-05', 'system'],
    ['DuckCreek', '/Session/Data/PolicyData/ID', '', 'SILVER.RISK', 'policy_id', '', 'Foreign key', 'CP001', '2025-10-05', 'system'],
]

cp_df = pd.DataFrame(cp_mappings, columns=[
    'source_system', 'source_node', 'source_attribute', 'target_table', 
    'target_column', 'transformation', 'notes', 'product_code', 'created_date', 'created_by'
])

with pd.ExcelWriter('reference_data/historical_mappings_commercial.xlsx', engine='openpyxl') as writer:
    cp_df.to_excel(writer, sheet_name='Mappings', index=False)

print(f"✅ Created historical_mappings_commercial.xlsx ({len(cp_df)} mappings)")

print("\n✅ All Excel mapping files created successfully!")
