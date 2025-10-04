# create_insurance_silver_tables.py
from loaders.snowflake_loader import SnowflakeStageLoader
from utils.logging_config import setup_logging
from dotenv import load_dotenv
import os
import logging

load_dotenv()
setup_logging(log_level='INFO')
logger = logging.getLogger(__name__)

def create_insurance_silver_tables():
    """Create 5 Silver layer tables for Insurance domain"""
    
    config = {
        'account': os.getenv('SF_ACCOUNT'),
        'user': os.getenv('SF_USER'),
        'password': os.getenv('SF_PASSWORD'),
        'warehouse': os.getenv('SF_WAREHOUSE'),
        'database': os.getenv('SF_DATABASE'),
        'schema': 'PUBLIC',
        'role': os.getenv('SF_ROLE')
    }
    
    loader = SnowflakeStageLoader(**config)
    cursor = loader.conn.cursor()
    
    # Create SILVER schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS SILVER")
    cursor.execute("USE SCHEMA SILVER")
    
    # 1. POLICY Table
    logger.info("Creating SILVER.POLICY table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE POLICY (
            policy_id VARCHAR(50) PRIMARY KEY,
            policy_number VARCHAR(100) NOT NULL,
            policy_type VARCHAR(50),
            product_code VARCHAR(50),
            product_name VARCHAR(200),
            policy_status VARCHAR(50),
            effective_date DATE,
            expiration_date DATE,
            cancellation_date DATE,
            renewal_date DATE,
            policy_term_months INTEGER,
            premium_amount DECIMAL(15,2),
            total_insured_value DECIMAL(15,2),
            underwriting_company VARCHAR(200),
            distribution_channel VARCHAR(100),
            account_id VARCHAR(50),
            quote_id VARCHAR(50),
            created_date TIMESTAMP_NTZ,
            modified_date TIMESTAMP_NTZ,
            created_by VARCHAR(100),
            modified_by VARCHAR(100),
            source_system VARCHAR(50),
            transaction_id VARCHAR(100)
        )
    """)
    
    # 2. QUOTE Table
    logger.info("Creating SILVER.QUOTE table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE QUOTE (
            quote_id VARCHAR(50) PRIMARY KEY,
            quote_number VARCHAR(100) NOT NULL,
            quote_status VARCHAR(50),
            product_code VARCHAR(50),
            product_name VARCHAR(200),
            quote_date DATE,
            expiry_date DATE,
            effective_date DATE,
            quoted_premium DECIMAL(15,2),
            discount_amount DECIMAL(15,2),
            tax_amount DECIMAL(15,2),
            total_premium DECIMAL(15,2),
            account_id VARCHAR(50),
            agent_code VARCHAR(50),
            agent_name VARCHAR(200),
            underwriter_name VARCHAR(200),
            business_type VARCHAR(50),
            distribution_channel VARCHAR(100),
            conversion_status VARCHAR(50),
            converted_policy_id VARCHAR(50),
            created_date TIMESTAMP_NTZ,
            modified_date TIMESTAMP_NTZ,
            created_by VARCHAR(100),
            source_system VARCHAR(50)
        )
    """)
    
    # 3. RISK Table
    logger.info("Creating SILVER.RISK table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE RISK (
            risk_id VARCHAR(50) PRIMARY KEY,
            policy_id VARCHAR(50),
            quote_id VARCHAR(50),
            risk_type VARCHAR(100),
            risk_number INTEGER,
            risk_description VARCHAR(500),
            risk_address_line1 VARCHAR(200),
            risk_address_line2 VARCHAR(200),
            risk_city VARCHAR(100),
            risk_state VARCHAR(50),
            risk_zip VARCHAR(20),
            risk_country VARCHAR(50),
            construction_type VARCHAR(100),
            occupancy_type VARCHAR(100),
            building_year INTEGER,
            square_footage INTEGER,
            number_of_stories INTEGER,
            property_value DECIMAL(15,2),
            replacement_cost DECIMAL(15,2),
            vehicle_make VARCHAR(100),
            vehicle_model VARCHAR(100),
            vehicle_year INTEGER,
            vehicle_vin VARCHAR(50),
            vehicle_usage VARCHAR(100),
            annual_mileage INTEGER,
            driver_age INTEGER,
            driver_gender VARCHAR(10),
            created_date TIMESTAMP_NTZ,
            modified_date TIMESTAMP_NTZ,
            source_system VARCHAR(50)
        )
    """)
    
    # 4. COVERAGE Table
    logger.info("Creating SILVER.COVERAGE table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE COVERAGE (
            coverage_id VARCHAR(50) PRIMARY KEY,
            policy_id VARCHAR(50),
            quote_id VARCHAR(50),
            risk_id VARCHAR(50),
            coverage_code VARCHAR(50),
            coverage_name VARCHAR(200),
            coverage_type VARCHAR(100),
            coverage_category VARCHAR(100),
            coverage_limit DECIMAL(15,2),
            coverage_deductible DECIMAL(15,2),
            coverage_premium DECIMAL(15,2),
            per_occurrence_limit DECIMAL(15,2),
            aggregate_limit DECIMAL(15,2),
            coinsurance_percentage DECIMAL(5,2),
            effective_date DATE,
            expiration_date DATE,
            is_mandatory BOOLEAN,
            coverage_basis VARCHAR(50),
            rating_factor DECIMAL(10,4),
            created_date TIMESTAMP_NTZ,
            modified_date TIMESTAMP_NTZ,
            source_system VARCHAR(50)
        )
    """)
    
    # 5. PAYMENT Table
    logger.info("Creating SILVER.PAYMENT table...")
    cursor.execute("""
        CREATE OR REPLACE TABLE PAYMENT (
            payment_id VARCHAR(50) PRIMARY KEY,
            policy_id VARCHAR(50),
            quote_id VARCHAR(50),
            payment_number VARCHAR(100),
            payment_type VARCHAR(50),
            payment_method VARCHAR(50),
            payment_status VARCHAR(50),
            payment_date DATE,
            due_date DATE,
            payment_amount DECIMAL(15,2),
            outstanding_amount DECIMAL(15,2),
            payment_term VARCHAR(50),
            installment_number INTEGER,
            total_installments INTEGER,
            payment_frequency VARCHAR(50),
            account_holder_name VARCHAR(200),
            bank_name VARCHAR(200),
            account_number VARCHAR(50),
            routing_number VARCHAR(50),
            card_type VARCHAR(50),
            card_last_four VARCHAR(4),
            transaction_reference VARCHAR(100),
            payment_confirmation VARCHAR(100),
            paid_amount DECIMAL(15,2),
            paid_date DATE,
            created_date TIMESTAMP_NTZ,
            modified_date TIMESTAMP_NTZ,
            source_system VARCHAR(50)
        )
    """)
    
    logger.info("✅ All 5 Silver tables created successfully!")
    
    cursor.close()
    loader.close()

if __name__ == "__main__":
    create_insurance_silver_tables()
