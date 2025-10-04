-- Setup Enhanced Schema for 3-Stage Workflow
-- Run this in Snowflake before using the enhanced app

USE DATABASE INSURANCE;
USE SCHEMA ETL_MAPPER;

-- 1. Store Raw XML
CREATE TABLE IF NOT EXISTS STAGE_XML_RAW (
    xml_id VARCHAR(50) PRIMARY KEY,
    product_code VARCHAR(50),
    xml_filename VARCHAR(500),
    xml_content VARIANT,
    upload_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    uploaded_by VARCHAR(100),
    processing_status VARCHAR(50) DEFAULT 'Pending',
    metadata VARIANT
);

-- 2. Store Generated Mappings
CREATE TABLE IF NOT EXISTS GENERATED_MAPPINGS (
    mapping_id VARCHAR(50) PRIMARY KEY,
    xml_id VARCHAR(50),
    source_node VARCHAR(500),
    target_table VARCHAR(200),
    target_column VARCHAR(200),
    transformation_logic VARCHAR(2000),
    confidence_score FLOAT,
    reasoning VARCHAR(2000),
    ai_generated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    approval_status VARCHAR(50) DEFAULT 'Pending',
    approved_by VARCHAR(100),
    approved_date TIMESTAMP_NTZ,
    user_notes VARCHAR(2000),
    execution_status VARCHAR(50) DEFAULT 'Not Started',
    FOREIGN KEY (xml_id) REFERENCES STAGE_XML_RAW(xml_id)
);

-- 3. Track ETL Execution History
CREATE TABLE IF NOT EXISTS ETL_EXECUTION_LOG (
    execution_id VARCHAR(50) PRIMARY KEY,
    xml_id VARCHAR(50),
    mapping_id VARCHAR(50),
    target_table VARCHAR(200),
    execution_start TIMESTAMP_NTZ,
    execution_end TIMESTAMP_NTZ,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_failed INTEGER,
    execution_status VARCHAR(50),
    error_message VARCHAR(5000),
    executed_by VARCHAR(100)
);

-- 4. Data Reconciliation Results
CREATE TABLE IF NOT EXISTS RECONCILIATION_RESULTS (
    recon_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50),
    source_count INTEGER,
    target_count INTEGER,
    match_count INTEGER,
    mismatch_count INTEGER,
    missing_in_target INTEGER,
    extra_in_target INTEGER,
    reconciliation_status VARCHAR(50),
    details VARIANT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_xml_product ON STAGE_XML_RAW(product_code);
CREATE INDEX IF NOT EXISTS idx_mapping_status ON GENERATED_MAPPINGS(approval_status, execution_status);
CREATE INDEX IF NOT EXISTS idx_execution_status ON ETL_EXECUTION_LOG(execution_status);

SELECT 'Schema setup complete!' AS status;
