# etl/executor.py - COMPLETE FIXED VERSION

# At the top of executor.py, add file logging setup
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List
import uuid
import os

# ✅ Setup file logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_executor_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"ETL Executor initialized. Log file: {log_file}")



class ETLExecutor:
    """Enhanced ETL Executor that inserts data and generates reusable views"""
    
    def __init__(self, snowflake_config: dict):
        self.config = snowflake_config
        self._ensure_tables_exist()
        
    def get_connection(self):
        """Create Snowflake connection"""
        import snowflake.connector
        return snowflake.connector.connect(
            account=self.config['account'],
            user=self.config['user'],
            password=self.config['password'],
            warehouse=self.config['warehouse'],
            database=self.config['database'],
            schema=self.config['schema'],
            role=self.config.get('role')
        )
    
    def _ensure_tables_exist(self):
        """Ensure required tables exist on initialization"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Create VIEW_DEFINITIONS table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.VIEW_DEFINITIONS (
                    view_id VARCHAR(50) PRIMARY KEY,
                    xml_id VARCHAR(50),
                    target_table VARCHAR(500),
                    view_name VARCHAR(500),
                    view_query TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    created_by VARCHAR(100),
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            
            # Create EXECUTION_HISTORY table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.EXECUTION_HISTORY (
                    execution_id VARCHAR(50) PRIMARY KEY,
                    xml_id VARCHAR(50),
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    tables_processed INT,
                    total_rows_inserted INT,
                    successful_tables TEXT,
                    failed_tables TEXT,
                    status VARCHAR(50)
                )
            """)
            
            conn.commit()
            logger.info("✅ Required tables ensured")
            
        except Exception as e:
            logger.error(f"Error ensuring tables: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def execute_mappings(self, xml_id: str, mappings: pd.DataFrame) -> dict:
        """
        SIMPLIFIED: Just generate SQL VIEWs, don't load data yet
        """
        import uuid
        execution_id = str(uuid.uuid4())[:8]
        
        logger.info(f"Starting VIEW generation for {xml_id}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info(f"Mappings received: {len(mappings)} rows")
        
        results = {
            'execution_id': execution_id,
            'xml_id': xml_id,
            'views_created': [],
            'view_sqls': {},
            'errors': []
        }
        
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Group mappings by target table
            tables = mappings['target_table'].unique()
            logger.info(f"Generating VIEWs for {len(tables)} tables: {list(tables)}")
            
            for table in tables:
                try:
                    logger.info(f"Processing table: {table}")
                    table_mappings = mappings[mappings['target_table'] == table]
                    logger.info(f"  Mappings for {table}: {len(table_mappings)}")
                    
                    # Generate VIEW SQL
                    view_sql = self._generate_view_sql(xml_id, table, table_mappings)
                    logger.info(f"  Generated SQL ({len(view_sql)} chars)")
                    
                    # Create the VIEW
                    table_short = table.split('.')[-1]
                    view_name = f"INSURANCE.ETL_MAPPER.{table_short}_VW_{xml_id.replace('-', '_')}"
                    
                    create_view_sql = f"CREATE OR REPLACE VIEW {view_name} AS {view_sql}"
                    
                    logger.info(f"  Creating VIEW: {view_name}")
                    cursor.execute(create_view_sql)
                    logger.info(f"  ✅ VIEW created successfully: {view_name}")
                    
                    results['views_created'].append(view_name)
                    results['view_sqls'][table] = view_sql
                    
                except Exception as e:
                    error_msg = f"Table {table}: {str(e)}"
                    logger.error(f"  ❌ VIEW creation failed for {table}: {e}")
                    results['errors'].append(error_msg)
            
            # ✅ Commit any VIEWs created
            conn.commit()
            logger.info(f"✅ Committed changes")
            
            logger.info(f"✅ Execution complete: {len(results['views_created'])} VIEWs created, {len(results['errors'])} errors")
            return results
            
        except Exception as e:
            error_msg = f"Fatal error: {str(e)}"
            logger.error(f"❌ Execution failed: {e}")
            results['errors'].append(error_msg)
            
            if conn:
                conn.rollback()
            
            return results
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.info("Connection closed")

    
    # def _get_staging_data_info(self, cursor, xml_id: str) -> tuple:
        """
        Find and return the staging table structure
        Returns: (staging_table_name, has_processed_flag)
        """
        # Check XML_STAGING table
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as cnt, 
                       COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_cnt
                FROM INSURANCE.ETL_MAPPER.XML_STAGING
                WHERE xml_id = '{xml_id}'
            """)
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info(f"Found {result[0]} rows in XML_STAGING, {result[1]} already processed")
                return "INSURANCE.ETL_MAPPER.XML_STAGING", True
        except Exception as e:
            logger.debug(f"XML_STAGING check failed: {e}")
        
        # Check for dynamic staging table
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as cnt
                FROM INSURANCE.ETL_MAPPER.STG_XML_{xml_id}
            """)
            result = cursor.fetchone()
            if result and result[0] > 0:
                logger.info(f"Found {result[0]} rows in STG_XML_{xml_id}")
                return f"INSURANCE.ETL_MAPPER.STG_XML_{xml_id}", False
        except Exception as e:
            logger.debug(f"Dynamic staging table check failed: {e}")
        
        raise Exception(f"No staging data found for xml_id: {xml_id}")
    
    # def _insert_to_silver(self, cursor, xml_id: str, table: str, mappings: pd.DataFrame) -> int:
    #     """
    #     Extract from raw XML with debug logging for NULL values
    #     """
    #     logger.info(f"Processing {table} for xml_id: {xml_id}")
        
    #     # Verify Bronze data
    #     try:
    #         cursor.execute(f"""
    #             SELECT COUNT(*), raw_xml FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE 
    #             WHERE xml_id = '{xml_id}'
    #             GROUP BY raw_xml
    #         """)
    #         result = cursor.fetchone()
    #         if not result or result[0] == 0:
    #             raise Exception(f"No data in XML_RAW_BRONZE for xml_id: {xml_id}")
            
    #         bronze_count = result[0]
    #         logger.info(f"Found {bronze_count} rows in Bronze for {xml_id}")
            
    #         # ✅ Debug: Log sample of XML structure
    #         sample_xml = result[1][:500] if result[1] else "NO XML"
    #         logger.debug(f"Sample XML: {sample_xml}...")
            
    #     except Exception as e:
    #         logger.error(f"Bronze check failed: {e}")
    #         raise
        
    #     # Remove duplicates and skip ID columns
    #     seen_columns = set()
    #     unique_mappings = []
    #     skipped_ids = []
        
    #     for _, mapping in mappings.iterrows():
    #         target_col = mapping['target_column']
    #         col_upper = target_col.upper()
            
    #         # Skip ID columns
    #         if '_ID' in col_upper or col_upper == 'ID':
    #             skipped_ids.append(target_col)
    #             continue
            
    #         if target_col not in seen_columns:
    #             seen_columns.add(target_col)
    #             unique_mappings.append(mapping)
    #         else:
    #             logger.warning(f"⚠️ Skipping duplicate: {target_col}")
        
    #     if skipped_ids:
    #         logger.info(f"⏭️ Skipped ID columns: {', '.join(skipped_ids)}")
        
    #     if not unique_mappings:
    #         logger.warning(f"No data columns for {table}")
    #         return 0
        
    #     # Build column expressions
    #     columns = []
    #     select_expressions = []
        
    #     for mapping in unique_mappings:
    #         target_col = mapping['target_column']
    #         source_node = mapping['source_node']
            
    #         # Build XMLGET chain
    #         node_parts = source_node.split('/')
    #         xml_expr = "xml_variant"
            
    #         for node in node_parts:
    #             xml_expr = f"XMLGET({xml_expr}, '{node}')"
            
    #         extracted_value = f'{xml_expr}:"$"::STRING'
            
    #         # Type-aware conversion
    #         col_upper = target_col.upper()
            
    #         # BOOLEAN columns
    #         if 'IS_' in col_upper or 'HAS_' in col_upper or col_upper.endswith('_FLAG'):
    #             select_expr = f"TRY_TO_BOOLEAN(NULLIF({extracted_value}, '')) AS {target_col}"
            
    #         # DATE/TIME columns
    #         elif 'DATE' in col_upper or 'TIME' in col_upper:
    #             select_expr = f"TRY_TO_DATE(NULLIF({extracted_value}, '')) AS {target_col}"
            
    #         # NUMERIC columns (excluding *_NUMBER which are text)
    #         elif (('AMOUNT' in col_upper or 'PREMIUM' in col_upper or 
    #             'LIMIT' in col_upper or 'DEDUCTIBLE' in col_upper or
    #             'COUNT' in col_upper or 'QUANTITY' in col_upper or
    #             'PERCENT' in col_upper or 'RATE' in col_upper or
    #             'INSTALLMENTS' in col_upper or 'TERM' in col_upper) and 
    #             '_NUMBER' not in col_upper):
    #             select_expr = f"TRY_TO_NUMBER(NULLIF({extracted_value}, '')) AS {target_col}"
            
    #         # ✅ Special handling for critical NOT NULL text columns like POLICY_NUMBER
    #         elif col_upper.endswith('_NUMBER') or col_upper.endswith('NUMBER'):
    #             # Try extraction, but provide fallback for NOT NULL columns
    #             # Check if column allows NULL by trying to determine criticality
    #             is_critical = any(x in col_upper for x in ['POLICY', 'QUOTE', 'PAYMENT'])
                
    #             if is_critical:
    #                 # Use fallback value for critical identifiers
    #                 select_expr = f"""
    #                     COALESCE(
    #                         NULLIF(TRIM({extracted_value}), ''),
    #                         CONCAT('{table.split('.')[-1]}_', '{xml_id}', '_', UUID_STRING())
    #                     ) AS {target_col}
    #                 """.strip().replace('\n', ' ')
    #                 logger.warning(f"⚠️ {target_col} using fallback UUID if NULL")
    #             else:
    #                 # Allow NULL for non-critical number fields
    #                 select_expr = f"NULLIF(TRIM({extracted_value}), '') AS {target_col}"
            
    #         # All other text columns
    #         else:
    #             select_expr = f"NULLIF({extracted_value}, '') AS {target_col}"
            
    #         columns.append(target_col)
    #         select_expressions.append(select_expr)
        
    #     # Build INSERT
    #     insert_sql = f"""
    #     INSERT INTO {table} ({', '.join(columns)})
    #     SELECT {', '.join(select_expressions)}
    #     FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
    #     WHERE xml_id = '{xml_id}'
    #     """
        
    #     logger.info(f"Executing INSERT for {table} with {len(columns)} columns")
    #     logger.debug(f"SQL length: {len(insert_sql)} chars")
        
    #     try:
    #         cursor.execute(insert_sql)
    #         rows = cursor.rowcount
    #         logger.info(f"✅ Inserted {rows} rows into {table}")
    #         return rows
    #     except Exception as e:
    #         logger.error(f"❌ Insert failed: {e}")
    #         logger.error(f"Full SQL: {insert_sql}")
    #         raise

    def _insert_to_silver(self, cursor, xml_id: str, table: str, mappings: pd.DataFrame) -> int:
        """
        Extract from raw XML Bronze layer and insert to Silver table
        Simple approach with proper type handling
        """
        logger.info(f"Processing {table} for xml_id: {xml_id}")
        
        # ========== Step 1: Verify Bronze data exists ==========
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE 
                WHERE xml_id = '{xml_id}'
            """)
            bronze_count = cursor.fetchone()[0]
            logger.info(f"Found {bronze_count} rows in Bronze for {xml_id}")
            
            if bronze_count == 0:
                raise Exception(f"No data in XML_RAW_BRONZE for xml_id: {xml_id}")
        except Exception as e:
            logger.error(f"Bronze check failed: {e}")
            raise
        
        # ========== Step 2: Remove duplicates and skip ID columns ==========
        seen_columns = set()
        unique_mappings = []
        skipped_ids = []
        
        for _, mapping in mappings.iterrows():
            target_col = mapping['target_column']
            col_upper = target_col.upper()
            
            # Skip ID columns (they're auto-generated by IDENTITY)
            if '_ID' in col_upper or col_upper == 'ID':
                skipped_ids.append(target_col)
                continue
            
            # Remove duplicates
            if target_col not in seen_columns:
                seen_columns.add(target_col)
                unique_mappings.append(mapping)
            else:
                logger.warning(f"⚠️ Skipping duplicate column: {target_col}")
        
        if skipped_ids:
            logger.info(f"⏭️ Skipped ID columns (auto-generated): {', '.join(skipped_ids)}")
        
        if not unique_mappings:
            logger.warning(f"No data columns to insert for {table}")
            return 0
        
        # ========== Step 3: Build column expressions ==========
        columns = []
        select_expressions = []
        
        for mapping in unique_mappings:
            target_col = mapping['target_column']
            source_node = mapping['source_node']
            
            # Build XMLGET chain for nested paths
            node_parts = source_node.split('/')
            xml_expr = "xml_variant"
            
            for node in node_parts:
                xml_expr = f"XMLGET({xml_expr}, '{node}')"
            
            # Extract the text value
            extracted_value = f'{xml_expr}:"$"::STRING'
            
            # ========== Type-aware conversion ==========
            col_upper = target_col.upper()
            
            # BOOLEAN columns (IS_*, HAS_*, *_FLAG)
            if 'IS_' in col_upper or 'HAS_' in col_upper or col_upper.endswith('_FLAG'):
                select_expr = f"TRY_TO_BOOLEAN(NULLIF(TRIM({extracted_value}), '')) AS {target_col}"
            
            # DATE/TIME columns
            elif 'DATE' in col_upper or 'TIME' in col_upper or 'TIMESTAMP' in col_upper:
                select_expr = f"TRY_TO_DATE(NULLIF(TRIM({extracted_value}), '')) AS {target_col}"
            
            # NUMERIC columns - but NOT text columns that contain 'NUMBER' like POLICY_NUMBER
            elif (('AMOUNT' in col_upper or 'PREMIUM' in col_upper or 
                'LIMIT' in col_upper or 'DEDUCTIBLE' in col_upper or
                'COUNT' in col_upper or 'QUANTITY' in col_upper or
                'PERCENT' in col_upper or 'RATE' in col_upper or
                'INSTALLMENTS' in col_upper or 'TERM' in col_upper or
                'BALANCE' in col_upper or 'FEE' in col_upper or
                'TAX' in col_upper or 'DISCOUNT' in col_upper) and 
                '_NUMBER' not in col_upper):  # Exclude POLICY_NUMBER, QUOTE_NUMBER, etc.
                select_expr = f"TRY_TO_NUMBER(NULLIF(TRIM({extracted_value}), '')) AS {target_col}"
            
            # RISK_NUMBER is actually numeric (special case)
            elif col_upper == 'RISK_NUMBER':
                select_expr = f"TRY_TO_NUMBER(NULLIF(TRIM({extracted_value}), '')) AS {target_col}"
            
            # All other text columns (including POLICY_NUMBER, QUOTE_NUMBER, etc.)
            else:
                # Just extract as string, trim whitespace, convert empty to NULL
                select_expr = f"NULLIF(TRIM({extracted_value}), '') AS {target_col}"
            
            columns.append(target_col)
            select_expressions.append(select_expr)
        
        # ========== Step 4: Build and execute INSERT ==========
        insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        SELECT {', '.join(select_expressions)}
        FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
        WHERE xml_id = '{xml_id}'
        """
        
        logger.info(f"Executing INSERT for {table} with {len(columns)} columns")
        logger.debug(f"Columns: {', '.join(columns[:10])}{'...' if len(columns) > 10 else ''}")
        logger.debug(f"SQL length: {len(insert_sql)} characters")
        
        try:
            cursor.execute(insert_sql)
            rows = cursor.rowcount
            logger.info(f"✅ Inserted {rows} rows into {table}")
            return rows
        except Exception as e:
            logger.error(f"❌ Insert failed for {table}: {e}")
            logger.error(f"Full SQL: {insert_sql}")
            raise






    
    def _generate_view_query(self, xml_id: str, table: str, mappings: pd.DataFrame) -> str:
        """Generate reusable VIEW query"""
        staging_table = f"INSURANCE.ETL_MAPPER.XML_STAGING"
        table_name = table.split('.')[-1]
        view_name = f"INSURANCE.ETL_MAPPER.{table_name}_VW"
        
        # Build column mappings
        select_expressions = []
        
        for _, mapping in mappings.iterrows():
            target_col = mapping['target_column']
            source_node = mapping['source_node']
            transformation = mapping.get('transformation_logic', '')
            
            if transformation and str(transformation).strip() and str(transformation) != 'None':
                clean_transform = str(transformation).replace('\n', ' ').strip()
                select_expressions.append(f"    {clean_transform} AS {target_col}")
            else:
                clean_node = source_node.split('/')[-1] if '/' in source_node else source_node
                select_expressions.append(
                    f"    stg.xml_data:{clean_node}::STRING AS {target_col}"
                )
        
        # Generate CREATE OR REPLACE VIEW statement
        view_query = f"""-- Reusable VIEW for {table}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE OR REPLACE VIEW {view_name} AS
SELECT
{',\n'.join(select_expressions)}
FROM {staging_table} stg
WHERE stg.target_table = '{table}'
  AND stg.processed = FALSE;

-- Usage Example:
-- INSERT INTO {table} SELECT * FROM {view_name};
"""
        
        return view_query
    
    def _save_view_definition(self, cursor, xml_id: str, table: str, view_query: str):
        """Save VIEW definition to database"""
        view_id = str(uuid.uuid4())[:8]
        table_name = table.split('.')[-1]
        view_name = f"{table_name}_VW"
        
        cursor.execute("""
            INSERT INTO INSURANCE.ETL_MAPPER.VIEW_DEFINITIONS
            (view_id, xml_id, target_table, view_name, view_query, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (view_id, xml_id, table, view_name, view_query, 'etl_executor'))
        
        logger.info(f"💾 Saved VIEW definition: {view_name}")
    
    def _update_mapping_status(self, cursor, xml_id: str, table: str, 
                               status: str, execution_id: str, error: str = None):
        """Update mapping execution status"""
        cursor.execute("""
            UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
            SET execution_status = %s,
                last_execution_id = %s,
                last_execution_at = CURRENT_TIMESTAMP(),
                execution_error = %s
            WHERE xml_id = %s AND target_table = %s
        """, (status, execution_id, error, xml_id, table))
    
    def _record_execution(self, cursor, execution_id: str, xml_id: str, summary: Dict):
        """Record execution in history table"""
        status = 'Success' if not summary['failed_tables'] else 'Partial' if summary['successful_tables'] else 'Failed'
        
        cursor.execute("""
            INSERT INTO INSURANCE.ETL_MAPPER.EXECUTION_HISTORY
            (execution_id, xml_id, tables_processed, total_rows_inserted, 
             successful_tables, failed_tables, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            execution_id, xml_id, summary['tables_processed'],
            summary['total_rows'], 
            ','.join(summary['successful_tables']) if summary['successful_tables'] else '',
            ','.join(summary['failed_tables']) if summary['failed_tables'] else '',
            status
        ))
    
    def get_saved_views(self, xml_id: str = None) -> pd.DataFrame:
        """Retrieve saved VIEW definitions"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT view_id, xml_id, target_table, view_name, view_query, created_at
            FROM INSURANCE.ETL_MAPPER.VIEW_DEFINITIONS
            WHERE is_active = TRUE
        """
        
        if xml_id:
            query += f" AND xml_id = '{xml_id}'"
        
        query += " ORDER BY created_at DESC"
        
        try:
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
            if not df.empty:
                df.columns = df.columns.str.lower()
            return df
        except Exception as e:
            logger.error(f"Error loading saved views: {e}")
            return pd.DataFrame(columns=['view_id', 'xml_id', 'target_table', 'view_name', 'view_query', 'created_at'])
        finally:
            cursor.close()
            conn.close()

    def _insert_to_silver_from_raw_xml(self, cursor, xml_id: str, table: str, 
                                    mappings: pd.DataFrame) -> int:
        """
        Extract from raw XML using mappings - node by node
        """
        # Remove duplicates
        seen_columns = set()
        unique_mappings = []
        for _, mapping in mappings.iterrows():
            if mapping['target_column'] not in seen_columns:
                seen_columns.add(mapping['target_column'])
                unique_mappings.append(mapping)
        
        columns = []
        select_expressions = []
        
        for mapping in unique_mappings:
            target_col = mapping['target_column']
            source_node = mapping['source_node']  # e.g., "Policy/PolicyNumber"
            
            # Build XMLGET path for nested nodes
            node_parts = source_node.split('/')
            
            # Start with parsed XML
            xml_expr = "PARSE_XML(raw_xml)"
            
            # Chain XMLGET for each level
            for node in node_parts:
                xml_expr = f"XMLGET({xml_expr}, '{node}')"
            
            # Extract text value
            select_expr = f"COALESCE({xml_expr}:'$'::STRING, '') AS {target_col}"
            
            columns.append(target_col)
            select_expressions.append(select_expr)
        
        # Build INSERT from raw XML
        insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        SELECT {', '.join(select_expressions)}
        FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
        WHERE xml_id = '{xml_id}'
        """
        
        logger.info(f"Executing extraction for {table}")
        logger.debug(f"SQL: {insert_sql}")
        
        cursor.execute(insert_sql)
        return cursor.rowcount

    def execute_mappings(self, xml_id: str, mappings: pd.DataFrame) -> dict:
        """
        SIMPLIFIED: Just generate SQL VIEWs, don't load data yet
        """
        import uuid
        execution_id = str(uuid.uuid4())[:8]
        
        logger.info(f"Starting VIEW generation for {xml_id}")
        logger.info(f"Execution ID: {execution_id}")
        logger.info(f"Mappings received: {len(mappings)} rows")
        logger.info(f"Mappings columns: {list(mappings.columns)}")
        
        results = {
            'execution_id': execution_id,
            'xml_id': xml_id,
            'views_created': [],
            'view_sqls': {},
            'errors': []
        }
        
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Group mappings by target table
            tables = mappings['target_table'].unique()
            logger.info(f"Generating VIEWs for {len(tables)} tables: {list(tables)}")
            
            for table in tables:
                try:
                    logger.info(f"Processing table: {table}")
                    table_mappings = mappings[mappings['target_table'] == table]
                    logger.info(f"  Mappings for {table}: {len(table_mappings)}")
                    
                    # Generate VIEW SQL
                    view_sql = self._generate_view_sql(xml_id, table, table_mappings)
                    logger.info(f"  Generated SQL ({len(view_sql)} chars)")
                    logger.debug(f"  SQL: {view_sql[:300]}...")
                    
                    # Create the VIEW
                    table_short = table.split('.')[-1]
                    view_name = f"INSURANCE.ETL_MAPPER.{table_short}_VW_{xml_id.replace('-', '_')}"
                    
                    create_view_sql = f"CREATE OR REPLACE VIEW {view_name} AS {view_sql}"
                    
                    logger.info(f"  Creating VIEW: {view_name}")
                    cursor.execute(create_view_sql)
                    logger.info(f"  ✅ VIEW created successfully: {view_name}")
                    
                    results['views_created'].append(view_name)
                    results['view_sqls'][table] = view_sql
                    
                except Exception as e:
                    error_msg = f"Table {table}: {str(e)}"
                    logger.error(f"  ❌ VIEW creation failed for {table}: {e}")
                    logger.error(f"  SQL was: {view_sql if 'view_sql' in locals() else 'NOT GENERATED'}")
                    results['errors'].append(error_msg)
            
            # Save execution history
            try:
                status = 'SUCCESS' if not results['errors'] else 'PARTIAL'
                cursor.execute("""
                    INSERT INTO INSURANCE.ETL_MAPPER.EXECUTION_HISTORY
                    (execution_id, xml_id, execution_date, status, tables_processed, total_rows_inserted)
                    VALUES (%s, %s, CURRENT_TIMESTAMP(), %s, %s, %s)
                """, (execution_id, xml_id, status, len(results['views_created']), 0))
                
                conn.commit()
                logger.info(f"✅ Execution history saved")
            except Exception as e:
                logger.error(f"Failed to save execution history: {e}")
            
            logger.info(f"✅ Execution complete: {len(results['views_created'])} VIEWs created, {len(results['errors'])} errors")
            return results
            
        except Exception as e:
            error_msg = f"Fatal error: {str(e)}"
            logger.error(f"❌ Execution failed: {e}")
            results['errors'].append(error_msg)
            
            if conn:
                conn.rollback()
            
            return results
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            logger.info("Connection closed")


    def _generate_view_sql(self, xml_id: str, table: str, mappings: pd.DataFrame) -> str:
        """
        Generate SQL for a VIEW that extracts XML data
        """
        logger.info(f"Generating VIEW SQL for {table}")
        
        # Remove duplicates and skip ID columns
        seen = set()
        unique_mappings = []
        
        for _, mapping in mappings.iterrows():
            col = mapping['target_column']
            col_upper = col.upper()
            
            # Skip ID columns
            if '_ID' in col_upper or col_upper == 'ID':
                logger.debug(f"  Skipping ID column: {col}")
                continue
            
            if col not in seen:
                seen.add(col)
                unique_mappings.append(mapping)
                logger.debug(f"  Added column: {col} from node: {mapping['source_node']}")
            else:
                logger.warning(f"  Skipping duplicate column: {col}")
        
        if not unique_mappings:
            raise Exception(f"No mappable columns found for {table} (all were IDs or duplicates)")
        
        logger.info(f"  Building SELECT for {len(unique_mappings)} columns")
        
        # Build SELECT columns
        select_cols = []
        
        for mapping in unique_mappings:
            col = mapping['target_column']
            node = mapping['source_node']
            
            # Build XMLGET path
            xml_expr = "xml_variant"
            for part in node.split('/'):
                xml_expr = f"XMLGET({xml_expr}, '{part}')"
            
            # Simple extraction - just get the value as STRING
            col_expr = f"{xml_expr}:\"$\"::STRING AS {col}"
            select_cols.append(col_expr)
        
        # Build VIEW SQL
        view_sql = f"""SELECT
        '{xml_id}' AS SOURCE_XML_ID,
        {',\n    '.join(select_cols)}
    FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
    WHERE xml_id = '{xml_id}'"""
        
        logger.info(f"  VIEW SQL generated ({len(view_sql)} chars)")
        return view_sql
