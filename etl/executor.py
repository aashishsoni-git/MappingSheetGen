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
    
    def execute_mappings(self, xml_id: str, mappings_df: pd.DataFrame) -> Dict:
        """
        Execute ETL mappings:
        1. Insert data from staging to Silver layer
        2. Generate and save VIEW queries for reuse
        """
        execution_id = str(uuid.uuid4())[:8]
        conn = self.get_connection()
        cursor = conn.cursor()
        
        summary = {
            'execution_id': execution_id,
            'tables_processed': 0,
            'total_rows': 0,
            'successful_tables': [],
            'failed_tables': [],
            'errors': [],
            'view_queries': {}
        }
        
        try:
            # Group mappings by target table
            tables = mappings_df['target_table'].unique()
            logger.info(f"Processing {len(tables)} tables for xml_id: {xml_id}")
            
            for table in tables:
                try:
                    table_mappings = mappings_df[mappings_df['target_table'] == table]
                    logger.info(f"Processing table: {table} with {len(table_mappings)} mappings")
                    
                    # ✅ Step 1: Insert data to Silver layer
                    rows_inserted = self._insert_to_silver(
                        cursor, xml_id, table, table_mappings
                    )
                    
                    # ✅ Step 2: Generate reusable VIEW query
                    view_query = self._generate_view_query(
                        xml_id, table, table_mappings
                    )
                    
                    # ✅ Step 3: Save VIEW definition to database
                    self._save_view_definition(
                        cursor, xml_id, table, view_query
                    )
                    
                    summary['tables_processed'] += 1
                    summary['total_rows'] += rows_inserted
                    summary['successful_tables'].append(table)
                    summary['view_queries'][table] = view_query
                    
                    # Update execution status
                    self._update_mapping_status(
                        cursor, xml_id, table, 'Success', execution_id
                    )
                    
                    logger.info(f"✅ {table}: Inserted {rows_inserted} rows")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {table}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    
                    summary['failed_tables'].append(table)
                    summary['errors'].append(str(e))
                    
                    self._update_mapping_status(
                        cursor, xml_id, table, 'Failed', execution_id, str(e)
                    )
            
            # Record execution in history
            self._record_execution(cursor, execution_id, xml_id, summary)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Execution failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            cursor.close()
            conn.close()
        
        return summary
    
    def _get_staging_data_info(self, cursor, xml_id: str) -> tuple:
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
    
    def _insert_to_silver(self, cursor, xml_id: str, table: str, mappings: pd.DataFrame) -> int:
        """
        Insert data from XML staging to Silver layer table
        """
        # Find staging table
        staging_table, has_processed_flag = self._get_staging_data_info(cursor, xml_id)
        logger.info(f"Using staging table: {staging_table}")
        
        # ✅ FIX 1: Remove duplicate columns - keep only the first occurrence
        seen_columns = set()
        unique_mappings = []
        
        for _, mapping in mappings.iterrows():
            target_col = mapping['target_column']
            if target_col not in seen_columns:
                seen_columns.add(target_col)
                unique_mappings.append(mapping)
            else:
                logger.warning(f"⚠️ Skipping duplicate column: {target_col}")
        
        if not unique_mappings:
            logger.error(f"No unique mappings for {table}")
            return 0
        
        # Build column mappings
        columns = []
        select_expressions = []
        
        for mapping in unique_mappings:
            target_col = mapping['target_column']
            source_node = mapping['source_node']
            transformation = mapping.get('transformation_logic', '')
            
            columns.append(target_col)
            
            # Apply transformation if specified
            if transformation and str(transformation).strip() and str(transformation) != 'None':
                clean_transform = str(transformation).replace('\n', ' ').strip()
                # ✅ FIX 2: Wrap with COALESCE to handle NULLs in non-nullable columns
                select_expressions.append(f"COALESCE({clean_transform}, '') AS {target_col}")
            else:
                # ✅ FIX 3: Use proper nested path extraction with colon notation
                # Handle nested paths like "Data.PolicyData.PolicyNumber"
                path_parts = source_node.split('/')
                
                # Build the path using colon notation
                if len(path_parts) > 1:
                    # Nested path: stg.xml_data:Data:PolicyData:PolicyNumber
                    path = ':'.join(path_parts)
                    select_expressions.append(
                        f"COALESCE(stg.xml_data:{path}::STRING, '') AS {target_col}"
                    )
                else:
                    # Simple path: stg.xml_data:PolicyNumber
                    select_expressions.append(
                        f"COALESCE(stg.xml_data:{source_node}::STRING, '') AS {target_col}"
                    )
        
        # Build INSERT statement
        where_clause = f"WHERE stg.xml_id = '{xml_id}'"
        if has_processed_flag:
            where_clause += " AND stg.processed = FALSE"
        
        insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        SELECT {', '.join(select_expressions)}
        FROM {staging_table} stg
        {where_clause}
        """
        
        logger.info(f"Executing INSERT for {table}")
        logger.debug(f"Columns ({len(columns)}): {columns}")
        logger.debug(f"SQL: {insert_sql[:500]}...")
        
        try:
            cursor.execute(insert_sql)
            rows_inserted = cursor.rowcount
            
            logger.info(f"Inserted {rows_inserted} rows into {table}")
            
            # Mark staging records as processed if flag exists
            if has_processed_flag and rows_inserted > 0:
                cursor.execute(f"""
                    UPDATE {staging_table}
                    SET processed = TRUE, 
                        processed_at = CURRENT_TIMESTAMP()
                    WHERE xml_id = '{xml_id}'
                    AND processed = FALSE
                """)
                logger.info(f"Marked {cursor.rowcount} staging rows as processed")
            
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Insert failed for {table}: {e}")
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
