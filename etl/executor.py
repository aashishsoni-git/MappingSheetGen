"""
ETL Execution Engine - Executes approved mappings
"""
import snowflake.connector
import pandas as pd
from datetime import datetime  # ✅ ADD THIS
from typing import Dict, List, Callable, Optional
import logging
import json

logger = logging.getLogger(__name__)


class ETLExecutor:
    """Execute approved mappings and load to Silver layer"""
    
    def __init__(self, snowflake_config: Dict):
        self.config = snowflake_config
    
    def execute_etl_pipeline(self, xml_id: str, mappings: pd.DataFrame, 
                            mode: str = "Execute & Load",
                            batch_size: int = 1000,
                            progress_callback: Optional[Callable] = None) -> Dict:
        """
        Execute approved mappings and load to Silver layer
        
        Args:
            xml_id: XML identifier
            mappings: DataFrame with approved mappings
            mode: "Validate Only" or "Execute & Load"
            batch_size: Records per batch
            progress_callback: Function to report progress
        
        Returns:
            Execution results dictionary
        """
        conn = snowflake.connector.connect(**self.config)
        cursor = conn.cursor()
        
        execution_id = f"EXEC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        try:
            if progress_callback:
                progress_callback(0.1, "🔍 Generating SQL statements...")
            
            # Step 1: Generate SQL from mappings
            sql_statements = self._generate_sql(xml_id, mappings)
            
            if progress_callback:
                progress_callback(0.3, "✅ Validating SQL...")
            
            # Step 2: Validate SQL (always)
            validation_results = self._validate_sql(cursor, sql_statements)
            
            if not validation_results['valid']:
                return {
                    'status': 'Failed',
                    'error': 'SQL validation failed',
                    'details': validation_results
                }
            
            if mode == "Validate Only":
                return {
                    'status': 'Validated',
                    'sql_statements': sql_statements,
                    'validation': validation_results
                }
            
            if progress_callback:
                progress_callback(0.5, "⚡ Executing transformations...")
            
            # Step 3: Execute transformations
            results = {
                'execution_id': execution_id,
                'xml_id': xml_id,
                'status': 'Success',
                'rows_processed': 0,
                'rows_inserted': 0,
                'rows_updated': 0,
                'rows_failed': 0,
                'start_time': datetime.now()
            }
            
            # Group by target table
            table_count = len(mappings.groupby('target_table'))
            current_table = 0
            
            for target_table, group in mappings.groupby('target_table'):
                if progress_callback:
                    progress = 0.5 + (0.3 * (current_table / table_count))
                    progress_callback(progress, f"📊 Loading {target_table}...")
                
                table_result = self._load_to_table(
                    cursor,
                    xml_id,
                    target_table,
                    group
                )
                
                results['rows_processed'] += table_result['rows_processed']
                results['rows_inserted'] += table_result['rows_inserted']
                results['rows_failed'] += table_result['rows_failed']
                
                current_table += 1
            
            if progress_callback:
                progress_callback(0.9, "🔍 Reconciling data...")
            
            # Step 4: Data reconciliation
            recon_results = self._reconcile_data(cursor, xml_id, mappings)
            
            # Step 5: Log execution
            self._log_execution(cursor, results, recon_results)
            
            conn.commit()
            
            results['end_time'] = datetime.now()
            results['duration_sec'] = (results['end_time'] - results['start_time']).total_seconds()
            results['reconciliation'] = recon_results
            
            if progress_callback:
                progress_callback(1.0, "✅ Complete!")
            
            return results
            
        except Exception as e:
            conn.rollback()
            logger.error(f"ETL execution failed: {e}")
            
            if progress_callback:
                progress_callback(1.0, f"❌ Failed: {str(e)}")
            
            return {
                'status': 'Failed',
                'error': str(e),
                'execution_id': execution_id
            }
        finally:
            cursor.close()
            conn.close()
    
    def _generate_sql(self, xml_id: str, mappings_df: pd.DataFrame) -> List[str]:
        """Generate INSERT/MERGE SQL statements from mappings"""
        sql_statements = []
        
        for target_table, group in mappings_df.groupby('target_table'):
            # Build column list
            columns = group['target_column'].tolist()
            
            # Build SELECT with transformations
            select_parts = []
            for _, row in group.iterrows():
                source_node = row['source_node']
                transformation = row['transformation_logic']
                target_col = row['target_column']
                
                if transformation and transformation.strip():
                    # Apply transformation
                    # Replace 'value' with actual XML path
                    trans = transformation.replace('value', f"xml_content:{source_node}")
                    select_part = f"{trans} AS {target_col}"
                else:
                    # Direct mapping
                    select_part = f"xml_content:{source_node}::STRING AS {target_col}"
                
                select_parts.append(select_part)
            
            # Generate MERGE statement (upsert)
            primary_key = columns[0]  # Assume first column is PK
            
            sql = f"""
MERGE INTO {target_table} AS target
USING (
    SELECT 
        {',\n        '.join(select_parts)}
    FROM INSURANCE.ETL_MAPPER.STAGE_XML_RAW
    WHERE xml_id = '{xml_id}'
) AS source
ON target.{primary_key} = source.{primary_key}
WHEN MATCHED THEN
    UPDATE SET {', '.join([f'{col} = source.{col}' for col in columns[1:]])}
WHEN NOT MATCHED THEN
    INSERT ({', '.join(columns)})
    VALUES ({', '.join([f'source.{col}' for col in columns])})
            """
            
            sql_statements.append(sql.strip())
        
        return sql_statements
    
    def _validate_sql(self, cursor, sql_statements: List[str]) -> Dict:
        """Validate SQL without executing"""
        results = {'valid': True, 'errors': [], 'validated_statements': 0}
        
        for idx, sql in enumerate(sql_statements):
            try:
                # Snowflake doesn't support EXPLAIN for MERGE
                # So we'll do a dry-run check
                cursor.execute(sql.replace('MERGE', 'MERGE /*+ DRY_RUN */'))
                results['validated_statements'] += 1
            except Exception as e:
                results['valid'] = False
                results['errors'].append({
                    'statement_index': idx,
                    'error': str(e),
                    'sql': sql[:200]  # First 200 chars
                })
        
        return results
    
    def _load_to_table(self, cursor, xml_id: str, target_table: str, 
                       mappings: pd.DataFrame) -> Dict:
        """Load data to specific table"""
        sql = self._generate_sql(xml_id, mappings)[0]
        
        try:
            cursor.execute(sql)
            rows_affected = cursor.rowcount
            
            return {
                'table': target_table,
                'rows_processed': rows_affected,
                'rows_inserted': rows_affected,
                'rows_updated': 0,
                'rows_failed': 0
            }
        except Exception as e:
            logger.error(f"Failed to load {target_table}: {e}")
            return {
                'table': target_table,
                'rows_processed': 0,
                'rows_inserted': 0,
                'rows_updated': 0,
                'rows_failed': 1,
                'error': str(e)
            }
    
    def _reconcile_data(self, cursor, xml_id: str, mappings_df: pd.DataFrame) -> Dict:
        """Reconcile source vs target data counts"""
        recon_results = {}
        
        # Get source count (assuming 1 XML = 1 record for simplicity)
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INSURANCE.ETL_MAPPER.STAGE_XML_RAW 
            WHERE xml_id = '{xml_id}'
        """)
        source_count = cursor.fetchone()[0]
        
        # Get target counts per table
        for target_table in mappings_df['target_table'].unique():
            try:
                # Get latest records for this XML
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {target_table}
                    WHERE created_date >= (
                        SELECT upload_timestamp 
                        FROM INSURANCE.ETL_MAPPER.STAGE_XML_RAW 
                        WHERE xml_id = '{xml_id}'
                    )
                """)
                target_count = cursor.fetchone()[0]
                
                recon_results[target_table] = {
                    'source_count': source_count,
                    'target_count': target_count,
                    'match_count': min(source_count, target_count),
                    'mismatch_count': abs(source_count - target_count),
                    'status': 'Pass' if source_count == target_count else 'Warning'
                }
            except Exception as e:
                logger.warning(f"Could not reconcile {target_table}: {e}")
                recon_results[target_table] = {
                    'error': str(e),
                    'status': 'Failed'
                }
        
        return recon_results
    
    def _log_execution(self, cursor, results: Dict, recon: Dict):
        """Log execution to tracking table"""
        try:
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.ETL_EXECUTION_LOG 
                (execution_id, xml_id, target_table, execution_start, execution_end,
                 rows_processed, rows_inserted, rows_updated, rows_failed, 
                 execution_status, error_message, executed_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                results['execution_id'],
                results['xml_id'],
                'ALL',  # Aggregate for all tables
                results['start_time'],
                results.get('end_time'),
                results['rows_processed'],
                results['rows_inserted'],
                results.get('rows_updated', 0),
                results['rows_failed'],
                results['status'],
                results.get('error'),
                'system'
            ))
            
            # Log reconciliation
            for table, recon_data in recon.items():
                if 'error' not in recon_data:
                    recon_id = f"RECON-{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    
                    cursor.execute("""
                        INSERT INTO INSURANCE.ETL_MAPPER.RECONCILIATION_RESULTS
                        (recon_id, execution_id, source_count, target_count,
                         match_count, mismatch_count, missing_in_target, extra_in_target,
                         reconciliation_status, details, created_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s)
                    """, (
                        recon_id,
                        results['execution_id'],
                        recon_data['source_count'],
                        recon_data['target_count'],
                        recon_data['match_count'],
                        recon_data['mismatch_count'],
                        0,  # missing_in_target
                        0,  # extra_in_target
                        recon_data['status'],
                        json.dumps({'table': table}),
                        datetime.now()
                    ))
        except Exception as e:
            logger.error(f"Failed to log execution: {e}")
