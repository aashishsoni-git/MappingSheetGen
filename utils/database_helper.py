"""
Database helper functions for ETL workflow - FIXED for Snowflake
"""
import snowflake.connector
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging
import json
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


class DatabaseHelper:
    """Helper class for database operations"""
    
    def __init__(self, snowflake_config: Dict):
        self.config = snowflake_config
    
    def get_connection(self):
        """Get Snowflake connection"""
        return snowflake.connector.connect(**self.config)
    
    def save_xml_to_stage(self, xml_file_path: str, xml_content: str, 
                         product_code: str, uploaded_by: str = "system") -> str:
        """
        Save XML to STAGE_XML_RAW table
        
        Returns:
            xml_id: Unique identifier for this XML
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Generate unique ID
            xml_id = f"XML-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Parse XML to JSON (simplified)
            try:
                tree = ET.parse(xml_file_path)
                root = tree.getroot()
                xml_json = self._xml_to_json(root)
                xml_json_str = json.dumps(xml_json)
            except Exception as e:
                logger.warning(f"Could not parse XML to JSON: {e}. Storing minimal info.")
                xml_json_str = json.dumps({
                    'filename': xml_file_path.split('/')[-1],
                    'size': len(xml_content)
                })
            
            metadata = json.dumps({
                'filename': xml_file_path.split('/')[-1],
                'size': len(xml_content)
            })
            
            # ✅ FIX: Use %s for Snowflake (not ?)
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.STAGE_XML_RAW 
                (xml_id, product_code, xml_filename, xml_content, 
                 upload_timestamp, uploaded_by, processing_status, metadata)
                VALUES (%s, %s, %s, PARSE_JSON(%s), %s, %s, %s, PARSE_JSON(%s))
            """, (
                xml_id,
                product_code,
                xml_file_path.split('/')[-1],
                xml_json_str,
                datetime.now(),
                uploaded_by,
                'Mapped',
                metadata
            ))
            
            conn.commit()
            logger.info(f"✅ Saved XML to stage: {xml_id}")
            return xml_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to save XML: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def save_mappings_to_db(self, xml_id: str, mappings_result) -> int:
        """Save generated mappings to GENERATED_MAPPINGS table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            count = 0
            for mapping in mappings_result.mappings:
                mapping_id = f"MAP-{xml_id}-{count:04d}"
                
                cursor.execute("""
                    INSERT INTO INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                    (mapping_id, xml_id, source_node, target_table, target_column,
                     transformation_logic, confidence_score, reasoning,
                     ai_generated_date, approval_status, execution_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    mapping_id,
                    xml_id,
                    mapping.source_node,
                    mapping.target_table,
                    mapping.target_column,
                    mapping.transformation_logic or '',
                    mapping.confidence_score,
                    mapping.reasoning,
                    datetime.now(),
                    'Pending',
                    'Not Started'
                ))
                count += 1
            
            conn.commit()
            logger.info(f"✅ Saved {count} mappings to database")
            return count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to save mappings: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def load_pending_mappings(self) -> pd.DataFrame:
        """Load mappings with Pending approval status"""
        conn = self.get_connection()
        
        try:
            query = """
                SELECT 
                    mapping_id,
                    xml_id,
                    source_node,
                    target_table,
                    target_column,
                    transformation_logic,
                    confidence_score,
                    reasoning,
                    approval_status,
                    user_notes
                FROM INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                WHERE approval_status = 'Pending'
                ORDER BY xml_id, target_table, confidence_score DESC
            """
            
            df = pd.read_sql(query, conn)
            return df
            
        except Exception as e:
            logger.error(f"Failed to load pending mappings: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def load_approved_mappings(self) -> pd.DataFrame:
        """Load approved mappings ready for execution"""
        conn = self.get_connection()
        
        try:
            query = """
                SELECT 
                    mapping_id,
                    xml_id,
                    source_node,
                    target_table,
                    target_column,
                    transformation_logic,
                    confidence_score,
                    execution_status
                FROM INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                WHERE approval_status = 'Approved'
                  AND execution_status IN ('Not Started', 'Failed')
                ORDER BY xml_id, target_table
            """
            
            df = pd.read_sql(query, conn)
            return df
            
        except Exception as e:
            logger.error(f"Failed to load approved mappings: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def approve_mappings(self, xml_id: str, mappings_df: pd.DataFrame, 
                        approved_by: str = "system") -> int:
        """Approve mappings for execution"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            count = 0
            for _, row in mappings_df.iterrows():
                cursor.execute("""
                    UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                    SET approval_status = 'Approved',
                        approved_by = %s,
                        approved_date = %s,
                        transformation_logic = %s,
                        user_notes = %s
                    WHERE mapping_id = %s
                """, (
                    approved_by,
                    datetime.now(),
                    row.get('transformation_logic', ''),
                    row.get('user_notes', ''),
                    row['mapping_id']
                ))
                count += 1
            
            conn.commit()
            logger.info(f"✅ Approved {count} mappings")
            return count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to approve mappings: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def reject_mappings(self, xml_id: str) -> int:
        """Reject all mappings for an XML"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                SET approval_status = 'Rejected'
                WHERE xml_id = %s
            """, (xml_id,))
            
            count = cursor.rowcount
            conn.commit()
            logger.info(f"✅ Rejected {count} mappings")
            return count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to reject mappings: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def update_mappings(self, xml_id: str, mappings_df: pd.DataFrame) -> int:
        """Update mapping details"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            count = 0
            for _, row in mappings_df.iterrows():
                cursor.execute("""
                    UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                    SET transformation_logic = %s,
                        user_notes = %s
                    WHERE mapping_id = %s
                """, (
                    row.get('transformation_logic', ''),
                    row.get('user_notes', ''),
                    row['mapping_id']
                ))
                count += 1
            
            conn.commit()
            logger.info(f"✅ Updated {count} mappings")
            return count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update mappings: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def load_execution_history(self, limit: int = 10) -> pd.DataFrame:
        """Load recent execution history"""
        conn = self.get_connection()
        
        try:
            query = f"""
                SELECT 
                    execution_id,
                    xml_id,
                    target_table,
                    execution_start,
                    execution_end,
                    rows_processed,
                    rows_inserted,
                    rows_failed,
                    execution_status,
                    executed_by
                FROM INSURANCE.ETL_MAPPER.ETL_EXECUTION_LOG
                ORDER BY execution_start DESC
                LIMIT {limit}
            """
            
            df = pd.read_sql(query, conn)
            return df
            
        except Exception as e:
            logger.error(f"Failed to load execution history: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def load_reconciliation_results(self, limit: int = 10) -> pd.DataFrame:
        """Load reconciliation results"""
        conn = self.get_connection()
        
        try:
            query = f"""
                SELECT 
                    recon_id,
                    execution_id,
                    source_count,
                    target_count,
                    match_count,
                    mismatch_count,
                    missing_in_target,
                    extra_in_target,
                    reconciliation_status,
                    details,
                    created_timestamp
                FROM INSURANCE.ETL_MAPPER.RECONCILIATION_RESULTS
                ORDER BY created_timestamp DESC
                LIMIT {limit}
            """
            
            df = pd.read_sql(query, conn)
            return df
            
        except Exception as e:
            logger.error(f"Failed to load reconciliation results: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def _xml_to_json(self, element, max_depth: int = 5, current_depth: int = 0) -> Dict:
        """Convert XML element to JSON (with depth limit)"""
        if current_depth > max_depth:
            return {"#text": "...truncated"}
        
        result = {}
        
        # Add text content
        if element.text and element.text.strip():
            result['#text'] = element.text.strip()[:500]  # Limit text length
        
        # Add child elements (limit to 50 children)
        child_count = 0
        for child in element:
            if child_count >= 50:
                result['#truncated'] = f"...({len(list(element))} total)"
                break
            
            child_data = self._xml_to_json(child, max_depth, current_depth + 1)
            
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
            
            child_count += 1
        
        return result if result else {"#text": ""}
