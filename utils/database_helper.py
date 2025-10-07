"""
Database helper functions - FINAL FIX
"""
import snowflake.connector
import pandas as pd
from datetime import datetime
from typing import Dict
import logging
import json
import os 

logger = logging.getLogger(__name__)


class DatabaseHelper:
    """Helper class for database operations"""
    
    def __init__(self, snowflake_config: Dict):
        self.config = snowflake_config
    
    def get_connection(self):
        """Get Snowflake connection"""
        return snowflake.connector.connect(**self.config)
    
    # utils/database_helper.py - Add/Update these methods

    def save_xml_to_stage(self, xml_file_path: str, xml_content: str, 
                        product_code: str, uploaded_by: str) -> str:
        """
        Save XML to database AND load into staging table with VARIANT
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            xml_id = f"XML-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # 1. Save XML metadata
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.XML_FILES
                (xml_id, file_name, file_path, product_code, uploaded_by, upload_date)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """, (xml_id, os.path.basename(xml_file_path), xml_file_path, product_code, uploaded_by))
            
            # 2. ✅ Create staging table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.XML_STAGING (
                    staging_id VARCHAR(50) PRIMARY KEY DEFAULT UUID_STRING(),
                    xml_id VARCHAR(50) NOT NULL,
                    xml_data VARIANT,
                    target_table VARCHAR(500),
                    processed BOOLEAN DEFAULT FALSE,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # 3. ✅ Parse and load XML into staging as VARIANT
            # Parse XML to JSON-like structure
            import xml.etree.ElementTree as ET
            
            root = ET.fromstring(xml_content)
            
            # Convert XML to dict/JSON structure
            def xml_to_dict(element):
                """Convert XML element to dictionary"""
                result = {}
                
                # Add attributes
                if element.attrib:
                    result.update(element.attrib)
                
                # Add text content
                if element.text and element.text.strip():
                    if len(element) == 0:  # Leaf node
                        return element.text.strip()
                    result['_text'] = element.text.strip()
                
                # Add children
                for child in element:
                    child_data = xml_to_dict(child)
                    if child.tag in result:
                        # Handle multiple children with same tag
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_data)
                    else:
                        result[child.tag] = child_data
                
                return result if result else element.text
            
            xml_dict = xml_to_dict(root)
            
            # 4. ✅ Insert parsed XML as VARIANT (JSON string)
            import json
            xml_json = json.dumps(xml_dict)
            
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.XML_STAGING
                (xml_id, xml_data, target_table, processed)
                VALUES (%s, PARSE_JSON(%s), %s, FALSE)
            """, (xml_id, xml_json, 'UNKNOWN'))
            
            logger.info(f"✅ XML loaded into staging: {xml_id}")
            
            conn.commit()
            return xml_id
            
        except Exception as e:
            logger.error(f"Error saving XML to stage: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    
    def save_mappings_to_db(self, xml_id: str, mappings_result) -> int:
        """Save generated mappings"""
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
            logger.info(f"✅ Saved {count} mappings")
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
                SELECT mapping_id, xml_id, source_node, target_table, target_column,
                       transformation_logic, confidence_score, reasoning, 
                       approval_status, user_notes
                FROM INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                WHERE approval_status = 'Pending'
                ORDER BY xml_id, confidence_score DESC
            """
            df = pd.read_sql(query, conn)
            df.columns = df.columns.str.lower()  # Fix uppercase column names
            return df
        except Exception as e:
            logger.error(f"Error: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    # def load_approved_mappings(self) -> pd.DataFrame:
    #     conn = self.get_connection()
    #     try:
    #         query = """
    #             SELECT mapping_id, xml_id, source_node, target_table, target_column,
    #                    transformation_logic, confidence_score, execution_status
    #             FROM INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
    #             WHERE approval_status = 'Approved'
    #               AND execution_status IN ('Not Started', 'Failed')
    #             ORDER BY xml_id
    #         """
    #         return pd.read_sql(query, conn)
    #     except Exception as e:
    #         logger.error(f"Error: {e}")
    #         return pd.DataFrame()
    #     finally:
    #         conn.close()

    def load_approved_mappings(self) -> pd.DataFrame:
        """Load approved mappings using Snowflake's native pandas support"""
        conn = self.get_connection()
        try:
            query = """
                SELECT mapping_id, xml_id, source_node, target_table, target_column,
                    transformation_logic, confidence_score, execution_status
                FROM INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                WHERE approval_status = 'Approved'
                AND execution_status IN ('Not Started', 'Failed')
                ORDER BY xml_id
            """
            
            # ✅ Use Snowflake's native fetch_pandas_all() method
            cursor = conn.cursor()
            cursor.execute(query)
            df = cursor.fetch_pandas_all()
            cursor.close()
            
            # ✅ Normalize column names to lowercase
            df.columns = df.columns.str.lower()
            
            logger.info(f"Loaded {len(df)} approved mappings")
            logger.debug(f"Columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading approved mappings: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'mapping_id', 'xml_id', 'source_node', 'target_table', 
                'target_column', 'transformation_logic', 'confidence_score', 
                'execution_status'
            ])
        finally:
            conn.close()

    
    def approve_mappings(self, xml_id: str, mappings_df: pd.DataFrame, approved_by: str = "system") -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            count = 0
            for _, row in mappings_df.iterrows():
                cursor.execute("""
                    UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                    SET approval_status = 'Approved', approved_by = %s, approved_date = %s,
                        transformation_logic = %s, user_notes = %s
                    WHERE mapping_id = %s
                """, (approved_by, datetime.now(), row.get('transformation_logic', ''), 
                     row.get('user_notes', ''), row['mapping_id']))
                count += 1
            conn.commit()
            return count
        finally:
            cursor.close()
            conn.close()
    
    def reject_mappings(self, xml_id: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS SET approval_status = 'Rejected' WHERE xml_id = %s", (xml_id,))
            count = cursor.rowcount
            conn.commit()
            return count
        finally:
            cursor.close()
            conn.close()
    
    def update_mappings(self, xml_id: str, mappings_df: pd.DataFrame) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            count = 0
            for _, row in mappings_df.iterrows():
                cursor.execute("""
                    UPDATE INSURANCE.ETL_MAPPER.GENERATED_MAPPINGS
                    SET transformation_logic = %s, user_notes = %s
                    WHERE mapping_id = %s
                """, (row.get('transformation_logic', ''), row.get('user_notes', ''), row['mapping_id']))
                count += 1
            conn.commit()
            return count
        finally:
            cursor.close()
            conn.close()
    
    def load_execution_history(self, limit: int = 10) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            query = f"""
                SELECT execution_id, xml_id, target_table, execution_start, execution_end,
                       rows_processed, rows_inserted, rows_failed, execution_status, executed_by
                FROM INSURANCE.ETL_MAPPER.ETL_EXECUTION_LOG
                ORDER BY execution_start DESC LIMIT {limit}
            """
            return pd.read_sql(query, conn)
        except:
            return pd.DataFrame()
        finally:
            conn.close()
    
    def load_reconciliation_results(self, limit: int = 10) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            query = f"""
                SELECT recon_id, execution_id, source_count, target_count, match_count,
                       mismatch_count, missing_in_target, extra_in_target, 
                       reconciliation_status, details, created_timestamp
                FROM INSURANCE.ETL_MAPPER.RECONCILIATION_RESULTS
                ORDER BY created_timestamp DESC LIMIT {limit}
            """
            return pd.read_sql(query, conn)
        except:
            return pd.DataFrame()
        finally:
            conn.close()

    def save_xml_to_stage_with_copy(self, xml_file_path: str, product_code: str, 
                                    uploaded_by: str) -> str:
        """
        Load XML and parse into VARIANT - FIXED for large JSON
        """
        import xml.etree.ElementTree as ET
        import json
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            xml_id = f"XML-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # 1. Create staging table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.XML_STAGING (
                    staging_id VARCHAR(50) DEFAULT UUID_STRING(),
                    xml_id VARCHAR(50) NOT NULL,
                    xml_data VARIANT,
                    target_table VARCHAR(500),
                    processed BOOLEAN DEFAULT FALSE,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # 2. Parse XML file
            with open(xml_file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            root = ET.fromstring(xml_content)
            
            # 3. Convert XML to dict
            def xml_to_dict(element):
                result = {}
                if element.attrib:
                    result.update(element.attrib)
                if element.text and element.text.strip():
                    if len(element) == 0:
                        return element.text.strip()
                    result['_text'] = element.text.strip()
                for child in element:
                    child_data = xml_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_data)
                    else:
                        result[child.tag] = child_data
                return result if result else element.text
            
            xml_dict = xml_to_dict(root)
            xml_json = json.dumps(xml_dict)
            
            # 4. ✅ Insert using parameter binding (FIXED - handles large strings)
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.XML_STAGING
                (xml_id, xml_data, processed)
                SELECT %s, PARSE_JSON(%s), FALSE
            """, (xml_id, xml_json))
            
            rows_inserted = cursor.rowcount
            logger.info(f"✅ Loaded {rows_inserted} row(s) of XML into staging for {xml_id}")
            
            # 5. Save metadata
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.XML_FILES (
                    xml_id VARCHAR(50) PRIMARY KEY,
                    file_name VARCHAR(500),
                    file_path VARCHAR(1000),
                    product_code VARCHAR(50),
                    uploaded_by VARCHAR(100),
                    upload_date TIMESTAMP
                )
            """)
            
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.XML_FILES
                (xml_id, file_name, file_path, product_code, uploaded_by, upload_date)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """, (xml_id, os.path.basename(xml_file_path), xml_file_path, product_code, uploaded_by))
            
            conn.commit()
            logger.info(f"✅ XML metadata saved for {xml_id}")
            
            return xml_id
            
        except Exception as e:
            logger.error(f"Error loading XML: {e}")
            import traceback
            logger.error(traceback.format_exc())
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def save_xml_raw_bronze(self, xml_file_path: str, product_code: str, 
                        uploaded_by: str) -> str:
        """
        Store raw XML in Bronze layer (best practice)
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            xml_id = f"XML-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Create Bronze table for raw XML
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INSURANCE.ETL_MAPPER.XML_RAW_BRONZE (
                    xml_id VARCHAR(50) PRIMARY KEY,
                    file_name VARCHAR(500),
                    raw_xml TEXT,
                    xml_variant VARIANT,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    product_code VARCHAR(50),
                    uploaded_by VARCHAR(100)
                )
            """)
            
            # Read raw XML
            with open(xml_file_path, 'r', encoding='utf-8') as f:
                raw_xml = f.read()
            
            file_name = os.path.basename(xml_file_path)
            
            # Insert raw XML and parsed VARIANT
            cursor.execute("""
                INSERT INTO INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
                (xml_id, file_name, raw_xml, xml_variant, product_code, uploaded_by)
                SELECT %s, %s, %s, PARSE_XML(%s), %s, %s
            """, (xml_id, file_name, raw_xml, raw_xml, product_code, uploaded_by))
            
            logger.info(f"✅ Stored raw XML in Bronze: {xml_id}")
            
            conn.commit()
            return xml_id
            
        except Exception as e:
            logger.error(f"Error storing raw XML: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
